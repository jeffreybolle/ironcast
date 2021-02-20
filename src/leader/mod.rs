use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;

use election::{Election, ElectionInput, ElectionOutput};
use tag::Tag;

use crate::leader::heartbeat::{HeartbeatMonitor, HeartbeatOutput};
use crate::net;
use crate::net::message;
use crate::net::utils::*;

mod election;
mod heartbeat;
mod tag;
mod tally;

pub struct Node {
    output_receiver: Receiver<NodeOutput>,
    senders: HashMap<String, message::Sender>,
    _shutdown_sender: oneshot::Sender<()>,
}

pub enum NodeOutput {
    Leader(HashSet<String>),
    Follower(String),
    Message(String, Vec<u8>),
}

impl Node {
    pub fn new(id: String, port: u16, peers: Vec<net::Peer>) -> Node {
        let cluster_size = peers.len() + 1;
        let mut node = net::Node::new(id.clone(), port, peers);
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let (output_sender, output_receiver) = channel(1024);
        let senders = node.senders();

        tokio::spawn(async move {
            let mut election = Election::new(id.clone(), node.senders(), cluster_size);
            let mut heartbeat_monitor: Option<HeartbeatMonitor> = None;

            loop {
                tokio::select! {
                    event = node.recv() => {
                        handle_node_event(&id, event, &output_sender, &election, &heartbeat_monitor).await;
                    }
                    event = recv_heartbeat_monitor(&mut heartbeat_monitor), if heartbeat_monitor.is_some() => {
                        match event {
                            Some(HeartbeatOutput::HeartbeatNotReceived(peer)) => {
                                if let Err(_) = election.send(ElectionInput::Leave(peer)).await {
                                    log::trace!("could not send election input");
                                }
                            }
                            _ => {}
                        }
                    }
                    result = election.recv() => {
                        match result {
                            Some(ElectionOutput::ResultLeader(members)) => {
                                log::info!("{} is the leader", id);
                                if let Err(_) = output_sender.send(NodeOutput::Leader(members.clone())).await {
                                    log::trace!("could not send leader output");
                                }
                                heartbeat_monitor.replace(
                                    HeartbeatMonitor::new(
                                        node.senders().into_iter().filter(|(peer, _)| members.contains(peer)).collect(),
                                        None));
                            }
                            Some(ElectionOutput::ResultFollower(leader)) => {
                                log::info!("{} is the leader", leader);
                                if let Err(_) = output_sender.send(NodeOutput::Follower(leader.clone())).await {
                                    log::trace!("could not send follower output");
                                }
                                heartbeat_monitor.replace(
                                    HeartbeatMonitor::new(
                                        node.senders().into_iter().filter(|(peer, _)| peer == &leader).collect(),
                                        Some(leader)));
                            }
                            _ => {}
                        }
                    }
                    _ = &mut shutdown_receiver => {
                        return;
                    }
                }
            }
        });
        Node {
            output_receiver,
            senders,
            _shutdown_sender: shutdown_sender,
        }
    }

    pub async fn recv(&mut self) -> Option<NodeOutput> {
        self.output_receiver.recv().await
    }

    pub async fn send(&mut self, peer_id: &str, data: &[u8]) {
        let mut msg = Vec::with_capacity(data.len() + 1);
        msg.push(Tag::Message as u8);
        msg.extend_from_slice(data);

        if let Some(sender) = self.senders.get(peer_id) {
            sender.send(msg).await;
        }
    }
}

async fn recv_heartbeat_monitor(heartbeat_monitor: &mut Option<HeartbeatMonitor>) -> Option<heartbeat::HeartbeatOutput> {
    match heartbeat_monitor {
        None => None,
        Some(heartbeat_monitor) => heartbeat_monitor.recv().await
    }
}

async fn handle_node_event(_id: &str, event: Option<net::Event>, output_sender: &Sender<NodeOutput>,
                           election: &Election, heartbeat_monitor: &Option<HeartbeatMonitor>) {
    match event {
        Some(net::Event::MemberJoined(peer)) => {
            if let Err(_) = election.send(ElectionInput::Join(peer)).await {
                log::trace!("could not send election input");
            }
        }
        Some(net::Event::MemberLeave(peer)) => {
            if let Err(_) = election.send(ElectionInput::Leave(peer)).await {
                log::trace!("could not send election input");
            }
        }
        Some(net::Event::MessageReceived(message)) => {
            if message.data.len() == 0 {
                panic!("invalid message - too few bytes");
            }
            match Tag::from_u8(message.data[0]) {
                Tag::Vote => {
                    let generation = read_u64(&message.data[1..9]);
                    let len = read_u32(&message.data[9..13]) as usize;
                    let peer_id = String::from_utf8_lossy(&message.data[13..13 + len]).into_owned();
                    if let Err(_) = election.send(ElectionInput::Vote(generation, message.peer_id, peer_id)).await {
                        log::trace!("could not send election input");
                    }
                }
                Tag::Ack => {
                    let generation = read_u64(&message.data[1..9]);
                    let len = read_u32(&message.data[9..13]) as usize;
                    let peer_id = String::from_utf8_lossy(&message.data[13..13 + len]).into_owned();
                    if let Err(_) = election.send(ElectionInput::AckVote(generation, message.peer_id, peer_id)).await {
                        log::trace!("could not send election input");
                    }
                }
                Tag::Heartbeat => {
                    if let Some(heartbeat_monitor) = heartbeat_monitor {
                        heartbeat_monitor.heartbeat_received(message.peer_id).await;
                    }
                }
                Tag::MembershipList => {
                    // membership list
                    // it is up to the leader to send a definitive list of all connected
                    // peers
                }
                Tag::Message => {
                    if let Err(_) = output_sender.send(NodeOutput::Message(message.peer_id, message.data[1..].to_vec())).await {
                        log::trace!("could not send message to output");
                    }
                }
            }
        }
        None => {}
    }
}

#[cfg(test)]
mod tests {
    use simple_logger::SimpleLogger;

    use crate::leader::{Node, NodeOutput};
    use crate::net;

    #[tokio::test(flavor = "multi_thread")]
    async fn test() {
        if let Err(_) = SimpleLogger::new().with_level(log::LevelFilter::Debug).init() {
            // ignore
        }

        let mut node_a = Node::new(String::from("alice"), 4000, vec![
            net::Peer { id: String::from("bob"), addr: String::from("127.0.0.1:4001") },
            net::Peer { id: String::from("charlie"), addr: String::from("127.0.0.1:4002") }
        ]);
        let mut node_b = Node::new(String::from("bob"), 4001, vec![
            net::Peer { id: String::from("alice"), addr: String::from("127.0.0.1:4000") },
            net::Peer { id: String::from("charlie"), addr: String::from("127.0.0.1:4002") }
        ]);
        let mut node_c = Node::new(String::from("charlie"), 4002, vec![
            net::Peer { id: String::from("alice"), addr: String::from("127.0.0.1:4000") },
            net::Peer { id: String::from("bob"), addr: String::from("127.0.0.1:4001") }
        ]);

        let leader_from_a;
        let leader_from_b;
        let leader_from_c;
        match node_a.recv().await {
            Some(NodeOutput::Leader(_)) => {
                leader_from_a = String::from("alice");
            }
            Some(NodeOutput::Follower(peer_id)) => {
                leader_from_a = peer_id;
            }
            _ => {
                leader_from_a = String::new();
            }
        }
        match node_b.recv().await {
            Some(NodeOutput::Leader(_)) => {
                leader_from_b = String::from("bob");
            }
            Some(NodeOutput::Follower(peer_id)) => {
                leader_from_b = peer_id;
            }
            _ => {
                leader_from_b = String::new();
            }
        }
        match node_c.recv().await {
            Some(NodeOutput::Leader(_)) => {
                leader_from_c = String::from("charlie");
            }
            Some(NodeOutput::Follower(peer_id)) => {
                leader_from_c = peer_id;
            }
            _ => {
                leader_from_c = String::new();
            }
        }

        assert_eq!(leader_from_a, leader_from_b);
        assert_eq!(leader_from_a, leader_from_c);
        assert_eq!(leader_from_b, leader_from_c);

        drop(node_a);
        drop(node_b);
        drop(node_c);

        log::info!("test finished");
    }
}