use std::collections::{HashMap, HashSet};
use std::string::FromUtf8Error;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::{channel, Receiver};

use crate::net::acceptor::Acceptor;
use crate::net::connection::Connection;
use crate::net::ConnectionError::HandshakeError;
use crate::net::connector::Connector;
use crate::net::coordinator::ConnectionCoordinator;

mod acceptor;
mod connection;
mod connector;
mod coordinator;
pub mod message;
pub mod utils;

#[derive(Clone)]
pub struct Peer {
    pub id: String,
    pub addr: String,
}

pub enum InternalEvent {
    NewConnection(String, TcpStream),
    ConnectionError(String),
    RemovePeer(String),
    IncomingMessage {
        peer_id: String,
        data: Vec<u8>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("handshake sequence incomplete or incorrect")]
    HandshakeError,

    #[error("io error")]
    IOError(#[from]std::io::Error),

    #[error("bad UTF-8 encoding")]
    Utf8Error(#[from]FromUtf8Error),
}

pub struct Message {
    pub peer_id: String,
    pub data: Vec<u8>,
}

#[derive(Debug)]
enum PeerStatus {
    Establishing(Connector),
    Established(Connection),
    Disconnected,
}

pub struct Node {
    pub id: String,
    _shutdown_sender: oneshot::Sender<()>,
    senders: HashMap<String, message::Sender>,
    event_receiver: Receiver<Event>,
    members: Arc<Mutex<HashSet<String>>>,
}

pub enum Event {
    MemberJoined(String),
    MemberLeave(String),
    MessageReceived(Message),
}

impl Node {
    pub fn new(id: String, port: u16, peers: Vec<Peer>) -> Self {
        let cc = Arc::new(ConnectionCoordinator::new());

        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel::<()>();
        let (sender, mut receiver) = channel(1024);

        let node_sender = sender.clone();
        let node_id = id.clone();

        let (event_sender, event_receiver) = channel(1024);

        let mut senders = HashMap::<String, message::Sender>::new();
        for peer in peers.iter() {
            senders.insert(peer.id.clone(), message::Sender::new(id.clone(), peer.id.clone()));
        }

        let members = Arc::new(Mutex::new(HashSet::new()));
        {
            let members = members.clone();
            let senders = senders.clone();
            tokio::spawn(async move {
                log::info!("Starting {}", node_id);

                let mut statuses = HashMap::new();

                for peer in peers.iter() {
                    let connector = Connector::new(node_id.clone(), peer.id.clone(), peer.addr.clone(), node_sender.clone(), cc.clone());
                    statuses.insert(peer.id.clone(), PeerStatus::Establishing(connector));
                }

                let acceptor = Acceptor::new(node_id.clone(), port, node_sender.clone(), cc.clone());

                loop {
                    let e = tokio::select! {
                        _ = &mut shutdown_receiver => {
                            log::info!("Shutting down {}", node_id);
                            drop(acceptor);
                            return;
                        }
                        result = receiver.recv() => {
                            match result {
                                Some(e) => e,
                                None => panic!("this cannot happen")
                            }
                        }
                    };

                    match e {
                        InternalEvent::NewConnection(peer_id, conn) => {
                            match statuses.remove(&peer_id) {
                                Some(PeerStatus::Establishing(_)) => {}
                                os => {
                                    panic!(format!("invalid state: {:?}", os));
                                }
                            }
                            let sender = senders.get(&peer_id).unwrap().clone();
                            let connection = Connection::new(node_id.clone(), peer_id.clone(), node_sender.clone(), conn, sender);
                            statuses.insert(peer_id.clone(), PeerStatus::Established(connection));
                            let mut guard = members.lock().await;
                            if !guard.contains(&peer_id) {
                                guard.insert(peer_id.clone());
                                let _ = event_sender.send(Event::MemberJoined(peer_id.clone())).await;
                            }
                        }
                        InternalEvent::RemovePeer(peer_id) => {
                            statuses.remove(&peer_id);
                            let _ = event_sender.send(Event::MemberLeave(peer_id.clone())).await;
                        }
                        InternalEvent::ConnectionError(peer_id) => {
                            match statuses.remove(&peer_id) {
                                Some(PeerStatus::Established(_)) => {
                                    senders.get(&peer_id).unwrap().disconnect().await;
                                    statuses.insert(peer_id.clone(), PeerStatus::Disconnected);
                                }
                                Some(_) => {
                                    panic!("this cannot happen");
                                }
                                None => {
                                    // TODO think about whether this can happen
                                    panic!("this shouldn't happen");
                                }
                            }
                            cc.peer_states.lock().await.remove(&peer_id);
                            if let Some(peer) = peers.iter().find(|peer| peer.id == peer_id) {
                                let connector = Connector::new(node_id.clone(), peer.id.clone(), peer.addr.clone(), node_sender.clone(), cc.clone());
                                statuses.insert(peer.id.clone(), PeerStatus::Establishing(connector));
                            }
                            let mut guard = members.lock().await;
                            if guard.contains(&peer_id) {
                                guard.remove(&peer_id);
                                let _ = event_sender.send(Event::MemberLeave(peer_id.clone())).await;
                            }
                        }
                        InternalEvent::IncomingMessage { peer_id, data } => {
                            let _ = event_sender.send(Event::MessageReceived(Message { peer_id, data })).await;
                        }
                    }
                }
            });
        }


        Node {
            id,
            _shutdown_sender: shutdown_sender,
            senders,
            event_receiver,
            members,
        }
    }

    #[allow(dead_code)]
    pub fn get_sender(&mut self, peer_id: &str) -> Option<message::Sender> {
        self.senders.get(peer_id).map(|r| r.clone())
    }

    pub fn senders(&self) -> HashMap<String, message::Sender> {
        self.senders.clone()
    }

    #[allow(dead_code)]
    pub async fn peers(&mut self) -> Vec<String> {
        let guard = self.members.lock().await;
        guard.iter().map(|item| item.clone()).collect()
    }

    pub async fn recv(&mut self) -> Option<Event> {
        self.event_receiver.recv().await
    }
}

#[cfg(test)]
mod tests {
    use simple_logger::SimpleLogger;

    use crate::net::{Event, Node, Peer};

    #[tokio::test(flavor = "multi_thread")]
    async fn test() {
        if let Err(_) = SimpleLogger::new().with_level(log::LevelFilter::Debug).init() {
            // ignore
        }

        let mut node_a = Node::new(String::from("alice"), 3000, vec![
            Peer { id: String::from("bob"), addr: String::from("127.0.0.1:3001") },
            Peer { id: String::from("charlie"), addr: String::from("127.0.0.1:3002") }
        ]);

        let mut node_b = Node::new(String::from("bob"), 3001, vec![
            Peer { id: String::from("alice"), addr: String::from("127.0.0.1:3000") },
            Peer { id: String::from("charlie"), addr: String::from("127.0.0.1:3002") }
        ]);

        let mut node_c = Node::new(String::from("charlie"), 3002, vec![
            Peer { id: String::from("alice"), addr: String::from("127.0.0.1:3000") },
            Peer { id: String::from("bob"), addr: String::from("127.0.0.1:3001") }
        ]);

        tokio::spawn(async move {
            if let Some(Event::MessageReceived(message)) = node_b.recv().await {
                log::info!("bob message from {}: {:?}", message.peer_id, String::from_utf8_lossy(&message.data));
            }
            drop(node_b);
        });
        tokio::spawn(async move {
            if let Some(Event::MessageReceived(message)) = node_c.recv().await {
                log::info!("charlie message from {}: {:?}", message.peer_id, String::from_utf8_lossy(&message.data));
            }
            drop(node_c);
        });

        let mut count = 4;
        while count > 0 {
            match node_a.recv().await {
                Some(Event::MemberJoined(peer)) => {
                    log::info!("{} joined", &peer);
                    node_a.get_sender(&peer).unwrap().send(b"Welcome to the cluster".to_vec()).await;
                    count -= 1;
                }
                Some(Event::MemberLeave(peer)) => {
                    log::info!("{} left", &peer);
                    count -= 1;
                }
                Some(Event::MessageReceived(_)) => {}
                None => {}
            }
        }

        log::info!("peers: {:?}", node_a.peers().await);
        drop(node_a);
    }
}

