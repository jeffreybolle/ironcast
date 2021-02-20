use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Sender, channel, Receiver};
use tokio::sync::{oneshot, Notify};
use tokio::time::{sleep, interval_at, Instant};
use std::time::Duration;
use tokio::sync::mpsc::error::SendError;
use crate::net::message;
use crate::net::utils::*;
use crate::leader::tag::Tag;
use crate::leader::tally::{Tally, VoteResult};
use std::sync::Arc;

pub struct Election {
    input_sender: Sender<ElectionInput>,
    output_receiver: Receiver<ElectionOutput>,
    _shutdown_sender: oneshot::Sender<()>,
}

pub enum ElectionInput {
    Vote(u64, String, String),
    AckVote(u64, String, String),
    Join(String),
    Leave(String),
}

pub enum ElectionOutput {
    ResultLeader(HashSet<String>),
    ResultFollower(String),
}

impl Election {
    pub fn new(id: String, peers: HashMap<String, message::Sender>, cluster_size: usize) -> Election {
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let (input_sender, mut input_receiver) = channel(1024);
        let (output_sender, output_receiver) = channel(1024);

        tokio::spawn(async move {
            let mut tally = Tally::new(id.clone(), cluster_size);

            let notify = Arc::new(Notify::new());
            Election::notify_after_rand_interval(notify.clone());

            let mut members = HashSet::new();
            let mut leader = None;

            let mut repeat = interval_at(Instant::now() + Duration::from_secs(1), Duration::from_secs(1));

            loop {
                let (vote_result, ack) = tokio::select! {
                    event = input_receiver.recv() => {
                        match event {
                            Some(event) => {
                                match event {
                                    ElectionInput::Vote(generation, voter, candidate) => {
                                        (tally.vote(&voter, &candidate, generation), Some(voter))
                                    }
                                    ElectionInput::AckVote(generation, voter, candidate) => {
                                        (tally.vote(&voter, &candidate, generation), None)
                                    }
                                    ElectionInput::Join(peer) => {
                                        (VoteResult::Continue, Some(peer)) // resend vote to newly joined peer
                                    }
                                    ElectionInput::Leave(peer) => {
                                        (tally.remove(&peer), None)
                                    }
                                }
                            }
                            None => { return; }
                        }
                    }
                    _ = notify.notified() => {
                        (tally.vote_for_myself(), None)
                    }
                    _ = repeat.tick(), if tally.quorum().is_none() => {
                        if let Some(candidate) = tally.voted() {
                            Election::send_vote(&peers, &candidate, tally.generation()).await;
                        }
                        (VoteResult::Continue, None)
                    }
                    _ = &mut shutdown_receiver => {
                        return;
                    }
                };
                match vote_result {
                    VoteResult::Continue => {
                        Election::send_ack_vote(&peers, &tally, ack).await;
                    }
                    VoteResult::Vote(candidate, generation) => {
                        Election::send_vote(&peers, &candidate, generation).await;
                    }
                    VoteResult::Quorum(new_leader) => {
                        Election::handle_quorum(&id, &tally, &mut members, &mut leader, &output_sender, new_leader).await;
                        Election::send_ack_vote(&peers, &tally, ack).await;
                    }
                    VoteResult::VoteAndQuorum(new_leader, generation) => {
                        Election::handle_quorum(&id, &tally, &mut members, &mut leader, &output_sender, new_leader.clone()).await;
                        Election::send_vote(&peers, &new_leader, generation).await;
                    }
                    VoteResult::RestartElection(_) => {
                        Election::notify_after_rand_interval(notify.clone());
                    }
                    VoteResult::TooManyVoters => {
                        panic!("too many voters");
                    }
                }
            }
        });
        Election {
            input_sender,
            output_receiver,
            _shutdown_sender: shutdown_sender,
        }
    }

    async fn handle_quorum(id: &str, tally: &Tally, members: &mut HashSet<String>, leader: &mut Option<String>, output_sender: &Sender<ElectionOutput>, new_leader: String) {
        let new_members = tally.members();
        if leader.as_ref() == Some(&new_leader) && *members == new_members {
            return;
        }
        if new_leader == id {
            if let Err(_) = output_sender.send(ElectionOutput::ResultLeader(new_members.clone())).await {
                log::trace!("could not send result");
            };
        } else {
            if let Err(_) = output_sender.send(ElectionOutput::ResultFollower(new_leader.clone())).await {
                log::trace!("could not send result");
            }
        }
        leader.replace(new_leader);
        members.clear();
        members.extend(new_members);
    }

    async fn send_vote(peers: &HashMap<String, message::Sender>, id: &str, generation: u64) {
        let msg = Election::vote_message(Tag::Vote, id, generation);
        for (_, sender) in peers.iter() {
            let msg = msg.clone();
            let sender = sender.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = sender.send(msg) => {}
                    _ = sleep(Duration::from_millis(100)) => {
                        log::debug!("sending timed out");
                    }
                }
            });
        }
    }

    async fn send_ack_vote(peers: &HashMap<String, message::Sender>, tally: &Tally, ack: Option<String>) {
        if let Some(voted) = tally.voted() {
            if let Some(peer) = ack {
                if let Some(sender) = peers.get(&peer) {
                    let msg = Election::vote_message(Tag::Ack, &voted, tally.generation());
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = sender.send(msg) => {}
                            _ = sleep(Duration::from_millis(100)) => {
                                log::debug!("sending timed out");
                            }
                        }
                    });
                }
            }
        }
    }

    fn vote_message(tag: Tag, id: &str, generation: u64) -> Vec<u8> {
        let mut msg = Vec::with_capacity(id.len() + 4 + 4 + 8);
        msg.push(tag as u8);
        write_u64(&mut msg, generation);
        write_u32(&mut msg, id.len() as u32);
        msg.extend_from_slice(id.as_bytes());
        msg
    }

    fn notify_after_rand_interval(notify: Arc<Notify>) {
        tokio::spawn(async move {
            sleep(rand_interval()).await;
            notify.notify_one();
        });
    }

    pub async fn recv(&mut self) -> Option<ElectionOutput> {
        self.output_receiver.recv().await
    }

    pub async fn send(&self, input: ElectionInput) -> Result<(), SendError<ElectionInput>> {
        self.input_sender.send(input).await
    }
}

// Returns an interval between 0 and 100 ms
fn rand_interval() -> Duration {
    Duration::from_millis(rand::random::<u64>() % 100)
}

#[cfg(test)]
mod tests {
    use crate::leader::election::{Election, ElectionInput, ElectionOutput};
    use std::collections::HashMap;
    use tokio::sync::mpsc::{channel, Receiver};
    use crate::net::message;

    async fn make_senders() -> (HashMap<String, message::Sender>, Receiver<Vec<u8>>, Receiver<Vec<u8>>) {
        let mut senders = HashMap::new();

        let (channel_sender, bob_receiver) = channel(100);
        let sender = message::Sender::new(String::from("alice"), String::from("bob"));
        sender.install_channel(channel_sender).await;
        senders.insert(String::from("bob"), sender);

        let (channel_sender, charlie_receiver) = channel(100);
        let sender = message::Sender::new(String::from("alice"), String::from("charlie"));
        sender.install_channel(channel_sender).await;
        senders.insert(String::from("charlie"), sender);

        (senders, bob_receiver, charlie_receiver)
    }

    #[tokio::test]
    #[allow(unused_must_use)]
    async fn two_votes_received_for_bob() {
        let (senders, mut bob_receiver, mut charlie_receiver) = make_senders().await;

        let mut election = Election::new(String::from("alice"), senders, 3);

        // bob votes
        election.send(ElectionInput::Vote(1, String::from("bob"), String::from("bob"))).await;

        // charlie votes
        election.send(ElectionInput::Vote(1, String::from("charlie"), String::from("bob"))).await;

        assert_eq!(Vec::from([0, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 98, 111, 98]), bob_receiver.recv().await.unwrap());
        assert_eq!(Vec::from([0, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 98, 111, 98]), charlie_receiver.recv().await.unwrap());

        if let Some(ElectionOutput::ResultFollower(peer)) = election.recv().await {
            assert_eq!(String::from("bob"), peer);
        } else {
            panic!("unexpected election outcome");
        }
    }
}