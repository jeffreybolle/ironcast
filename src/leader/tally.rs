use std::collections::{HashMap, HashSet};
use maplit::hashset;

#[derive(PartialEq, Debug)]
pub enum VoteResult {
    Continue,
    Vote(String, u64),
    Quorum(String),
    VoteAndQuorum(String, u64),
    RestartElection(u64),
    TooManyVoters,
}

pub struct Tally {
    id: String,
    votes_by_candidate: HashMap<String, HashSet<String>>,
    votes_by_voter: HashMap<String, String>,
    generation: u64,
    cluster_size: usize,
    quorum: Option<String>,
    voted: Option<String>,
    members: HashSet<String>,
}

impl Tally {
    pub fn new(id: String, cluster_size: usize) -> Self {
        let members = hashset! {id.clone()};
        Self {
            id,
            votes_by_candidate: HashMap::new(),
            votes_by_voter: HashMap::new(),
            generation: 1,
            cluster_size,
            quorum: None,
            voted: None,
            members,
        }
    }

    pub fn quorum(&self) -> Option<String> {
        self.quorum.as_ref().cloned()
    }

    pub fn members(&self) -> HashSet<String> {
        self.members.clone()
    }

    pub fn voted(&self) -> Option<String> {
        self.voted.clone()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn quorum_size(&self) -> usize {
        if self.cluster_size == 1 {
            1
        } else {
            self.cluster_size / 2 + 1
        }
    }

    fn increase_generation(&mut self, generation: u64) {
        self.votes_by_voter.clear();
        self.votes_by_candidate.clear();
        self.quorum = None;
        self.voted = None;
        self.members = hashset! {self.id.clone()};
        self.generation = generation;
    }

    pub fn vote_for_myself(&mut self) -> VoteResult {
        if self.voted.is_some() {
            return match &self.quorum {
                Some(quorum) => VoteResult::Quorum(quorum.to_string()),
                None => VoteResult::Continue
            };
        }
        log::debug!("[{}] {} votes for {}", self.generation, self.id, self.id);
        self.voted = Some(self.id.clone());
        self.votes_by_voter.insert(self.id.clone(), self.id.clone());
        self.votes_by_candidate.insert(self.id.clone(), hashset![self.id.clone()]);
        if self.cluster_size == 1 {
            self.quorum = Some(self.id.clone());
            return VoteResult::VoteAndQuorum(self.id.clone(), self.generation);
        }
        return VoteResult::Vote(self.id.clone(), self.generation);
    }

    pub fn vote(&mut self, voter: &str, candidate: &str, generation: u64) -> VoteResult {
        log::debug!("[{}] {} votes for {}", generation, voter, candidate);
        if self.generation > generation {
            return match &self.voted {
                None => VoteResult::Continue,
                Some(voted) => VoteResult::Vote(voted.clone(), self.generation)
            };
        }
        if self.generation < generation {
            self.increase_generation(generation);
        }
        self.members.insert(voter.to_string());
        let first_vote = self.votes_by_voter.len() == 0;
        let quorum_size = self.quorum_size();
        match self.votes_by_voter.get(voter) {
            None => {
                if self.votes_by_voter.len() >= self.cluster_size {
                    return VoteResult::TooManyVoters;
                }
                self.votes_by_voter.insert(voter.to_string(), candidate.to_string());
                let reached_quorum;
                match self.votes_by_candidate.get_mut(candidate) {
                    None => {
                        self.votes_by_candidate.insert(candidate.to_string(), hashset![voter.to_string()]);
                        reached_quorum = quorum_size == 1;
                    }
                    Some(voters) => {
                        voters.insert(voter.to_string());
                        reached_quorum = voters.len() >= quorum_size;
                    }
                }
                if first_vote {
                    self.votes_by_voter.insert(self.id.clone(), candidate.to_string());
                    self.votes_by_candidate.get_mut(candidate).unwrap().insert(self.id.clone());
                    self.voted = Some(candidate.to_string());
                    if quorum_size == 2 {
                        self.quorum = Some(candidate.to_string());
                        VoteResult::VoteAndQuorum(candidate.to_string(), self.generation)
                    } else {
                        VoteResult::Vote(candidate.to_string(), self.generation)
                    }
                } else if self.quorum.is_some() {
                    VoteResult::Quorum(self.quorum.as_ref().unwrap().clone())
                } else if reached_quorum {
                    self.quorum = Some(candidate.to_string());
                    if self.voted.as_ref().expect("we should have voted by now").as_str() != candidate {
                        self.votes_by_candidate.get_mut(self.voted.as_ref().unwrap()).unwrap().remove(&self.id);
                        self.voted = Some(candidate.to_string());
                        self.votes_by_voter.insert(self.id.clone(), candidate.to_string());
                        self.votes_by_candidate.get_mut(candidate).unwrap().insert(self.id.clone());
                        VoteResult::VoteAndQuorum(candidate.to_string(), self.generation)
                    } else {
                        VoteResult::Quorum(candidate.to_string())
                    }
                } else if self.cluster_size == self.votes_by_voter.len() {
                    self.increase_generation(self.generation + 1);
                    VoteResult::RestartElection(self.generation)
                } else {
                    VoteResult::Continue
                }
            }
            Some(previous_candidate) => {
                if candidate == previous_candidate {
                    match &self.quorum {
                        Some(candidate) => VoteResult::Quorum(candidate.clone()),
                        None => VoteResult::Continue
                    }
                } else if self.quorum.as_ref().map(String::as_str) == Some(candidate) {
                    self.votes_by_candidate.insert(candidate.to_string(), hashset![voter.to_string()]);
                    self.votes_by_voter.insert(voter.to_string(), candidate.to_string());
                    return VoteResult::Quorum(candidate.to_string());
                } else {
                    let voted = self.voted.as_ref().expect("we should have voted by now").clone();
                    self.increase_generation(self.generation + 1);
                    self.voted = Some(voted.clone());

                    self.votes_by_candidate.insert(voted.clone(), hashset![self.id.clone()]);
                    self.votes_by_voter.insert(self.id.clone(), voted.clone());
                    if quorum_size == 1 {
                        self.quorum = Some(voted.clone());
                        return VoteResult::Quorum(voted.clone());
                    }
                    VoteResult::Vote(voted.clone(), self.generation)
                }
            }
        }
    }

    pub fn remove(&mut self, peer: &str) -> VoteResult {
        log::info!("peer removed {}", peer);
        if self.quorum == None || self.quorum.as_ref().map(String::as_str) == Some(peer) {
            self.increase_generation(self.generation + 1);
            return VoteResult::RestartElection(self.generation);
        }
        // TODO we need to check whether we still have enough members for quorum
        // TODO it is also possible for us to have received a vote for the removed peer that not everyone received, so not everyone might agree that we have quorum
        self.members.remove(peer);
        match &self.quorum {
            None => VoteResult::Continue,
            Some(quorum) => VoteResult::Quorum(quorum.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::leader::tally::{Tally, VoteResult};
    use std::collections::HashSet;

    #[test]
    fn no_votes() {
        let mut tally = Tally::new(String::from("alice"), 2);

        assert_eq!(VoteResult::Vote(String::from("alice"), 1), tally.vote_for_myself());
        assert_eq!(VoteResult::Continue, tally.vote_for_myself());
    }

    #[test]
    fn reach_quorum() {
        let mut tally = Tally::new(String::from("alice"), 2);

        assert_eq!(VoteResult::VoteAndQuorum(String::from("bob"), 1), tally.vote("bob", "bob", 1));

        assert_eq!(Some(String::from("bob")), tally.quorum());
        let mut members = HashSet::new();
        members.insert(String::from("alice"));
        members.insert(String::from("bob"));
        assert_eq!(members, tally.members());
    }

    #[test]
    fn restart_election_after_stalemate() {
        let mut tally = Tally::new(String::from("alice"), 2);

        assert_eq!(VoteResult::Vote(String::from("alice"), 1), tally.vote_for_myself());
        assert_eq!(VoteResult::RestartElection(2), tally.vote("bob", "bob", 1));

        assert_eq!(VoteResult::Vote(String::from("alice"), 2), tally.vote_for_myself());
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("bob", "alice", 2));
    }

    #[test]
    fn candidate_changes_vote() {
        let mut tally = Tally::new(String::from("alice"), 3);

        assert_eq!(VoteResult::Vote(String::from("alice"), 1), tally.vote_for_myself());
        // bob votes for alice
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("bob", "alice", 1));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("charlie", "charlie", 1));

        // bob changes vote to charlie
        assert_eq!(VoteResult::Vote(String::from("alice"), 2), tally.vote("bob", "charlie", 1));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("bob", "alice", 2));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("charlie", "alice", 2));
    }

    #[test]
    fn generation_increased() {
        let mut tally = Tally::new(String::from("alice"), 3);
        assert_eq!(VoteResult::Vote(String::from("alice"), 1), tally.vote_for_myself());
        assert_eq!(VoteResult::VoteAndQuorum(String::from("bob"), 2), tally.vote("bob", "bob", 2));
    }

    #[test]
    fn continue_needed() {
        let mut tally = Tally::new(String::from("alice"), 5);
        assert_eq!(VoteResult::Vote(String::from("alice"), 1), tally.vote_for_myself());
        assert_eq!(VoteResult::Continue, tally.vote("bob", "alice", 1));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("charlie", "alice", 1));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("david", "alice", 1));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("ezra", "alice", 1));
    }

    #[test]
    fn missed_messages() {
        let mut tally = Tally::new(String::from("alice"), 3);

        assert_eq!(VoteResult::Vote(String::from("alice"), 1), tally.vote_for_myself());
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("bob", "alice", 1));
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("charlie", "charlie", 1));

        // charlie has missed the votes from alice and bob, so repeats his vote on a timer because he has not yet reached quorum
        assert_eq!(VoteResult::Quorum(String::from("alice")), tally.vote("charlie", "charlie", 1));
    }
}