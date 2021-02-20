use std::collections::{HashMap, HashSet};
use crate::net::utils::*;
use std::fmt;
use std::fmt::{Formatter, Debug};

pub const PARTITIONS: u16 = 2520; // 2*3*5*7*2*2*3 divisible by 2,3,4,5,6,7,8,9,10

pub struct PartitionTable {
    partition_mapping: HashMap<u16, u16>,
    peer_id_mapping: HashMap<u16, String>,
}

impl PartitionTable {
    pub fn new(id: String, mut members: HashSet<String>) -> PartitionTable {
        members.remove(&id);
        let mut partition_mapping = HashMap::new();
        let mut peer_id_mapping = HashMap::new();

        let per_peer_partition = PARTITIONS / ((members.len() as u16) + 1);

        let mut next_id = 0u16;
        let mut next_partition = 0u16;

        for peer in members {
            peer_id_mapping.insert(next_id, peer);
            for partition in next_partition..(next_partition + per_peer_partition) {
                partition_mapping.insert(partition, next_id);
            }
            next_partition += per_peer_partition;
            next_id += 1;
        }

        peer_id_mapping.insert(next_id, id);
        for partition in next_partition..PARTITIONS {
            partition_mapping.insert(partition, next_id);
        }

        PartitionTable {
            partition_mapping,
            peer_id_mapping,
        }
    }

    pub fn get_owner(&self, partition: u16) -> Option<String> {
        self.partition_mapping.get(&partition)
            .and_then(|owner| self.peer_id_mapping.get(owner).map(String::clone))
    }

    pub fn get_backup(&self, partition: u16) -> Option<String> {
        let partition_owner_id = match self.partition_mapping.get(&partition) {
            None => return None,
            Some(x) => *x,
        };
        let partition_backup_id = (partition_owner_id + 1) % (self.peer_id_mapping.len() as u16);
        self.peer_id_mapping.get(&partition_backup_id).map(String::clone)
    }

    pub fn to_message(&self) -> Vec<u8> {
        let mut msg = Vec::new();
        write_u16(&mut msg, self.peer_id_mapping.len() as u16);

        let mut peer_id = *self.partition_mapping.get(&0u16).unwrap();
        let mut start_partition = 0u16;

        for partition in 1..PARTITIONS as u16 {
            let next_peer_id = *self.partition_mapping.get(&partition).unwrap();
            if next_peer_id != peer_id {
                let peer = self.peer_id_mapping.get(&peer_id).unwrap();
                write_u16(&mut msg, start_partition);
                write_u16(&mut msg, partition - 1);
                write_peer(&mut msg, peer);
                peer_id = next_peer_id;
                start_partition = partition;
            }
        }
        let peer = self.peer_id_mapping.get(&peer_id).unwrap();
        write_u16(&mut msg, start_partition);
        write_u16(&mut msg, PARTITIONS - 1);
        write_peer(&mut msg, peer);
        msg
    }

    pub fn from_message(msg: &[u8]) -> PartitionTable {
        let mut partition_mapping = HashMap::new();
        let mut peer_id_mapping = HashMap::new();

        let mut pos = 0usize;
        let len = read_u16(&msg[pos..pos + 2]);
        pos += 2;

        let mut next_id = 0u16;

        for _ in 0..len {
            let start = read_u16(&msg[pos..pos + 2]);
            pos += 2;
            let end = read_u16(&msg[pos..pos + 2]);
            pos += 2;
            let (peer, forward) = read_peer(&msg[pos..]);
            pos += forward;

            peer_id_mapping.insert(next_id, peer);
            for partition in start..end + 1 {
                partition_mapping.insert(partition, next_id);
            }
            next_id += 1;
        }

        PartitionTable {
            partition_mapping,
            peer_id_mapping,
        }
    }

    pub fn get_difference(&self, other: &PartitionTable, id: &String) -> Vec<(String, u16)> {
        let mut result = Vec::new();

        for (partition_id, peer_id) in other.partition_mapping.iter() {
            let owner = other.peer_id_mapping.get(&peer_id).unwrap();
            if owner == id {
                let old_backup_owner = self.get_backup(*partition_id).unwrap();
                if old_backup_owner != *id {
                    result.push((old_backup_owner, *partition_id));
                }
            }
            let backup_owner = other.peer_id_mapping.get(&((peer_id + 1) % (other.peer_id_mapping.len() as u16))).unwrap();
            if backup_owner == id {
                let old_backup_owner = self.get_backup(*partition_id).unwrap();
                if old_backup_owner != *id {
                    result.push((old_backup_owner, *partition_id));
                }
            }
        }

        result
    }

    pub fn members(&self) -> HashSet<String> {
        self.peer_id_mapping.values().cloned().collect()
    }
}

fn read_peer(msg: &[u8]) -> (String, usize) {
    if msg.len() == 0 {
        panic!("not enough bytes for a peer id");
    }
    let len = msg[0] as usize;
    if msg.len() < len + 1 {
        panic!("invalid bytes for a peer id");
    }
    (
        String::from_utf8_lossy(&msg[1..len + 1]).to_string(),
        len + 1
    )
}

fn write_peer(msg: &mut Vec<u8>, peer: &str) {
    msg.push(peer.len() as u8);
    msg.extend_from_slice(peer.as_bytes());
}

impl Debug for PartitionTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut output = Vec::new();

        let mut peer_id = *self.partition_mapping.get(&0u16).unwrap();
        let mut start_partition = 0u16;

        for partition in 1..PARTITIONS as u16 {
            let next_peer_id = *self.partition_mapping.get(&partition).unwrap();
            if next_peer_id != peer_id {
                let peer = self.peer_id_mapping.get(&peer_id).unwrap();
                output.push(format!("{}-{}: {}", start_partition, partition - 1, peer));
                peer_id = next_peer_id;
                start_partition = partition;
            }
        }
        let peer = self.peer_id_mapping.get(&peer_id).unwrap();
        output.push(format!("{}-{}: {}", start_partition, PARTITIONS - 1, peer));
        output.fmt(f)?;
        Ok(())
    }
}

pub fn calculate_partition(storage_key: &[u8]) -> u16 {
    if storage_key.len() < 8 {
        panic!("invalid storage_key used");
    }
    let high_u64 = ((storage_key[7] as u64) << 56) +
        ((storage_key[6] as u64) << 48) +
        ((storage_key[5] as u64) << 40) +
        ((storage_key[4] as u64) << 32) +
        ((storage_key[3] as u64) << 24) +
        ((storage_key[2] as u64) << 16) +
        ((storage_key[1] as u64) << 8) +
        (storage_key[0] as u64);

    (high_u64 % PARTITIONS as u64) as u16
}

#[cfg(test)]
mod tests {
    use crate::storage::partition::{PartitionTable, PARTITIONS};
    use maplit::hashset;

    #[test]
    fn test() {
        let pt = PartitionTable::new(String::from("alice"), hashset! {String::from("alice"), String::from("bob"), String::from("charlie")});
        for partition in 0..PARTITIONS {
            let owner = match pt.get_owner(partition) {
                None => panic!("no owner"),
                Some(owner) => owner,
            };
            let backup = match pt.get_backup(partition) {
                None => panic!("no backup"),
                Some(backup) => backup,
            };
            assert_ne!(owner, backup);
        }
    }
}