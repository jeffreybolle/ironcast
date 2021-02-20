use std::collections::HashMap;
use crate::storage::partition::{PARTITIONS, calculate_partition};

pub struct Storage {
    inner: HashMap<u16, HashMap<Vec<u8>, Vec<u8>>>
}

impl Storage {
    pub fn new() -> Self {
        Storage { inner: HashMap::new() }
    }

    pub fn insert(&mut self, storage_key: Vec<u8>, data: Vec<u8>) -> Option<Vec<u8>> {
        let partition_id = calculate_partition(&storage_key);
        let partition = match self.inner.get_mut(&partition_id) {
            None => {
                self.inner.insert(partition_id, HashMap::new());
                self.inner.get_mut(&partition_id).unwrap()
            }
            Some(m) => m
        };
        partition.insert(storage_key, data)
    }

    pub fn get(&self, storage_key: &Vec<u8>) -> Option<&Vec<u8>> {
        let partition_id = calculate_partition(&storage_key);
        let partition = match self.inner.get(&partition_id) {
            None => return None,
            Some(m) => m
        };
        partition.get(storage_key)
    }

    pub fn remove(&mut self, storage_key: &Vec<u8>) -> Option<Vec<u8>> {
        let partition_id = calculate_partition(&storage_key);
        let partition = match self.inner.get_mut(&partition_id) {
            None => return None,
            Some(m) => m
        };
        partition.remove(storage_key)
    }

    pub fn get_partition(&self, partition: u16) -> Vec<(&Vec<u8>, &Vec<u8>)> {
        if partition >= PARTITIONS {
            panic!("invalid partition: {}", partition);
        }
        match self.inner.get(&partition) {
            None => vec!{},
            Some(partition) => partition.iter().collect()
        }
    }

    pub fn put_partition(&mut self, partition: Vec<(Vec<u8>, Vec<u8>)>) {
        if !partition.is_empty() {
            log::info!("put_partition {:?}", partition);
        }
        for (storage_key, data) in partition.into_iter() {
            self.insert(storage_key, data);
        }
    }
}