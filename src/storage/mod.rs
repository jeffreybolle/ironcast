use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sha3::{Digest, Sha3_256};
use tokio::sync::{Mutex, oneshot};
use tokio::sync::mpsc::{channel, Sender};

use crate::{leader, net};
use crate::leader::NodeOutput;
use crate::net::utils::*;
use crate::storage::partition::{PartitionTable, calculate_partition};
use crate::storage::tag::Tag;
use crate::storage::request::{RequestType, Response, Request};
use crate::storage::structure::Storage;
use crate::storage::retrieve::*;

mod partition;
mod tag;
mod request;
mod structure;
mod retrieve;

#[derive(Debug)]
pub enum StorageError {
    UnknownError
}

pub struct Node {
    id: String,
    storage: Arc<Mutex<Storage>>,
    partition_table: Arc<Mutex<Option<PartitionTable>>>,
    _shutdown_sender: oneshot::Sender<()>,
    request_sender: Sender<WrappedRequest>,
    ready_receiver: oneshot::Receiver<()>,
}

// TODO 1. make sure partitions only include live members
// TODO 2. make sure backups are retrieve by the new backups as well

impl Node {
    pub fn new(id: String, port: u16, peers: Vec<net::Peer>) -> Node {
        let mut node = leader::Node::new(id.clone(), port, peers.clone());
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let storage = Arc::new(Mutex::new(Storage::new()));
        let partition_table = Arc::new(Mutex::new(None));
        let (request_sender, mut request_receiver) = channel::<WrappedRequest>(1024);
        let (ready_sender, ready_receiver) = oneshot::channel();

        {
            let id = id.clone();
            let partition_table = partition_table.clone();
            let storage = storage.clone();
            tokio::spawn(async move {
                let mut pending: HashMap<u64, oneshot::Sender<Option<Vec<u8>>>> = HashMap::new();
                let mut next_id = 0u64;
                let mut ready_sender = Some(ready_sender);

                loop {
                    tokio::select! {
                        e = node.recv() => {
                            match e {
                                None => return,
                                Some(e) => handle_node_event(&id, &mut node, &partition_table, &storage,
                                                             &mut pending, &mut ready_sender, &mut next_id, e).await
                            }
                        },
                        req = request_receiver.recv() => {
                            match req {
                                None => return,
                                Some(req) => handle_request(&id, &mut node, &storage, &partition_table,
                                                            &mut pending, &mut next_id, req).await
                            }
                        },
                        _ = &mut shutdown_receiver => {
                            return;
                        }
                    }
                    ;
                }
            });
        }

        Node {
            id,
            storage,
            partition_table,
            _shutdown_sender: shutdown_sender,
            request_sender,
            ready_receiver,
        }
    }


    pub async fn put(&self, namespace: &str, key: &[u8], value: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError> {
        let storage_key = calculate_storage_key(namespace, key);
        let partition = calculate_partition(&storage_key);
        log::info!("partition: {}", partition);
        let owner = self.get_owner(partition).await?;
        self.request_put(owner, storage_key, value).await
    }

    async fn get_owner(&self, partition: u16) -> Result<String, StorageError> {
        let pt = &*self.partition_table.lock().await;
        let pt = match pt {
            None => {
                return Err(StorageError::UnknownError);
            }
            Some(pt) => pt
        };
        let owner = pt.get_owner(partition).unwrap();
        log::info!("owner: {}", owner);
        Ok(owner)
    }

    async fn request_put(&self, owner: String, storage_key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError> {
        let (req, receiver) = new_request(RequestType::PUT, owner, storage_key, Some(value));
        if let Err(_) = self.request_sender.send(req).await {
            return Err(StorageError::UnknownError);
        }

        match receiver.await {
            Ok(value) => Ok(value),
            Err(_) => Err(StorageError::UnknownError)
        }
    }

    async fn request_get(&self, owner: String, storage_key: Vec<u8>) -> Result<Option<Vec<u8>>, StorageError> {
        let (req, receiver) = new_request(RequestType::GET, owner, storage_key, None);
        if let Err(_) = self.request_sender.send(req).await {
            return Err(StorageError::UnknownError);
        }

        match receiver.await {
            Ok(value) => Ok(value),
            Err(_) => Err(StorageError::UnknownError)
        }
    }

    pub async fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let storage_key = calculate_storage_key(namespace, key);
        let partition = calculate_partition(&storage_key);
        log::info!("partition: {}", partition);
        let owner = self.get_owner(partition).await?;
        if owner == self.id {
            return Ok(self.storage.lock().await.get(&storage_key).cloned());
        }
        self.request_get(owner, storage_key).await
    }

    pub async fn ready(&mut self) {
        (&mut self.ready_receiver).await.unwrap()
    }
}

async fn handle_request(id: &String,
                        node: &mut leader::Node,
                        storage: &Arc<Mutex<Storage>>,
                        partition_table: &Arc<Mutex<Option<PartitionTable>>>,
                        pending: &mut HashMap<u64, oneshot::Sender<Option<Vec<u8>>>>,
                        next_id: &mut u64,
                        req: WrappedRequest) {
    if req.owner == *id {
        let response = process_request(&storage, req.request.clone()).await;
        if let Err(_) = req.sender.send(response.into_value()) {
            log::trace!("could not send response");
        }
        if req.request.request_type() == RequestType::PUT {
            let partition = calculate_partition(req.request.storage_key());
            let backup = match partition_table.lock().await.as_ref() {
                None => return,
                Some(pt) => pt.get_backup(partition).unwrap()
            };
            let mut tagged_message = Vec::new();
            tagged_message.push(Tag::BackupReq as u8);
            write_u64(&mut tagged_message, *next_id);
            *next_id += 1;
            tagged_message.extend_from_slice(&req.request.to_message());
            node.send(&backup, &tagged_message).await;
        }
    } else {
        let msg = req.request.to_message();
        let mut tagged_msg = Vec::with_capacity(msg.len() + 9);
        tagged_msg.push(Tag::Request as u8);
        write_u64(&mut tagged_msg, *next_id);
        tagged_msg.extend_from_slice(&msg);
        pending.insert(*next_id, req.sender);
        *next_id += 1;
        node.send(&req.owner, &tagged_msg).await;
    }
}

async fn process_request(storage: &Arc<Mutex<Storage>>, request: Request) -> Response {
    let mut map = storage.lock().await;
    match request.request_type() {
        RequestType::GET => {
            Response::new(map.get(request.storage_key()).map(Vec::clone))
        }
        RequestType::PUT => {
            match request.value() {
                None => {
                    Response::new(map.remove(request.storage_key()))
                }
                Some(v) => {
                    Response::new(map.insert(request.storage_key().clone(), v.clone()))
                }
            }
        }
    }
}


async fn send_old_partition_table_to_new_members(node: &mut leader::Node,
                                                 partition_table: &Arc<Mutex<Option<PartitionTable>>>,
                                                 members: &HashSet<String>) {
    let partition_table = partition_table.lock().await;
    if let Some(partition_table) = partition_table.as_ref() {
        let msg = partition_table.to_message();
        let mut tagged_msg = Vec::with_capacity(msg.len() + 1);
        tagged_msg.push(Tag::NewPartitionTable as u8);
        tagged_msg.extend_from_slice(&msg);
        for new_member in members.difference(&partition_table.members()) {
            node.send(new_member, &tagged_msg).await;
        }
    }
}

async fn handle_node_event(id: &String,
                           node: &mut leader::Node,
                           partition_table: &Arc<Mutex<Option<PartitionTable>>>,
                           storage: &Arc<Mutex<Storage>>,
                           pending: &mut HashMap<u64, oneshot::Sender<Option<Vec<u8>>>>,
                           ready_sender: &mut Option<oneshot::Sender<()>>,
                           next_id: &mut u64,
                           event: leader::NodeOutput) {
    match event {
        NodeOutput::Leader(members) => {
            send_old_partition_table_to_new_members(node, partition_table, &members).await;
            let pt = PartitionTable::new(id.clone(), members.clone());
            let msg = pt.to_message();
            update_partition_table(id, node, partition_table, next_id, pt).await;
            let mut tagged_msg = Vec::with_capacity(msg.len() + 1);
            tagged_msg.push(Tag::NewPartitionTable as u8);
            tagged_msg.extend_from_slice(&msg);
            for member in members.iter() {
                node.send(member, &tagged_msg).await;
            }
            if let Some(ready_sender) = ready_sender.take() {
                if let Err(_) = ready_sender.send(()) {
                    log::trace!("could not send ready signal");
                }
            }
        }
        NodeOutput::Follower(_) => {}
        NodeOutput::Message(peer, message) => {
            if message.len() == 0 {
                return;
            }
            if message[0] == Tag::NewPartitionTable as u8 {
                let pt = PartitionTable::from_message(&message[1..]);
                log::info!("{} {:?}", id, pt);
                update_partition_table(id, node, partition_table, next_id, pt).await;
                if let Some(ready_sender) = ready_sender.take() {
                    if let Err(_) = ready_sender.send(()) {
                        log::trace!("could not send ready signal");
                    }
                }
            }
            if message[0] == Tag::Request as u8 {
                let request_id = read_u64(&message[1..9]);
                let request = Request::from_message(&message[9..]);
                let response = process_request(storage, request.clone()).await;

                if request.request_type() == RequestType::PUT {
                    let partition = calculate_partition(request.storage_key());
                    let backup = match partition_table.lock().await.as_ref() {
                        None => return,
                        Some(pt) => pt.get_backup(partition).unwrap()
                    };
                    let mut tagged_message = Vec::new();
                    tagged_message.push(Tag::BackupReq as u8);
                    write_u64(&mut tagged_message, *next_id);
                    *next_id += 1;
                    tagged_message.extend_from_slice(&request.to_message());
                    node.send(&backup, &tagged_message).await;
                }

                let mut tagged_message = Vec::new();
                tagged_message.push(Tag::Response as u8);
                write_u64(&mut tagged_message, request_id);
                tagged_message.extend_from_slice(&response.to_message());
                node.send(&peer, &tagged_message).await;
            }
            if message[0] == Tag::Response as u8 {
                let request_id = read_u64(&message[1..9]);
                let response = Response::from_message(&message[9..]);
                match pending.remove(&request_id) {
                    Some(sender) => {
                        if let Err(_) = sender.send(response.into_value()) {
                            log::trace!("could not send response");
                        }
                    }
                    None => {}
                }
            }
            if message[0] == Tag::BackupReq as u8 {
                let request_id = read_u64(&message[1..9]);
                let request = Request::from_message(&message[9..]);
                let response = process_request(storage, request.clone()).await;
                let mut tagged_message = Vec::new();
                tagged_message.push(Tag::BackupResp as u8);
                write_u64(&mut tagged_message, request_id);
                tagged_message.extend_from_slice(&response.to_message());
                node.send(&peer, &tagged_message).await;
            }
            if message[0] == Tag::BackupResp as u8 {
                let _request_id = read_u64(&message[1..9]);
                let _response = Response::from_message(&message[9..]);
                // TODO we need to wait for the response but currently we do not
            }
            if message[0] == Tag::RetrieveBackupReq as u8 {
                let request_id = read_u64(&message[1..9]);
                let partition_id = retrieve_request_from_message(&message[9..]);
                let storage = storage.lock().await;
                let partition = storage.get_partition(partition_id);
                let response = retrieve_response_to_message(partition);
                let mut tagged_message = Vec::new();
                tagged_message.push(Tag::RetrieveBackupResp as u8);
                write_u64(&mut tagged_message, request_id);
                tagged_message.extend_from_slice(&response);
                node.send(&peer, &tagged_message).await;
            }
            if message[0] == Tag::RetrieveBackupResp as u8 {
                let _ = read_u64(&message[1..9]); // TODO this is not needed
                let partition = retrieve_response_from_message(&message[9..]);
                storage.lock().await.put_partition(partition);
            }
        }
    }
}

fn calculate_storage_key(namespace: &str, key: &[u8]) -> Vec<u8> {
    let mut hasher = Sha3_256::new();
    hasher.update(namespace.as_bytes());
    hasher.update(key);
    hasher.finalize().to_vec()
}

struct WrappedRequest {
    request: Request,
    owner: String,
    sender: oneshot::Sender<Option<Vec<u8>>>,
}

fn new_request(request_type: RequestType, owner: String, storage_key: Vec<u8>, value: Option<Vec<u8>>) -> (WrappedRequest, oneshot::Receiver<Option<Vec<u8>>>) {
    let (sender, receiver) = oneshot::channel();
    let request = Request::new(request_type, storage_key, value);
    (WrappedRequest { request, owner, sender }, receiver)
}

async fn update_partition_table(id: &String, node: &mut leader::Node, partition_table: &Arc<Mutex<Option<PartitionTable>>>, next_id: &mut u64, pt: PartitionTable) {
    let mut partition_table = partition_table.lock().await;
    if partition_table.is_some() {
        let diff = partition_table.as_ref().unwrap().get_difference(&pt, id);
        for (peer, partition_id) in diff.into_iter() {
            let mut tagged_message = Vec::new();
            tagged_message.push(Tag::RetrieveBackupReq as u8);
            write_u64(&mut tagged_message, *next_id);
            *next_id += 1;
            tagged_message.extend_from_slice(&retrieve_request_to_message(partition_id));
            node.send(&peer, &tagged_message).await;
        }
    }
    partition_table.replace(pt);
}

#[cfg(test)]
mod tests {
    use simple_logger::SimpleLogger;

    use crate::net;
    use crate::storage::{Node, StorageError};

    #[tokio::test]
    async fn test() -> Result<(), StorageError> {
        if let Err(_) = SimpleLogger::new().with_level(log::LevelFilter::Debug).init() {
            // ignore
        }

        let mut node_a = Node::new(String::from("alice"), 5000, vec![
            net::Peer { id: String::from("bob"), addr: String::from("127.0.0.1:5001") },
            net::Peer { id: String::from("charlie"), addr: String::from("127.0.0.1:5002") }
        ]);
        let mut node_b = Node::new(String::from("bob"), 5001, vec![
            net::Peer { id: String::from("alice"), addr: String::from("127.0.0.1:5000") },
            net::Peer { id: String::from("charlie"), addr: String::from("127.0.0.1:5002") }
        ]);
        let mut node_c = Node::new(String::from("charlie"), 5002, vec![
            net::Peer { id: String::from("alice"), addr: String::from("127.0.0.1:5000") },
            net::Peer { id: String::from("bob"), addr: String::from("127.0.0.1:5001") }
        ]);

        node_a.ready().await;
        node_b.ready().await;
        node_c.ready().await;

        match node_a.put("__default__", "key".as_bytes(), "value1".as_bytes().to_vec()).await {
            Ok(v) => {
                log::info!("v: {:?}", v);
            }
            Err(e) => { panic!("{:?}", e); }
        }

        match node_b.put("__default__", "key".as_bytes(), "value2".as_bytes().to_vec()).await {
            Ok(v) => {
                log::info!("v: {:?}", v);
            }
            Err(e) => { panic!("{:?}", e); }
        }

        match node_c.put("__default__", "key".as_bytes(), "value3".as_bytes().to_vec()).await {
            Ok(v) => {
                log::info!("v: {:?}", v);
            }
            Err(e) => { panic!("{:?}", e); }
        }

        let value = node_a.get("__default__", "key".as_bytes()).await?.unwrap();
        log::info!("value: {}", String::from_utf8_lossy(&value));

        Ok(())
    }
}