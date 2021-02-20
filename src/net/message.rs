use tokio::net::TcpStream;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::io::{WriteHalf, AsyncWriteExt};
use crate::net::utils::*;

pub enum Downstream {
    TcpStream(WriteHalf<TcpStream>),
    #[allow(dead_code)]
    Channel(mpsc::Sender<Vec<u8>>)
}

impl Downstream {
    async fn write(&mut self, peer_id: &str, message: Vec<u8>) {
        match self {
            Downstream::TcpStream(downstream) => {
                if let Err(_) = downstream.write_all(&message).await {
                    log::debug!("could not write to {}, dropping message", peer_id);
                }
            }
            Downstream::Channel(downstream) => {
                if let Err(_) = downstream.send(message).await {
                    log::debug!("could not write to {}, dropping message", peer_id);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Sender {
    id: String,
    peer_id: String,
    downstream: Arc<Mutex<Option<Downstream>>>,
}

impl Sender {
    pub fn new(id: String, peer_id: String) -> Sender {
        Sender {
            id,
            peer_id,
            downstream: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn install_tcp_stream(&self, downstream: WriteHalf<TcpStream>) {
        self.downstream.lock().await.replace(Downstream::TcpStream(downstream));
    }

    #[cfg(test)]
    pub async fn install_channel(&self, downstream: mpsc::Sender<Vec<u8>>) {
        self.downstream.lock().await.replace(Downstream::Channel(downstream));
    }

    pub async fn disconnect(&self) {
        self.downstream.lock().await.take();
    }

    pub async fn send(&self, message: Vec<u8>) {
        let mut buffer = Vec::with_capacity(message.len() + 4);
        write_u32(&mut buffer, message.len() as u32);
        buffer.extend_from_slice(&message);

        let mut downstream = self.downstream.lock().await;

        let downstream = match &mut *downstream {
            None => {
                log::trace!("no connection to {}, dropping message", &self.peer_id);
                return;
            }
            Some(downstream) => downstream,
        };

        log::trace!("{} -> {}: {:?}", self.id, self.peer_id,  buffer);
        downstream.write(&self.peer_id, buffer).await;
    }
}