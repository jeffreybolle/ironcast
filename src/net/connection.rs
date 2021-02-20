use tokio::io::{AsyncReadExt, split};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use crate::net::message;
use crate::net::InternalEvent;
use crate::net::utils::*;

#[derive(Debug)]
pub struct Connection {
    read_shutdown_sender: Option<oneshot::Sender<()>>,
}

impl Connection {
    pub fn new(node_id: String, peer_id: String, node_sender: Sender<InternalEvent>, conn: TcpStream, message_sender: message::Sender) -> Connection {
        let (read_shutdown_sender, mut read_shutdown_receiver) = oneshot::channel();

        let (mut read_half, write_half) = split(conn);

        tokio::spawn(async move {
            message_sender.install_tcp_stream(write_half).await;
        });


        tokio::spawn(async move {
            let mut msg = Vec::with_capacity(1024);
            'outer: loop {
                let mut buf = vec![0u8; 1024];
                tokio::select! {
                    result = read_half.read(&mut buf) => {
                        let n = match result {
                            Ok(n) => n,
                            Err(_) => {
                                if let Err(_) = node_sender.send(InternalEvent::ConnectionError(peer_id)).await {
                                    log::warn!("could not send to node");
                                };
                                return;
                            }
                        };
                        if n == 0 {
                            if let Err(_) = node_sender.send(InternalEvent::ConnectionError(peer_id)).await {
                                log::warn!("could not send to node");
                            };
                            return;
                        }
                        msg.extend_from_slice(&buf[..n]);
                        loop {
                            if msg.len() < 4 {
                                continue 'outer;
                            }
                            let len = read_u32(&msg[..4]) as usize;
                            if msg.len() < len + 4 {
                                continue 'outer;
                            }
                            log::trace!("{} -> {}: {:?}", peer_id, node_id, msg[0..len+4].to_vec());
                            if let Err(_) = node_sender.send(InternalEvent::IncomingMessage {peer_id: peer_id.clone(), data: msg[4..len+4].to_vec()}).await {
                                log::warn!("could not send to node");
                            };
                            msg.drain(0..len+4);
                        }
                    }
                    _ = &mut read_shutdown_receiver => {
                        return;
                    },
                }
            }
        });


        Connection {
            read_shutdown_sender: Some(read_shutdown_sender),
        }
    }
}