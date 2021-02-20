use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

use crate::net::coordinator::ConnectionCoordinator;
use crate::net::InternalEvent;

#[derive(Debug)]
pub struct Connector(oneshot::Sender<()>);

impl Connector {
    pub fn new(node_id: String, peer_id: String, addr: String, node_sender: Sender<InternalEvent>, cc: Arc<ConnectionCoordinator>) -> Connector {
        let (stop_sender, mut receiver) = oneshot::channel();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut receiver => {
                        return
                    }
                    _ = sleep(Duration::from_millis(1000)) => {
                        continue
                    }
                    result = TcpStream::connect(addr.clone()) => {
                        let mut conn = match result {
                            Ok(conn) => conn,
                            Err(_) => {
                                // On error sleep to avoid a busy wait
                                sleep(Duration::from_millis(1000)).await;
                                continue;
                            }
                        };
                        let received_peer_id = match cc.start_handshake(&node_id, &mut conn).await {
                            Ok(Some(peer_id)) => peer_id,
                            Ok(None) => {
                                return;
                            }
                            Err(_) => {
                                continue;
                            }
                        };
                        if received_peer_id != peer_id {
                            if let Err(_) = node_sender.send(InternalEvent::RemovePeer(peer_id)).await {
                                log::warn!("handshake error, received incorrect peer id");
                            }
                            return;
                        }
                        if let Err(_) = node_sender.send(InternalEvent::NewConnection(peer_id, conn)).await {
                            log::warn!("could not send to node");
                        }
                        return;
                    }
                }
            }
        });
        Connector(stop_sender)
    }
}