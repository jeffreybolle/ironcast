use std::sync::Arc;

use tokio::io::ErrorKind;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

use crate::net::coordinator::ConnectionCoordinator;
use crate::net::InternalEvent;

pub struct Acceptor(oneshot::Sender<()>);

impl Acceptor {
    pub fn new(node_id: String, port: u16, node_sender: Sender<InternalEvent>, cc: Arc<ConnectionCoordinator>) -> Acceptor {
        let (sender, mut receiver) = oneshot::channel();

        tokio::spawn(async move {
            let listener;
            loop {
                match TcpListener::bind(format!("127.0.0.1:{}", port)).await {
                    Ok(l) => {
                        listener = l;
                        break;
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::AddrInUse {
                            sleep(Duration::from_millis(1000)).await;
                            continue;
                        }
                        // TODO handle panic better
                        panic!(e);
                    }
                }
            }
            log::info!("Listening on port {}", port);
            loop {
                tokio::select! {
                    _ = &mut receiver => {
                        drop(listener);
                        return;
                    }
                    result = listener.accept() => {
                        let (mut conn, _) = result.unwrap();
                        match cc.receive_handshake(&node_id, &mut conn).await {
                            Ok(Some(peer_id)) => {
                                if let Err(_) = node_sender.send(InternalEvent::NewConnection(peer_id, conn)).await {
                                    return;
                                }
                            }
                            Ok(None) => {
                                continue;
                            }
                            Err(_) => {
                                continue;
                            }
                        }
                    }
                }
            }
        });
        Acceptor(sender)
    }
}