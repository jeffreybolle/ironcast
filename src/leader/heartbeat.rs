use std::collections::HashMap;
use crate::net::message;
use crate::leader::tag::Tag;
use tokio::time::{sleep, Duration, Instant};
use tokio::sync::{oneshot, Mutex, mpsc};
use std::sync::Arc;

#[derive(Debug)]
pub enum HeartbeatOutput {
    HeartbeatNotReceived(String),
}

pub struct HeartbeatMonitor {
    following: Option<String>,
    peers: HashMap<String, message::Sender>,
    last_heartbeat: Arc<Mutex<HashMap<String,Instant>>>,
    output_receiver: mpsc::Receiver<HeartbeatOutput>,
    _shutdown_sender: oneshot::Sender<()>,
}

impl HeartbeatMonitor {
    pub fn new(peers: HashMap<String, message::Sender>, following: Option<String>) -> Self {
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let (mut output_sender, output_receiver) = mpsc::channel(1);
        let mut last_heartbeat = HashMap::new();
        if let Some(following) = &following {
            last_heartbeat.insert(following.clone(), Instant::now());
        } else {
            for (peer, _) in peers.iter() {
                last_heartbeat.insert(peer.clone(), Instant::now());
            }
        }
        let last_heartbeat = Arc::new(Mutex::new(last_heartbeat));

        if following.is_none() {
            let peers = peers.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sleep(Duration::from_millis(100)) => {}
                        _ = &mut shutdown_receiver => {
                            return;
                        }
                    }
                    send_heartbeats(&peers).await;
                    check(&mut output_sender).await;
                }
            });
        } else {
            let following = following.as_ref().unwrap().clone();
            let last_heartbeat = last_heartbeat.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = sleep(Duration::from_millis(100)) => {}
                        _ = &mut shutdown_receiver => {
                            return;
                        }
                    }
                    check_heartbeat_received(&following, &last_heartbeat, &mut output_sender).await;
                }
            });
        }
        HeartbeatMonitor {
            following,
            peers,
            last_heartbeat,
            output_receiver,
            _shutdown_sender: shutdown_sender
        }
    }

    pub async fn recv(&mut self) -> Option<HeartbeatOutput> {
        self.output_receiver.recv().await
    }

    pub async fn heartbeat_received(&self, from: String) {
        self.last_heartbeat.lock().await.insert(from, Instant::now());
        if let Some(following) = &self.following {
            if let Some(sender) = self.peers.get(following) {
                let sender = sender.clone();
                let msg = vec![Tag::Heartbeat as u8];
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

async fn check_heartbeat_received(following: &String,
                                  last_heartbeat: &Arc<Mutex<HashMap<String,Instant>>>,
                                  output_sender: &mut mpsc::Sender<HeartbeatOutput>) {
    if last_heartbeat.lock().await.get(following).unwrap().elapsed() > Duration::from_millis(250) {
        log::debug!("heartbeat missed for {}", following);
        if let Err(_) = output_sender.send(HeartbeatOutput::HeartbeatNotReceived(following.clone())).await {
            log::trace!("could not send heartbeat output");
        }
    }
}

async fn check(_output_sender: &mut mpsc::Sender<HeartbeatOutput>) {
    // TODO
}

async fn send_heartbeats(peers: &HashMap<String, message::Sender>) {
    let msg = vec![Tag::Heartbeat as u8];
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
