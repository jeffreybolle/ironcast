use std::collections::HashMap;
use std::sync::atomic;
use std::sync::atomic::Ordering;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::net::{ConnectionError, HandshakeError};

pub struct ConnectionState {
    connecting: Vec<String>,
    connected: Option<String>,
}

pub struct ConnectionCoordinator {
    pub peer_states: Mutex<HashMap<String, ConnectionState>>,
    pub next_id: atomic::AtomicU64,
}

impl ConnectionCoordinator {
    pub fn new() -> ConnectionCoordinator {
        ConnectionCoordinator {
            peer_states: Mutex::new(HashMap::new()),
            next_id: atomic::AtomicU64::new(1),
        }
    }

    pub async fn start_handshake(&self, node_id: &str, conn: &mut TcpStream) -> Result<Option<String>, ConnectionError> {
        let conn_id = format!("{}-{}", node_id, self.next_id.fetch_add(1, Ordering::SeqCst));

        write_handshake_bytes(node_id.as_bytes(), conn).await?;
        let peer_id = read_handshake_bytes(conn).await?;

        let mut guard = self.peer_states.lock().await;
        let proceed = match guard.get_mut(&peer_id) {
            None => {
                guard.insert(peer_id.clone(), ConnectionState {
                    connecting: vec! {conn_id.clone()},
                    connected: None,
                });
                true
            }
            Some(cs) => {
                if cs.connected.is_some() {
                    false
                } else {
                    cs.connecting.push(conn_id.clone());
                    true
                }
            }
        };
        drop(guard);

        if !proceed {
            write_bye(conn).await?;
            return Ok(None);
        }

        write_connection_id_bytes(conn_id.as_bytes(), conn).await?;
        let proceed = read_connection_id_confirmation(conn).await?;

        if !proceed {
            return Ok(None);
        }

        let mut guard = self.peer_states.lock().await;
        let proceed = match guard.get_mut(&peer_id) {
            None => {
                return Err(HandshakeError);
            }
            Some(cs) => {
                if cs.connected.is_some() {
                    false
                } else {
                    let mut present = false;
                    let mut win = true;
                    for item in cs.connecting.iter() {
                        if *item == conn_id {
                            present = true;
                        } else if *item < conn_id {
                            win = false;
                        }
                    }
                    if !present {
                        return Err(HandshakeError);
                    }
                    if win {
                        cs.connected = Some(conn_id);
                        cs.connecting.clear();
                        true
                    } else {
                        cs.connecting.remove(cs.connecting.iter().position(|item| *item == conn_id).unwrap());
                        false
                    }
                }
            }
        };
        drop(guard);

        if proceed {
            write_success(conn).await?;
            Ok(Some(peer_id))
        } else {
            write_failure(conn).await?;
            Ok(None)
        }
    }

    pub async fn receive_handshake(&self, node_id: &str, conn: &mut TcpStream) -> Result<Option<String>, ConnectionError> {
        let peer_id = read_handshake_bytes(conn).await?;
        write_handshake_bytes(node_id.as_bytes(), conn).await?;

        let conn_id = match read_connection_id_or_bye(conn).await? {
            Some(conn_id) => conn_id,
            None => {
                return Ok(None);
            }
        };

        let mut guard = self.peer_states.lock().await;
        let proceed = match guard.get_mut(&peer_id) {
            None => {
                guard.insert(peer_id.clone(), ConnectionState {
                    connecting: vec! {conn_id.clone()},
                    connected: None,
                });
                true
            }
            Some(cs) => {
                if cs.connected.is_some() {
                    false
                } else {
                    cs.connecting.push(conn_id.clone());
                    true
                }
            }
        };
        drop(guard);

        write_connection_id_confirmation(proceed, conn).await?;
        let success = read_success_or_failure(conn).await?;

        let mut guard = self.peer_states.lock().await;
        match guard.get_mut(&peer_id) {
            None => {
                if success {
                    guard.insert(peer_id.clone(), ConnectionState {
                        connecting: vec! {},
                        connected: Some(conn_id),
                    });
                }
            }
            Some(cs) => {
                if success {
                    if cs.connected.is_some() {
                        panic!("handshake algo failed");
                    }
                    cs.connected = Some(conn_id);
                    cs.connecting.clear();
                } else {
                    if let Some(pos) = cs.connecting.iter().position(|item| *item == conn_id) {
                        cs.connecting.remove(pos);
                    }
                    return Ok(None);
                }
            }
        };

        Ok(Some(peer_id))
    }
}

async fn write_handshake_bytes(node_id: &[u8], conn: &mut TcpStream) -> Result<(), ConnectionError> {
    let mut msg = vec![0u8; 128];
    msg[0] = Tag::StartHandShake as u8;
    msg[1] = node_id.len() as u8;
    for i in 2..node_id.len() + 2 {
        msg[i] = node_id[i - 2];
    }
    conn.write_all(&msg).await?;
    conn.flush().await?;
    Ok(())
}

async fn read_handshake_bytes(conn: &mut TcpStream) -> Result<String, ConnectionError> {
    let mut buf = vec![0u8; 128];
    let mut read = 0usize;
    loop {
        let n = conn.read(&mut buf[read..]).await?;

        if n == 0 {
            return Err(ConnectionError::HandshakeError);
        }

        read += n;

        if read < 128 {
            continue;
        }

        if buf[0] != Tag::StartHandShake as u8 {
            return Err(ConnectionError::HandshakeError);
        }
        let size = buf[1] as usize;

        let peer_id = String::from_utf8(buf[2..size + 2].to_vec())?;
        return Ok(peer_id);
    }
}

async fn write_connection_id_bytes(conn_id: &[u8], conn: &mut TcpStream) -> Result<(), ConnectionError> {
    let mut msg = vec![0u8; 128];
    msg[0] = Tag::SendConnectionId as u8;
    msg[1] = conn_id.len() as u8;
    for i in 2..conn_id.len() + 2 {
        msg[i] = conn_id[i - 2];
    }
    conn.write_all(&msg).await?;
    conn.flush().await?;
    Ok(())
}

async fn read_connection_id_or_bye(conn: &mut TcpStream) -> Result<Option<String>, ConnectionError> {
    let mut buf = vec![0u8; 128];
    let mut read = 0usize;
    loop {
        let n = conn.read(&mut buf[read..]).await?;

        if n == 0 {
            return Err(ConnectionError::HandshakeError);
        }

        read += n;

        if buf[0] == Tag::Bye as u8 {
            return Ok(None);
        }

        if read < 128 {
            continue;
        }

        if buf[0] != Tag::SendConnectionId as u8 {
            return Err(ConnectionError::HandshakeError);
        }
        let size = buf[1] as usize;

        let conn_id = String::from_utf8(buf[2..size + 2].to_vec())?;
        return Ok(Some(conn_id));
    }
}

async fn write_connection_id_confirmation(proceed: bool, conn: &mut TcpStream) -> Result<(), ConnectionError> {
    conn.write_u8(Tag::IdConfirmation as u8).await?;
    conn.write_u8(if proceed { 1u8 } else { 0u8 }).await?;
    conn.flush().await?;
    Ok(())
}

async fn read_connection_id_confirmation(conn: &mut TcpStream) -> Result<bool, ConnectionError> {
    let mut buf = vec![0u8; 2];
    let mut read = 0usize;
    loop {
        let n = conn.read(&mut buf[read..]).await?;

        if n == 0 {
            return Err(ConnectionError::HandshakeError);
        }

        read += n;

        if read < 2 {
            continue;
        }

        if buf[0] != Tag::IdConfirmation as u8 {
            return Err(ConnectionError::HandshakeError);
        }
        let result = buf[1] == 1u8;
        return Ok(result);
    }
}

async fn write_bye(conn: &mut TcpStream) -> Result<(), ConnectionError> {
    conn.write_u8(Tag::Bye as u8).await?;
    conn.flush().await?;
    Ok(())
}

async fn write_success(conn: &mut TcpStream) -> Result<(), ConnectionError> {
    conn.write_u8(Tag::Success as u8).await?;
    conn.flush().await?;
    Ok(())
}

async fn write_failure(conn: &mut TcpStream) -> Result<(), ConnectionError> {
    conn.write_u8(Tag::Failure as u8).await?;
    conn.flush().await?;
    Ok(())
}

async fn read_success_or_failure(conn: &mut TcpStream) -> Result<bool, ConnectionError> {
    let mut buf = vec![0u8; 1];
    let n = conn.read(&mut buf).await?;
    if n == 0 {
        return Err(ConnectionError::HandshakeError);
    }
    Ok(buf[0] == Tag::Success as u8)
}

enum Tag {
    StartHandShake = 1,
    SendConnectionId = 2,
    Bye = 3,
    IdConfirmation = 4,
    Success = 5,
    Failure = 6,
}