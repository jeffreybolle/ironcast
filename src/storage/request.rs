use crate::net::utils::*;

#[derive(PartialEq, Clone, Debug)]
pub enum RequestType {
    GET,
    PUT,
}

impl RequestType {
    pub fn to_byte(&self) -> u8 {
        match self {
            RequestType::GET => 0x0,
            RequestType::PUT => 0x1
        }
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    request_type: RequestType,
    storage_key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl Request {
    pub fn new(request_type: RequestType, storage_key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Request{request_type, storage_key, value}
    }

    pub fn request_type(&self) -> RequestType {
        self.request_type.clone()
    }

    pub fn storage_key(&self) -> &Vec<u8> {
        &self.storage_key
    }

    pub fn value(&self) -> &Option<Vec<u8>> {
        &self.value
    }

    pub fn to_message(&self) -> Vec<u8> {
        let mut msg;
        match &self.value {
            None => {
                msg = Vec::with_capacity(1 + 32);
                msg.push(self.request_type.to_byte());
                msg.extend_from_slice(&self.storage_key);
            }
            Some(v) => {
                msg = Vec::with_capacity(1 + 32 + 4 + v.len());
                msg.push(self.request_type.to_byte() | 0x2);
                msg.extend_from_slice(&self.storage_key);
                write_u32(&mut msg, v.len() as u32);
                msg.extend_from_slice(v);
            }
        }
        msg
    }

    pub fn from_message(msg: &[u8]) -> Request {
        if msg.len() < 33 {
            panic!("invalid message for Request");
        }
        let discriminator = msg[0];
        let storage_key = msg[1..33].to_vec();
        let request_type = match discriminator & 0x1 {
            0x0 => RequestType::GET,
            0x1 => RequestType::PUT,
            _ => {
                panic!("invalid discriminator: {}", discriminator);
            }
        };
        match discriminator & 0x2 {
            0x0 => {
                Request {
                    request_type,
                    storage_key,
                    value: None,

                }
            }
            0x2 => {
                if msg.len() < 33 + 4 {
                    panic!("invalid message for PUT request with value");
                }
                let len = read_u32(&msg[33..37]) as usize;
                if msg.len() < 37 + len {
                    panic!("invalid message for PUT request with value, not enough bytes");
                }
                Request {
                    request_type: RequestType::PUT,
                    storage_key,
                    value: Some(msg[37..37 + len].to_vec()),
                }
            }
            _ => {
                panic!("invalid discriminator: {}", discriminator);
            }
        }
    }
}

#[derive(Debug)]
pub struct Response {
    value: Option<Vec<u8>>
}

impl Response {
    pub fn new(value: Option<Vec<u8>>) -> Response {
        Response {
            value,
        }
    }

    pub fn into_value(self) -> Option<Vec<u8>> {
        self.value
    }

    pub fn to_message(&self) -> Vec<u8> {
        let mut msg;
        match &self.value {
            None => {
                msg = Vec::with_capacity(1);
                msg.push(0u8);
            }
            Some(v) => {
                msg = Vec::with_capacity(1 + 4 + v.len());
                msg.push(1u8);
                write_u32(&mut msg, v.len() as u32);
                msg.extend_from_slice(&v);
            }
        }
        msg
    }

    pub fn from_message(msg: &[u8]) -> Response {
        if msg.len() < 1 {
            panic!("not enough bytes for response");
        }
        if msg[0] == 0 {
            return Response {
                value: None
            };
        }
        if msg.len() < 5 {
            panic!("not enough bytes for non-empty response");
        }
        let len = read_u32(&msg[1..5]) as usize;
        if msg.len() < 5 + len {
            panic!("not enough bytes for response value");
        }
        Response {
            value: Some(msg[5..5 + len].to_vec())
        }
    }
}
