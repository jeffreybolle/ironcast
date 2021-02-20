use crate::net::utils::*;

pub fn retrieve_request_to_message(partition_id: u16) -> Vec<u8> {
    let mut message = Vec::with_capacity(2);
    write_u16(&mut message, partition_id);
    message
}

pub fn retrieve_request_from_message(message: &[u8]) -> u16 {
    read_u16(message)
}

pub fn retrieve_response_to_message(partition: Vec<(&Vec<u8>, &Vec<u8>)>) -> Vec<u8> {
    let mut message = Vec::with_capacity(1024);
    write_u32(&mut message, partition.len() as u32);
    for (key, value) in partition.into_iter() {
        message.extend_from_slice(key);
        write_u32(&mut message, value.len() as u32);
        message.extend_from_slice(value);
    }
    message
}

pub fn retrieve_response_from_message(message: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let len = read_u32(message);
    let mut partition = Vec::with_capacity(len as usize);
    let mut pos = 4;
    for _ in 0..len {
        let key = message[pos..pos+32].to_vec();
        pos += 32;
        let value_len = read_u32(&message[pos..pos+4]) as usize;
        pos += 4;
        let value = message[pos..pos+value_len].to_vec();
        partition.push((key, value));
    }
    partition
}