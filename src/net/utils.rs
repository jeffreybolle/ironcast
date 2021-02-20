
pub fn write_u16(msg: &mut Vec<u8>, value: u16) {
    msg.push(((value >> 8) & 0xff) as u8);
    msg.push((value & 0xff) as u8);
}

pub fn write_u32(msg: &mut Vec<u8>, value: u32) {
    msg.push(((value >> 24) & 0xff) as u8);
    msg.push(((value >> 16) & 0xff) as u8);
    msg.push(((value >> 8) & 0xff) as u8);
    msg.push((value & 0xff) as u8);
}

pub fn write_u64(msg: &mut Vec<u8>, value: u64) {
    msg.push(((value >> 56) & 0xff) as u8);
    msg.push(((value >> 48) & 0xff) as u8);
    msg.push(((value >> 40) & 0xff) as u8);
    msg.push(((value >> 32) & 0xff) as u8);
    msg.push(((value >> 24) & 0xff) as u8);
    msg.push(((value >> 16) & 0xff) as u8);
    msg.push(((value >> 8) & 0xff) as u8);
    msg.push((value & 0xff) as u8);
}

pub fn read_u16(msg: &[u8]) -> u16 {
    if msg.len() < 2 {
        panic!("not enough bytes for a u16");
    }
    ((msg[0] as u16) << 8) + (msg[1] as u16)
}

pub fn read_u32(msg: &[u8]) -> u32 {
    if msg.len() < 4 {
        panic!("not enough bytes for a u32");
    }
    ((msg[0] as u32) << 24) +
        ((msg[1] as u32) << 16) +
        ((msg[2] as u32) << 8) +
        (msg[3] as u32)
}

pub fn read_u64(buf: &[u8]) -> u64 {
    if buf.len() < 8 {
        panic!("not enough bytes for a u64");
    }
    ((buf[0] as u64) << 56) +
        ((buf[1] as u64) << 48) +
        ((buf[2] as u64) << 40) +
        ((buf[3] as u64) << 32) +
        ((buf[4] as u64) << 24) +
        ((buf[5] as u64) << 16) +
        ((buf[6] as u64) << 8) +
        (buf[7] as u64)
}