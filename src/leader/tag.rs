
pub enum Tag {
    Vote = 1,
    Ack = 2,
    Heartbeat = 3,
    MembershipList = 4,
    Message = 5,
}

impl Tag {
    pub fn from_u8(value: u8) -> Tag {
        match value {
            1 => Tag::Vote,
            2 => Tag::Ack,
            3 => Tag::Heartbeat,
            4 => Tag::MembershipList,
            5 => Tag::Message,
            _ => {
                panic!("invalid tag: {}", value);
            }
        }
    }
}