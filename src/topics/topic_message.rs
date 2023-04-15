use bytes::Bytes;
use std::fmt::{Display, Formatter};
use std::time::SystemTime;

/// Represents a published message to a topic.
pub struct TopicMessage {
    pub id: MessageId,
    pub published_at: SystemTime,
    pub data: Bytes,
}

impl TopicMessage {
    /// Creates a new `TopicMessage` from the data.
    pub fn new(data: Bytes) -> Self {
        Self {
            data,
            id: MessageId::default(),
            published_at: SystemTime::UNIX_EPOCH,
        }
    }

    /// Sets the post-publish values.
    pub fn publish(&mut self, id: MessageId, published_at: SystemTime) {
        self.id = id;
        self.published_at = published_at;
    }
}

/// Represents a message ID.
#[derive(Debug, PartialEq, Eq, Hash, Default, Copy, Clone)]
pub struct MessageId {
    /// The actual value. Globally uniqueness is maintained
    /// by ensuring a topic generates IDs for its' messages.
    pub value: u64,
}

impl MessageId {
    /// Creates a new `MessageId` using the topic's internal ID and
    /// a topic-local message ID.
    pub fn new(topic_internal_id: u32, topic_local_message_id: u32) -> Self {
        Self {
            value: ((topic_internal_id as u64) << 32) | (topic_local_message_id as u64),
        }
    }
}

/// Implements `Display` by returning the inner value as a string.
impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn new_works() {
        assert_eq!(MessageId::new(1, 1), MessageId::new(1, 1));
        assert_ne!(MessageId::new(1, 1), MessageId::new(2, 1));
        assert_ne!(MessageId::new(1, 1), MessageId::new(1, 2));
        assert_ne!(MessageId::new(2, 1), MessageId::new(1, 2));
    }

    #[test]
    fn display() {
        assert_eq!("1", MessageId::new(0, 1).to_string());
        assert_eq!("4294967298", MessageId::new(1, 2).to_string());
    }
}
