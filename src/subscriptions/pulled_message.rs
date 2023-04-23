use crate::topics::TopicMessage;
use std::sync::Arc;
use std::time::SystemTime;

/// A message that has been pulled.
pub struct PulledMessage {
    /// The message that was pulled.
    message: Arc<TopicMessage>,

    /// The ID used to ack the message.
    ack_id: u64,

    /// The deadline for the message after which it will expire.
    deadline: SystemTime,
}

impl PulledMessage {
    /// Creates a new `PulledMessage`.
    pub fn new(message: Arc<TopicMessage>, ack_id: u64, deadline: SystemTime) -> Self {
        Self {
            message,
            ack_id,
            deadline,
        }
    }

    /// Gets the underlying message.
    pub fn message(&self) -> &Arc<TopicMessage> {
        &self.message
    }

    /// Gets the ack ID.
    pub fn ack_id(&self) -> u64 {
        self.ack_id
    }

    /// Gets the deadline.
    pub fn deadline(&self) -> &SystemTime {
        &self.deadline
    }
}
