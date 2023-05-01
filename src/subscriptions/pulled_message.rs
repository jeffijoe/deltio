use crate::subscriptions::AckId;
use crate::topics::TopicMessage;
use std::sync::Arc;
use std::time::SystemTime;

/// A message that has been pulled.
#[derive(Debug, Clone)]
pub struct PulledMessage {
    /// The message that was pulled.
    message: Arc<TopicMessage>,

    /// The ID used to ack the message.
    ack_id: AckId,

    /// The deadline for the message after which it will expire.
    deadline: SystemTime,

    /// The number of times that an attempt has been made at delivering a message to
    /// the subscription.
    #[allow(dead_code)]
    delivery_attempt: u16,
}

impl PulledMessage {
    /// Creates a new `PulledMessage`.
    pub fn new(
        message: Arc<TopicMessage>,
        ack_id: AckId,
        deadline: SystemTime,
        delivery_attempt: u16,
    ) -> Self {
        Self {
            message,
            ack_id,
            deadline,
            delivery_attempt,
        }
    }

    /// Gets the underlying message.
    pub fn message(&self) -> &Arc<TopicMessage> {
        &self.message
    }

    /// Gets the ack ID.
    pub fn ack_id(&self) -> AckId {
        self.ack_id
    }

    /// Gets the deadline.
    pub fn deadline(&self) -> &SystemTime {
        &self.deadline
    }

    /// Gets the delivery attempt.
    pub fn delivery_attempt(&self) -> u16 {
        self.delivery_attempt
    }
}
