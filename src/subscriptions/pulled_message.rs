use crate::subscriptions::AckId;
use crate::topics::TopicMessage;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A message that has been pulled.
#[derive(Debug, Clone)]
pub struct PulledMessage {
    /// The message that was pulled.
    message: Arc<TopicMessage>,

    /// The ID used to ack the message.
    ack_id: AckId,

    /// The deadline for the message after which it will expire.
    deadline: AckDeadline,

    /// The number of times that an attempt has been made at delivering a message to
    /// the subscription.
    delivery_attempt: u16,
}

lazy_static! {
    /// Used as a baseline to calculate durations in order to round.
    static ref EPOCH: Instant = Instant::now();
}

/// Represents the deadline by which a message should be acked before it is considered expired.
///
/// These are rounded up to the nearest 10th of a second in order to capture as many expirations
/// at the same time as possible.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct AckDeadline {
    /// The actual deadline time.
    time: Instant,
}

impl PulledMessage {
    /// Creates a new `PulledMessage`.
    pub fn new(
        message: Arc<TopicMessage>,
        ack_id: AckId,
        deadline: AckDeadline,
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

    /// Gets the underlying message and consumes the pulled message.
    pub fn into_message(self) -> Arc<TopicMessage> {
        self.message
    }

    /// Gets the ack ID.
    pub fn ack_id(&self) -> AckId {
        self.ack_id
    }

    /// Gets the deadline.
    pub fn deadline(&self) -> &AckDeadline {
        &self.deadline
    }

    /// Creates an expiration key for this message.
    pub fn expiration_key(&self) -> (AckDeadline, AckId) {
        (*self.deadline(), self.ack_id)
    }

    /// Gets the delivery attempt.
    pub fn delivery_attempt(&self) -> u16 {
        self.delivery_attempt
    }

    /// Modifies the deadline of this message.
    pub fn modify_deadline(&mut self, new_deadline: AckDeadline) {
        self.deadline = new_deadline;
    }
}

impl AckDeadline {
    /// Creates a new `AckDeadline`.
    pub fn new(time: &Instant) -> Self {
        // Round up to nearest 100th millisecond
        static PRECISION_MICROS: u64 = 100_000;

        let duration_since_epoch = time.duration_since(*EPOCH);
        let time_in_micros = duration_since_epoch.as_micros() as u64;
        let rounded_in_micros = time_in_micros % PRECISION_MICROS;
        let rounded_time = EPOCH
            .checked_add(Duration::from_micros(time_in_micros + rounded_in_micros))
            .unwrap();
        Self { time: rounded_time }
    }

    /// Gets the time.
    pub fn time(&self) -> Instant {
        self.time
    }
}

impl From<AckDeadline> for Instant {
    fn from(value: AckDeadline) -> Self {
        value.time()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rounds() {
        let input = EPOCH.checked_add(Duration::from_millis(50)).unwrap();
        let expected = EPOCH.checked_add(Duration::from_millis(100)).unwrap();
        let actual = AckDeadline::new(&input);
        assert_eq!(actual.time(), expected);

        let input = EPOCH.checked_add(Duration::from_millis(150)).unwrap();
        let expected = EPOCH.checked_add(Duration::from_millis(200)).unwrap();
        let actual = AckDeadline::new(&input);
        assert_eq!(actual.time(), expected);
    }
}
