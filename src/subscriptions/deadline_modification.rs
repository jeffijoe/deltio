use crate::subscriptions::{AckDeadline, AckId};

/// A requested modification to the deadline of an acknowledgement.
#[derive(Debug)]
pub struct DeadlineModification {
    pub ack_id: AckId,
    pub new_deadline: Option<AckDeadline>,
}

impl DeadlineModification {
    /// Creates a new `DeadlineModification`.
    pub fn new(ack_id: AckId, new_deadline: AckDeadline) -> Self {
        Self {
            ack_id,
            new_deadline: Some(new_deadline),
        }
    }

    /// Nack the message right away.
    pub fn nack(ack_id: AckId) -> Self {
        Self {
            ack_id,
            new_deadline: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nack() {
        let ack_id = AckId::new(1);
        assert_eq!(DeadlineModification::nack(ack_id).new_deadline, None);
    }
}
