use crate::subscriptions::{AckDeadline, AckId, PulledMessage};
use std::collections::{BTreeSet, HashMap};
use std::time::SystemTime;
use tokio::sync::Notify;

/// Keeps track of pulled messages and their deadlines.
pub(crate) struct OutstandingMessageTracker {
    /// A map of Ack IDs to the pulled message.
    messages: HashMap<AckId, PulledMessage>,

    /// A sorted set of expirations. Used for scheduling NACKs.
    ///
    /// Since we want a min-heap (popping the lowest value first), we
    /// need to wrap the items in `Reverse`.
    expirations: BTreeSet<(AckDeadline, AckId)>,

    /// Notify when a new, earlier deadline has been inserted.
    /// Used by [`poll_next`].
    notify: Notify,
}

impl OutstandingMessageTracker {
    /// Creates a new `OutstandingMessageTracker`.
    pub fn new() -> Self {
        Self {
            messages: HashMap::new(),
            expirations: BTreeSet::new(),
            notify: Notify::new(),
        }
    }

    /// Adds the given message to the tracker.
    pub fn add(&mut self, message: PulledMessage) {
        let expiration_key = (*message.deadline(), message.ack_id());
        self.messages.insert(message.ack_id(), message);
        self.expirations.insert(expiration_key);
        self.notify.notify_waiters();
    }

    /// Returns the next expiration.
    #[allow(dead_code)]
    pub fn next_expiration(&self) -> Option<AckDeadline> {
        self.expirations.first().map(|s| s.0)
    }

    /// Takes all the messages from the tracker that have expired since or at the given [`time`].
    #[allow(dead_code)]
    pub fn take_expired(&mut self, time: &SystemTime) -> Vec<PulledMessage> {
        let mut result = Vec::new();
        while let Some(key) = self.expirations.first() {
            // If the time has not elapsed, then we are done since the expirations are ordered.
            if time < &key.0.time() {
                return result;
            }

            // Take the expiration.
            // SAFETY: We know it exists because we just tested it above.
            let (_, ack_id) = unsafe { self.expirations.pop_first().unwrap_unchecked() };

            // Remove the message from the map.
            // SAFETY: We know the message exists because we track both side by side.
            let message = unsafe { self.messages.remove(&ack_id).unwrap_unchecked() };

            // Add the message to the result.
            result.push(message);
        }

        result
    }

    /// Removes the message with the given ack ID.
    pub fn remove(&mut self, ack_id: &AckId) -> Option<PulledMessage> {
        if let Some(message) = self.messages.remove(ack_id) {
            let expiration_key = (*message.deadline(), message.ack_id());
            self.expirations.remove(&expiration_key);
            self.notify.notify_waiters();
            return Some(message);
        }

        None
    }

    /// Clears the contents of the tracker.
    pub fn clear(&mut self) {
        self.expirations.clear();
        self.messages.clear();
        self.notify.notify_waiters();
    }

    /// Returns the amount of messages outstanding.
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    /// Polls for the next batch of expired deadlines.
    pub async fn poll_next(&mut self) -> Option<Vec<PulledMessage>> {
        loop {
            let now = SystemTime::now();
            let taken = self.take_expired(&now);
            if !taken.is_empty() {
                return Some(taken);
            }

            let notified = self.notify.notified();
            if let Some(next_expiration) = self.next_expiration() {
                let when = std::time::Instant::now();
                // TODO: Use actual expiration.
                tokio::select! {
                    _ = notified => {},
                    _ = tokio::time::sleep_until(when.into()) => {}
                };
            } else {
                notified.await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::TopicMessage;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn take_expired() {
        let mut tracker = OutstandingMessageTracker::new();
        tracker.add(new_pulled_message(3, 1)); // #1
        tracker.add(new_pulled_message(4, 2)); // #2
        tracker.add(new_pulled_message(1, 3)); // #3
        tracker.add(new_pulled_message(2, 3)); // #4

        assert_eq!(tracker.messages.len(), 4);
        assert_eq!(tracker.expirations.len(), 4);

        let next = tracker.next_expiration().unwrap();
        assert_eq!(next, deadline_for(1));

        // Take out messages that have expired at 1.
        let expired = tracker.take_expired(&deadline_for(1).time());
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].ack_id(), AckId::new(3));
        assert_eq!(tracker.messages.len(), 3);
        assert_eq!(tracker.expirations.len(), 3);

        // Take out messages that have expired at 2.
        let expired = tracker.take_expired(&deadline_for(2).time());
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].ack_id(), AckId::new(4));
        assert_eq!(tracker.messages.len(), 2);
        assert_eq!(tracker.expirations.len(), 2);

        // Take out messages that have expired at 3.
        let expired = tracker.take_expired(&deadline_for(3).time());
        assert_eq!(expired.len(), 2);
        assert_eq!(expired[0].ack_id(), AckId::new(1));
        assert_eq!(expired[1].ack_id(), AckId::new(2));
        assert_eq!(tracker.messages.len(), 0);
        assert_eq!(tracker.expirations.len(), 0);

        // Verify there is nothing left.
        let expired = tracker.take_expired(&deadline_for(1).time());
        assert_eq!(expired.len(), 0);
    }

    /// Helper for creating a pulled message.
    fn new_pulled_message(ack_id: u64, time: u64) -> PulledMessage {
        let message = Arc::new(TopicMessage::new(Bytes::from("hello")));
        let ack_id = AckId::new(ack_id);
        let deadline = deadline_for(time);
        let delivery_attempt = 1;
        PulledMessage::new(message, ack_id, deadline, delivery_attempt)
    }

    fn deadline_for(time: u64) -> AckDeadline {
        AckDeadline::new(&UNIX_EPOCH.checked_add(Duration::from_secs(time)).unwrap())
    }
}
