use crate::subscriptions::{AckDeadline, AckId, DeadlineModification, PulledMessage};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;
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

        // Before inserting into the expirations set, check whether this new key
        // is less than the first one. If it is, then we need to notify any polling listeners
        // that there is a new first expiration.
        let should_notify = self
            .expirations
            .first()
            .map(|e| &expiration_key < e)
            .unwrap_or(false);

        self.expirations.insert(expiration_key);

        if should_notify {
            self.notify.notify_waiters();
        }
    }

    /// Returns the next expiration.
    pub fn next_expiration(&self) -> Option<AckDeadline> {
        self.expirations.first().map(|s| s.0)
    }

    /// Takes all the messages from the tracker that have expired since or at the given [`time`].
    pub fn take_expired(&mut self, time: &Instant) -> Vec<PulledMessage> {
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

    /// Removes messages with the given ack IDs produced by the iterator.
    pub fn remove<I>(&mut self, ack_ids: I) -> Vec<PulledMessage>
    where
        I: Iterator<Item = AckId>,
    {
        let mut should_notify = false;
        let mut result = Vec::new();
        let first_expiration_key = self.expirations.first().cloned();
        for ack_id in ack_ids {
            if let Some(message) = self.messages.remove(&ack_id) {
                let expiration_key = message.expiration_key();
                if !should_notify {
                    // If we are removing a key that is equal
                    // to the first expiration key, then we need to notify waiters.
                    should_notify = first_expiration_key
                        .map(|e| expiration_key == e)
                        .unwrap_or(false)
                }

                self.expirations.remove(&expiration_key);
                result.push(message);
            }
        }

        if should_notify {
            self.notify.notify_waiters();
        }

        result
    }

    /// Given a list of modifications, removes and re-adds the ack IDs
    /// based on the new deadline. If there is no new deadline, then
    /// the message is NACK'ed. All NACK'ed messages will be returned.
    pub fn modify(&mut self, modifications: Vec<DeadlineModification>) -> Vec<PulledMessage> {
        let mut result = Vec::new();
        let current_first_expiration = self.expirations.first().cloned();
        let mut should_notify = false;
        for mut modification in modifications {
            // Remove the message since we'll need to modify it and add it back.
            if let Entry::Occupied(mut entry) = self.messages.entry(modification.ack_id) {
                // Check if this message was the upcoming expiration. If it was, we need to
                // notify waiters.
                let message = entry.get_mut();
                let current_expiration_key = message.expiration_key();
                if !should_notify
                    && current_first_expiration
                        .map(|e| current_expiration_key == e)
                        .unwrap_or(false)
                {
                    should_notify = true;
                }

                // Remove the existing expiration, if any.
                self.expirations.remove(&current_expiration_key);

                // If we are extending the deadline, modify the deadline on
                // the pulled message and add a new expiration entry.
                // If the new entry's expiration is earlier than the current
                // upcoming one, we need to notify.
                if let Some(new_deadline) = modification.new_deadline.take() {
                    message.modify_deadline(new_deadline);
                    let expiration_key = message.expiration_key();
                    if !should_notify
                        && current_first_expiration
                            .map(|e| e > expiration_key)
                            .unwrap_or(false)
                    {
                        should_notify = true;
                    }
                    println!("Extending dawg, should: {}", &should_notify);
                    self.expirations.insert(expiration_key);
                } else {
                    // The message is being NACK'ed, remove the entry from the tracker.
                    let message = entry.remove();
                    result.push(message);
                }
            }
        }

        if should_notify {
            self.notify.notify_waiters();
        }

        result
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

    /// Polls for the next batch of expired messages.
    pub async fn next_expired(&mut self) -> Option<Vec<PulledMessage>> {
        loop {
            let now = Instant::now();
            let taken = self.take_expired(&now);
            if !taken.is_empty() {
                return Some(taken);
            }

            let notified = self.notify.notified();
            if let Some(next_expiration) = self.next_expiration() {
                let when = next_expiration.time();
                tokio::select! {
                    _ = notified => {},
                    _ = tokio::time::sleep_until(when.into()) => {}
                }
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
    use lazy_static::lazy_static;
    use std::sync::Arc;
    use std::time::Duration;

    lazy_static! {
        static ref EPOCH: Instant = Instant::now();
    }

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

    #[test]
    fn remove_iter() {
        let mut tracker = OutstandingMessageTracker::new();
        tracker.add(new_pulled_message(3, 1)); // #1
        tracker.add(new_pulled_message(4, 2)); // #2
        tracker.add(new_pulled_message(1, 3)); // #3
        tracker.add(new_pulled_message(2, 3)); // #4

        // Remove all except #4
        tracker.remove(vec![AckId::new(1), AckId::new(3), AckId::new(4)].into_iter());

        // Verify the last one left is the one representing #4.
        assert_eq!(tracker.len(), 1);
        assert_eq!(tracker.next_expiration().unwrap(), deadline_for(3));
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
        AckDeadline::new(&EPOCH.checked_add(Duration::from_secs(time)).unwrap())
    }
}
