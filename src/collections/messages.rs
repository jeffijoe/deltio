use crate::topics::TopicMessage;
use std::collections::VecDeque;
use std::sync::Arc;

/// Maintains a list of messages that can be enumerated in insertion order.
pub struct Messages {
    /// The message list.
    ///
    /// This is implemented as a `VecDeque` because we will be
    /// iterating and popping from the front.
    pub list: VecDeque<Arc<TopicMessage>>,
}

impl Messages {
    /// Creates a new `Messages`.
    pub fn new() -> Self {
        Self {
            list: VecDeque::new(),
        }
    }

    /// Adds the given messages to the end of the list.
    pub fn append<I: IntoIterator<Item = Arc<TopicMessage>>>(&mut self, messages_iter: I) {
        let iter = messages_iter.into_iter();

        // Attempt to reserve the additional capacity needed.
        let size_hint = iter.size_hint().0;
        self.list.reserve(size_hint);
        self.list.extend(iter);
    }

    /// Returns the amount of messages contained within
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns true if there are no messages in the queue.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes a message from the front of the queue.
    pub fn pop_front(&mut self) -> Option<Arc<TopicMessage>> {
        self.list.pop_front()
    }

    /// Clears all the messages.
    pub fn clear(&mut self) {
        self.list.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::MessageId;

    #[test]
    fn append_adds_messages_to_list_and_map() {
        let mut messages = Messages::new();
        let ids = vec![MessageId::new(1, 1), MessageId::new(1, 2)];

        let iter = ids.iter().enumerate().map(|(i, id)| {
            let mut m = TopicMessage::new(vec![i as u8].into());
            m.publish(*id, std::time::SystemTime::now());
            Arc::new(m)
        });

        messages.append(iter);

        assert_eq!(messages.len(), 2);

        assert_eq!(messages.list.len(), 2);

        assert_eq!(messages.list[0].data[0], 0);
        assert_eq!(messages.list[1].data[0], 1);
    }

    #[test]
    fn pop_front() {
        let mut messages = Messages::new();

        let message1 = new_message(1);
        let message2 = new_message(2);

        messages.append([message1, message2]);

        assert_eq!(messages.len(), 2);

        let popped = messages.pop_front().unwrap();
        assert_eq!(popped.data[0], 1);
        assert_eq!(messages.len(), 1);

        let popped = messages.pop_front().unwrap();
        assert_eq!(popped.data[0], 2);
        assert_eq!(messages.len(), 0);

        assert!(messages.pop_front().is_none());
    }

    fn new_message(data_value: u8) -> Arc<TopicMessage> {
        let id = MessageId::new(1, rand::random());
        let mut message = TopicMessage::new(vec![data_value].into());
        message.publish(id, std::time::SystemTime::now());
        Arc::new(message)
    }
}
