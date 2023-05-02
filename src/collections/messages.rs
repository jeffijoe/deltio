use crate::topics::{MessageId, TopicMessage};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Maintains a list of messages that can be enumerated in insertion order
/// as well as looked up by ID.
pub struct Messages {
    /// The message list.
    ///
    /// This is implemented as a `VecDeque` because we will be
    /// iterating and popping from the front.
    pub list: VecDeque<Arc<TopicMessage>>,

    /// The map of messages.
    /// Used for retrieving a message by ID.
    /// TODO: Remove if this is not used.
    pub map: HashMap<MessageId, Arc<TopicMessage>>,
}

impl Messages {
    /// Creates a new `Messages`.
    pub fn new() -> Self {
        Self {
            list: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    /// Adds the given messages to the end of the list.
    pub fn append<I: IntoIterator<Item = Arc<TopicMessage>>>(&mut self, messages_iter: I) {
        let iter = messages_iter.into_iter();

        // Attempt to reserve the additional capacity needed.
        let size_hint = iter.size_hint().0;
        self.map.reserve(size_hint);
        self.list.reserve(size_hint);
        self.list.extend(iter.map(|m| {
            self.map.insert(m.id, Arc::clone(&m));
            m
        }));
    }

    /// Returns the amount of messages contained within
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Removes a message from the front of the queue.
    pub fn pop_front(&mut self) -> Option<Arc<TopicMessage>> {
        self.list.pop_front().map(|message| {
            // Also remove it from the map.
            self.map.remove(&message.id);
            message
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(messages.map.len(), 2);

        assert_eq!(messages.list[0].data[0], 0);
        assert_eq!(messages.list[1].data[0], 1);

        assert_eq!(messages.list[0].id, messages.map[&ids[0]].id);
        assert_eq!(messages.list[1].id, messages.map[&ids[1]].id);
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
