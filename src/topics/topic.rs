use crate::topics::topic_actor::{TopicActor, TopicRequest};
use crate::topics::TopicName;
use std::cmp::Ordering;
use tokio::sync::mpsc;

/// The `Topic` that we interact with.
pub struct Topic {
    /// Info about the topic, such as its' name.
    pub info: TopicInfo,

    /// The internal ID of the topic which is an auto-incrementing
    /// number.
    pub internal_id: u32,

    /// Handle for the actor, used for interacting with it.
    pub handle: TopicHandle,
}

// A handle for sending messages to a `Topic`.
// Cheap to clone.
#[derive(Clone)]
pub struct TopicHandle {
    /// The sender for the internal actor.
    pub sender: mpsc::Sender<TopicRequest>,
}

impl Topic {
    /// Creates a new `Topic`.
    pub fn new(info: TopicInfo, internal_id: u32) -> Self {
        let sender = TopicActor::start(internal_id);
        let topic = Self {
            info,
            internal_id,
            handle: TopicHandle::new(sender),
        };
        topic
    }
}

impl TopicHandle {
    /// Creates a new `TopicHandle`.
    pub fn new(sender: mpsc::Sender<TopicRequest>) -> Self {
        Self { sender }
    }

    /// Sends a request to the topic.
    pub fn send(&self, request: TopicRequest) {
        let sender = self.sender.clone();
        tokio::spawn(async move {
            let _ = sender.send(request).await;
        });
    }
}

/// Topics are considered equal when they have the same ID.
impl PartialEq<Self> for Topic {
    fn eq(&self, other: &Self) -> bool {
        self.internal_id == other.internal_id
    }
}

impl Eq for Topic {}

/// Topics are ordered by their internal ID.
impl PartialOrd<Self> for Topic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.internal_id.partial_cmp(&other.internal_id)
    }
}

impl Ord for Topic {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_id.cmp(&other.internal_id)
    }
}

/// Provides information about the topic.
#[derive(Clone)]
pub struct TopicInfo {
    /// The name of the topic.
    pub name: TopicName,
}

impl TopicInfo {
    /// Creates a new `TopicInfo`.
    pub fn new(name: TopicName) -> Self {
        Self { name }
    }
}
