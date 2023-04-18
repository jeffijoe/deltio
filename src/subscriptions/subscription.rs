use crate::subscriptions::subscription_actor::{SubscriptionActor, SubscriptionRequest};
use crate::subscriptions::SubscriptionName;
use crate::topics::Topic;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Represents a subscription.
pub struct Subscription {
    pub info: SubscriptionInfo,
    pub topic: Arc<Topic>,
    pub internal_id: u32,
    sender: mpsc::Sender<SubscriptionRequest>,
}

/// Information about a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub name: SubscriptionName,
}

impl Subscription {
    /// Creates a new `Subscription`.
    pub fn new(info: SubscriptionInfo, internal_id: u32, topic: Arc<Topic>) -> Self {
        let sender = SubscriptionActor::start(topic.clone());
        Self {
            info,
            topic,
            sender,
            internal_id,
        }
    }

    pub fn pull_messages(&self) {
        let _ = self.sender;
        todo!()
    }
}

/// Subscriptions are considered equal when they have the same ID.
impl PartialEq<Self> for Subscription {
    fn eq(&self, other: &Self) -> bool {
        self.internal_id == other.internal_id
    }
}

impl Eq for Subscription {}

/// Subscriptions are ordered by their internal ID.
impl PartialOrd<Self> for Subscription {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.internal_id.partial_cmp(&other.internal_id)
    }
}

impl Ord for Subscription {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_id.cmp(&other.internal_id)
    }
}

impl SubscriptionInfo {
    /// Creates a new `SubscriptionInfo`.
    pub fn new(name: SubscriptionName) -> Self {
        Self { name }
    }
}
