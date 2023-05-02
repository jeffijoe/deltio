use crate::subscriptions::SubscriptionName;
use crate::topics::TopicName;

/// Stats for the subscription.
///
/// Provides insight into a subscription's health.
#[derive(Clone, Debug)]
pub struct SubscriptionStats {
    /// The subscription name.
    pub subscription_name: SubscriptionName,

    /// The topic that the subscription is attached to.
    pub topic_name: TopicName,

    /// The count of messages that are outstanding.
    pub outstanding_messages_count: usize,

    /// The count of messages that are in the backlog.
    pub backlog_messages_count: usize,
}

impl SubscriptionStats {
    /// Creates a new `SubscriptionStats`.
    pub fn new(
        subscription_name: SubscriptionName,
        topic_name: TopicName,
        outstanding_messages_count: usize,
        backlog_messages_count: usize,
    ) -> Self {
        Self {
            subscription_name,
            topic_name,
            outstanding_messages_count,
            backlog_messages_count,
        }
    }
}
