use crate::subscriptions::subscription_actor::{SubscriptionActor, SubscriptionRequest};
use crate::subscriptions::{PostMessagesError, PullMessagesError, PulledMessage, SubscriptionName};
use crate::topics::{Topic, TopicMessage};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::futures::Notified;
use tokio::sync::{mpsc, oneshot, Notify};

/// Represents a subscription.
pub struct Subscription {
    /// Info for the subscription, used mostly for reads.
    pub info: SubscriptionInfo,

    /// A reference to the attached topic.
    pub topic: Arc<Topic>,

    /// The internal subscription ID.
    pub internal_id: u32,

    /// The sender for the actor.
    sender: mpsc::Sender<SubscriptionRequest>,

    /// Notifies when there are new messages to pull.
    on_new_messages: Arc<Notify>,
}

/// Information about a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub name: SubscriptionName,
}

impl Subscription {
    /// Creates a new `Subscription`.
    pub fn new(info: SubscriptionInfo, internal_id: u32, topic: Arc<Topic>) -> Self {
        let on_new_messages = Arc::new(Notify::new());

        // Create the actor and pass in a callback for notifying on new messages.
        let sender = SubscriptionActor::start(topic.clone(), {
            let on_new_messages = on_new_messages.clone();
            move || {
                on_new_messages.notify_waiters();
            }
        });
        Self {
            info,
            topic,
            sender,
            internal_id,
            on_new_messages,
        }
    }

    /// Returns a signal for new messages.
    /// When new messages arrive, any waiters of the signal will be
    /// notified.
    pub fn new_messages_signal(&self) -> Notified<'_> {
        self.on_new_messages.notified()
    }

    /// Pulls messages from the subscription.
    pub async fn pull_messages(
        &self,
        max_count: u16,
    ) -> Result<Vec<PulledMessage>, PullMessagesError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::PullMessages {
                max_count,
                responder,
            })
            .await
            .map_err(|_| PullMessagesError::Closed)?;
        recv.await.map_err(|_| PullMessagesError::Closed)?
    }

    /// Posts new messages to the subscription.
    pub async fn post_messages(
        &self,
        new_messages: Vec<Arc<TopicMessage>>,
    ) -> Result<(), PostMessagesError> {
        self.sender
            .send(SubscriptionRequest::PostMessages {
                messages: new_messages,
            })
            .await
            .map_err(|_| PostMessagesError::Closed)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::{TopicInfo, TopicName};
    use bytes::Bytes;

    #[tokio::test]
    async fn pulling_messages() {
        let topic = Arc::new(Topic::new(
            TopicInfo::new(TopicName::new("cheezburger", "kittens")),
            1,
        ));
        let subscription = Arc::new(Subscription::new(
            SubscriptionInfo::new(SubscriptionName::new("cheezburger", "kitty_feed")),
            1,
            Arc::clone(&topic),
        ));

        // Attach the subscription.
        topic
            .attach_subscription(Arc::clone(&subscription))
            .await
            .unwrap();

        // Subscribe to the notifier.
        let notified = subscription.new_messages_signal();

        // Publish messages.
        topic
            .publish_messages(vec![
                TopicMessage::new(Bytes::from("meow")),
                TopicMessage::new(Bytes::from("can haz cheezburger?")),
            ])
            .await
            .unwrap();

        // Wait for the notification.
        tokio::select! {
            _ = notified => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timed out waiting for notify")
            }
        }

        // Pull messages.
        let pulled_messages = subscription.pull_messages(10).await.unwrap();
        assert_eq!(pulled_messages.len(), 2);

        assert_eq!(pulled_messages[0].message().data, Bytes::from("meow"));
        assert_eq!(
            pulled_messages[1].message().data,
            Bytes::from("can haz cheezburger?")
        );
    }
}
