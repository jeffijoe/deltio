use crate::subscriptions::subscription_actor::{SubscriptionActor, SubscriptionRequest};
use crate::subscriptions::{
    AckId, AcknowledgeMessagesError, DeadlineModification, GetStatsError, ModifyDeadlineError,
    PostMessagesError, PullMessagesError, PulledMessage, SubscriptionName, SubscriptionStats,
};
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
        let sender = SubscriptionActor::start(info.clone(), topic.clone(), {
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
    /// notified. The signal will be subscribed to immediately, so the time at which
    /// this method is called is important.
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

    /// Acknowledges messages.
    pub async fn acknowledge_messages(
        &self,
        ack_ids: Vec<AckId>,
    ) -> Result<(), AcknowledgeMessagesError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::AcknowledgeMessages { ack_ids, responder })
            .await
            .map_err(|_| AcknowledgeMessagesError::Closed)?;
        recv.await.map_err(|_| AcknowledgeMessagesError::Closed)?
    }

    /// Modify the acknowledgment deadlines.
    pub async fn modify_ack_deadlines(
        &self,
        deadline_modifications: Vec<DeadlineModification>,
    ) -> Result<(), ModifyDeadlineError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::ModifyDeadline {
                deadline_modifications,
                responder,
            })
            .await
            .map_err(|_| ModifyDeadlineError::Closed)?;
        recv.await.map_err(|_| ModifyDeadlineError::Closed)?
    }

    /// Gets stats for the subscription.
    pub async fn get_stats(&self) -> Result<SubscriptionStats, GetStatsError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::GetStats { responder })
            .await
            .map_err(|_| GetStatsError::Closed)?;
        recv.await.map_err(|_| GetStatsError::Closed)?
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
    use uuid::Uuid;

    #[tokio::test]
    async fn pulling_messages() {
        let (topic, subscription) = new_topic_and_subscription().await;

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
        wait_for_notification(notified).await;

        // Pull messages.
        let pulled_messages = subscription.pull_messages(10).await.unwrap();
        assert_eq!(pulled_messages.len(), 2);

        assert_eq!(pulled_messages[0].message().data, Bytes::from("meow"));
        assert_eq!(
            pulled_messages[1].message().data,
            Bytes::from("can haz cheezburger?")
        );

        // Pull again, the messages should not be returned again as they are outstanding.
        assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);

        // Check the stats to verify that the messages are outstanding.
        let stats = subscription.get_stats().await.unwrap();
        assert_eq!(stats.outstanding_messages_count, 2);

        // Acknowledge the messages.
        subscription
            .acknowledge_messages(pulled_messages.iter().map(|m| m.ack_id()).collect())
            .await
            .unwrap();

        // Verify that pulling still does not return anything.
        assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);

        // Verify that there are no more outstanding messages.
        let stats = subscription.get_stats().await.unwrap();
        assert_eq!(stats.outstanding_messages_count, 0);
    }

    #[tokio::test]
    async fn nack_messages() {
        let (topic, subscription) = new_topic_and_subscription().await;

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
        wait_for_notification(notified).await;

        // Pull messages.
        let pulled_messages = subscription.pull_messages(10).await.unwrap();
        assert_eq!(pulled_messages.len(), 2);

        // Check the stats to verify that the messages are outstanding.
        let stats = subscription.get_stats().await.unwrap();
        assert_eq!(stats.outstanding_messages_count, 2);

        // Set up a notification for nack'ing.
        let notified = subscription.new_messages_signal();

        // NACK the 2nd message.
        subscription
            .modify_ack_deadlines(vec![DeadlineModification::nack(
                pulled_messages[1].ack_id(),
            )])
            .await
            .unwrap();

        // Verify that nack'ing triggers the notification.
        wait_for_notification(notified).await;

        // Verify that after NACK'ing the message is not considered outstanding (as it was
        // returned to the backlog).
        let stats = subscription.get_stats().await.unwrap();
        assert_eq!(stats.outstanding_messages_count, 1);
        assert_eq!(stats.backlog_messages_count, 1);

        // Verify that pulling returns the NACK'ed message.
        let pulled_messages = subscription.pull_messages(10).await.unwrap();
        assert_eq!(pulled_messages.len(), 1);
        assert_eq!(
            pulled_messages[0].message().data,
            Bytes::from("can haz cheezburger?")
        );

        // Verify that after pulling again, the message is outstanding once more.
        let stats = subscription.get_stats().await.unwrap();
        assert_eq!(stats.outstanding_messages_count, 2);
        assert_eq!(stats.backlog_messages_count, 0);
    }

    async fn new_topic_and_subscription() -> (Arc<Topic>, Arc<Subscription>) {
        let project_id = Uuid::new_v4().to_string();
        let topic_id = Uuid::new_v4().to_string();
        let sub_id = Uuid::new_v4().to_string();
        let topic = Arc::new(Topic::new(
            TopicInfo::new(TopicName::new(&project_id, &topic_id)),
            1,
        ));
        let subscription = Arc::new(Subscription::new(
            SubscriptionInfo::new(SubscriptionName::new(&project_id, &sub_id)),
            1,
            Arc::clone(&topic),
        ));

        // Attach the subscription.
        topic
            .attach_subscription(Arc::clone(&subscription))
            .await
            .unwrap();

        (topic, subscription)
    }

    async fn wait_for_notification(notified: Notified<'_>) {
        tokio::select! {
            _ = notified => {},
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                panic!("Timed out waiting for notify")
            }
        }
    }
}
