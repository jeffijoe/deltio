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
