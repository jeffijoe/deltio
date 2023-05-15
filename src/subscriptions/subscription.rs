use crate::subscriptions::errors::*;
use crate::subscriptions::futures::{Deleted, MessagesAvailable};
use crate::subscriptions::subscription_actor::{
    SubscriptionActor, SubscriptionObserver, SubscriptionRequest,
};
use crate::subscriptions::subscription_manager::SubscriptionManagerDelegate;
use crate::subscriptions::{
    AckId, DeadlineModification, PulledMessage, SubscriptionName, SubscriptionStats,
};
use crate::topics::{Topic, TopicMessage};
use std::cmp::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Represents a subscription.
pub struct Subscription {
    /// Name of the subscription, used mostly for reads.
    pub name: SubscriptionName,

    /// A reference to the attached topic.
    pub topic: Weak<Topic>,

    /// The internal subscription ID.
    pub internal_id: u32,

    /// The sender for the actor.
    sender: mpsc::Sender<SubscriptionRequest>,

    /// Used by the actor to notify of interesting events.
    observer: Arc<SubscriptionObserver>,
}

/// Information about a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    /// The subscription name.
    pub name: SubscriptionName,

    /// The ACK deadline duration (if not specified in pull call).
    pub ack_deadline: Duration,
}

impl Subscription {
    /// Creates a new `Subscription`.
    pub fn new(
        info: SubscriptionInfo,
        internal_id: u32,
        topic: Arc<Topic>,
        delegate: SubscriptionManagerDelegate,
    ) -> Self {
        let observer = Arc::new(SubscriptionObserver::new());

        // Create the actor, pass in the observer.
        let name = info.name.clone();
        let sender = SubscriptionActor::start(info, topic.clone(), Arc::clone(&observer), delegate);
        let topic = Arc::downgrade(&topic);
        Self {
            name,
            topic,
            sender,
            internal_id,
            observer,
        }
    }

    /// Returns a signal for new messages.
    /// When new messages arrive, any waiters of the signal will be
    /// notified. The signal will be subscribed to immediately, so the time at which
    /// this method is called is important.
    pub fn messages_available(&self) -> MessagesAvailable {
        self.observer.new_messages_available()
    }

    /// Returns a signal for when the subscription gets deleted.
    pub fn deleted(&self) -> Deleted {
        self.observer.deleted()
    }

    /// Returns the info for the subscription.
    pub async fn get_info(&self) -> Result<SubscriptionInfo, GetInfoError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::GetInfo { responder })
            .await
            .map_err(|_| GetInfoError::Closed)?;
        recv.await.map_err(|_| GetInfoError::Closed)?
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
    ///
    /// This does not wait for the message to be processed.
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

    /// Deletes the subscription.
    pub async fn delete(&self) -> Result<(), DeleteError> {
        let (responder, recv) = oneshot::channel();
        self.sender
            .send(SubscriptionRequest::Delete { responder })
            .await
            .map_err(|_| DeleteError::Closed)?;
        recv.await.map_err(|_| DeleteError::Closed)?
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
    pub fn new(name: SubscriptionName, ack_deadline: Duration) -> Self {
        Self { name, ack_deadline }
    }

    /// Creates a new `SubscriptionInfo` with default values.
    pub fn new_with_defaults(name: SubscriptionName) -> Self {
        Self {
            name,
            ack_deadline: Duration::from_secs(10),
        }
    }
}
