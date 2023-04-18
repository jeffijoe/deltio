use crate::subscriptions::Subscription;
use crate::topics::topic_actor::{PublishMessagesResponse, TopicActor, TopicRequest};
use crate::topics::{AttachSubscriptionError, PublishMessagesError, TopicMessage, TopicName};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// The `Topic` that we interact with.
/// Any mutable state is kept within the actor.
pub struct Topic {
    /// Info about the topic, such as its' name.
    pub info: TopicInfo,

    /// The internal ID of the topic which is an auto-incrementing
    /// number.
    pub internal_id: u32,

    /// The topic actor's mailbox.
    sender: mpsc::Sender<TopicRequest>,
}

/// Provides information about the topic.
#[derive(Clone)]
pub struct TopicInfo {
    /// The name of the topic.
    pub name: TopicName,
}

impl Topic {
    /// Creates a new `Topic`.
    pub fn new(info: TopicInfo, internal_id: u32) -> Self {
        let sender = TopicActor::start(internal_id);
        Self {
            info,
            internal_id,
            sender,
        }
    }

    /// Publishes the messages.
    pub async fn publish_messages(
        &self,
        messages: Vec<TopicMessage>,
    ) -> Result<PublishMessagesResponse, PublishMessagesError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::PublishMessages {
            messages,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| PublishMessagesError::Closed)?;
        recv.await.map_err(|_| PublishMessagesError::Closed)?
    }

    /// Attaches the subscription to the topic.
    pub async fn attach_subscription(
        &self,
        subscription: Arc<Subscription>,
    ) -> Result<(), AttachSubscriptionError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::AttachSubscription {
            subscription,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| AttachSubscriptionError::Closed)?;
        recv.await.map_err(|_| AttachSubscriptionError::Closed)?
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

impl TopicInfo {
    /// Creates a new `TopicInfo`.
    pub fn new(name: TopicName) -> Self {
        Self { name }
    }
}
