use crate::paging::Paging;
use crate::subscriptions::paging::SubscriptionsPage;
use crate::subscriptions::{Subscription, SubscriptionName};
use crate::topics::errors::*;
use crate::topics::topic_actor::{PublishMessagesResponse, TopicActor, TopicRequest};
use crate::topics::topic_manager::TopicManagerDelegate;
use crate::topics::{TopicMessage, TopicName};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// The `Topic` that we interact with.
/// Any mutable state is kept within the actor.
#[derive(Debug)]
pub struct Topic {
    /// Name of the topic.
    pub name: TopicName,

    /// The internal ID of the topic which is an auto-incrementing
    /// number.
    pub internal_id: u32,

    /// The topic actor's mailbox.
    sender: mpsc::Sender<TopicRequest>,
}

/// Provides information about the topic.
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// The name of the topic.
    pub name: TopicName,
}

impl Topic {
    /// Creates a new `Topic`.
    pub fn new(delegate: TopicManagerDelegate, info: TopicInfo, internal_id: u32) -> Self {
        let name = info.name.clone();
        let sender = TopicActor::start(delegate, info, internal_id);
        Self {
            name,
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

    /// Lists subscriptions in the topic.
    pub async fn list_subscriptions(
        &self,
        paging: Paging,
    ) -> Result<SubscriptionsPage, ListSubscriptionsError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::ListSubscriptions {
            paging,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| ListSubscriptionsError::Closed)?;
        recv.await.map_err(|_| ListSubscriptionsError::Closed)?
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

    /// Removes the subscription from the topic.
    /// Called after the subscription itself is deleted.
    pub async fn remove_subscription(
        &self,
        name: SubscriptionName,
    ) -> Result<(), RemoveSubscriptionError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::RemoveSubscription {
            name,
            responder: send,
        };
        self.sender
            .send(request)
            .await
            .map_err(|_| RemoveSubscriptionError::Closed)?;
        recv.await.map_err(|_| RemoveSubscriptionError::Closed)?
    }

    /// Deletes the topic.
    pub async fn delete(&self) -> Result<(), DeleteError> {
        let (send, recv) = oneshot::channel();
        let request = TopicRequest::Delete { responder: send };
        self.sender
            .send(request)
            .await
            .map_err(|_| DeleteError::Closed)?;
        recv.await.map_err(|_| DeleteError::Closed)?
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
