use crate::subscriptions::{PostMessagesError, Subscription, SubscriptionName};
use crate::topics::topic_message::{MessageId, TopicMessage};
use crate::topics::{AttachSubscriptionError, PublishMessagesError, RemoveSubscriptionError};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};

/// Requests for the `TopicActor`.
pub enum TopicRequest {
    PublishMessages {
        messages: Vec<TopicMessage>,
        responder: oneshot::Sender<Result<PublishMessagesResponse, PublishMessagesError>>,
    },

    AttachSubscription {
        subscription: Arc<Subscription>,
        responder: oneshot::Sender<Result<(), AttachSubscriptionError>>,
    },

    RemoveSubscription {
        name: SubscriptionName,
        responder: oneshot::Sender<Result<(), RemoveSubscriptionError>>,
    },
}

/// Response for publishing messages.
pub struct PublishMessagesResponse {
    /// The message ids.
    pub message_ids: Vec<MessageId>,
}

/// Manages the topic.
pub struct TopicActor {
    /// The messages that have been published to the topic.
    /// Since messages can be big, they are passed around as references.
    messages: Vec<Arc<TopicMessage>>,

    /// The list of attached subscriptions for the topic.
    subscriptions: HashMap<SubscriptionName, Arc<Subscription>>,

    // The internal ID of the topic.
    topic_internal_id: u32,

    // Used to generate message IDs.
    next_message_id: u32,
}

impl TopicActor {
    pub fn start(topic_internal_id: u32) -> mpsc::Sender<TopicRequest> {
        let (sender, mut receiver) = mpsc::channel(2048);
        let mut actor = Self {
            topic_internal_id,
            messages: Default::default(),
            next_message_id: 0,
            subscriptions: Default::default(),
        };

        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                actor.receive(request).await;
            }
        });

        sender
    }

    async fn receive(&mut self, request: TopicRequest) {
        match request {
            TopicRequest::PublishMessages {
                messages,
                responder,
            } => {
                let result = self.publish_messages(messages).await;
                let _ = responder.send(result);
            }

            TopicRequest::AttachSubscription {
                subscription,
                responder,
            } => {
                let result = self.attach_subscription(subscription);
                let _ = responder.send(result);
            }

            TopicRequest::RemoveSubscription { name, responder } => {
                let result = self.remove_subscription(name);
                let _ = responder.send(result);
            }
        }
    }

    async fn publish_messages(
        &mut self,
        messages: Vec<TopicMessage>,
    ) -> Result<PublishMessagesResponse, PublishMessagesError> {
        // Define the publish time as now.
        let publish_time = SystemTime::now();

        // We'll need to return the published message IDs.
        let mut message_ids = Vec::with_capacity(messages.len());

        // Ensure we have capacity.
        self.messages.reserve(messages.len());

        // Mark the messages as published.
        let messages = messages
            .into_iter()
            .map(|mut m| {
                self.next_message_id += 1;
                let message_id = MessageId::new(self.topic_internal_id, self.next_message_id);
                m.publish(message_id, publish_time);
                message_ids.push(message_id);

                Arc::new(m)
            })
            .collect::<Vec<_>>();

        // Add them to the topic.
        self.messages.extend(messages.iter().map(Arc::clone));

        // Post them to all subscriptions.
        let mut set = tokio::task::JoinSet::new();
        for subscription in self.subscriptions.values() {
            // Spawn a future to post messages to each subscription.
            let subscription = Arc::clone(subscription);
            set.spawn({
                // It's unfortunate that we need to clone the vec here, but since it contains
                // references only it should be ok.
                let messages = messages.clone();

                // This moves the clones into the future so the borrow checker doesn't yell at us.
                async move { subscription.post_messages(messages).await }
            });
        }

        // Wait for all the tasks to complete.
        while let Some(task) = set.join_next().await {
            // Handle any errors at the Tokio level.
            let result = task.unwrap_or(Err(PostMessagesError::Closed));

            // Handle errors from posting the messages.
            result.map_err(|e| match e {
                PostMessagesError::Closed => PublishMessagesError::Closed,
            })?;
        }

        // Return the list of message IDs that we published.
        Ok(PublishMessagesResponse { message_ids })
    }

    fn attach_subscription(
        &mut self,
        subscription: Arc<Subscription>,
    ) -> Result<(), AttachSubscriptionError> {
        // Insert the subscription.
        if let Entry::Vacant(entry) = self.subscriptions.entry(subscription.info.name.clone()) {
            entry.insert(subscription);
        }

        Ok(())
    }

    fn remove_subscription(
        &mut self,
        name: SubscriptionName,
    ) -> Result<(), RemoveSubscriptionError> {
        // Remove the subscription. This is called from the `Subscription` itself.
        self.subscriptions.remove(&name);
        Ok(())
    }
}
