use crate::subscriptions::{Subscription, SubscriptionName};
use crate::topics::topic_message::{MessageId, TopicMessage};
use crate::topics::{AttachSubscriptionError, PublishMessagesError};
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
                let result = self.publish_messages(messages);
                let _ = responder.send(result);
            }

            TopicRequest::AttachSubscription {
                subscription,
                responder,
            } => {
                let result = self.attach_subscription(subscription);
                let _ = responder.send(result);
            }
        }
    }

    fn publish_messages(
        &mut self,
        messages: Vec<TopicMessage>,
    ) -> Result<PublishMessagesResponse, PublishMessagesError> {
        // Define the publish time as now.
        let publish_time = SystemTime::now();

        // We'll need to return the published message IDs.
        let mut message_ids = Vec::with_capacity(messages.len());

        // Ensure we have capacity.
        self.messages.reserve(messages.len());

        // Mark the messages as published and add them to the topic.
        self.messages.extend(messages.into_iter().map(|mut m| {
            self.next_message_id += 1;
            let message_id = MessageId::new(self.topic_internal_id, self.next_message_id);
            m.publish(message_id, publish_time);
            message_ids.push(message_id);

            Arc::new(m)
        }));

        Ok(PublishMessagesResponse { message_ids })
    }

    fn attach_subscription(
        &mut self,
        subscription: Arc<Subscription>,
    ) -> Result<(), AttachSubscriptionError> {
        let name = &subscription.info.name;
        if self.subscriptions.contains_key(&name) {
            return Ok(());
        }

        self.subscriptions.insert(name.clone(), subscription);
        Ok(())
    }
}
