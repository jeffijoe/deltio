use crate::topics::topic_message::{MessageId, TopicMessage};
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};

/// Requests for the `TopicActor`.
pub enum TopicRequest {
    PublishMessages {
        messages: Vec<TopicMessage>,
        responder: oneshot::Sender<Result<PublishMessagesResponse, PublishMessagesError>>,
    },
}

/// Request for publishing messages.
pub struct PublishMessagesRequest {
    messages: Vec<TopicMessage>,
    responder: oneshot::Sender<Result<PublishMessagesResponse, PublishMessagesError>>,
}

/// Errors for publishing messages.
#[derive(thiserror::Error, Debug)]
pub enum PublishMessagesError {
    #[error("The topic does not exist")]
    TopicDoesNotExist,

    #[error("The topic is closed")]
    Closed,
}

/// Response for publishing messages.
pub struct PublishMessagesResponse {
    /// The message ids.
    pub message_ids: Vec<MessageId>,
}

/// Manages the topic.
pub struct TopicActor {
    /// The messages that have been published to the topic.
    messages: Vec<TopicMessage>,

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
            messages: Vec::default(),
            next_message_id: 0,
        };
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                actor.receive(request)
            }
        });

        sender
    }

    fn receive(&mut self, request: TopicRequest) {
        match request {
            TopicRequest::PublishMessages {
                messages,
                responder,
            } => {
                let result = self.publish_messages(messages);
                let _ = responder.send(result);
            }
        }
    }

    fn publish_messages(
        &mut self,
        mut messages: Vec<TopicMessage>,
    ) -> Result<PublishMessagesResponse, PublishMessagesError> {
        // Define the publish time as now.
        let publish_time = SystemTime::now();

        // We'll need to return the published message IDs.
        let mut message_ids = Vec::with_capacity(messages.len());

        // Mark the messages as published.
        for m in messages.iter_mut() {
            self.next_message_id += 1;
            let message_id = MessageId::new(self.topic_internal_id, self.next_message_id);
            m.publish(message_id, publish_time);
            message_ids.push(message_id);
        }

        self.messages.append(&mut messages);
        Ok(PublishMessagesResponse { message_ids })
    }
}
