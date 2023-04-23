use crate::collections::Messages;
use crate::subscriptions::{PullMessagesError, PulledMessage};
use crate::topics::{Topic, TopicMessage};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};

/// The max amount of messages that can be pulled.
const MAX_PULL_COUNT: u16 = 1_000;

/// Requests for the `SubscriptionActor`.
pub enum SubscriptionRequest {
    PostMessages {
        messages: Vec<Arc<TopicMessage>>,
    },
    // TODO: Maybe we can optimize this by passing in a buffer, but I am not
    //  very hopeful. Maybe if we pass it in and send it back, thereby doing a round-trip
    //  for the ownership?
    PullMessages {
        max_count: u16,
        responder: oneshot::Sender<Result<Vec<PulledMessage>, PullMessagesError>>,
    },
}

/// Actor for the subscription.
pub struct SubscriptionActor<S> {
    /// The topic that the subscription is attached to.
    #[allow(dead_code)]
    topic: Arc<Topic>,

    /// A list of messages that are to be pulled.
    outgoing: Messages,

    /// A signal that notifies of new messages having been posted.
    signal_new_messages: S,

    /// The next ID to use as the ACK ID for a pulled message.
    next_ack_id: u64,
}

impl<S> SubscriptionActor<S>
where
    S: Fn() + Send + 'static,
{
    /// Starts the actor.
    pub fn start(topic: Arc<Topic>, signal_new_messages: S) -> mpsc::Sender<SubscriptionRequest> {
        let (sender, mut receiver) = mpsc::channel(2048);
        let mut actor = Self {
            topic,
            signal_new_messages,
            outgoing: Messages::new(),
            next_ack_id: 1,
        };

        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                actor.receive(request).await;
            }
        });

        sender
    }

    /// Receives a request.
    async fn receive(&mut self, request: SubscriptionRequest) {
        match request {
            SubscriptionRequest::PostMessages { messages } => {
                self.post_messages(messages).await;
            }
            SubscriptionRequest::PullMessages {
                max_count,
                responder,
            } => {
                let result = self.pull_messages(max_count).await;
                let _ = responder.send(result);
            }
        }
    }

    /// Posts new messages to the subscription.
    async fn post_messages(&mut self, new_messages: Vec<Arc<TopicMessage>>) {
        self.outgoing.append(new_messages);
        (self.signal_new_messages)();
    }

    /// Pulls messages from the subscription, marking them as outstanding so they won't be
    /// delivered to anyone else.
    async fn pull_messages(
        &mut self,
        max_count: u16,
    ) -> Result<Vec<PulledMessage>, PullMessagesError> {
        let outgoing_len = self.outgoing.len() as u16;
        let capacity = max_count.clamp(0, outgoing_len.max(MAX_PULL_COUNT)) as usize;
        let mut result = Vec::with_capacity(capacity);

        while let Some(message) = self.outgoing.pop_front() {
            let ack_id = self.next_ack_id;
            self.next_ack_id += 1;

            // TODO: Compute based on subscription message ack deadline.
            let deadline = SystemTime::now() + Duration::from_secs(10);
            let pulled_message = PulledMessage::new(Arc::clone(&message), ack_id, deadline);
            result.push(pulled_message);

            if result.len() >= capacity {
                break;
            }
        }

        Ok(result)
    }
}
