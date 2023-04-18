use crate::topics::Topic;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Requests for the `SubscriptionActor`.
pub enum SubscriptionRequest {}

/// Actor for the subscription.
pub struct SubscriptionActor {
    topic: Arc<Topic>,
}

impl SubscriptionActor {
    /// Starts the actor.
    pub fn start(topic: Arc<Topic>) -> mpsc::Sender<SubscriptionRequest> {
        let (sender, mut receiver) = mpsc::channel(2048);
        let actor = Self { topic };

        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                actor.receive(request).await;
            }
        });

        sender
    }

    /// Receives a request.
    async fn receive(&self, _request: SubscriptionRequest) {
        let _ = self.topic;
        todo!()
    }
}
