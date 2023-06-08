use crate::push::PushSubscriptionsRegistry;
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::subscriptions::{
    DeadlineModification, GetSubscriptionError, PullMessagesError, PulledMessage, PushConfig,
    Subscription,
};
use std::collections::HashMap;

use crate::topics::TopicMessage;
use base64::Engine;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Contains everything needed to run the push loop.
pub struct PushLoop {
    interval: Duration,
    subscription_manager: Arc<SubscriptionManager>,
    push_registry: PushSubscriptionsRegistry,
}

impl PushLoop {
    /// Creates a new push loop.
    pub fn new(
        interval: Duration,
        subscription_manager: Arc<SubscriptionManager>,
        push_registry: PushSubscriptionsRegistry,
    ) -> Self {
        Self {
            interval,
            subscription_manager,
            push_registry,
        }
    }

    /// Consumes the `PushLoop` by running it.
    pub async fn run(self) {
        run(self.interval, self.subscription_manager, self.push_registry).await
    }
}

/// Runs the global push subscription loop.
pub async fn run(
    interval: Duration,
    subscription_manager: Arc<SubscriptionManager>,
    push_registry: PushSubscriptionsRegistry,
) {
    // This is a pretty crude implementation with little to no resilience in
    // terms of dealing with push endpoint issues. Additionally, there will be no
    // mercy towards slow endpoints.
    // In the future, an alternative implementation could be running a push
    // loop per subscription and only send a few messages at a time.
    log::trace!("Starting push loop (interval = {:?})", &interval);

    // The HTTP client to use.
    let client = reqwest::Client::new();

    loop {
        let entries = push_registry.entries();
        if !entries.is_empty() {
            for (name, push_config) in entries {
                let subscription = match subscription_manager.get_subscription(&name) {
                    Ok(s) => s,
                    // The subscription was likely deleted and haven't been cleaned up
                    // yet. We can safely ignore it.
                    Err(GetSubscriptionError::Closed | GetSubscriptionError::DoesNotExist) => {
                        continue
                    }
                };

                tokio::spawn(pull_and_dispatch_messages(
                    subscription,
                    push_config,
                    client.clone(),
                ));
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Pushes and dispatches messages for the given subscription and push config.
async fn pull_and_dispatch_messages(
    subscription: Arc<Subscription>,
    push_config: PushConfig,
    client: reqwest::Client,
) {
    // Pull the subscription.
    let deleted_signal = subscription.deleted();
    let fut = async move {
        let page = match subscription.pull_messages(1_000).await {
            Ok(page) => page,
            Err(PullMessagesError::Closed) => return,
        };

        let mut join_set = tokio::task::JoinSet::new();
        for pulled_message in page {
            // We'll share the dispatch future so we can ensure it gets polled to completion
            // inside the join set, but we are also able to use it
            // on the outside to wait for either the dispatch to complete, or a short sleep
            // whichever is faster. The idea is that if the dispatch is faster than the sleep,
            // then we don't need to wait for that entire duration.
            let dispatch_fut = dispatch_message(
                Arc::clone(&subscription),
                pulled_message,
                push_config.clone(),
                client.clone(),
            )
            .shared();
            join_set.spawn(dispatch_fut.clone());

            // Wait a bit to increase the likelihood of delivering
            // the messages in order.
            tokio::select!(
                _ = dispatch_fut => {},
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            )
        }

        // Join all the tasks.
        while (join_set.join_next().await).is_some() {}
    };

    // Wait until the push is done or until the subscription is deleted.
    tokio::select! {
        _ = deleted_signal => {},
        _ = fut => {}
    }
}

/// Dispatches the message to the push endpoint and ACK/NACks it accordingly.
async fn dispatch_message(
    subscription: Arc<Subscription>,
    pulled_message: PulledMessage,
    push_config: PushConfig,
    client: reqwest::Client,
) {
    let message = pulled_message.message();
    log::trace!(
        "{}: dispatching push message to {}",
        &subscription.name,
        &push_config.endpoint
    );
    let result = client
        .request(reqwest::Method::POST, &push_config.endpoint)
        .header("Content-Type", "application/json;charset=utf8")
        .body(encode_message_payload(&subscription, message))
        .send()
        .await;
    let success = match result {
        Ok(response) => {
            let status: u16 = response.status().into();

            if !matches!(status, 102 | 200 | 201 | 202 | 204) {
                log::error!(
                    "{}: failed pushing to endpoint '{}': the endpoint returned a non-successful status code {}",
                    &subscription.name,
                    &push_config.endpoint,
                    status
                );
                false
            } else {
                true
            }
        }
        Err(send_error) => {
            log::error!(
                "{}: failed pushing to endpoint '{}': {}",
                &subscription.name,
                &push_config.endpoint,
                send_error
            );
            false
        }
    };

    if !success {
        let _ = subscription
            .modify_ack_deadlines(vec![DeadlineModification::nack(pulled_message.ack_id())])
            .await;
    } else {
        let _ = subscription
            .acknowledge_messages(vec![pulled_message.ack_id()])
            .await;
    }
}

/// The payload in JSON form being sent in the request.
#[derive(Serialize, Deserialize)]
pub struct PushPayload {
    pub message: PushPayloadMessage,
    pub subscription: String,
}

/// The message in JSON form.
///
/// The `_dupe` fields are to support both casings, as per
/// https://cloud.google.com/pubsub/docs/push#receive_push
#[derive(Serialize, Deserialize)]
pub struct PushPayloadMessage {
    pub attributes: HashMap<String, String>,
    pub data: String,
    pub message_id: String,
    pub publish_time: String,
    #[serde(rename = "messageId")]
    pub message_id_dupe: String,
    #[serde(rename = "publishTime")]
    pub publish_time_dupe: String,
}

/// Encodes the message payload to JSON.
fn encode_message_payload(subscription: &Arc<Subscription>, message: &Arc<TopicMessage>) -> String {
    // TODO: Format publish time correctly. chrono/time crate?
    let encoded_data = base64::engine::general_purpose::STANDARD.encode(message.data.clone());

    let payload = PushPayload {
        subscription: subscription.name.to_string(),
        message: PushPayloadMessage {
            data: encoded_data,
            message_id: message.id.to_string(),
            message_id_dupe: message.id.to_string(),
            attributes: HashMap::default(),
            publish_time: "2000-01-01T12:00:00Z".to_string(),
            publish_time_dupe: "2000-01-01T12:00:00Z".to_string(),
        },
    };

    serde_json::to_string(&payload).unwrap()
}
