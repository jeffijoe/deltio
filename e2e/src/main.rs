use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

async fn run() -> Result<()> {
    std::env::set_var("PUBSUB_EMULATOR_HOST", "localhost:8085");
    // Create pubsub client.
    let config = ClientConfig::default();
    let client = Client::new(config).await.unwrap();

    // Create topic.
    let topic = client.topic("test-topic");
    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    // Create subscription
    // If subscription name does not contain a "/", then the project is taken from client above. Otherwise, the
    // name will be treated as a fully qualified resource name
    let config = SubscriptionConfig {
        ..Default::default()
    };

    // Create subscription
    let subscription = client.subscription("test-subscription");
    if !subscription.exists(None).await? {
        subscription
            .create(topic.fully_qualified_name(), config, None)
            .await?;
    }

    // Start subscription.
    let subscription = client.subscription("test-subscription");

    // Start publisher.
    let publisher = topic.new_publisher(None);

    // How many messages to publish.
    let message_count = 10;

    // Publish message.
    let tasks: Vec<JoinHandle<Result<String>>> = (0..message_count)
        .map(|_i| {
            let publisher = publisher.clone();
            tokio::spawn(async move {
                let msg = PubsubMessage {
                    data: "abc".into(),
                    attributes: vec![("Attr".to_string(), "Value".to_string())]
                        .into_iter()
                        .collect(),
                    ..Default::default()
                };

                // Send a message. There are also `publish_bulk` and `publish_immediately` methods.
                let awaiter = publisher.publish(msg).await;

                // The get method blocks until a server-generated ID or an error is returned for the published message.
                awaiter.get().await.context("publishing failed")
            })
        })
        .collect();

    // Wait for all publish task finish
    for task in tasks {
        let message_id = task.await.context("could not get message id")??;
        println!("Published message with id {}", message_id)
    }

    // Wait for publishers in topic finish.
    let mut publisher = publisher;
    publisher.shutdown().await;

    // Token for cancel.
    let cancel = CancellationToken::new();
    let timer_cancel = cancel.clone();
    tokio::spawn(async move {
        // Cancel after 10 seconds.
        tokio::time::sleep(Duration::from_secs(10)).await;
        timer_cancel.cancel();
    });

    // Consume the messages.
    let mut stream = subscription.subscribe(None).await?;
    let mut received_count = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                return Err(anyhow!("Cancelled"))
            },
            Some(message) = stream.next() => {
                // Handle data.
                println!("Got Message: {:?}", message.message);

                // Ack.
                let _ = message.ack().await;

                received_count += 1;

                if received_count == message_count {
                    return Ok(())
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
