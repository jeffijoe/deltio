use crate::push_server::{decode_data, TestPushServer};
use deltio::pubsub_proto::Subscription;
use deltio::pubsub_proto::{PullRequest, PushConfig};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use std::time::Duration;
use test_helpers::*;

pub mod push_server;
pub mod test_helpers;

#[tokio::test]
async fn test_push_subscription() {
    let mut server = TestHost::start().await.unwrap();
    let mut push_server = TestPushServer::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription configured for push.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .subscriber
        .create_subscription(Subscription {
            push_config: Some(PushConfig {
                attributes: Default::default(),
                authentication_method: None,
                push_endpoint: push_server.url(),
            }),
            ..map_to_subscription_resource(&subscription_name, &topic_name)
        })
        .await
        .unwrap();

    // Publish some messages.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    // Wait for the push server to receive them.
    // Advance time to make that happen a bit faster.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let payload1 = push_server.next().await.unwrap();
    assert_eq!(payload1.subscription, subscription_name.to_string());
    assert_eq!(
        payload1.message.message_id,
        payload1.message.message_id_dupe
    );
    assert_eq!(decode_data(&payload1), "Hello");
    payload1.succeed();

    let payload2 = push_server.next().await.unwrap();
    assert_eq!(decode_data(&payload2), "World");
    payload2.succeed();

    // Advance enough time to ensure that no redelivery will occur.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(120)).await;
    tokio::time::resume();

    // Pull using RPC to verify.
    // Need to use `return_immediately` since we expect no messages.
    #[allow(deprecated)]
    let pull_response = server
        .subscriber
        .pull(PullRequest {
            subscription: subscription_name.to_string(),
            max_messages: 10,
            return_immediately: true,
        })
        .await
        .unwrap();
    let pull_response = pull_response.get_ref();
    assert!(
        pull_response.received_messages.is_empty(),
        "there should be no more messages as they were all acked"
    );
}

#[tokio::test]
async fn test_push_subscription_redelivery() {
    let mut server = TestHost::start().await.unwrap();
    let mut push_server = TestPushServer::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription configured for push.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .subscriber
        .create_subscription(Subscription {
            push_config: Some(PushConfig {
                attributes: Default::default(),
                authentication_method: None,
                push_endpoint: push_server.url(),
            }),
            ..map_to_subscription_resource(&subscription_name, &topic_name)
        })
        .await
        .unwrap();

    // Publish some messages.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    // Wait for the push server to receive them.
    // Advance time to make that happen a bit faster.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    let payload1 = push_server.next().await.unwrap();
    assert_eq!(payload1.subscription, subscription_name.to_string());
    assert_eq!(
        payload1.message.message_id,
        payload1.message.message_id_dupe
    );
    assert_eq!(decode_data(&payload1), "Hello");
    payload1.succeed();

    let payload2 = push_server.next().await.unwrap();
    assert_eq!(decode_data(&payload2), "World");
    payload2.fail();

    // Advance enough time to ensure that the message we NACK'ed is redelivered.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();

    let payload2 = push_server.next().await.unwrap();
    assert_eq!(decode_data(&payload2), "World");
    payload2.succeed();
}
