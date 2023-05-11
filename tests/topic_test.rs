use bytes::Bytes;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::SubscriptionName;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::{GetTopicError, TopicMessage, TopicName};
use std::sync::Arc;

#[tokio::test]
async fn delete_topic() {
    let topic_manager = TopicManager::new();
    let subscription_manager = SubscriptionManager::new();

    let topic_name = TopicName::new("test", "topic");
    let subscription_name = SubscriptionName::new("test", "subscription");

    let topic = topic_manager.create_topic(topic_name.clone()).unwrap();
    let subscription = subscription_manager
        .create_subscription(subscription_name, Arc::clone(&topic))
        .await
        .unwrap();

    // Publish a message to the topic, but don't pull the subscription yet.
    topic
        .publish_messages(vec![TopicMessage::new(Bytes::from("hello"))])
        .await
        .unwrap();

    // Delete the topic.
    topic.delete().await.unwrap();

    // Verify it's gone.
    assert_eq!(
        topic_manager.get_topic(&topic_name).unwrap_err(),
        GetTopicError::DoesNotExist
    );

    // Check the ref count.
    assert_eq!(
        Arc::strong_count(&topic),
        1,
        "because we should have the only reference"
    );

    // Drop it here so we can verify the subscription reports it as deleted.
    drop(topic);
    assert!(subscription.topic.upgrade().is_none());

    // Verify the messages were still delivered to the subscription.
    assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 1);
}
