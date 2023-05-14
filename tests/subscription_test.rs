use bytes::Bytes;
use deltio::subscriptions::futures::MessagesAvailable;
use deltio::subscriptions::subscription_manager::SubscriptionManager;
use deltio::subscriptions::*;
use deltio::topics::topic_manager::TopicManager;
use deltio::topics::*;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn pulling_messages() {
    let topic_manager = TopicManager::new();
    let subscription_manager = SubscriptionManager::new();
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifier.
    let notified = subscription.messages_available();

    // Publish messages.
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("meow")),
            TopicMessage::new(Bytes::from("can haz cheezburger?")),
        ])
        .await
        .unwrap();

    // Wait for the notification.
    wait_for_notification(notified).await;

    // Pull messages.
    let pulled_messages = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled_messages.len(), 2);

    assert_eq!(pulled_messages[0].message().data, Bytes::from("meow"));
    assert_eq!(
        pulled_messages[1].message().data,
        Bytes::from("can haz cheezburger?")
    );

    // Pull again, the messages should not be returned again as they are outstanding.
    assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);

    // Check the stats to verify that the messages are outstanding.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);

    // Acknowledge the messages.
    subscription
        .acknowledge_messages(pulled_messages.iter().map(|m| m.ack_id()).collect())
        .await
        .unwrap();

    // Verify that pulling still does not return anything.
    assert_eq!(subscription.pull_messages(10).await.unwrap().len(), 0);

    // Verify that there are no more outstanding messages.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 0);
}

#[tokio::test]
async fn nack_messages() {
    let topic_manager = TopicManager::new();
    let subscription_manager = SubscriptionManager::new();
    let (topic, subscription) =
        new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifier.
    let notified = subscription.messages_available();

    // Publish messages.
    topic
        .publish_messages(vec![
            TopicMessage::new(Bytes::from("meow")),
            TopicMessage::new(Bytes::from("can haz cheezburger?")),
        ])
        .await
        .unwrap();

    // Wait for the notification.
    wait_for_notification(notified).await;

    // Pull messages.
    let pulled_messages = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled_messages.len(), 2);

    // Check the stats to verify that the messages are outstanding.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);

    // Set up a notification for nack'ing.
    let notified = subscription.messages_available();

    // NACK the 2nd message.
    subscription
        .modify_ack_deadlines(vec![DeadlineModification::nack(
            pulled_messages[1].ack_id(),
        )])
        .await
        .unwrap();

    // Verify that nack'ing triggers the notification.
    wait_for_notification(notified).await;

    // Verify that after NACK'ing the message is not considered outstanding (as it was
    // returned to the backlog).
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 1);
    assert_eq!(stats.backlog_messages_count, 1);

    // Verify that pulling returns the NACK'ed message.
    let pulled_messages = subscription.pull_messages(10).await.unwrap();
    assert_eq!(pulled_messages.len(), 1);
    assert_eq!(
        pulled_messages[0].message().data,
        Bytes::from("can haz cheezburger?")
    );

    // Verify that after pulling again, the message is outstanding once more.
    let stats = subscription.get_stats().await.unwrap();
    assert_eq!(stats.outstanding_messages_count, 2);
    assert_eq!(stats.backlog_messages_count, 0);
}

async fn new_topic_and_subscription(
    topic_manager: &TopicManager,
    subscription_manager: &SubscriptionManager,
) -> (Arc<Topic>, Arc<Subscription>) {
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = topic_manager
        .create_topic(TopicName::new(&project_id, &topic_id))
        .unwrap();

    let subscription = subscription_manager
        .create_subscription(
            SubscriptionInfo::new_with_defaults(SubscriptionName::new(&project_id, &sub_id)),
            Arc::clone(&topic),
        )
        .await
        .unwrap();

    // Attach the subscription.
    topic
        .attach_subscription(Arc::clone(&subscription))
        .await
        .unwrap();

    (topic, subscription)
}

#[tokio::test]
async fn test_delete_subscription() {
    let topic_manager = TopicManager::new();
    let subscription_manager = SubscriptionManager::new();
    let (_, subscription) = new_topic_and_subscription(&topic_manager, &subscription_manager).await;

    // Subscribe to the notifiers, make sure it gets dropped.
    let messages_notified = subscription.messages_available();
    let delete_notified = subscription.deleted();

    // Delete the subscription.
    subscription.delete().await.unwrap();

    assert_eq!(
        Arc::strong_count(&subscription),
        1,
        "because we should have the only reference left"
    );

    let joined = async {
        delete_notified.await;
        messages_notified.await;
    };

    tokio::select! {
        _ = joined => {},
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Timed out waiting for notifications")
        }
    }
}

async fn wait_for_notification(notified: MessagesAvailable<'_>) {
    tokio::select! {
        _ = notified => {},
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
            panic!("Timed out waiting for notify")
        }
    }
}
