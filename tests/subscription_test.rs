use bytes::Bytes;
use deltio::subscriptions::*;
use deltio::topics::*;
use std::sync::Arc;
use tokio::sync::futures::Notified;
use uuid::Uuid;

#[tokio::test]
async fn pulling_messages() {
    let (topic, subscription) = new_topic_and_subscription().await;

    // Subscribe to the notifier.
    let notified = subscription.new_messages_signal();

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
    let (topic, subscription) = new_topic_and_subscription().await;

    // Subscribe to the notifier.
    let notified = subscription.new_messages_signal();

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
    let notified = subscription.new_messages_signal();

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

async fn new_topic_and_subscription() -> (Arc<Topic>, Arc<Subscription>) {
    let project_id = Uuid::new_v4().to_string();
    let topic_id = Uuid::new_v4().to_string();
    let sub_id = Uuid::new_v4().to_string();
    let topic = Arc::new(Topic::new(
        TopicInfo::new(TopicName::new(&project_id, &topic_id)),
        1,
    ));
    let subscription = Arc::new(Subscription::new(
        SubscriptionInfo::new(SubscriptionName::new(&project_id, &sub_id)),
        1,
        Arc::clone(&topic),
    ));

    // Attach the subscription.
    topic
        .attach_subscription(Arc::clone(&subscription))
        .await
        .unwrap();

    (topic, subscription)
}

async fn wait_for_notification(notified: Notified<'_>) {
    tokio::select! {
        _ = notified => {},
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
            panic!("Timed out waiting for notify")
        }
    }
}
