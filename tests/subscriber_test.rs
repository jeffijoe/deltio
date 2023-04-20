use deltio::pubsub_proto::{GetSubscriptionRequest, ListSubscriptionsRequest, Subscription, Topic};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;

use test_helpers::*;
use tonic::Code;
use uuid::Uuid;

mod test_helpers;

#[tokio::test]
async fn test_subscription_management() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "publishing");
    server
        .publisher
        .create_topic(Topic {
            name: topic_name.to_string(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
        })
        .await
        .unwrap();

    // Create a subscription
    let subscription_name = SubscriptionName::new("test", "subscribing");
    let subscription = server
        .subscriber
        .create_subscription(map_to_resource(&subscription_name, &topic_name))
        .await
        .unwrap();

    let subscription = subscription.get_ref();
    assert_eq!(subscription.topic, topic_name.to_string());
    assert_eq!(subscription.name, subscription_name.to_string());

    // Verify that we can retrieve it.
    let subscription = server
        .subscriber
        .get_subscription(GetSubscriptionRequest {
            subscription: subscription_name.to_string(),
        })
        .await
        .unwrap();
    let subscription = subscription.get_ref();
    assert_eq!(subscription.topic, topic_name.to_string());
    assert_eq!(subscription.name, subscription_name.to_string());

    server.dispose().await;
}

#[tokio::test]
async fn test_enforce_same_project() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("one", "publishing");
    server
        .publisher
        .create_topic(Topic {
            name: topic_name.to_string(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
        })
        .await
        .unwrap();

    // Create a subscription, this should fail because it's a different project.
    let subscription_name = SubscriptionName::new("two", "subscribing");

    let status = server
        .subscriber
        .create_subscription(map_to_resource(&subscription_name, &topic_name))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().contains("same project"));

    server.dispose().await;
}

#[tokio::test]
async fn test_list() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());
    let subscription_name1 = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    let subscription_name2 = SubscriptionName::new("test", &Uuid::new_v4().to_string());

    // Create the topic.
    server
        .publisher
        .create_topic(Topic {
            name: topic_name.to_string(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
        })
        .await
        .unwrap();

    // Create the subscriptions.
    server
        .subscriber
        .create_subscription(map_to_resource(&subscription_name1, &topic_name))
        .await
        .unwrap();
    server
        .subscriber
        .create_subscription(map_to_resource(&subscription_name2, &topic_name))
        .await
        .unwrap();

    // List the subscriptions that were created.
    let list_response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "test".to_string(),
            page_size: 1,
            page_token: "".to_string(),
        })
        .await
        .unwrap();

    let list_response = list_response.get_ref();
    assert_eq!(list_response.subscriptions.len(), 1);
    assert_eq!(
        list_response.subscriptions[0].name,
        subscription_name1.to_string()
    );
    assert_ne!(
        list_response.next_page_token,
        String::default(),
        "the page token should be returned"
    );

    // Get the next page.
    let list_response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "test".to_string(),
            page_size: 1,
            page_token: list_response.next_page_token.clone(),
        })
        .await
        .unwrap();
    let list_response = list_response.get_ref();
    assert_eq!(list_response.subscriptions.len(), 1);
    assert_eq!(
        list_response.subscriptions[0].name,
        subscription_name2.to_string()
    );
    assert_ne!(
        list_response.next_page_token,
        String::default(),
        "the page token should be returned"
    );

    // When we call again, there will be nothing left.
    let list_response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: "test".to_string(),
            page_size: 1,
            page_token: list_response.next_page_token.clone(),
        })
        .await
        .unwrap();
    let list_response = list_response.get_ref();
    assert_eq!(list_response.subscriptions.len(), 0);
    assert_eq!(
        list_response.next_page_token,
        String::default(),
        "the page token should not be returned"
    );
    server.dispose().await;
}

fn map_to_resource(subscription_name: &SubscriptionName, topic_name: &TopicName) -> Subscription {
    Subscription {
        name: subscription_name.to_string(),
        topic: topic_name.to_string(),
        push_config: None,
        bigquery_config: None,
        ack_deadline_seconds: 0,
        retain_acked_messages: false,
        message_retention_duration: None,
        labels: Default::default(),
        enable_message_ordering: false,
        expiration_policy: None,
        filter: "".to_string(),
        dead_letter_policy: None,
        retry_policy: None,
        detached: false,
        enable_exactly_once_delivery: false,
        topic_message_retention_duration: None,
        state: 0,
    }
}
