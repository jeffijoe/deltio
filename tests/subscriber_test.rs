use deltio::pubsub_proto::{
    DeleteSubscriptionRequest, GetSubscriptionRequest, ListSubscriptionsRequest,
    StreamingPullRequest, StreamingPullResponse,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use futures::StreamExt;
use test_helpers::*;
use tonic::{Code, Status};
use uuid::Uuid;

pub mod test_helpers;

#[tokio::test]
async fn test_subscription_management() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "publishing");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription
    let subscription_name = SubscriptionName::new("test", "subscribing");
    let subscription = server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name,
            &topic_name,
        ))
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
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription, this should fail because it's a different project.
    let subscription_name = SubscriptionName::new("two", "subscribing");

    let status = server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name,
            &topic_name,
        ))
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
    server.create_topic_with_name(&topic_name).await;

    // Create the subscriptions.
    server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name1,
            &topic_name,
        ))
        .await
        .unwrap();
    server
        .subscriber
        .create_subscription(map_to_subscription_resource(
            &subscription_name2,
            &topic_name,
        ))
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

#[tokio::test]
async fn test_streaming_pull() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start polling for messages.
    let (sender, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish some messages, wait for them to be retrieved.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    // ACK the 2 messages.
    sender
        .send(StreamingPullRequest {
            subscription: Default::default(),
            ack_ids: pull_response
                .received_messages
                .iter()
                .map(|r| r.ack_id.clone())
                .collect(),
            modify_deadline_seconds: vec![],
            modify_deadline_ack_ids: vec![],
            stream_ack_deadline_seconds: 0,
            client_id: Default::default(),
            max_outstanding_messages: 0,
            max_outstanding_bytes: 0,
        })
        .await
        .unwrap();

    // Publish more messages and wait again.
    server
        .publish_text_messages(&topic_name, vec!["Woah".into(), "Much Resilient".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);
    assert_eq!(
        collect_text_messages(&pull_response),
        vec!["Woah", "Much Resilient"]
    );

    let ack_ids = pull_response
        .received_messages
        .iter()
        .map(|r| r.ack_id.clone())
        .collect::<Vec<_>>();

    // NACK the messages so we receive them again.
    sender
        .send(StreamingPullRequest {
            subscription: Default::default(),
            ack_ids: Default::default(),
            modify_deadline_seconds: ack_ids.iter().map(|_| 0).collect(),
            modify_deadline_ack_ids: ack_ids,
            stream_ack_deadline_seconds: 0,
            client_id: Default::default(),
            max_outstanding_messages: 0,
            max_outstanding_bytes: 0,
        })
        .await
        .unwrap();

    // Pull all the messages again, we should get all the ones we nack'ed.
    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);
    assert_eq!(
        collect_text_messages(&pull_response),
        vec!["Woah", "Much Resilient"]
    );
}

#[tokio::test]
async fn test_deleting_subscription() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to subscribe to.
    let topic_name = TopicName::new("test", "topic");
    server.create_topic_with_name(&topic_name).await;

    // Create a subscription.
    let subscription_name = SubscriptionName::new("test", "subscription");
    server
        .create_subscription_with_name(&topic_name, &subscription_name)
        .await;

    // Start polling for messages.
    let (_, mut inbound) = server.streaming_pull(&subscription_name).await;

    // Publish some messages, wait for them to be retrieved.
    server
        .publish_text_messages(&topic_name, vec!["Hello".into(), "World".into()])
        .await;

    let pull_response = inbound.next().await.unwrap().unwrap();
    assert_eq!(pull_response.received_messages.len(), 2);

    // Delete the subscription
    server
        .subscriber
        .delete_subscription(DeleteSubscriptionRequest {
            subscription: subscription_name.to_string(),
        })
        .await
        .unwrap();

    // Verify that we get a Not Found on the streaming pull.
    // Alternatively, if we get `None`, then pretend it was a not found.
    // I believe there may be a race condition with the Tonic client
    // where a streaming error response may or may not be received?
    let stream_resp = inbound
        .next()
        .await
        .unwrap_or(Err(Status::not_found("fallback")));
    let stream_resp = stream_resp.unwrap_err();
    assert_eq!(stream_resp.code(), Code::NotFound);
    assert!(
        inbound.next().await.is_none(),
        "the stream should have ended"
    );

    // Verify that the subscription is gone.
    let response = server
        .subscriber
        .list_subscriptions(ListSubscriptionsRequest {
            project: subscription_name.project_id(),
            page_size: 10,
            page_token: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.subscriptions.len(), 0);
}

fn collect_text_messages(pull_response: &StreamingPullResponse) -> Vec<String> {
    pull_response
        .received_messages
        .iter()
        .map(|m| String::from_utf8(m.message.clone().unwrap().data).unwrap())
        .collect::<Vec<_>>()
}
