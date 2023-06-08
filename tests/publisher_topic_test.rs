use deltio::pubsub_proto::{
    DeleteTopicRequest, GetTopicRequest, ListTopicSubscriptionsRequest, ListTopicsRequest, Topic,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use std::collections::HashMap;
use test_helpers::*;
use tonic::Code;
use uuid::Uuid;

pub mod test_helpers;

#[tokio::test]
async fn test_create_and_get() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());

    // Create the topic.
    let topic_input = Topic {
        name: topic_name.to_string(),
        labels: HashMap::default(),
        message_storage_policy: None,
        kms_key_name: String::default(),
        schema_settings: None,
        satisfies_pzs: false,
        message_retention_duration: None,
    };

    let topic_output = server
        .publisher
        .create_topic(topic_input.clone())
        .await
        .unwrap();

    let topic_output = topic_output.get_ref();
    assert_eq!(topic_output.name, topic_input.name);

    // Get the topic that was created.
    let topic_got = server
        .publisher
        .get_topic(GetTopicRequest {
            topic: topic_name.to_string(),
        })
        .await
        .unwrap();

    let topic_got = topic_got.get_ref();
    assert_eq!(topic_got.name, topic_input.name);

    server.dispose().await;
}

#[tokio::test]
async fn test_list() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name1 = TopicName::new("test", &Uuid::new_v4().to_string());
    let topic_name2 = TopicName::new("test", &Uuid::new_v4().to_string());

    // Create the topic.
    server.create_topic_with_name(&topic_name1).await;
    server.create_topic_with_name(&topic_name2).await;

    // List the topics that were created.
    let list_response = server
        .publisher
        .list_topics(ListTopicsRequest {
            project: "projects/test".to_string(),
            page_size: 1,
            page_token: "".to_string(),
        })
        .await
        .unwrap();

    let list_response = list_response.get_ref();
    assert_eq!(list_response.topics.len(), 1);
    assert_eq!(list_response.topics[0].name, topic_name1.to_string());
    assert_ne!(
        list_response.next_page_token,
        String::default(),
        "the page token should be returned"
    );

    // Get the next page.
    let list_response = server
        .publisher
        .list_topics(ListTopicsRequest {
            project: "projects/test".to_string(),
            page_size: 1,
            page_token: list_response.next_page_token.clone(),
        })
        .await
        .unwrap();
    let list_response = list_response.get_ref();
    assert_eq!(list_response.topics.len(), 1);
    assert_eq!(list_response.topics[0].name, topic_name2.to_string());
    assert_ne!(
        list_response.next_page_token,
        String::default(),
        "the page token should be returned"
    );

    // When we call again, there will be nothing left.
    let list_response = server
        .publisher
        .list_topics(ListTopicsRequest {
            project: "projects/test".to_string(),
            page_size: 1,
            page_token: list_response.next_page_token.clone(),
        })
        .await
        .unwrap();
    let list_response = list_response.get_ref();
    assert_eq!(list_response.topics.len(), 0);
    assert_eq!(
        list_response.next_page_token,
        String::default(),
        "the page token should not be returned"
    );
    server.dispose().await;
}

#[tokio::test]
async fn test_delete() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());

    // Create the topic.
    server.create_topic_with_name(&topic_name).await;

    // Verify we can retrieve it.
    server
        .publisher
        .get_topic(GetTopicRequest {
            topic: topic_name.to_string(),
        })
        .await
        .unwrap();

    // Delete it.
    server
        .publisher
        .delete_topic(DeleteTopicRequest {
            topic: topic_name.to_string(),
        })
        .await
        .unwrap();

    // Verify it's gone.
    let status = server
        .publisher
        .get_topic(GetTopicRequest {
            topic: topic_name.to_string(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::NotFound);

    server.dispose().await;
}

#[tokio::test]
async fn test_list_topic_subscriptions() {
    let mut server = TestHost::start().await.unwrap();
    let topic_name = TopicName::new("test", &Uuid::new_v4().to_string());

    // Create the topic.
    server.create_topic_with_name(&topic_name).await;

    // Create the subscriptions.
    let subscription_name1 = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    let subscription_name2 = SubscriptionName::new("test", &Uuid::new_v4().to_string());
    server
        .create_subscription_with_name(&topic_name, &subscription_name1)
        .await;
    server
        .create_subscription_with_name(&topic_name, &subscription_name2)
        .await;

    // List subscriptions for the topic, get a single page.
    let page = server
        .publisher
        .list_topic_subscriptions(ListTopicSubscriptionsRequest {
            topic: topic_name.to_string(),
            page_token: Default::default(),
            page_size: 1,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(page.subscriptions.len(), 1);
    assert_eq!(page.subscriptions[0], subscription_name1.to_string());
    assert_ne!(page.next_page_token, String::default());

    // Get the next page.
    let page = server
        .publisher
        .list_topic_subscriptions(ListTopicSubscriptionsRequest {
            topic: topic_name.to_string(),
            page_token: page.next_page_token,
            page_size: 1,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(page.subscriptions.len(), 1);
    assert_eq!(page.subscriptions[0], subscription_name2.to_string());
    assert_ne!(page.next_page_token, String::default());

    // No more pages.
    let page = server
        .publisher
        .list_topic_subscriptions(ListTopicSubscriptionsRequest {
            topic: topic_name.to_string(),
            page_token: page.next_page_token,
            page_size: 1,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(page.subscriptions.len(), 0);
    assert_eq!(page.next_page_token, String::default());

    server.dispose().await;
}

// #[tokio::test]
// async fn test_list_bench() {
//     let mut server = TestHost::start().await.unwrap();
//     let topic_names = (0..10_000).map(|_| TopicName::new("test", &Uuid::new_v4().to_string()));
//
//     let now = std::time::Instant::now();
//     futures::future::join_all(topic_names.map(|n| {
//         let mut publisher = server.publisher.clone();
//         tokio::spawn(async move { publisher.create_topic(map_to_topic_resource(&n)).await })
//     }))
//     .await;
//     let elapsed = now.elapsed();
//     println!("Creating took {:?}", elapsed);
//
//     // Get the topic that was created.
//     let list_response = server
//         .publisher
//         .list_topics(ListTopicsRequest {
//             project: "projects/test".to_string(),
//             page_size: 1_000,
//             page_token: "".to_string(),
//         })
//         .await
//         .unwrap();
//
//     let list_response = list_response.get_ref();
//     assert_eq!(list_response.topics.len(), 1_000);
//
//     let now = std::time::Instant::now();
//     // Get the next page.
//     server
//         .publisher
//         .list_topics(ListTopicsRequest {
//             project: "projects/test".to_string(),
//             page_size: 1_000,
//             page_token: list_response.next_page_token.clone(),
//         })
//         .await
//         .unwrap();
//     let elapsed = now.elapsed();
//     println!("Getting page 2 took {:?}", elapsed);
//
//     server.dispose().await;
// }
