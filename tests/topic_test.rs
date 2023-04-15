use deltio::pubsub_proto::publisher_client::PublisherClient;
use deltio::pubsub_proto::{GetTopicRequest, ListTopicsRequest, Topic};
use deltio::topics::TopicName;
use std::collections::HashMap;
use test_helpers::*;
use tonic::transport::Channel;
use uuid::Uuid;

mod test_helpers;

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
    create_topic(&mut server.publisher, &topic_name1).await;
    create_topic(&mut server.publisher, &topic_name2).await;

    // List the topics that were created.
    let list_response = server
        .publisher
        .list_topics(ListTopicsRequest {
            project: "test".to_string(),
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
            project: "test".to_string(),
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
            project: "test".to_string(),
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

// #[tokio::test]
// async fn test_list_bench() {
//     let mut server = TestHost::start().await.unwrap();
//     let topic_names = (0..10_000).map(|_| TopicName::new("test", &Uuid::new_v4().to_string()));
//
//     let now = std::time::Instant::now();
//     futures::future::join_all(topic_names.map(|n| {
//         let name = n.clone();
//         let mut publisher = server.publisher.clone();
//         tokio::spawn(async move { create_topic(&mut publisher, &name).await })
//     }))
//     .await;
//     let elapsed = now.elapsed();
//     println!("Creating took {:?}", elapsed);
//
//     // Get the topic that was created.
//     let list_response = server
//         .publisher
//         .list_topics(ListTopicsRequest {
//             project: "test".to_string(),
//             page_size: 5_000,
//             page_token: "".to_string(),
//         })
//         .await
//         .unwrap();
//
//     let list_response = list_response.get_ref();
//     assert_eq!(list_response.topics.len(), 5_000);
//
//     let now = std::time::Instant::now();
//     // Get the next page.
//     let list_response = server
//         .publisher
//         .list_topics(ListTopicsRequest {
//             project: "test".to_string(),
//             page_size: 5_000,
//             page_token: list_response.next_page_token.clone(),
//         })
//         .await
//         .unwrap();
//     let elapsed = now.elapsed();
//     println!("Getting page 2 took {:?}", elapsed);
//     let list_response = list_response.get_ref();
//     assert_eq!(list_response.topics.len(), 5_000);
//
//     server.dispose().await;
// }

/// Helper for creating a topic.
async fn create_topic(publisher: &mut PublisherClient<Channel>, name: &TopicName) -> Topic {
    let topic_input = Topic {
        name: name.to_string(),
        labels: HashMap::default(),
        message_storage_policy: None,
        kms_key_name: String::default(),
        schema_settings: None,
        satisfies_pzs: false,
        message_retention_duration: None,
    };

    let topic_output = publisher.create_topic(topic_input.clone()).await.unwrap();

    let topic_output = topic_output.get_ref();
    topic_output.clone()
}
