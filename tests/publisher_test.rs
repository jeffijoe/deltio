use deltio::pubsub_proto::{PublishRequest, PubsubMessage};
use deltio::topics::TopicName;
use std::collections::HashMap;
use test_helpers::*;

pub mod test_helpers;

#[tokio::test]
async fn test_publish() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to publish to.
    let topic_name = TopicName::new("test", "publishing");
    server.create_topic_with_name(&topic_name).await;

    // Publish some messages.
    let message1 = PubsubMessage {
        publish_time: None,
        attributes: HashMap::new(),
        message_id: "".to_string(),
        ordering_key: "".to_string(),
        data: "hello".as_bytes().to_vec(),
    };
    let message2 = PubsubMessage {
        publish_time: None,
        attributes: HashMap::new(),
        message_id: "".to_string(),
        ordering_key: "".to_string(),
        data: "world".as_bytes().to_vec(),
    };

    let response = server
        .publisher
        .publish(PublishRequest {
            topic: topic_name.to_string(),
            messages: vec![message1, message2],
        })
        .await
        .unwrap();

    let response = response.get_ref();
    assert_eq!(2, response.message_ids.len(), "we published 2 messages");
    server.dispose().await;
}
