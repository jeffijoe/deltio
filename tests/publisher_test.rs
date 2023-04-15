use deltio::pubsub_proto::{PublishRequest, PubsubMessage, Topic};
use deltio::topics::TopicName;
use std::collections::HashMap;
use test_helpers::*;

mod test_helpers;

#[tokio::test]
async fn test_publish() {
    let mut server = TestHost::start().await.unwrap();

    // Create a topic to publish to.
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
