use deltio::pubsub::{PublishRequest, PubsubMessage, PullRequest};
use std::collections::HashMap;
use test_helpers::*;

mod test_helpers;

#[tokio::test]
async fn test_publish() {
    let mut server = TestHost::start().await.unwrap();
    let message = PubsubMessage {
        publish_time: None,
        attributes: HashMap::new(),
        message_id: "".to_string(),
        ordering_key: "".to_string(),
        data: "hello".as_bytes().to_vec(),
    };

    server
        .publisher
        .publish(PublishRequest {
            topic: "test".into(),
            messages: vec![message],
        })
        .await
        .unwrap();

    server.dispose().await;
}
