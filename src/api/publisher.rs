use crate::pubsub::publisher_server::Publisher;
use crate::pubsub::*;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct PublisherService {}

#[async_trait::async_trait]
impl Publisher for PublisherService {
    async fn create_topic(&self, _request: Request<Topic>) -> Result<Response<Topic>, Status> {
        todo!()
    }

    async fn update_topic(
        &self,
        _request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        todo!()
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let message = &request.get_ref().messages[0];
        let msg = &message.data;
        let msg = std::str::from_utf8(msg).map_err(|_| Status::internal("data not a string"))?;
        let publish_time = &message
            .publish_time
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or("not specified".into());
        println!(
            "Publishing: {} (id: {}, ordering key: {}, published at: {})",
            msg, &message.message_id, &message.ordering_key, publish_time
        );
        Ok(Response::new(PublishResponse {
            message_ids: vec![],
        }))
    }

    async fn get_topic(
        &self,
        _request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        todo!()
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        todo!()
    }

    async fn list_topic_subscriptions(
        &self,
        _request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        todo!()
    }

    async fn list_topic_snapshots(
        &self,
        _request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        todo!()
    }

    async fn delete_topic(
        &self,
        _request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn detach_subscription(
        &self,
        _request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        todo!()
    }
}
