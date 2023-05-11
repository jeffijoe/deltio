use crate::api::page_token::PageToken;
use crate::api::parser;
use crate::pubsub_proto::publisher_server::Publisher;
use crate::pubsub_proto::*;
use crate::topics::topic_manager::TopicManager;
use crate::topics::{
    CreateTopicError, DeleteError, GetTopicError, ListTopicsError, PublishMessagesError,
};
use crate::topics::{TopicMessage, TopicName};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct PublisherService {
    pub topic_manager: Arc<TopicManager>,
}

impl PublisherService {
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self { topic_manager }
    }

    /// Gets the internal topic.
    async fn get_topic_internal(
        &self,
        topic_name: &TopicName,
    ) -> Result<Arc<crate::topics::Topic>, Status> {
        self.topic_manager
            .get_topic(topic_name)
            .map_err(|e| match e {
                GetTopicError::DoesNotExist => topic_not_found(topic_name),
                GetTopicError::Closed => Status::internal("System is shutting down"),
            })
    }
}

#[async_trait::async_trait]
impl Publisher for PublisherService {
    async fn create_topic(&self, request: Request<Topic>) -> Result<Response<Topic>, Status> {
        let start = std::time::SystemTime::now();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.name)?;
        let topic_name_str = topic_name.to_string();

        self.topic_manager
            .create_topic(topic_name)
            .map_err(|e| match e {
                CreateTopicError::AlreadyExists => Status::already_exists("Topic already exists"),
                // TODO: Figure out a more generic way to handle this.
                CreateTopicError::Closed => Status::internal("System is shutting down"),
            })?;

        let response = Topic {
            name: topic_name_str.clone(),
            kms_key_name: String::default(),
            labels: HashMap::default(),
            message_retention_duration: None,
            satisfies_pzs: false,
            schema_settings: None,
            message_storage_policy: None,
        };

        println!(
            "{}: creating topic took {:?}",
            topic_name_str,
            start.elapsed()
        );
        Ok(Response::new(response))
    }

    async fn update_topic(
        &self,
        _request: Request<UpdateTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        Err(Status::unimplemented(
            "update_topic is not implemented in Deltio",
        ))
    }

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let start = std::time::SystemTime::now();
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        let mut messages = Vec::with_capacity(request.messages.len());

        for m in request.messages.iter() {
            let data = Bytes::from(m.data.clone());
            let message = TopicMessage::new(data);
            messages.push(message);
        }
        println!("{}: publishing {} messages", &topic_name, messages.len());

        let result = topic
            .publish_messages(messages)
            .await
            .map_err(|e| match e {
                // TODO: Verify what the real Pub/Sub service returns.
                PublishMessagesError::TopicDoesNotExist => topic_not_found(&topic_name),
                PublishMessagesError::Closed => closed_status(),
            })?;

        let response = Response::new(PublishResponse {
            message_ids: result.message_ids.iter().map(|m| m.to_string()).collect(),
        });

        println!("{}: publishing took {:?}", &topic_name, start.elapsed());

        Ok(response)
    }

    async fn get_topic(
        &self,
        request: Request<GetTopicRequest>,
    ) -> Result<Response<Topic>, Status> {
        let request = request.get_ref();
        let topic_name = parser::parse_topic_name(&request.topic)?;

        let topic = self.get_topic_internal(&topic_name).await?;

        Ok(Response::new(Topic {
            name: topic.info.name.to_string(),
            labels: Default::default(),
            message_storage_policy: None,
            kms_key_name: "".to_string(),
            schema_settings: None,
            satisfies_pzs: false,
            message_retention_duration: None,
        }))
    }

    async fn list_topics(
        &self,
        request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let request = request.get_ref();
        let page_token_value = if request.page_token.is_empty() {
            None
        } else {
            let decoded = PageToken::try_decode(&request.page_token)
                .ok_or_else(|| Status::invalid_argument("Page token malformed"))?;
            Some(decoded)
        };

        let page_size = request
            .page_size
            .try_into()
            .map_err(|_| Status::invalid_argument("Not a valid page size"))?;

        let page = self
            .topic_manager
            .list_topics(
                Box::from(request.project.clone()),
                page_size,
                page_token_value.map(|v| v.value),
            )
            .map_err(|e| match e {
                ListTopicsError::Closed => closed_status(),
            })?;

        let topics = page
            .topics
            .into_iter()
            .map(|topic| Topic {
                name: topic.info.name.to_string(),
                labels: HashMap::default(),
                message_storage_policy: None,
                kms_key_name: "".to_string(),
                schema_settings: None,
                satisfies_pzs: false,
                message_retention_duration: None,
            })
            .collect();

        let page_token = page.offset.map(|v| PageToken::new(v).encode());
        let response = ListTopicsResponse {
            topics,
            next_page_token: page_token.unwrap_or(String::default()),
        };
        Ok(Response::new(response))
    }

    async fn list_topic_subscriptions(
        &self,
        _request: Request<ListTopicSubscriptionsRequest>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        // TODO: Implement
        Ok(Response::new(ListTopicSubscriptionsResponse {
            subscriptions: Default::default(),
            next_page_token: Default::default(),
        }))
    }

    async fn list_topic_snapshots(
        &self,
        _request: Request<ListTopicSnapshotsRequest>,
    ) -> Result<Response<ListTopicSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "list_topic_snapshots is not implemented in Deltio",
        ))
    }

    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.get_ref();

        let topic_name = parser::parse_topic_name(&request.topic)?;
        println!("{}: deleting topic", &topic_name);

        let topic = self.get_topic_internal(&topic_name).await?;

        topic.delete().await.map_err(|e| match e {
            DeleteError::Closed => closed_status(),
        })?;

        Ok(Response::new(()))
    }

    async fn detach_subscription(
        &self,
        _request: Request<DetachSubscriptionRequest>,
    ) -> Result<Response<DetachSubscriptionResponse>, Status> {
        Err(Status::unimplemented(
            "detach_subscription is not implemented in Deltio",
        ))
    }
}

#[inline]
fn closed_status() -> Status {
    Status::internal("System is shutting down")
}

#[inline]
fn topic_not_found(topic_name: &TopicName) -> Status {
    Status::not_found(format!(
        "Topic does not exist (resource={})",
        &topic_name.topic_id()
    ))
}
