use crate::api::parser;
use crate::pubsub_proto::subscriber_server::Subscriber;
use crate::pubsub_proto::{
    AcknowledgeRequest, CreateSnapshotRequest, DeleteSnapshotRequest, DeleteSubscriptionRequest,
    GetSnapshotRequest, GetSubscriptionRequest, ListSnapshotsRequest, ListSnapshotsResponse,
    ListSubscriptionsRequest, ListSubscriptionsResponse, ModifyAckDeadlineRequest,
    ModifyPushConfigRequest, PullRequest, PullResponse, SeekRequest, SeekResponse, Snapshot,
    StreamingPullRequest, StreamingPullResponse, Subscription, UpdateSnapshotRequest,
    UpdateSubscriptionRequest,
};
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::subscriptions::{CreateSubscriptionError, GetSubscriptionError, ListSubscriptionsError};
use crate::topics::topic_manager::TopicManager;
use crate::topics::GetTopicError;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use crate::api::page_token::PageToken;

pub struct SubscriberService {
    topic_manager: Arc<TopicManager>,
    subscription_manager: Arc<SubscriptionManager>,
}

impl SubscriberService {
    /// Creates a new subscriber service.
    pub fn new(
        topic_manager: Arc<TopicManager>,
        subscription_manager: Arc<SubscriptionManager>,
    ) -> Self {
        Self {
            topic_manager,
            subscription_manager,
        }
    }
}

#[async_trait::async_trait]
impl Subscriber for SubscriberService {
    async fn create_subscription(
        &self,
        request: Request<Subscription>,
    ) -> Result<Response<Subscription>, Status> {
        let request = request.get_ref();

        let topic_name = parser::parse_topic_name(&request.topic)?;
        let subscription_name = parser::parse_subscription_name(&request.name)?;

        let topic = self
            .topic_manager
            .get_topic(&topic_name)
            .map_err(|e| match e {
                GetTopicError::DoesNotExist => {
                    Status::not_found(format!("The topic {} does not exist", &topic_name))
                }
                GetTopicError::Closed => Status::internal("System is shutting down"),
            })?;

        let subscription = self
            .subscription_manager
            .create_subscription(subscription_name.clone(), topic.clone())
            .await
            .map_err(|e| match e {
                CreateSubscriptionError::AlreadyExists => Status::already_exists(format!(
                    "The subscription {} already exists",
                    subscription_name
                )),
                CreateSubscriptionError::MustBeInSameProjectAsTopic => Status::invalid_argument(
                    "The subscription must be in the same project as the topic",
                ),
                CreateSubscriptionError::Closed => Status::internal("System is shutting down"),
            })?;

        Ok(Response::new(map_to_subscription_resource(&subscription)))
    }

    async fn get_subscription(
        &self,
        request: Request<GetSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        let request = request.get_ref();

        let subscription_name = parser::parse_subscription_name(&request.subscription)?;

        let subscription = self
            .subscription_manager
            .get_subscription(&subscription_name)
            .map_err(|e| match e {
                GetSubscriptionError::DoesNotExist => Status::not_found(format!(
                    "The subscription {} does not exist",
                    &subscription_name
                )),
                GetSubscriptionError::Closed => Status::internal("System is shutting down"),
            })?;

        Ok(Response::new(map_to_subscription_resource(&subscription)))
    }

    async fn update_subscription(
        &self,
        _request: Request<UpdateSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        todo!()
    }

    async fn list_subscriptions(
        &self,
        request: Request<ListSubscriptionsRequest>,
    ) -> Result<Response<ListSubscriptionsResponse>, Status> {
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
            .subscription_manager
            .list_subscriptions_in_project(
                Box::from(request.project.clone()),
                page_size,
                page_token_value.map(|v| v.value),
            )
            .map_err(|e| match e {
                ListSubscriptionsError::Closed => Status::internal("System is shutting down"),
            })?;

        let subscriptions = page
            .subscriptions
            .into_iter()
            .map(|s| map_to_subscription_resource(&s))
            .collect();

        let page_token = page.offset.map(|v| PageToken::new(v).encode());
        let response = ListSubscriptionsResponse {
            subscriptions,
            next_page_token: page_token.unwrap_or(String::default()),
        };
        Ok(Response::new(response))
    }

    async fn delete_subscription(
        &self,
        _request: Request<DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn modify_ack_deadline(
        &self,
        _request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn acknowledge(
        &self,
        _request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn pull(&self, _request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        todo!()
    }

    type StreamingPullStream =
        Pin<Box<dyn Stream<Item = Result<StreamingPullResponse, Status>> + Send + Sync + 'static>>;

    async fn streaming_pull(
        &self,
        _request: Request<Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        todo!()
    }

    async fn modify_push_config(
        &self,
        _request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        todo!()
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        todo!()
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        todo!()
    }

    async fn update_snapshot(
        &self,
        _request: Request<UpdateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        todo!()
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        todo!()
    }

    async fn seek(&self, _request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        todo!()
    }
}

fn map_to_subscription_resource(subscription: &crate::subscriptions::Subscription) -> Subscription {
    Subscription {
        name: subscription.info.name.to_string(),
        topic: subscription.topic.info.name.to_string(),
        push_config: None,
        bigquery_config: None,
        ack_deadline_seconds: 10,
        retain_acked_messages: false,
        message_retention_duration: None,
        labels: Default::default(),
        enable_message_ordering: false,
        expiration_policy: None,
        filter: Default::default(),
        dead_letter_policy: None,
        retry_policy: None,
        detached: false,
        enable_exactly_once_delivery: false,
        topic_message_retention_duration: None,
        state: 0,
    }
}
