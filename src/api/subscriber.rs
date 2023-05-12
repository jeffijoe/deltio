use crate::api::page_token::PageToken;
use crate::api::parser;
use crate::pubsub_proto::subscriber_server::Subscriber;
use crate::pubsub_proto::{
    AcknowledgeRequest, CreateSnapshotRequest, DeleteSnapshotRequest, DeleteSubscriptionRequest,
    GetSnapshotRequest, GetSubscriptionRequest, ListSnapshotsRequest, ListSnapshotsResponse,
    ListSubscriptionsRequest, ListSubscriptionsResponse, ModifyAckDeadlineRequest,
    ModifyPushConfigRequest, PubsubMessage, PullRequest, PullResponse, ReceivedMessage,
    SeekRequest, SeekResponse, Snapshot, StreamingPullRequest, StreamingPullResponse, Subscription,
    UpdateSnapshotRequest, UpdateSubscriptionRequest,
};
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::subscriptions::{
    AcknowledgeMessagesError, CreateSubscriptionError, DeleteError, GetSubscriptionError,
    ListSubscriptionsError, ModifyDeadlineError, PullMessagesError, PulledMessage,
    SubscriptionName,
};
use crate::topics::topic_manager::TopicManager;
use crate::topics::GetTopicError;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

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
        let start = std::time::SystemTime::now();
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
                GetTopicError::Closed => conflict(),
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
                CreateSubscriptionError::Closed => conflict(),
            })?;
        println!(
            "{}: creating subscription took {:?}",
            subscription_name.clone(),
            start.elapsed()
        );
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
                GetSubscriptionError::DoesNotExist => subscription_not_found(&subscription_name),
                GetSubscriptionError::Closed => conflict(),
            })?;

        Ok(Response::new(map_to_subscription_resource(&subscription)))
    }

    async fn update_subscription(
        &self,
        _request: Request<UpdateSubscriptionRequest>,
    ) -> Result<Response<Subscription>, Status> {
        Err(Status::unimplemented(
            "UpdateSubscription is not implemented in Deltio",
        ))
    }

    async fn list_subscriptions(
        &self,
        request: Request<ListSubscriptionsRequest>,
    ) -> Result<Response<ListSubscriptionsResponse>, Status> {
        let request = request.get_ref();
        let paging = parser::parse_paging(request.page_size, &request.page_token)?;

        let page = self
            .subscription_manager
            .list_subscriptions_in_project(Box::from(request.project.clone()), paging)
            .map_err(|e| match e {
                ListSubscriptionsError::Closed => conflict(),
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
        request: Request<DeleteSubscriptionRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.get_ref();
        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        println!("{}: deleting subscription", &subscription_name);
        subscription.delete().await.map_err(|e| match e {
            DeleteError::Closed => conflict(),
        })?;

        Ok(Response::new(()))
    }

    async fn modify_ack_deadline(
        &self,
        request: Request<ModifyAckDeadlineRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.get_ref();
        let now = std::time::SystemTime::now();
        let deadline_modifications = parser::parse_deadline_modifications(
            now,
            &request.ack_ids,
            &request
                .ack_ids
                .iter()
                .map(|_| request.ack_deadline_seconds)
                .collect::<Vec<_>>(),
        )?;
        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        subscription
            .modify_ack_deadlines(deadline_modifications)
            .await
            .map_err(|e| match e {
                ModifyDeadlineError::Closed => Status::internal("System is shutting down"),
            })?;

        Ok(Response::new(()))
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        let start = std::time::SystemTime::now();
        let request = request.get_ref();
        let ack_ids = request
            .ack_ids
            .iter()
            .map(|ack_id| parser::parse_ack_id(ack_id))
            .collect::<Result<Vec<_>, Status>>()?;

        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        subscription
            .acknowledge_messages(ack_ids.clone())
            .await
            .map_err(|e| match e {
                AcknowledgeMessagesError::Closed => Status::internal("System is shutting down"),
            })?;

        println!(
            "{}: ack {} messages (took {:?})",
            &subscription_name,
            &ack_ids.len(),
            start.elapsed()
        );

        Ok(Response::new(()))
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let request = request.get_ref();
        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        // Then, pull the available messages from the subscription.
        let pulled = subscription
            .pull_messages(request.max_messages as u16)
            .await
            .map_err(|e| match e {
                PullMessagesError::Closed => conflict(),
            })?;

        // Map them to the protocol format.
        let received_messages = pulled
            .iter()
            .map(map_to_received_message)
            .collect::<Vec<_>>();

        println!(
            "{}: pulled {} messages",
            &subscription_name,
            received_messages.len()
        );

        Ok(Response::new(PullResponse { received_messages }))
    }

    type StreamingPullStream =
        Pin<Box<dyn Stream<Item = Result<StreamingPullResponse, Status>> + Send>>;

    async fn streaming_pull(
        &self,
        request: Request<Streaming<StreamingPullRequest>>,
    ) -> Result<Response<Self::StreamingPullStream>, Status> {
        let start = std::time::SystemTime::now();
        let mut stream = request.into_inner();
        let request = match stream.next().await {
            None => return Err(Status::cancelled("The request was canceled")),
            Some(req) => req?,
        };

        let subscription_name = parser::parse_subscription_name(&request.subscription)?;
        let subscription = get_subscription(&self.subscription_manager, &subscription_name)?;

        println!(
            "{}: starting streaming pull took {:?}",
            subscription_name,
            start.elapsed()
        );

        // Pulls messages and streams them to the client.
        let pull_stream = {
            let subscription = Arc::clone(&subscription);
            let max_count: u16 = request.max_outstanding_messages.try_into().map_err(|_| {
                Status::invalid_argument("max_outstanding_messages is out of range")
            })?;

            async_stream::try_stream! {
                // TODO: Respect the max_* settings if possible?
                let mut was_deleted = false;
                while !was_deleted {
                    // First, subscribe to the messages signal so that
                    // any new messages from this point forward will trigger
                    // the notification.
                    let signal = subscription.messages_available();

                    // Subscribe to the deletion signal.
                    let deleted = subscription.deleted();

                    // Then, pull the available messages from the subscription.
                    let pulled = match subscription.pull_messages(max_count).await {
                        Err(PullMessagesError::Closed) => return,
                        Ok(pulled) => pulled,
                    };

                    // Map them to the protocol format.
                    let received_messages = pulled
                        .iter()
                        .map(map_to_received_message)
                        .collect::<Vec<_>>();

                    // If we received any messages, yield them back.
                    if !received_messages.is_empty() {
                        println!(
                            "{}: streaming-pulled {} messages",
                            &subscription_name,
                            received_messages.len()
                        );
                        yield StreamingPullResponse {
                            received_messages,
                            acknowledge_confirmation: None,
                            modify_ack_deadline_confirmation: None,
                            subscription_properties: None,
                        };
                    }

                    // Wait for the next signal and do it all over again.
                    // If the subscription is deleted while we wait, return a not found.
                    was_deleted = tokio::select! {
                        _ = signal => false,
                        _ = deleted => true
                    };
                }

                // If we ever get here, it means the loop above broke due to
                // the subscription being deleted.
                yield Err(subscription_not_found(&subscription_name))?;
            }
        };

        // Handles requests from the client after the initial request.
        let push_stream = {
            let subscription = Arc::clone(&subscription);

            async_stream::try_stream! {
                while let Some(request) = stream.next().await {
                    let request = request?;
                    let response =
                        handle_streaming_pull_request(request, Arc::clone(&subscription)).await?;

                    if let Some(response) = response {
                        yield response;
                    }
                }
            }
        };

        // Create a stream from the channel.
        let output = push_stream.merge(pull_stream);
        Ok(Response::new(Box::pin(output) as Self::StreamingPullStream))
    }

    async fn modify_push_config(
        &self,
        _request: Request<ModifyPushConfigRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "ModifyPushConfig is not implemented in Deltio",
        ))
    }

    async fn get_snapshot(
        &self,
        _request: Request<GetSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "GetSnapshot is not implemented in Deltio",
        ))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented(
            "ListSnapshots is not implemented in Deltio",
        ))
    }

    async fn create_snapshot(
        &self,
        _request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "CreateSnapshot is not implemented in Deltio",
        ))
    }

    async fn update_snapshot(
        &self,
        _request: Request<UpdateSnapshotRequest>,
    ) -> Result<Response<Snapshot>, Status> {
        Err(Status::unimplemented(
            "UpdateSnapshot is not implemented in Deltio",
        ))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "DeleteSnapshot is not implemented in Deltio",
        ))
    }

    async fn seek(&self, _request: Request<SeekRequest>) -> Result<Response<SeekResponse>, Status> {
        Err(Status::unimplemented("Seek is not implemented in Deltio"))
    }
}

/// Handles the control message for a streaming pull request.
///
/// This is only called for **subsequent** requests on the incoming stream.
async fn handle_streaming_pull_request(
    request: StreamingPullRequest,
    subscription: Arc<crate::subscriptions::Subscription>,
) -> Result<Option<StreamingPullResponse>, Status> {
    if !request.subscription.is_empty() {
        return Err(Status::invalid_argument(
            "subscription must only be specified in the initial request.",
        ));
    }

    if request.max_outstanding_bytes > 0 {
        return Err(Status::invalid_argument(
            "max_outstanding_bytes must only be specified in the initial request.",
        ));
    }

    if request.max_outstanding_messages > 0 {
        return Err(Status::invalid_argument(
            "max_outstanding_messages must only be specified in the initial request.",
        ));
    }

    if request.modify_deadline_seconds.len() != request.modify_deadline_ack_ids.len() {
        return Err(Status::invalid_argument(
            "modify_deadline_seconds and modify_deadline_ack_ids must be the same length",
        ));
    }

    // Ack messages if appropriate.
    if !request.ack_ids.is_empty() {
        let ack_ids = request
            .ack_ids
            .iter()
            .map(|ack_id| parser::parse_ack_id(ack_id))
            .collect::<Result<Vec<_>, Status>>()?;

        subscription
            .acknowledge_messages(ack_ids)
            .await
            .map_err(|e| match e {
                AcknowledgeMessagesError::Closed => conflict(),
            })?;
    }

    // Extend deadlines if requested to do so.
    if !request.modify_deadline_ack_ids.is_empty() {
        let now = std::time::SystemTime::now();
        let deadline_modifications = parser::parse_deadline_modifications(
            now,
            &request.modify_deadline_ack_ids,
            &request.modify_deadline_seconds,
        )?;

        subscription
            .modify_ack_deadlines(deadline_modifications)
            .await
            .map_err(|e| match e {
                ModifyDeadlineError::Closed => conflict(),
            })?;
    }

    // We never actually return any responses, but it helps with the necessary type inference
    // for the async stream.
    Ok(None)
}

/// Helper function to get a subscription from the subscription manager.
fn get_subscription(
    subscription_manager: &Arc<SubscriptionManager>,
    subscription_name: &SubscriptionName,
) -> Result<Arc<crate::subscriptions::Subscription>, Status> {
    subscription_manager
        .get_subscription(subscription_name)
        .map_err(|e| match e {
            GetSubscriptionError::DoesNotExist => subscription_not_found(subscription_name),
            GetSubscriptionError::Closed => Status::internal("System is shutting down"),
        })
}

/// Maps a pulled message to a received message (protocol representation).
fn map_to_received_message(m: &PulledMessage) -> ReceivedMessage {
    ReceivedMessage {
        ack_id: m.ack_id().to_string(),
        delivery_attempt: 0, // m.delivery_attempt() as i32,
        message: {
            let message = m.message();
            Some(PubsubMessage {
                attributes: Default::default(),
                publish_time: Some(prost_types::Timestamp::from(message.published_at)),
                ordering_key: String::default(),
                message_id: message.id.to_string(),
                data: message.data.to_vec(),
            })
        },
    }
}

/// Maps the subscription to a subscription resource.
fn map_to_subscription_resource(subscription: &crate::subscriptions::Subscription) -> Subscription {
    // The topic is stored as a weak reference on the subscription.
    // If it's no longer alive, then the topic was deleted.
    let topic_name = subscription
        .topic
        .upgrade()
        .map(|t| t.info.name.to_string())
        .unwrap_or_else(|| "_deleted_topic_".to_string());

    Subscription {
        name: subscription.info.name.to_string(),
        topic: topic_name,
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

/// Status for when returned errors indicate that the resource is no longer
/// accepting requests, which usually indicates that it has been deleted, or
/// that the system is currently shutting down. The former is more likely.
#[inline]
fn conflict() -> Status {
    Status::failed_precondition("The operation resulted in a conflict.")
}

/// Returns a status indicating that the resource was not found.
#[inline]
fn subscription_not_found(subscription_name: &SubscriptionName) -> Status {
    Status::not_found(format!(
        "Resource not found (resource={}).",
        &subscription_name.subscription_id()
    ))
}
