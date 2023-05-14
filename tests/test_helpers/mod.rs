use deltio::make_server_builder;
use deltio::pubsub_proto::publisher_client::PublisherClient;
use deltio::pubsub_proto::subscriber_client::SubscriberClient;
use deltio::pubsub_proto::{
    AcknowledgeRequest, ModifyAckDeadlineRequest, PublishRequest, PubsubMessage,
    StreamingPullRequest, StreamingPullResponse, Subscription, Topic,
};
use deltio::subscriptions::SubscriptionName;
use deltio::topics::TopicName;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;
use tower::service_fn;
use uuid::Uuid;

/// Error related to running a test host.
#[derive(thiserror::Error, Debug)]
pub enum TestHostError {
    #[error("Could not bind socket: {0}")]
    SocketError(std::io::Error),

    #[error("Could not connect to test server: {0}")]
    ClientError(tonic::transport::Error),

    #[error(transparent)]
    IOError(#[from] std::io::Error),
}

/// A `TestHost` is used for integration testing the PubSub server.
pub struct TestHost {
    /// The Publisher client.
    pub publisher: PublisherClient<Channel>,

    /// The Subscriber client.
    pub subscriber: SubscriberClient<Channel>,

    /// Used for waiting for the server to terminate.
    server_join_handle: tokio::task::JoinHandle<()>,

    /// Sender to send the shutdown signal to the server.
    shutdown_send: tokio::sync::oneshot::Sender<()>,
}

impl TestHost {
    /// Starts a new `TestHost`.
    pub async fn start() -> Result<Self, TestHostError> {
        // We'll use a unix socket instead of TCP as its cheaper.
        // A socket is represented as a file path, so we'll generate a new one in the
        // temp directory.
        let sock_file = {
            let dir = std::env::temp_dir().into_os_string().into_string().unwrap();
            format!("{}/{}.sock", dir, Uuid::new_v4())
        };

        // Start a listener stream, this is used by the server.
        let listener = UnixListener::bind(&sock_file).map_err(TestHostError::SocketError)?;
        let uds_stream = UnixListenerStream::new(listener);

        // Create a oneshot channel to trigger shutdown.
        let (shutdown_send, shutdown_recv) = tokio::sync::oneshot::channel::<()>();

        // Future for starting the server using the Unix socket and the shutdown signal.
        let server_fut = async move {
            let shutdown_fut = async { shutdown_recv.await.unwrap_or(()) };
            make_server_builder()
                .serve_with_incoming_shutdown(uds_stream, shutdown_fut)
                .await
                .unwrap();
        };

        // Poll the server future in a spawned task to start the server.
        let server_join_handle = tokio::spawn(server_fut);

        // Create a channel used for connecting to the server using
        // the Unix socket.
        let channel = Endpoint::try_from("http://doesnt.matter")
            .map_err(TestHostError::ClientError)?
            .connect_with_connector(service_fn(move |_| UnixStream::connect(sock_file.clone())))
            .await
            .map_err(TestHostError::ClientError)?;

        // Create the clients.
        let publisher = PublisherClient::new(channel.clone());
        let subscriber = SubscriberClient::new(channel);
        Ok(Self {
            publisher,
            subscriber,
            server_join_handle,
            shutdown_send,
        })
    }

    /// Disposes the test host and waits for it to terminate.
    pub async fn dispose(self) {
        self.shutdown_send.send(()).unwrap();
        self.server_join_handle.await.unwrap();
    }

    /// Creates a new topic with the given name.
    pub async fn create_topic_with_name(&mut self, topic_name: &TopicName) -> Topic {
        let response = self
            .publisher
            .create_topic(map_to_topic_resource(topic_name))
            .await
            .unwrap();
        response.get_ref().clone()
    }

    /// Creates a subscription with the given name for the specified topic.
    pub async fn create_subscription_with_name(
        &mut self,
        topic_name: &TopicName,
        subscription_name: &SubscriptionName,
    ) -> Subscription {
        let response = self
            .subscriber
            .create_subscription(map_to_subscription_resource(subscription_name, topic_name))
            .await
            .unwrap();
        response.get_ref().clone()
    }

    /// Publishes the given messages
    pub async fn publish_text_messages(
        &mut self,
        topic_name: &TopicName,
        messages: Vec<String>,
    ) -> Vec<String> {
        let response = self
            .publisher
            .publish(PublishRequest {
                topic: topic_name.to_string(),
                messages: messages
                    .iter()
                    .map(|content| PubsubMessage {
                        publish_time: None,
                        attributes: Default::default(),
                        message_id: Default::default(),
                        ordering_key: Default::default(),
                        data: content.as_bytes().to_vec(),
                    })
                    .collect(),
            })
            .await
            .unwrap();
        let response = response.get_ref();
        response.message_ids.clone()
    }

    pub async fn streaming_pull(
        &mut self,
        subscription_name: &SubscriptionName,
    ) -> (
        Sender<StreamingPullRequest>,
        Streaming<StreamingPullResponse>,
    ) {
        let client_id = Uuid::new_v4().to_string();
        let (send_request, mut outgoing) = tokio::sync::mpsc::channel::<StreamingPullRequest>(100);
        let subscription = subscription_name.to_string();
        let response = self
            .subscriber
            .streaming_pull(async_stream::stream! {
                // Sends the initial request to connect to the subscription.
                yield StreamingPullRequest {
                    subscription,
                    ack_ids: vec![],
                    modify_deadline_seconds: vec![],
                    modify_deadline_ack_ids: vec![],
                    stream_ack_deadline_seconds: 0,
                    client_id,
                    max_outstanding_messages: 100,
                    max_outstanding_bytes: 100_000_000,
                };

                // Deliver each request from the channel.
                while let Some(request) = outgoing.recv().await {
                    yield request;
                }
            })
            .await
            .unwrap();

        let inbound = response.into_inner();
        (send_request, inbound)
    }

    /// ACKs the given ack IDs using an RPC call.
    /// Used when we need an acknowledgment (no pun intended) of the ack.
    pub async fn ack(&mut self, subscription_name: &SubscriptionName, ack_ids: Vec<String>) {
        self.subscriber
            .acknowledge(AcknowledgeRequest {
                ack_ids,
                subscription: subscription_name.to_string(),
            })
            .await
            .unwrap();
    }

    /// Extend the deadline of the given ACK ids.
    /// Used when we need an acknowledgment (no pun intended) of the deadline extensions.
    pub async fn modify_deadlines(
        &mut self,
        subscription_name: &SubscriptionName,
        seconds: i32,
        ack_ids: Vec<String>,
    ) {
        self.subscriber
            .modify_ack_deadline(ModifyAckDeadlineRequest {
                ack_ids,
                ack_deadline_seconds: seconds,
                subscription: subscription_name.to_string(),
            })
            .await
            .unwrap();
    }
}

/// Maps the given parameters to a `Topic` resource.
pub fn map_to_topic_resource(topic_name: &TopicName) -> Topic {
    Topic {
        name: topic_name.to_string(),
        labels: Default::default(),
        message_storage_policy: None,
        kms_key_name: "".to_string(),
        schema_settings: None,
        satisfies_pzs: false,
        message_retention_duration: None,
    }
}

/// Maps the given parameters to a `Subscription` resource.
pub fn map_to_subscription_resource(
    subscription_name: &SubscriptionName,
    topic_name: &TopicName,
) -> Subscription {
    Subscription {
        name: subscription_name.to_string(),
        topic: topic_name.to_string(),
        push_config: None,
        bigquery_config: None,
        ack_deadline_seconds: 0,
        retain_acked_messages: false,
        message_retention_duration: None,
        labels: Default::default(),
        enable_message_ordering: false,
        expiration_policy: None,
        filter: "".to_string(),
        dead_letter_policy: None,
        retry_policy: None,
        detached: false,
        enable_exactly_once_delivery: false,
        topic_message_retention_duration: None,
        state: 0,
    }
}

/// Constructs a streaming ACK request.
pub fn streaming_ack(ack_ids: Vec<String>) -> StreamingPullRequest {
    StreamingPullRequest {
        ack_ids,
        subscription: Default::default(),
        modify_deadline_seconds: vec![],
        modify_deadline_ack_ids: vec![],
        stream_ack_deadline_seconds: 0,
        client_id: Default::default(),
        max_outstanding_messages: 0,
        max_outstanding_bytes: 0,
    }
}

/// Constructs a streaming NACK request.
pub fn streaming_nack(ack_ids: Vec<String>) -> StreamingPullRequest {
    StreamingPullRequest {
        ack_ids: Default::default(),
        modify_deadline_seconds: ack_ids.iter().map(|_| 0).collect(),
        modify_deadline_ack_ids: ack_ids,
        subscription: Default::default(),
        stream_ack_deadline_seconds: 0,
        client_id: Default::default(),
        max_outstanding_messages: 0,
        max_outstanding_bytes: 0,
    }
}

/// Constructs a streaming modify ack deadlines request.
pub fn streaming_modify_ack_deadline(ack_ids: Vec<String>, seconds: i32) -> StreamingPullRequest {
    StreamingPullRequest {
        ack_ids: Default::default(),
        modify_deadline_seconds: ack_ids.iter().map(|_| seconds).collect(),
        modify_deadline_ack_ids: ack_ids,
        subscription: Default::default(),
        stream_ack_deadline_seconds: 0,
        client_id: Default::default(),
        max_outstanding_messages: 0,
        max_outstanding_bytes: 0,
    }
}
