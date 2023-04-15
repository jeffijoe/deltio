use deltio::make_server_builder;
use deltio::pubsub_proto::publisher_client::PublisherClient;
use deltio::pubsub_proto::subscriber_client::SubscriberClient;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint};
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
        let subscriber = SubscriberClient::new(channel.clone());
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
}
