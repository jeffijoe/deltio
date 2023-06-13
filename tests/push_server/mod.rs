use base64::Engine;
use std::convert::Infallible;
use std::error::Error;
use std::ops::Deref;

use deltio::push::push_loop::PushPayload;
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A test server for receiving push messages.
pub struct TestPushServer {
    /// The URL to register for push.
    url: String,
    /// The join handle for joining the server future.
    join_handle: JoinHandle<()>,
    /// Receiver for push payloads.
    recv: tokio::sync::mpsc::UnboundedReceiver<PushHandle>,
}

/// What to do with a pushed message.
#[derive(Debug)]
pub enum PushResult {
    Succeed,
    Fail,
}

/// A handle for controlling what to do with a pushed message.
pub struct PushHandle {
    /// The pushed payload.
    payload: PushPayload,
    /// Whether to succeed or fail the handler.
    responder: tokio::sync::oneshot::Sender<PushResult>,
}

impl TestPushServer {
    /// Starts a test push server.
    pub async fn start() -> Result<TestPushServer, Box<dyn Error>> {
        // Bind to a random available port.
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        let url = format!("http://{addr}");
        let (sender, recv) = tokio::sync::mpsc::unbounded_channel::<PushHandle>();

        let join_handle = tokio::spawn({
            async move {
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(r) => r,
                        Err(err) => {
                            println!("Failed to accept socket: {:?}", err);
                            continue;
                        }
                    };

                    let sender = sender.clone();
                    let handle_request = move |req: Request<hyper::body::Incoming>| {
                        let sender = sender.clone();
                        async move {
                            let body = req.collect().await.unwrap().aggregate();
                            let payload: PushPayload =
                                serde_json::from_reader(bytes::Buf::reader(body)).unwrap();
                            let (responder, recv) = tokio::sync::oneshot::channel();
                            let _ = sender.send(PushHandle::new(payload, responder));
                            let result = recv.await.expect("no response");

                            let mut resp = Response::new(Empty::<Bytes>::default());
                            *resp.status_mut() = match result {
                                PushResult::Succeed => StatusCode::OK,
                                PushResult::Fail => StatusCode::BAD_REQUEST,
                            };
                            Ok::<_, Infallible>(resp)
                        }
                    };

                    // Spawn a tokio task to serve multiple connections concurrently
                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new()
                            .serve_connection(stream, service_fn(handle_request))
                            .await
                        {
                            println!("Error serving connection: {:?}", err);
                        }
                    });
                }
            }
        });

        let server = TestPushServer {
            url,
            join_handle,
            recv,
        };
        Ok(server)
    }

    /// Returns the URL for the server.
    pub fn url(&self) -> String {
        self.url.clone()
    }

    /// Waits for the next push payload.
    pub async fn next(&mut self) -> Option<PushHandle> {
        self.recv.recv().await
    }

    /// Dispose the server.
    pub async fn dispose(self) {
        // Abort the running task.
        self.join_handle.abort();
        let _ = self.join_handle.await;
    }
}

impl PushHandle {
    /// Creates a new push handle.
    fn new(payload: PushPayload, responder: tokio::sync::oneshot::Sender<PushResult>) -> Self {
        Self { payload, responder }
    }

    /// Succeeds the pushed message.
    pub fn succeed(self) {
        self.responder
            .send(PushResult::Succeed)
            .expect("failed to send response");
    }

    /// Fails the pushed message.
    pub fn fail(self) {
        self.responder
            .send(PushResult::Fail)
            .expect("failed to send response");
    }
}

impl Deref for PushHandle {
    type Target = PushPayload;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

/// Decodes the data field from the push message.
pub fn decode_data(push_payload: &PushPayload) -> String {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&push_payload.message.data)
        .ok()
        .unwrap();
    String::from_utf8(decoded).unwrap()
}
