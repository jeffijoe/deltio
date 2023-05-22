mod api;
mod collections;
pub mod paging;
pub mod pubsub_proto;
pub mod subscriptions;
pub mod topics;
mod tracing;

use crate::api::subscriber::SubscriberService;
use crate::pubsub_proto::publisher_server::PublisherServer;
use crate::pubsub_proto::subscriber_server::SubscriberServer;
use crate::subscriptions::subscription_manager::SubscriptionManager;
use crate::topics::topic_manager::TopicManager;
use api::publisher::PublisherService;
use std::sync::Arc;
use tonic::transport::server::Router;
use tonic::transport::Server;

#[cfg(not(all(target_arch = "x86", target_os = "linux")))]
use mimalloc::MiMalloc;

#[cfg(not(all(target_arch = "x86", target_os = "linux")))]
// Use MiMalloc as the global allocator for supported targets.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Creates a server builder configured with the Pub/Sub gRPC services.
pub fn make_server_builder() -> Router {
    let topic_manager = Arc::new(TopicManager::new());
    let subscription_manager = Arc::new(SubscriptionManager::new());

    let publisher_service = PublisherService::new(topic_manager.clone());
    let subscriber_service = SubscriberService::new(topic_manager, subscription_manager);

    Server::builder()
        .add_service(PublisherServer::new(publisher_service))
        .add_service(SubscriberServer::new(subscriber_service))
}
