mod api;
mod collections;
pub mod paging;
pub mod pubsub_proto;
pub mod push;
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
use std::time::Duration;
use tonic::transport::server::Router;
use tonic::transport::Server;

use crate::push::push_loop::PushLoop;
use crate::push::PushSubscriptionsRegistry;
#[cfg(not(all(target_arch = "x86", target_os = "linux")))]
use mimalloc::MiMalloc;

#[cfg(not(all(target_arch = "x86", target_os = "linux")))]
// Use MiMalloc as the global allocator for supported targets.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Represents the application and it's components.
pub struct Deltio {
    /// The push subscriptions registry.
    /// Used by subscriptions to register and unregister themselves,
    /// as well as the push loop in order to retrieve registered subscriptions
    /// and their push config.
    push_subscriptions_registry: PushSubscriptionsRegistry,

    /// The topic manager, which manages Pub/Sub topics.
    topic_manager: Arc<TopicManager>,

    /// The subscription manager, which manages Pub/Sub subscriptions.
    subscription_manager: Arc<SubscriptionManager>,
}

impl Deltio {
    /// Creates a new Deltio components wrapper.
    pub fn new() -> Self {
        let push_subscriptions_registry = PushSubscriptionsRegistry::new();
        Self {
            push_subscriptions_registry: push_subscriptions_registry.clone(),
            topic_manager: Arc::new(TopicManager::new()),
            subscription_manager: Arc::new(SubscriptionManager::new(push_subscriptions_registry)),
        }
    }

    /// Creates a Tonic gRPC server builder with the
    /// Pub/Sub gRPC services registered.
    pub fn server_builder(&self) -> Router {
        let publisher_service = PublisherService::new(Arc::clone(&self.topic_manager));
        let subscriber_service = SubscriberService::new(
            Arc::clone(&self.topic_manager),
            Arc::clone(&self.subscription_manager),
        );

        Server::builder()
            .add_service(PublisherServer::new(publisher_service))
            .add_service(SubscriberServer::new(subscriber_service))
    }

    /// Creates the push loop.
    pub fn push_loop(&self, interval: Duration) -> PushLoop {
        PushLoop::new(
            interval,
            Arc::clone(&self.subscription_manager),
            self.push_subscriptions_registry.clone(),
        )
    }
}

impl Default for Deltio {
    fn default() -> Self {
        Self::new()
    }
}
