mod api;
pub mod pubsub_proto;
pub mod topics;

use crate::pubsub_proto::publisher_server::PublisherServer;
use std::sync::Arc;

use tonic::transport::server::Router;
use tonic::transport::Server;

use crate::topics::topic_manager::TopicManager;
use api::publisher::PublisherService;

pub fn make_server_builder() -> Router {
    let topic_manager = Arc::new(TopicManager::new());
    let publisher_service = PublisherService::new(topic_manager);

    Server::builder().add_service(PublisherServer::new(publisher_service))
}
