mod api;
pub mod pubsub;

use crate::pubsub::publisher_server::PublisherServer;

use tonic::transport::server::Router;
use tonic::transport::Server;

use api::publisher::PublisherService;

pub fn make_server_builder() -> Router {
    let publisher_service = PublisherService::default();

    Server::builder().add_service(PublisherServer::new(publisher_service))
}
