use crate::topics::topic_actor::PublishMessagesResponse;
use crate::topics::topic_manager_actor::*;
use crate::topics::topic_message::TopicMessage;
use crate::topics::*;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub use self::topic_actor::PublishMessagesError;
pub use self::topic_manager_actor::{CreateTopicError, GetTopicError, ListTopicsError};

/// Provides an interface over the topic manager actor.
pub struct TopicManager {
    /// The inbox for the actor.
    sender: mpsc::Sender<TopicManagerRequest>,
}

impl TopicManager {
    /// Creates a new `TopicManager`.
    pub fn new() -> Self {
        Self {
            sender: TopicManagerActor::start(),
        }
    }

    /// Create a new topic.
    pub async fn create_topic(&self, name: TopicName) -> Result<TopicInfo, CreateTopicError> {
        let (send, recv) = oneshot::channel();
        let request = TopicManagerRequest::CreateTopic {
            name,
            responder: send,
        };

        self.sender
            .send(request)
            .await
            .map_err(|_| CreateTopicError::Closed)?;
        recv.await.map_err(|_| CreateTopicError::Closed)?
    }

    /// Gets a topic.
    pub async fn get_topic(&self, name: TopicName) -> Result<TopicInfo, GetTopicError> {
        let (send, recv) = oneshot::channel();
        let request = TopicManagerRequest::GetTopic {
            name,
            responder: send,
        };

        self.sender
            .send(request)
            .await
            .map_err(|_| GetTopicError::Closed)?;
        recv.await.map_err(|_| GetTopicError::Closed)?
    }

    /// Lists topics.
    pub async fn list_topics(
        &self,
        project_id: Box<str>,
        page_size: usize,
        page_offset: Option<usize>,
    ) -> Result<TopicsPage, ListTopicsError> {
        let (send, recv) = oneshot::channel();
        let request = TopicManagerRequest::ListTopics {
            project_id,
            page_size,
            page_offset,
            responder: send,
        };

        self.sender
            .send(request)
            .await
            .map_err(|_| ListTopicsError::Closed)?;
        recv.await.map_err(|_| ListTopicsError::Closed)?
    }

    /// Publishes messages to a topic.
    pub async fn publish_messages(
        &self,
        topic_name: TopicName,
        messages: Vec<TopicMessage>,
    ) -> Result<PublishMessagesResponse, PublishMessagesError> {
        let (send, recv) = oneshot::channel();
        let request = TopicManagerRequest::PublishMessages {
            topic_name,
            messages,
            responder: send,
        };

        self.sender
            .send(request)
            .await
            .map_err(|_| PublishMessagesError::Closed)?;

        recv.await.map_err(|_| PublishMessagesError::Closed)?
    }
}
