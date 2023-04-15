use crate::topics::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

/// Requests for the `TopicManager`.
pub(crate) enum TopicManagerRequest {
    CreateTopic {
        name: TopicName,
        responder: oneshot::Sender<Result<TopicInfo, CreateTopicError>>,
    },
    GetTopic {
        name: TopicName,
        responder: oneshot::Sender<Result<Topic, GetTopicError>>,
    },
    ListTopics {
        project_id: Box<str>,
        page_size: usize,
        /// If specified, the offset to use for paging.
        /// For example, if the value is 1, then we only want to return
        /// items after 1.
        page_offset: Option<usize>,
        responder: oneshot::Sender<Result<TopicsPage, ListTopicsError>>,
    },
}

/// Errors for creating a topic.
#[derive(thiserror::Error, Debug)]
pub enum CreateTopicError {
    #[error("The topic already exists")]
    AlreadyExists,

    #[error("The topic manager is closed")]
    Closed,
}

/// Errors for getting a topic.
#[derive(thiserror::Error, Debug)]
pub enum GetTopicError {
    #[error("The topic does not exists")]
    DoesNotExist,

    #[error("The topic manager is closed")]
    Closed,
}

/// Errors for listing topics.
#[derive(thiserror::Error, Debug)]
pub enum ListTopicsError {
    #[error("The topic manager is closed")]
    Closed,
}

/// Result for listing topics.
pub struct TopicsPage {
    /// The topics on the page.
    pub topics: Vec<TopicInfo>,
    /// The offset to use for getting the next page.
    pub offset: Option<usize>,
}

impl TopicsPage {
    /// Creates a new `TopicsPage`.
    pub fn new(topics: Vec<TopicInfo>, offset: Option<usize>) -> Self {
        Self { topics, offset }
    }
}

/// The actor that manages the topics internally and communicates
/// through messages. It also maintains the state.
pub(crate) struct TopicManagerActor {
    topics: HashMap<TopicName, Topic>,
    /// The next ID for when creating a topic.
    next_id: u32,
    /// The receiver part of the request channel.
    receiver: mpsc::Receiver<TopicManagerRequest>,
}

impl TopicManagerActor {
    /// Starts a new actor.
    pub fn start() -> mpsc::Sender<TopicManagerRequest> {
        let (sender, receiver) = mpsc::channel::<_>(2048);
        let mut actor = Self {
            receiver,
            next_id: 0,
            topics: HashMap::new(),
        };
        tokio::spawn(async move {
            while let Some(request) = actor.receiver.recv().await {
                actor.receive(request)
            }
        });
        sender
    }

    fn receive(&mut self, request: TopicManagerRequest) {
        match request {
            TopicManagerRequest::CreateTopic { name, responder } => {
                let result = self.create_topic(name);
                let _ = responder.send(result);
            }
            TopicManagerRequest::GetTopic { name, responder } => {
                let result = self.get_topic(name);
                let _ = responder.send(result);
            }
            TopicManagerRequest::ListTopics {
                project_id,
                page_size,
                page_offset,
                responder,
            } => {
                let result = self.list_topics(&project_id, page_size, page_offset);
                let _ = responder.send(result);
            }
        };
    }

    fn get_topic(&self, name: TopicName) -> Result<Topic, GetTopicError> {
        self.topics
            .get(&name)
            .map(|c| c.clone())
            .ok_or(GetTopicError::DoesNotExist)
    }

    fn create_topic(&mut self, name: TopicName) -> Result<TopicInfo, CreateTopicError> {
        if let Entry::Vacant(entry) = self.topics.entry(name.clone()) {
            let topic_info = TopicInfo::new(name.clone());
            self.next_id += 1;
            let internal_id = self.next_id;
            let topic = Topic::new(topic_info.clone(), internal_id);
            entry.insert(topic);
            return Ok(topic_info);
        }

        Err(CreateTopicError::AlreadyExists)
    }

    fn list_topics(
        &self,
        project_id: &str,
        page_size: usize,
        offset: Option<usize>,
    ) -> Result<TopicsPage, ListTopicsError> {
        let skip_value = match offset {
            Some(v) => v,
            None => 0,
        };

        // We need to collect them into a vec to sort.
        // NOTE: If this ever becomes a bottleneck, we can use another level
        // of `HashMap` of project ID -> topics.
        let mut topics_for_project = self
            .topics
            .values()
            .into_iter()
            .filter(|t| t.info.name.is_in_project(&project_id))
            .collect::<Vec<_>>();

        // We know that no topics are considered equal because
        // we have a monotonically increasing ID that is used for
        // comparisons.
        topics_for_project.sort_unstable();

        // Cap the page size.
        let page_size = page_size.min(10_000);

        // Now apply pagination.
        let topics_for_project = topics_for_project
            .into_iter()
            .skip(skip_value)
            .take(page_size)
            .map(|t| t.info.clone())
            .collect::<Vec<_>>();

        // If we got at least one element, then we want to return a new offset.
        let new_offset = if topics_for_project.len() > 0 {
            Some(skip_value + topics_for_project.len())
        } else {
            None
        };

        let page = TopicsPage::new(topics_for_project, new_offset);
        Ok(page)
    }
    //
    // fn publish_messages(
    //     &mut self,
    //     topic_name: TopicName,
    //     messages: Vec<TopicMessage>,
    //     responder: oneshot::Sender<Result<PublishMessagesResponse, PublishMessagesError>>,
    // ) {
    //     if let Some(topic) = self.topics.get(&topic_name) {
    //         let request = PublishMessagesRequest {
    //             messages,
    //             responder,
    //         };
    //         topic.handle.send(TopicRequest::PublishMessages(request));
    //     } else {
    //         let _ = responder.send(Err(PublishMessagesError::TopicDoesNotExist));
    //     }
    // }
}
