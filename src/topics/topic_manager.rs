use crate::paging::Paging;
use crate::topics::paging::TopicsPage;
use crate::topics::*;
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// Provides an interface over the topic manager actor.
pub struct TopicManager {
    /// The topics state.
    state: Arc<RwLock<State>>,
}

/// The topic manager internal state.
struct State {
    /// The topics map.
    pub topics: HashMap<TopicName, Arc<Topic>>,
    /// The next ID for when creating a topic.
    pub next_id: u32,
}

/// Provides callbacks from the topic actor to the topic manager,
/// such as for topic deletion.
pub struct TopicManagerDelegate {
    /// The topics state.
    state: Arc<RwLock<State>>,
}

impl TopicManager {
    /// Creates a new `TopicManager`.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::new())),
        }
    }

    /// Create a new topic.
    pub fn create_topic(&self, name: TopicName) -> Result<Arc<Topic>, CreateTopicError> {
        let delegate = TopicManagerDelegate::new(Arc::clone(&self.state));
        let mut state = self.state.write();
        state.create_topic(name, delegate)
    }

    /// Gets a topic.
    pub fn get_topic(&self, name: &TopicName) -> Result<Arc<Topic>, GetTopicError> {
        let state = self.state.read();
        state
            .topics
            .get(name)
            .cloned()
            .ok_or(GetTopicError::DoesNotExist)
    }

    /// Lists topics.
    pub fn list_topics(
        &self,
        project_id: Box<str>,
        paging: Paging,
    ) -> Result<TopicsPage, ListTopicsError> {
        let skip_value = paging.to_skip();

        // We need to collect them into a vec to sort.
        // NOTE: If this ever becomes a bottleneck, we can use another level
        // of `HashMap` of project ID -> topics.
        // However, we generally only use a single project ID, so probably not worth it.
        let mut topics_for_project = {
            // Hold the lock for as little time as possible.
            // It's fine that we'll be cloning the topics here since
            // listing topics is not a frequent operation.
            let state = self.state.read();
            let topics = state
                .topics
                .values()
                .filter(|t| t.name.is_in_project(&project_id))
                .cloned()
                .collect::<Vec<_>>();
            topics
        };

        // We know that no topics are considered equal because
        // we have a monotonically increasing ID that is used for
        // comparisons.
        topics_for_project.sort_unstable();

        // Now apply pagination.
        let topics_for_project = topics_for_project
            .into_iter()
            .skip(skip_value)
            .take(paging.size())
            .collect::<Vec<_>>();

        let next_page = paging.next_page_from_slice_result(&topics_for_project);
        let page = TopicsPage::new(topics_for_project, next_page.offset());
        Ok(page)
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    /// Creates a new `State`.
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            next_id: 1,
        }
    }

    /// Creates a new `Topic`.
    ///
    /// This is implemented here for interior mutability.
    pub fn create_topic(
        &mut self,
        name: TopicName,
        delegate: TopicManagerDelegate,
    ) -> Result<Arc<Topic>, CreateTopicError> {
        if let Entry::Vacant(entry) = self.topics.entry(name.clone()) {
            let topic_info = TopicInfo::new(name);
            self.next_id += 1;
            let internal_id = self.next_id;
            let topic = Arc::new(Topic::new(delegate, topic_info, internal_id));

            entry.insert(Arc::clone(&topic));
            return Ok(topic);
        }

        Err(CreateTopicError::AlreadyExists)
    }
}

impl TopicManagerDelegate {
    /// Creates a new `TopicManagerDelegate`.
    fn new(state: Arc<RwLock<State>>) -> Self {
        Self { state }
    }

    /// Delete the topic from the state.
    pub fn delete(&self, topic_name: &TopicName) {
        let mut state = self.state.write();
        state.topics.remove(topic_name);
    }
}
