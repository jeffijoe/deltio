use crate::topics::*;
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// Provides an interface over the topic manager actor.
pub struct TopicManager {
    /// The topics state.
    state: RwLock<State>,
}

/// The topic manager internal state.
struct State {
    /// The topics map.
    pub topics: HashMap<TopicName, Arc<Topic>>,
    /// The next ID for when creating a topic.
    pub next_id: u32,
}

impl TopicManager {
    /// Creates a new `TopicManager`.
    pub fn new() -> Self {
        Self {
            state: RwLock::new(State::new()),
        }
    }

    /// Create a new topic.
    pub fn create_topic(&self, name: TopicName) -> Result<Arc<Topic>, CreateTopicError> {
        let mut state = self.state.write();
        state.create_topic(name)
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
        page_size: usize,
        page_offset: Option<usize>,
    ) -> Result<TopicsPage, ListTopicsError> {
        let skip_value = page_offset.unwrap_or(0);

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
                .filter(|t| t.info.name.is_in_project(&project_id))
                .cloned()
                .collect::<Vec<_>>();
            topics
        };

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
            .collect::<Vec<_>>();

        // If we got at least one element, then we want to return a new offset.
        let new_offset = if !topics_for_project.is_empty() {
            Some(skip_value + topics_for_project.len())
        } else {
            None
        };

        let page = TopicsPage::new(topics_for_project, new_offset);
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
    pub fn create_topic(&mut self, name: TopicName) -> Result<Arc<Topic>, CreateTopicError> {
        if let Entry::Vacant(entry) = self.topics.entry(name.clone()) {
            let topic_info = TopicInfo::new(name);
            self.next_id += 1;
            let internal_id = self.next_id;
            let topic = Arc::new(Topic::new(topic_info, internal_id));

            entry.insert(Arc::clone(&topic));
            return Ok(topic);
        }

        Err(CreateTopicError::AlreadyExists)
    }
}

/// Result for listing topics.
pub struct TopicsPage {
    /// The topics on the page.
    pub topics: Vec<Arc<Topic>>,
    /// The offset to use for getting the next page.
    pub offset: Option<usize>,
}

impl TopicsPage {
    /// Creates a new `TopicsPage`.
    pub fn new(topics: Vec<Arc<Topic>>, offset: Option<usize>) -> Self {
        Self { topics, offset }
    }
}
