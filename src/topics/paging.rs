use crate::topics::Topic;
use std::sync::Arc;

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
