use crate::subscriptions::Subscription;
use std::sync::Arc;

/// Result for listing subscriptions.
pub struct SubscriptionsPage {
    /// The subscriptions on the page.
    pub subscriptions: Vec<Arc<Subscription>>,
    /// The offset to use for getting the next page.
    pub offset: Option<usize>,
}

impl SubscriptionsPage {
    /// Creates a new `SubscriptionsPage`.
    pub fn new(subscriptions: Vec<Arc<Subscription>>, offset: Option<usize>) -> Self {
        Self {
            subscriptions,
            offset,
        }
    }
}
