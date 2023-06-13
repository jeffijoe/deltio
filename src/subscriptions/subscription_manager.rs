use crate::paging::Paging;
use crate::push::PushSubscriptionsRegistry;
use crate::subscriptions::paging::SubscriptionsPage;
use crate::subscriptions::*;
use crate::topics::{AttachSubscriptionError, Topic};
use parking_lot::RwLock;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// Provides an interface over the subscription manager actor.
pub struct SubscriptionManager {
    /// The subscriptions state.
    state: Arc<RwLock<State>>,

    /// Registry for push subscriptions.
    push_registry: PushSubscriptionsRegistry,
}

/// The subscription manager internal state.
struct State {
    /// The subscriptions map.
    pub subscriptions: HashMap<SubscriptionName, Arc<Subscription>>,
    /// The next ID for when creating a subscription.
    pub next_id: u32,
}

/// Passed in by the subscription manager to subscriptions
/// in order to call back out.
pub struct SubscriptionManagerDelegate {
    /// The subscription state.
    state: Arc<RwLock<State>>,
}

impl SubscriptionManager {
    /// Creates a new `SubscriptionManager`.
    pub fn new(push_registry: PushSubscriptionsRegistry) -> Self {
        let state = Arc::new(RwLock::new(State::new()));
        Self {
            push_registry,
            state: Arc::clone(&state),
        }
    }

    /// Create a new subscription.
    pub async fn create_subscription(
        &self,
        info: SubscriptionInfo,
        topic: Arc<Topic>,
    ) -> Result<Arc<Subscription>, CreateSubscriptionError> {
        // Topics and subscriptions must be in the same project.
        if !topic.name.is_in_project(info.name.project_id()) {
            return Err(CreateSubscriptionError::MustBeInSameProjectAsTopic);
        }

        // Create the subscription and store it in state.
        let subscription = {
            let mut state = self.state.write();
            // Create a delegate that the subscription can use to call back out.
            let delegate = SubscriptionManagerDelegate::new(Arc::clone(&self.state));
            state.create_subscription(info, topic.clone(), self.push_registry.clone(), delegate)?
        };

        topic
            .attach_subscription(subscription.clone())
            .await
            .map_err(|e| match e {
                AttachSubscriptionError::Closed => CreateSubscriptionError::Closed,
            })?;

        Ok(subscription)
    }

    /// Gets a subscription.
    pub fn get_subscription(
        &self,
        name: &SubscriptionName,
    ) -> Result<Arc<Subscription>, GetSubscriptionError> {
        let state = self.state.read();
        state
            .subscriptions
            .get(name)
            .cloned()
            .ok_or(GetSubscriptionError::DoesNotExist)
    }

    /// Lists subscriptions.
    pub fn list_subscriptions_in_project(
        &self,
        project_id: Box<str>,
        paging: Paging,
    ) -> Result<SubscriptionsPage, ListSubscriptionsError> {
        // We need to collect them into a vec to sort.
        // NOTE: If this ever becomes a bottleneck, we can use another level
        // of `HashMap` of project ID -> subscriptions.
        // However, we generally only use a single project ID, so probably not worth it.
        let mut subscriptions_for_project = {
            // Hold the lock for as little time as possible.
            // It's fine that we'll be cloning the subscriptions here since
            // listing subscriptions is not a frequent operation.
            let state = self.state.read();
            let subscriptions = state
                .subscriptions
                .values()
                .filter(|t| t.name.is_in_project(&project_id))
                .cloned()
                .collect::<Vec<_>>();
            subscriptions
        };

        // We know that no subscriptions are considered equal because
        // we have a monotonically increasing ID that is used for
        // comparisons.
        subscriptions_for_project.sort_unstable();

        // Now apply pagination.
        let subscriptions_for_project = subscriptions_for_project
            .into_iter()
            .skip(paging.to_skip())
            .take(paging.size())
            .collect::<Vec<_>>();

        // If we got at least one element, then we want to return a new offset.
        let next_page = paging.next_page_from_slice_result(&subscriptions_for_project);

        let page = SubscriptionsPage::new(subscriptions_for_project, next_page.offset());
        Ok(page)
    }
}

impl State {
    /// Creates a new `State`.
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            next_id: 1,
        }
    }

    /// Creates a new `Subscription`.
    pub fn create_subscription(
        &mut self,
        info: SubscriptionInfo,
        topic: Arc<Topic>,
        push_registry: PushSubscriptionsRegistry,
        delegate: SubscriptionManagerDelegate,
    ) -> Result<Arc<Subscription>, CreateSubscriptionError> {
        if let Entry::Vacant(entry) = self.subscriptions.entry(info.name.clone()) {
            self.next_id += 1;
            let internal_id = self.next_id;
            let subscription = Arc::new(Subscription::new(
                info,
                internal_id,
                topic,
                push_registry,
                delegate,
            ));
            entry.insert(subscription.clone());
            return Ok(subscription);
        }

        Err(CreateSubscriptionError::AlreadyExists)
    }
}

impl SubscriptionManagerDelegate {
    /// Creates a new `SubscriptionManagerDelegate`.
    fn new(state: Arc<RwLock<State>>) -> Self {
        Self { state }
    }

    /// Deletes the subscription from the manager's state.
    pub fn delete(&self, name: &SubscriptionName) {
        let mut state = self.state.write();
        let _ = state.subscriptions.remove(name);
    }
}
