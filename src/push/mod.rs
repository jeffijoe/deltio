pub mod push_loop;

use crate::subscriptions::{PushConfig, SubscriptionName};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Used for registering, unregistering and querying for push subscriptions.
///
/// Since push subscriptions are done at an interval rather than in real-time,
/// we need the ability to retrieve the registered subscriptions in order to pull
/// messages from them. `Subscription`s have a reference to the registry and
/// may register or unregister themselves based on config changes.
#[derive(Clone)]
pub struct PushSubscriptionsRegistry {
    /// The registry state.
    ///
    /// We use an `RwLock` because the values are only modified
    /// when push subscriptions are created or their push config is updated.
    state: Arc<RwLock<PushSubscriptionsRegistryState>>,
}

/// Maintains a map of subscriptions that are configured for HTTP push
/// as well as their push configuration.
struct PushSubscriptionsRegistryState {
    /// A map of subscriptions that have HTTP push enabled.
    pub push_subscriptions: HashMap<SubscriptionName, PushConfig>,
}

impl PushSubscriptionsRegistry {
    /// Creates a new `PushSubscriptionsRegistry`.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(PushSubscriptionsRegistryState {
                push_subscriptions: HashMap::default(),
            })),
        }
    }

    /// Sets the push config for the given subscription ID.
    /// If `config` is `None`, removes the current entry.
    pub fn set(&self, name: SubscriptionName, config: Option<PushConfig>) {
        let mut state = self.state.write();
        if let Some(config) = config {
            state.push_subscriptions.entry(name).or_insert(config);
        } else {
            let _ = state.push_subscriptions.remove(&name);
        }
    }

    /// Gets the entries in the registry.
    pub fn entries(&self) -> Vec<(SubscriptionName, PushConfig)> {
        let state = self.state.read();
        state
            .push_subscriptions
            .iter()
            .map(|(id, pc)| (id.clone(), pc.clone()))
            .collect::<Vec<_>>()
    }
}

impl Default for PushSubscriptionsRegistry {
    fn default() -> Self {
        Self::new()
    }
}
