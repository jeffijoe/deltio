use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

/// Provides best-effort flow control for a streaming pull subscriber.
///
/// Once either of the conditions have been met, backpressure will be applied.
pub struct FlowControl {
    /// The max amount of outstanding bytes before applying backpressure.
    max_outstanding_bytes: u64,

    /// The max amount of outstanding messages before applying backpressure.
    max_outstanding_messages: u64,

    /// The current amount of outstanding bytes.
    outstanding_bytes: AtomicU64,

    /// The current amount of outstanding messages.
    outstanding_messages: AtomicU64,

    /// The notifier used for checking whether we are able to continue.
    notifier: Notify,
}

impl FlowControl {
    /// Returns a future that completes when there is space available.
    /// The returned value contains how much is available.
    pub async fn wait_for_available_space(&self) {
        // Check if we have space available right now.
        // Flow control doesn't actually trigger that frequently, so checking twice
        // is acceptable.
        if self.has_available_space() {
            return;
        }

        loop {
            // We didn't have space available; set up a notification
            // so we can wait for it and check again.
            let notified = self.notifier.notified();
            if self.has_available_space() {
                return;
            }
            notified.await;
        }
    }

    /// Increments the outstanding values.
    pub fn inc(&self, outstanding_bytes_delta: u64, outstanding_messages_delta: u64) {
        // We only need Acq/Rel ordering because our changes are commutative.
        self.outstanding_bytes
            .fetch_add(outstanding_bytes_delta, Ordering::AcqRel);
        self.outstanding_messages
            .fetch_add(outstanding_messages_delta, Ordering::AcqRel);
        self.notifier.notify_waiters();
    }

    /// Increments the outstanding values.
    pub fn dec(&self, outstanding_bytes_delta: u64, outstanding_messages_delta: u64) {
        // We only need Acq/Rel ordering because our changes are commutative.
        self.outstanding_bytes
            .fetch_sub(outstanding_bytes_delta, Ordering::AcqRel);
        self.outstanding_messages
            .fetch_sub(outstanding_messages_delta, Ordering::AcqRel);
        self.notifier.notify_waiters();
    }

    /// Checks whether there is available space.
    ///
    /// This uses atomic load operations. It is acceptable that we go above
    /// the limits.
    pub fn has_available_space(&self) -> bool {
        let available_messages = self.outstanding_messages.load(Ordering::Acquire);
        if available_messages >= self.max_outstanding_messages {
            return false;
        }

        let available_bytes = self.outstanding_bytes.load(Ordering::Acquire);
        if available_bytes >= self.max_outstanding_bytes {
            return false;
        }

        true
    }
}

/// Provides flow control for a streaming pull subscriber.
///
/// Once either of the conditions have been met, backpressure will be applied.
pub fn create(max_outstanding_bytes: u64, max_outstanding_messages: u64) -> FlowControl {
    let outstanding_bytes = AtomicU64::new(0);
    let outstanding_messages = AtomicU64::new(0);

    let notifier = Notify::new();

    let control = FlowControl {
        max_outstanding_messages,
        max_outstanding_bytes,
        outstanding_bytes,
        outstanding_messages,
        notifier,
    };

    #[allow(clippy::let_and_return)]
    control
}

#[cfg(test)]
#[allow(clippy::bool_assert_comparison)]
mod tests {
    use super::*;
    use futures::future;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn has_available_space() {
        let control = create(16, 5);
        assert_eq!(control.has_available_space(), true);

        control.inc(8, 1); // 8, 1
        assert_eq!(control.has_available_space(), true);

        control.inc(8, 1); // 16, 2
        assert_eq!(control.has_available_space(), false);

        control.dec(8, 1); // 8, 1
        assert_eq!(control.has_available_space(), true);

        control.inc(0, 5); // 8, 6
        assert_eq!(control.has_available_space(), false);

        control.dec(0, 2); // 8, 4
        assert_eq!(control.has_available_space(), true);
    }

    #[tokio::test]
    async fn wait_for_available_space() {
        let control = Arc::new(create(16, 5));

        // Completes right away as we have space available.
        wait(Arc::clone(&control)).await;

        // Max out.
        control.inc(16, 1);

        // Spawn 2 tasks that wait.
        let join_handle1 = tokio::spawn(wait(Arc::clone(&control)));
        let join_handle2 = tokio::spawn(wait(Arc::clone(&control)));

        // Sleep for a bit to make sure we trigger the waiting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Decrement so we have space available.
        control.dec(16, 1);

        // Wait for the spawned waiters to complete as it should get resumed.
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            future::join(join_handle1, join_handle2),
        )
        .await
        .unwrap();
    }

    // Helper for waiting.
    async fn wait(control: Arc<FlowControl>) {
        let join_handle = tokio::spawn({
            let control = Arc::clone(&control);
            async move {
                control.wait_for_available_space().await;
            }
        });

        tokio::select! {
            _ = join_handle => (),
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                  panic!("Timed out waiting for available space")
              }
        }
    }
}
