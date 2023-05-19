use log::log_enabled;
use log::Level::Debug;
use std::fmt::{Display, Formatter};
use tokio::time::Instant;

/// Tracks the start time of an activity span.
/// This is used for measuring how long operations take
/// but only when the appropriate log level is set to avoid
/// a potentially expensive syscall.
pub struct ActivitySpan(Option<Instant>);

impl ActivitySpan {
    /// Starts a new activity span.
    pub fn start() -> Self {
        if log_enabled!(Debug) {
            Self(Some(Instant::now()))
        } else {
            Self(None)
        }
    }
}

impl Display for ActivitySpan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(started) = self.0 {
            write!(f, "({:?})", &started.elapsed())
        } else {
            Ok(())
        }
    }
}
