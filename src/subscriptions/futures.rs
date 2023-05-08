use futures::future::Shared;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::futures::Notified;
use tokio::sync::oneshot;

/// A future that signals when messages are available.
pub struct MessagesAvailable<'a>(Notified<'a>);

impl<'a> MessagesAvailable<'a> {
    /// Creates a new `MessagesAvailable`.
    pub(crate) fn new(notified: Notified<'a>) -> Self {
        Self(notified)
    }
}

impl<'a> Future for MessagesAvailable<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: This is okay because we are not moving anything.
        let notified = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        notified.poll(cx)
    }
}

/// A future that signals when the subscription has been deleted.
pub struct Deleted(Shared<oneshot::Receiver<()>>);

impl Deleted {
    /// Creates a new `Deleted`.
    pub fn new(recv: Shared<oneshot::Receiver<()>>) -> Self {
        Self(recv)
    }
}

impl Future for Deleted {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: This is okay because we are not moving anything.
        let recv = unsafe { self.map_unchecked_mut(|s| &mut s.0) };

        // We don't care about the result; once the channel has either been dropped
        // or it has been sent to, we want to resolve the future.
        recv.poll(cx).map(|_| ())
    }
}
