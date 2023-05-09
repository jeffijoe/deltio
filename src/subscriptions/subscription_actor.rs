use crate::collections::Messages;
use crate::subscriptions::errors::*;
use crate::subscriptions::futures::{Deleted, MessagesAvailable};
use crate::subscriptions::subscription_manager::SubscriptionManagerDelegate;
use crate::subscriptions::{
    AckId, AcknowledgeMessagesError, DeadlineModification, PulledMessage, SubscriptionInfo,
    SubscriptionStats,
};
use crate::topics::{RemoveSubscriptionError, Topic, TopicMessage};
use futures::future::Shared;
use futures::FutureExt;
use parking_lot::Mutex;
use std::collections::{hash_map, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot, Notify};

/// The max amount of messages that can be pulled.
const MAX_PULL_COUNT: u16 = 1_000;

/// Requests for the `SubscriptionActor`.
pub enum SubscriptionRequest {
    PostMessages {
        messages: Vec<Arc<TopicMessage>>,
    },
    PullMessages {
        max_count: u16,
        responder: oneshot::Sender<Result<Vec<PulledMessage>, PullMessagesError>>,
    },
    AcknowledgeMessages {
        ack_ids: Vec<AckId>,
        responder: oneshot::Sender<Result<(), AcknowledgeMessagesError>>,
    },
    ModifyDeadline {
        deadline_modifications: Vec<DeadlineModification>,
        responder: oneshot::Sender<Result<(), ModifyDeadlineError>>,
    },
    Delete {
        responder: oneshot::Sender<Result<(), DeleteError>>,
    },
    GetStats {
        responder: oneshot::Sender<Result<SubscriptionStats, GetStatsError>>,
    },
}

/// Actor for the subscription.
pub(crate) struct SubscriptionActor {
    /// The topic that the subscription is attached to.
    #[allow(dead_code)]
    topic: Arc<Topic>,

    /// Info about the subscription.
    info: SubscriptionInfo,

    /// A list of messages that are to be pulled.
    backlog: Messages,

    /// A map of messages have been pulled but not acked/nacked yet.
    outstanding: HashMap<AckId, PulledMessage>,

    /// An observer to notify of various things such as new messages being available.
    observer: Arc<SubscriptionObserver>,

    /// Used for communicating to the manager of changes to the subscription.
    delegate: SubscriptionManagerDelegate,

    /// The next ID to use as the ACK ID for a pulled message.
    next_ack_id: AckId,

    /// Whether the subscription has been marked as deleted.
    deleted: bool,
}

impl SubscriptionActor {
    /// Starts the actor.
    pub fn start(
        info: SubscriptionInfo,
        topic: Arc<Topic>,
        observer: Arc<SubscriptionObserver>,
        delegate: SubscriptionManagerDelegate,
    ) -> mpsc::Sender<SubscriptionRequest> {
        let (sender, mut receiver) = mpsc::channel(2048);
        let mut actor = Self {
            topic,
            info,
            observer,
            delegate,
            backlog: Messages::new(),
            outstanding: HashMap::new(),
            next_ack_id: AckId::new(1),
            deleted: false,
        };

        tokio::spawn(async move {
            let deleted = actor.observer.deleted();
            let poll = async {
                loop {
                    while let Some(request) = receiver.recv().await {
                        actor.receive(request).await
                    }
                }
            };
            tokio::select! {
                _ = deleted => (),
                _ = poll => (),
            }
        });

        sender
    }

    /// Receives a request.
    async fn receive(&mut self, request: SubscriptionRequest) {
        match request {
            SubscriptionRequest::PostMessages { messages } => {
                self.post_messages(messages).await;
            }
            SubscriptionRequest::PullMessages {
                max_count,
                responder,
            } => {
                let result = self.pull_messages(max_count).await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::AcknowledgeMessages { ack_ids, responder } => {
                let result = self.acknowledge_messages(ack_ids).await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::ModifyDeadline {
                deadline_modifications,
                responder,
            } => {
                let result = self.modify_deadline(deadline_modifications).await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::Delete { responder } => {
                let result = self.delete().await;
                let _ = responder.send(result);
            }
            SubscriptionRequest::GetStats { responder } => {
                let result = self.get_stats().await;
                let _ = responder.send(result);
            }
        }
    }

    /// Posts new messages to the subscription.
    async fn post_messages(&mut self, new_messages: Vec<Arc<TopicMessage>>) {
        if self.deleted {
            return;
        }

        self.backlog.append(new_messages);
        self.observer.notify_new_messages_available();
    }

    /// Pulls messages from the subscription, marking them as outstanding so they won't be
    /// delivered to anyone else.
    async fn pull_messages(
        &mut self,
        max_count: u16,
    ) -> Result<Vec<PulledMessage>, PullMessagesError> {
        if self.deleted {
            return Ok(Default::default());
        }

        let outgoing_len = self.backlog.len() as u16;
        let capacity = max_count.clamp(0, outgoing_len.max(MAX_PULL_COUNT)) as usize;
        let mut result = Vec::with_capacity(capacity);

        while let Some(message) = self.backlog.pop_front() {
            let ack_id = self.next_ack_id;
            self.next_ack_id = ack_id.next();

            // TODO: Compute based on subscription message ack deadline.
            // TODO: Handle deadline expiration.
            let deadline = SystemTime::now() + Duration::from_secs(10);

            let pulled_message = PulledMessage::new(Arc::clone(&message), ack_id, deadline, 1);
            result.push(pulled_message.clone());

            // Track the outstanding message so we can ACK it later (and also expire it).
            self.outstanding.insert(ack_id, pulled_message);

            if result.len() >= capacity {
                break;
            }
        }

        // If there are still messages left in the backlog, trigger another signal.
        if !self.backlog.is_empty() {
            self.observer.notify_new_messages_available();
        }

        Ok(result)
    }

    /// Acknowledges messages that have been pulled.
    async fn acknowledge_messages(
        &mut self,
        ack_ids: Vec<AckId>,
    ) -> Result<(), AcknowledgeMessagesError> {
        if self.deleted {
            return Ok(());
        }

        ack_ids
            .iter()
            .for_each(|ack_id| match self.outstanding.entry(*ack_id) {
                hash_map::Entry::Vacant(_) => (),
                hash_map::Entry::Occupied(occupied) => {
                    occupied.remove();
                }
            });

        Ok(())
    }

    /// Modifies the deadline for messages that have been pulled.
    async fn modify_deadline(
        &mut self,
        deadline_modifications: Vec<DeadlineModification>,
    ) -> Result<(), ModifyDeadlineError> {
        if self.deleted {
            return Ok(());
        }

        let nacks = deadline_modifications
            .into_iter()
            .filter_map(|modification| match modification.new_deadline {
                Some(_) => {
                    eprintln!("Deadline extension not supported yet");
                    None
                }
                None => self.nack_message(modification.ack_id),
            })
            .collect::<Vec<_>>();

        self.backlog.append(nacks);
        if !self.backlog.is_empty() {
            self.observer.notify_new_messages_available();
        }

        Ok(())
    }

    /// Marks the subscription as deleted. Further requests will be no-ops.
    async fn delete(&mut self) -> Result<(), DeleteError> {
        if self.deleted {
            return Ok(());
        }

        self.deleted = true;
        self.topic
            .remove_subscription(self.info.name.clone())
            .await
            .map_err(|e| match e {
                RemoveSubscriptionError::Closed => DeleteError::Closed,
            })?;
        self.delegate.delete(&self.info.name);
        self.observer.notify_deleted();

        Ok(())
    }

    /// Gets the stats for the subscription.
    async fn get_stats(&mut self) -> Result<SubscriptionStats, GetStatsError> {
        let stats = SubscriptionStats::new(
            self.info.name.clone(),
            self.topic.info.name.clone(),
            self.outstanding.len(),
            self.backlog.len(),
        );
        Ok(stats)
    }

    /// NACKs the outstanding message referred to by `ack_id` and returns it.
    #[inline]
    fn nack_message(&mut self, ack_id: AckId) -> Option<Arc<TopicMessage>> {
        let entry = match self.outstanding.entry(ack_id) {
            hash_map::Entry::Vacant(_) => return None,
            hash_map::Entry::Occupied(occupied) => occupied,
        };

        let message = Arc::clone(entry.get().message());
        entry.remove();
        Some(message)
    }
}

/// Observer for propagating signals to the `Subscription`.
pub(crate) struct SubscriptionObserver {
    /// Notifies when there are new messages to pull.
    notify_messages_available: Notify,

    /// Notifies when the subscription gets deleted.
    /// Used by consumers to cancel any in-progress long-running operations.
    deleted_recv: Shared<oneshot::Receiver<()>>,

    // See above.
    // This shouldn't impact performance since it's only used for deletion,
    // which happens at most once per subscription.
    deleted_send: Mutex<Option<oneshot::Sender<()>>>,
}

impl SubscriptionObserver {
    /// Creates a new `SubscriptionObserver`.
    pub fn new() -> Self {
        let (deleted_send, deleted_recv) = oneshot::channel();
        Self {
            deleted_send: Mutex::new(Some(deleted_send)),
            deleted_recv: deleted_recv.shared(),
            notify_messages_available: Notify::new(),
        }
    }

    /// Notifies of new messages being available.
    pub fn notify_new_messages_available(&self) {
        self.notify_messages_available.notify_one();
    }

    /// Notifies that the subscription was deleted.
    pub fn notify_deleted(&self) {
        // The oneshot sender is consumed when sending, so we need
        // to put it in an `Option` backed by a mutex.
        // First, acquire the lock and attempt to take out the value.
        // This will leave `None` in it's place, so if this method were to run
        // again, it would no-op.
        let taken = {
            let mut unlocked = self.deleted_send.lock();
            unlocked.take()
        };

        // If we were able to take out the sender, send the notification.
        if let Some(sender) = taken {
            let _ = sender.send(());
            // Also notify everyone waiting for messages.
            self.notify_messages_available.notify_waiters();
        }
    }

    /// Returns a signal for new messages.
    /// When new messages arrive, any waiters of the signal will be
    /// notified. The signal will be subscribed to immediately, so the time at which
    /// this method is called is important.
    pub fn new_messages_available(&self) -> MessagesAvailable {
        MessagesAvailable::new(self.notify_messages_available.notified())
    }

    /// Returns a signal for when the subscription is deleted.
    pub fn deleted(&self) -> Deleted {
        Deleted::new(Shared::clone(&self.deleted_recv))
    }
}
