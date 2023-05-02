use crate::collections::Messages;
use crate::subscriptions::{
    AckId, AcknowledgeMessagesError, DeadlineModification, GetStatsError, ModifyDeadlineError,
    PullMessagesError, PulledMessage, SubscriptionInfo, SubscriptionStats,
};
use crate::topics::{Topic, TopicMessage};
use std::collections::{hash_map, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};

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
    GetStats {
        responder: oneshot::Sender<Result<SubscriptionStats, GetStatsError>>,
    },
}

/// Actor for the subscription.
pub struct SubscriptionActor<S> {
    /// The topic that the subscription is attached to.
    #[allow(dead_code)]
    topic: Arc<Topic>,

    /// Info about the subscription.
    info: SubscriptionInfo,

    /// A list of messages that are to be pulled.
    backlog: Messages,

    /// A map of messages have been pulled but not acked/nacked yet.
    outstanding: HashMap<AckId, PulledMessage>,

    /// A signal that notifies of messages being available to pull in the backlog.
    signal_messages_available: S,

    /// The next ID to use as the ACK ID for a pulled message.
    next_ack_id: AckId,
}

impl<SignalNewMessages> SubscriptionActor<SignalNewMessages>
where
    SignalNewMessages: Fn() + Send + 'static,
{
    /// Starts the actor.
    pub fn start(
        info: SubscriptionInfo,
        topic: Arc<Topic>,
        signal_new_messages: SignalNewMessages,
    ) -> mpsc::Sender<SubscriptionRequest> {
        let (sender, mut receiver) = mpsc::channel(2048);
        let mut actor = Self {
            topic,
            info,
            signal_messages_available: signal_new_messages,
            backlog: Messages::new(),
            outstanding: HashMap::new(),
            next_ack_id: AckId::new(1),
        };

        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                actor.receive(request).await;
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
            SubscriptionRequest::GetStats { responder } => {
                let result = self.get_stats().await;
                let _ = responder.send(result);
            }
        }
    }

    /// Posts new messages to the subscription.
    async fn post_messages(&mut self, new_messages: Vec<Arc<TopicMessage>>) {
        self.backlog.append(new_messages);
        (self.signal_messages_available)();
    }

    /// Pulls messages from the subscription, marking them as outstanding so they won't be
    /// delivered to anyone else.
    async fn pull_messages(
        &mut self,
        max_count: u16,
    ) -> Result<Vec<PulledMessage>, PullMessagesError> {
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
        (self.signal_messages_available)();

        Ok(result)
    }

    /// Acknowledges messages that have been pulled.
    async fn acknowledge_messages(
        &mut self,
        ack_ids: Vec<AckId>,
    ) -> Result<(), AcknowledgeMessagesError> {
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
        (self.signal_messages_available)();

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
