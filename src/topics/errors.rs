/// Errors for creating a topic.
#[derive(thiserror::Error, Debug)]
pub enum CreateTopicError {
    #[error("The topic already exists")]
    AlreadyExists,

    #[error("The topic manager is closed")]
    Closed,
}

/// Errors for getting a topic.
#[derive(thiserror::Error, Debug)]
pub enum GetTopicError {
    #[error("The topic does not exists")]
    DoesNotExist,

    #[error("The topic manager is closed")]
    Closed,
}

/// Errors for listing topics.
#[derive(thiserror::Error, Debug)]
pub enum ListTopicsError {
    #[error("The topic manager is closed")]
    Closed,
}

/// Errors for publishing messages.
#[derive(thiserror::Error, Debug)]
pub enum PublishMessagesError {
    #[error("The topic does not exist")]
    TopicDoesNotExist,

    #[error("The topic is closed")]
    Closed,
}

/// Errors for attaching a subscription.
#[derive(thiserror::Error, Debug)]
pub enum AttachSubscriptionError {
    #[error("The topic is closed")]
    Closed,
}