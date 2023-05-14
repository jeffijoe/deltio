/// Errors for creating a subscription.
#[derive(thiserror::Error, Debug)]
pub enum CreateSubscriptionError {
    #[error("The subscription already exists")]
    AlreadyExists,

    #[error("The topic and the subscription must be in the same project")]
    MustBeInSameProjectAsTopic,

    #[error("The subscription manager is closed")]
    Closed,
}

/// Errors for getting a subscription.
#[derive(thiserror::Error, Debug)]
pub enum GetSubscriptionError {
    #[error("The subscription does not exists")]
    DoesNotExist,

    #[error("The subscription manager is closed")]
    Closed,
}

/// Errors for listing subscriptions.
#[derive(thiserror::Error, Debug)]
pub enum ListSubscriptionsError {
    #[error("The subscription manager is closed")]
    Closed,
}

/// Errors for getting subscription info.
#[derive(thiserror::Error, Debug)]
pub enum GetInfoError {
    #[error("The subscription is closed")]
    Closed,
}

/// Errors for posting messages.
#[derive(thiserror::Error, Debug)]
pub enum PostMessagesError {
    #[error("The subscription is closed")]
    Closed,
}

/// Errors for pulling messages.
#[derive(thiserror::Error, Debug)]
pub enum PullMessagesError {
    #[error("The subscription is closed")]
    Closed,
}

/// Errors for acknowledging messages.
#[derive(thiserror::Error, Debug)]
pub enum AcknowledgeMessagesError {
    #[error("The subscription is closed")]
    Closed,
}

/// Errors for modifying deadlines.
#[derive(thiserror::Error, Debug)]
pub enum ModifyDeadlineError {
    #[error("The subscription is closed")]
    Closed,
}

/// Errors for deleting subscriptions..
#[derive(thiserror::Error, Debug)]
pub enum DeleteError {
    #[error("The subscription is closed")]
    Closed,
}

/// Errors for getting stats.
#[derive(thiserror::Error, Debug)]
pub enum GetStatsError {
    #[error("The subscription is closed")]
    Closed,
}
