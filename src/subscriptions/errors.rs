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

/// Errors for publishing messages.
#[derive(thiserror::Error, Debug)]
pub enum PublishMessagesError {
    #[error("The subscription does not exist")]
    SubscriptionDoesNotExist,

    #[error("The subscription is closed")]
    Closed,
}
