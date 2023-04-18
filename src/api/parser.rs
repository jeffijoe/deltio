use crate::subscriptions::SubscriptionName;
use crate::topics::TopicName;
use tonic::Status;

/// Parses the topic name.
pub fn parse_topic_name(raw_value: &str) -> Result<TopicName, Status> {
    TopicName::try_parse(&raw_value)
        .ok_or_else(|| Status::invalid_argument(format!("Invalid topic name '{}'", &raw_value)))
}

/// Parses the subscription name.
pub fn parse_subscription_name(raw_value: &str) -> Result<SubscriptionName, Status> {
    SubscriptionName::try_parse(&raw_value).ok_or_else(|| {
        Status::invalid_argument(format!("Invalid subscription name '{}'", &raw_value))
    })
}
