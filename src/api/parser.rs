use crate::api::page_token::PageToken;
use crate::paging::Paging;
use crate::subscriptions::{AckId, AckIdParseError, DeadlineModification, SubscriptionName};
use crate::topics::TopicName;
use std::time::{Duration, SystemTime};
use tonic::Status;

/// Parses the topic name.
pub(crate) fn parse_topic_name(raw_value: &str) -> Result<TopicName, Status> {
    TopicName::try_parse(raw_value)
        .ok_or_else(|| Status::invalid_argument(format!("Invalid topic name '{}'", &raw_value)))
}

/// Parses the subscription name.
pub(crate) fn parse_subscription_name(raw_value: &str) -> Result<SubscriptionName, Status> {
    SubscriptionName::try_parse(raw_value).ok_or_else(|| {
        Status::invalid_argument(format!("Invalid subscription name '{}'", &raw_value))
    })
}

/// Parses an ACK ID.
pub(crate) fn parse_ack_id(raw_value: &str) -> Result<AckId, Status> {
    AckId::parse(raw_value).map_err(|e| match e {
        AckIdParseError::Malformed => {
            Status::invalid_argument(format!("Invalid ack ID '{}'", &raw_value))
        }
    })
}

/// Parses a deadline extension duration.
pub(crate) fn parse_deadline_extension_duration(
    raw_value: i32,
) -> Result<Option<Duration>, Status> {
    match raw_value {
        v if v < 0 => Err(Status::invalid_argument(
            "Seconds must not be less than zero",
        )),
        0 => Ok(None),
        _ => Ok(Some(Duration::from_secs(raw_value as u64))),
    }
}

/// Parses a list of deadline modifications.
pub(crate) fn parse_deadline_modifications(
    now: SystemTime,
    ack_ids: &[String],
    modify_deadline_seconds: &[i32],
) -> Result<Vec<DeadlineModification>, Status> {
    ack_ids
        .iter()
        .zip(modify_deadline_seconds)
        .map(|(ack_id, seconds)| {
            let ack_id = parse_ack_id(ack_id)?;
            let seconds = parse_deadline_extension_duration(*seconds)?;
            let modification = match seconds {
                Some(seconds) => DeadlineModification::new(ack_id, now + seconds),
                None => DeadlineModification::nack(ack_id),
            };
            Ok(modification)
        })
        .collect::<Result<Vec<_>, Status>>()
}

/// Parses paging parameters.
pub(crate) fn parse_paging(size: i32, token: &str) -> Result<Paging, Status> {
    let page_token = parse_page_token(token)?;
    let page_size = size
        .try_into()
        .map_err(|_| Status::invalid_argument("Not a valid page size"))?;

    Ok(Paging::new(page_size, page_token.map(|p| p.into())))
}

/// Parses a page token.
pub(crate) fn parse_page_token(raw_value: &str) -> Result<Option<PageToken>, Status> {
    if raw_value.is_empty() {
        return Ok(None);
    }

    let decoded = PageToken::try_decode(raw_value)
        .ok_or_else(|| Status::invalid_argument("Page token malformed"))?;

    Ok(Some(decoded))
}
