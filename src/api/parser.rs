use crate::api::page_token::PageToken;
use crate::paging::Paging;
use crate::pubsub_proto::push_config::AuthenticationMethod;
use crate::pubsub_proto::{PubsubMessage, PushConfig as PushConfigProto};
use crate::subscriptions::{
    AckDeadline, AckId, AckIdParseError, DeadlineModification, PushConfig, PushConfigOidcToken,
    SubscriptionName,
};
use crate::topics::{TopicMessage, TopicName};
use bytes::Bytes;
use std::time::Duration;
use tokio::time::Instant;
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
        // Max is 600 seconds (10 minutes)
        v if v >= 600 => Ok(Some(Duration::from_secs(600))),
        0 => Ok(None),
        _ => Ok(Some(Duration::from_secs(raw_value as u64))),
    }
}

/// Parses a list of deadline modifications.
pub(crate) fn parse_deadline_modifications(
    now: Instant,
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
                Some(seconds) => {
                    let deadline = now + seconds;
                    DeadlineModification::new(ack_id, AckDeadline::new(&deadline))
                }
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

/// Parses a project ID form the format `projects/{project_id}`.
pub(crate) fn parse_project_id(raw_value: &str) -> Result<String, Status> {
    return parse(raw_value)
        .ok_or_else(|| Status::invalid_argument(format!("Invalid project name '{}'", &raw_value)));

    /// The inner function that parses an option.
    #[inline(always)]
    fn parse(raw_value: &str) -> Option<String> {
        const PROJECT_PREFIX: &str = "projects/";
        const PROJECT_PREFIX_LEN: usize = PROJECT_PREFIX.len();
        // Check that we start with the topic prefix.
        if !raw_value.starts_with(PROJECT_PREFIX) {
            return None;
        }

        // Extract the project ID.
        let project_id = raw_value.get(PROJECT_PREFIX_LEN..)?;
        Some(project_id.into())
    }
}

/// Parses a push config.
pub(crate) fn parse_push_config(push_config_proto: &PushConfigProto) -> Result<PushConfig, Status> {
    let endpoint = push_config_proto.push_endpoint.trim().to_string();
    if !endpoint.starts_with("http") {
        return Err(Status::invalid_argument("Unsupported push_endpoint"));
    }

    let oidc_token = push_config_proto
        .authentication_method
        .as_ref()
        .map(|method| match method {
            AuthenticationMethod::OidcToken(token) => PushConfigOidcToken {
                audience: token.audience.clone(),
                service_account_email: token.service_account_email.clone(),
            },
        });

    let attributes = match push_config_proto.attributes.len() {
        0 => None,
        _ => Some(push_config_proto.attributes.clone()),
    };

    Ok(PushConfig::new(endpoint, oidc_token, attributes))
}

/// Parses a `TopicMessage`.
pub(crate) fn parse_topic_message(message_proto: &PubsubMessage) -> TopicMessage {
    let data = Bytes::from(message_proto.data.clone());
    let attributes = match message_proto.attributes.len() {
        0 => None,
        _ => Some(message_proto.attributes.clone()),
    };

    TopicMessage::new(data, attributes)
}
