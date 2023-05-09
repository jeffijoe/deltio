use std::fmt::{Display, Formatter};

const PROJECT_PREFIX: &str = "projects/";
const SUBSCRIPTION_PREFIX: &str = "/subscriptions/";
const PROJECT_PREFIX_LEN: usize = PROJECT_PREFIX.len();
const SUBSCRIPTION_PREFIX_LEN: usize = SUBSCRIPTION_PREFIX.len();

/// A `SubscriptionName` contains the project and the subscription.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionName {
    project_id: Box<str>,
    subscription_id: Box<str>,
}

impl SubscriptionName {
    /// Creates a new `SubscriptionName`.
    pub fn new(project_id: &str, subscription_id: &str) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            project_id: project_id.into(),
        }
    }

    /// Attempts to parse a subscription name.
    pub fn try_parse(unparsed: &str) -> Option<Self> {
        // Check that the length of the input is at least as long as something that contains
        // a valid subscription name.
        if unparsed.len() <= PROJECT_PREFIX_LEN + SUBSCRIPTION_PREFIX_LEN + 2 {
            return None;
        }

        // Check that we start with the subscription prefix.
        if !unparsed.starts_with(PROJECT_PREFIX) {
            return None;
        }

        // Extract the project ID.
        let project_id = unparsed.get(PROJECT_PREFIX_LEN..)?;
        let project_id = project_id.get(..project_id.find('/')?)?;

        // Extract the subscription ID
        let start = PROJECT_PREFIX_LEN + project_id.len() + SUBSCRIPTION_PREFIX_LEN;
        let subscription_id = unparsed.get(start..).map(|s| s.trim_matches('/'))?;

        Some(SubscriptionName {
            project_id: project_id.into(),
            subscription_id: subscription_id.into(),
        })
    }

    /// Returns whether the subscription is in the given project.
    pub fn is_in_project(&self, project_id: &str) -> bool {
        &*self.project_id == project_id
    }

    /// Returns a clone of the `project_id`.
    /// TODO: Try to avoid cloning
    pub fn project_id(&self) -> String {
        self.project_id.to_string()
    }

    /// Returns a clone of the `subscription_id`.
    pub fn subscription_id(&self) -> String {
        self.subscription_id.to_string()
    }
}

impl Display for SubscriptionName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}{}",
            PROJECT_PREFIX, self.project_id, SUBSCRIPTION_PREFIX, self.subscription_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid() {
        let parsed = SubscriptionName::try_parse("projects/lets-go/subscriptions/deltio");
        let expected = SubscriptionName {
            project_id: "lets-go".into(),
            subscription_id: "deltio".into(),
        };
        assert_eq!(parsed, Some(expected));
    }

    #[test]
    fn parse_invalid() {
        assert_eq!(
            SubscriptionName::try_parse("projects/lets-go/subscriptions"),
            None
        );
        assert_eq!(SubscriptionName::try_parse("nope"), None);
        assert_eq!(SubscriptionName::try_parse(""), None);
        assert_eq!(
            SubscriptionName::try_parse("brojects/lets-go/subscriptions/haha"),
            None
        );
    }

    #[test]
    fn to_string() {
        let subscription_name = SubscriptionName::new("lets-go", "deltio");
        let formatted = subscription_name.to_string();
        assert_eq!(formatted, "projects/lets-go/subscriptions/deltio");
    }
}
