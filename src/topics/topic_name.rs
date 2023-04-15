use std::fmt::{Display, Formatter};

const PROJECT_PREFIX: &'static str = "projects/";
const TOPIC_PREFIX: &'static str = "/topics/";
const PROJECT_PREFIX_LEN: usize = PROJECT_PREFIX.len();
const TOPIC_PREFIX_LEN: usize = TOPIC_PREFIX.len();

/// A `TopicName` contains the project and the topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicName {
    project_id: Box<str>,
    topic_id: Box<str>,
}

impl TopicName {
    /// Creates a new `TopicName`.
    pub fn new(project_id: &str, topic_id: &str) -> Self {
        Self {
            topic_id: topic_id.into(),
            project_id: project_id.into(),
        }
    }

    /// Attempts to parse a topic name.
    pub fn try_parse(unparsed: &str) -> Option<Self> {
        // Check that the length of the input is at least as long as something that contains
        // a valid topic name.
        if unparsed.len() <= PROJECT_PREFIX_LEN + TOPIC_PREFIX_LEN + 2 {
            return None;
        }

        // Check that we start with the topic prefix.
        if !unparsed.starts_with(PROJECT_PREFIX) {
            return None;
        }

        // Extract the project ID.
        let project_id = unparsed.get(PROJECT_PREFIX_LEN..)?;
        let project_id = project_id.get(..project_id.find('/')?)?;

        // Extract the topic ID
        let start = PROJECT_PREFIX_LEN + project_id.len() + TOPIC_PREFIX_LEN;
        let topic_id = unparsed.get(start..).map(|s| s.trim_matches('/'))?;

        Some(TopicName {
            project_id: project_id.into(),
            topic_id: topic_id.into(),
        })
    }

    /// Returns whether the topic is in the given project.
    pub fn is_in_project(&self, project_id: &str) -> bool {
        &*self.project_id == project_id
    }
}

impl Display for TopicName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}{}",
            PROJECT_PREFIX, self.project_id, TOPIC_PREFIX, self.topic_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid() {
        let parsed = TopicName::try_parse("projects/lets-go/topics/deltio");
        let expected = TopicName {
            project_id: "lets-go".into(),
            topic_id: "deltio".into(),
        };
        assert_eq!(parsed, Some(expected));
    }

    #[test]
    fn parse_invalid() {
        assert_eq!(TopicName::try_parse("projects/lets-go/topics"), None);
        assert_eq!(TopicName::try_parse("nope"), None);
        assert_eq!(TopicName::try_parse(""), None);
        assert_eq!(TopicName::try_parse("brojects/lets-go/topics/haha"), None);
    }

    #[test]
    fn to_string() {
        let topic_name = TopicName::new("lets-go", "deltio");
        let formatted = topic_name.to_string();
        assert_eq!(formatted, "projects/lets-go/topics/deltio");
    }
}
