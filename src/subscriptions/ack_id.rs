use std::fmt::{Display, Formatter};

/// Acknowledgement ID used for acknowledging and negative-acknowledging messages.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct AckId {
    value: u64,
}

/// Errors parsing an `AckId`.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum AckIdParseError {
    #[error("The ACK ID was malformed")]
    Malformed,
}

impl AckId {
    /// Creates a new `AckId` with the given value.
    pub fn new(value: u64) -> Self {
        Self { value }
    }

    /// Returns the next ack ID.
    pub fn next(&self) -> Self {
        Self {
            value: self.value + 1,
        }
    }

    /// Attempts to parse the given value into an `AckId`.
    pub fn parse(raw_value: &str) -> Result<Self, AckIdParseError> {
        raw_value
            .parse::<u64>()
            .map(Self::new)
            .map_err(|_| AckIdParseError::Malformed)
    }
}

impl Display for AckId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(AckId::parse("123").unwrap(), AckId::new(123));
        assert_eq!(
            AckId::parse("invalid").unwrap_err(),
            AckIdParseError::Malformed
        );
    }

    #[test]
    fn next() {
        assert_eq!(AckId::new(1).next(), AckId::new(2));
    }
}
