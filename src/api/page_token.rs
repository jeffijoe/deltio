use base64::{engine::general_purpose, Engine};

/// Represents a page token in gRPC calls.
#[derive(Debug)]
pub(crate) struct PageToken {
    /// Every page token is a number.
    value: usize,
}

impl PageToken {
    /// Creates a new `PageToken`.
    pub fn new(value: usize) -> Self {
        Self { value }
    }

    /// Encodes the page token.
    pub fn encode(&self) -> String {
        let bytes = self.value.to_ne_bytes();
        general_purpose::STANDARD.encode(bytes)
    }

    /// Decodes the page token.
    pub fn try_decode(encoded: &str) -> Option<PageToken> {
        let bytes = general_purpose::STANDARD.decode(encoded).ok()?;
        let value = usize::from_ne_bytes(bytes.try_into().ok()?);
        Some(PageToken::new(value))
    }
}

impl From<PageToken> for usize {
    fn from(value: PageToken) -> Self {
        value.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let token = PageToken::new(123);
        let encoded = token.encode();
        let decoded = PageToken::try_decode(&encoded).unwrap();

        assert_eq!(decoded.value, 123);
    }
}
