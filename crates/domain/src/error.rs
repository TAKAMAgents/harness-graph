//! Domain construction failures.

/// Failure to construct a domain value from untrusted input.
#[derive(Debug, thiserror::Error)]
pub enum DomainError {
    /// A value that must be non-empty was empty after trimming.
    #[error("{field} cannot be empty")]
    EmptyValue {
        /// Semantic field name.
        field: &'static str,
    },

    /// A session identifier was not a valid UUID.
    #[error("invalid session identifier: {source}")]
    InvalidSessionId {
        /// UUID parser error.
        #[source]
        source: uuid::Error,
    },

    /// A digest was not a valid SHA-256 hexadecimal string.
    #[error("invalid {kind} digest: expected 64 hexadecimal characters")]
    InvalidDigest {
        /// Digest kind.
        kind: &'static str,
    },

    /// A timestamp was not RFC 3339.
    #[error("invalid occurrence timestamp: {source}")]
    InvalidOccurredAt {
        /// Timestamp parser error.
        #[source]
        source: time::error::Parse,
    },
}
