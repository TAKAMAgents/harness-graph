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

    /// A graph namespace contained unsupported characters.
    #[error("graph namespace may contain only ASCII letters, digits, hyphen, and underscore")]
    InvalidGraphNamespace,

    /// A semantic collection that must contain evidence was empty.
    #[error("{field} must contain at least one item")]
    EmptyCollection {
        /// Semantic collection name.
        field: &'static str,
    },
}
