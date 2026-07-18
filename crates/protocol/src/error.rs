//! Protocol decoding failures.

use harness_graph_domain::{DomainError, RecordSequence};

/// Failure to decode one canonical Codex JSONL record.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// JSON syntax or shape was invalid.
    #[error("record {sequence:?} is not valid Codex JSON: {source}")]
    InvalidJson {
        /// Source record sequence.
        sequence: RecordSequence,
        /// Serde decoder error without raw payload content.
        #[source]
        source: serde_json::Error,
    },

    /// A required envelope or payload field was missing.
    #[error("record {sequence:?} is missing required field {field}")]
    MissingField {
        /// Source record sequence.
        sequence: RecordSequence,
        /// Static field name.
        field: &'static str,
    },

    /// A domain value failed validation.
    #[error("record {sequence:?} contains an invalid domain value: {source}")]
    InvalidDomainValue {
        /// Source record sequence.
        sequence: RecordSequence,
        /// Domain construction failure.
        #[source]
        source: DomainError,
    },

    /// Canonical payload serialization failed.
    #[error("record {sequence:?} payload could not be canonicalized: {source}")]
    Canonicalization {
        /// Source record sequence.
        sequence: RecordSequence,
        /// Serializer error.
        #[source]
        source: serde_json::Error,
    },
}
