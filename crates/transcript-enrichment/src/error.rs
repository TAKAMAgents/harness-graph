//! Source-safe transcript-enrichment failures.

use harness_graph_domain::{RecordSequence, SessionId};

/// Closed reason why mandatory local scanning blocked a source fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScannerBlockReason {
    /// NUL or disallowed control bytes indicate non-text content.
    NonTextControlData,
    /// Inline assets or encoded binary bodies are forbidden.
    AssetOrBinaryData,
    /// A suspicious encoded blob remained after deterministic redaction.
    SuspiciousEncodedBlob,
}

impl std::fmt::Display for ScannerBlockReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NonTextControlData => "non-text control data",
            Self::AssetOrBinaryData => "asset or binary data",
            Self::SuspiciousEncodedBlob => "suspicious encoded blob",
        })
    }
}

/// Failure in local transcript authorization, redaction, or chunking.
#[derive(Debug, thiserror::Error)]
pub enum TranscriptEnrichmentError {
    /// A required semantic value was empty.
    #[error("{field} cannot be empty")]
    EmptyValue {
        /// Safe semantic field name.
        field: &'static str,
    },

    /// Authorization identity was not a bounded source-safe identifier.
    #[error(
        "authorization identity must be 1-80 ASCII letters, digits, dot, underscore, colon, or hyphen"
    )]
    InvalidAuthorizationIdentity,

    /// Pseudonymization key is too short for durable HMAC use.
    #[error("transcript pseudonymization key must contain at least 32 bytes")]
    WeakPseudonymizationKey,

    /// A loaded secret is too short for reliable exact matching.
    #[error("loaded sensitive values must contain at least 8 bytes")]
    SensitiveValueTooShort,

    /// A chunk policy bound is outside its supported range.
    #[error("invalid transcript chunk policy bound for {field}")]
    InvalidChunkBound {
        /// Safe policy field name.
        field: &'static str,
    },

    /// Authorization targets another session.
    #[error("transcript authorization does not cover session {session_id}")]
    UnauthorizedSession {
        /// Requested session.
        session_id: SessionId,
    },

    /// Authorization targets an older or different source snapshot.
    #[error("transcript authorization does not cover the verified source snapshot")]
    UnauthorizedSourceSnapshot,

    /// A mandatory scanner pattern could not be compiled.
    #[error("failed to compile mandatory local scanner pattern {pattern}: {source}")]
    ScannerPattern {
        /// Static source-safe pattern name.
        pattern: &'static str,
        /// Regex compiler failure.
        #[source]
        source: regex::Error,
    },

    /// Mandatory scanning rejected a fragment without exposing its contents.
    #[error("record {sequence:?} was blocked by local scanning: {reason}")]
    ScannerBlocked {
        /// Source record sequence.
        sequence: RecordSequence,
        /// Closed source-safe reason.
        reason: ScannerBlockReason,
    },

    /// No sanitized transcript text remained after the selected disclosure scope.
    #[error("verified session contains no transcript text eligible for enrichment")]
    NoEligibleTranscript,

    /// A model-facing citation token was not valid SHA-256 hexadecimal data.
    #[error("invalid transcript citation token")]
    InvalidCitationToken,

    /// A knowledge text field was empty or exceeded its Unicode bound.
    #[error("{field} must contain between 1 and {maximum} characters")]
    InvalidKnowledgeText {
        /// Semantic field name.
        field: &'static str,
        /// Maximum accepted Unicode scalar count.
        maximum: usize,
    },

    /// Model output omitted all required citations.
    #[error("{field} must cite at least one supplied transcript span")]
    EmptyCitations {
        /// Semantic assertion class.
        field: &'static str,
    },

    /// A model citation was not supplied in its bounded input chunk.
    #[error("knowledge output cited a transcript span outside its bounded input")]
    UnknownTranscriptCitation,

    /// The same citation appeared more than once in one assertion.
    #[error("knowledge output repeated a transcript citation")]
    DuplicateTranscriptCitation,

    /// Deterministic semantic identities collided with conflicting content.
    #[error("duplicate semantic identity carries conflicting knowledge content")]
    ConflictingKnowledgeIdentity,

    /// A claim or relation references an entity absent from its validated set.
    #[error("knowledge assertion references an unknown entity")]
    UnknownKnowledgeEntity,

    /// Model inference used causal certainty that the evidence layer cannot support.
    #[error("causal and root-cause assertions must remain hypotheses")]
    UnsupportedCausalCertainty,

    /// Provider extraction result names a different bounded input chunk.
    #[error("knowledge extraction result does not match the requested transcript chunk")]
    KnowledgeChunkMismatch,

    /// Trusted archive discovery or streaming failed.
    #[error(transparent)]
    Ingestion(#[from] harness_graph_ingestion::IngestionError),
}
