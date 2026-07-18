//! Typed boundary for the additive, non-authoritative enrichment overlay.

use std::collections::HashSet;

use async_trait::async_trait;
use harness_graph_domain::{
    GraphNamespace, ObservationId, PayloadDigest, RecordCount, RecordSequence, SessionId,
    SourceDigest, TokenCount,
};
use serde::{Serialize, Serializer};

/// Construction failure at the enrichment graph boundary.
#[derive(Debug, thiserror::Error)]
pub enum EnrichmentGraphError {
    /// A content-addressed identifier was not a SHA-256 hexadecimal value.
    #[error("invalid {field}: expected 64 hexadecimal characters")]
    InvalidDigest {
        /// Semantic identifier field.
        field: &'static str,
    },
    /// An opaque invocation owner was not a 128-bit hexadecimal value.
    #[error("invalid enrichment invocation owner: expected 32 hexadecimal characters")]
    InvalidInvocationOwner,
    /// A version or model identifier was empty or contained unsafe characters.
    #[error("invalid {field}: expected 1 to {maximum} safe identifier characters")]
    InvalidIdentifier {
        /// Semantic identifier field.
        field: &'static str,
        /// Maximum accepted characters.
        maximum: usize,
    },
    /// A sanitized display value was empty, oversized, or contained controls.
    #[error("invalid {field}: expected 1 to {maximum} source-safe characters")]
    InvalidDisplayText {
        /// Semantic text field.
        field: &'static str,
        /// Maximum accepted characters.
        maximum: usize,
    },
    /// A collection whose graph meaning requires evidence was empty.
    #[error("{field} must contain at least one item")]
    EmptyCollection {
        /// Semantic collection field.
        field: &'static str,
    },
    /// A supposedly set-like collection contained the same identity twice.
    #[error("{field} contains a duplicate identity")]
    DuplicateIdentity {
        /// Semantic collection field.
        field: &'static str,
    },
    /// A bounded chunk count was invalid.
    #[error("expected enrichment chunk count must be between 1 and 10,000")]
    InvalidChunkCount,
    /// A paid-call lease duration fell outside the recovery safety bounds.
    #[error("enrichment chunk lease duration must be between 30 and 3,600 seconds")]
    InvalidLeaseDuration,
    /// A one-based ordinal was zero.
    #[error("{field} must be one-based")]
    InvalidOrdinal {
        /// Semantic ordinal field.
        field: &'static str,
    },
    /// A chunk referenced an object outside its validated payload.
    #[error("{field} contains an unresolved enrichment citation")]
    UnresolvedCitation {
        /// Citation collection field.
        field: &'static str,
    },
    /// A relation attempted to relate an entity to itself.
    #[error("knowledge relation endpoints must be distinct")]
    SelfRelation,
    /// A causal assertion was represented as stronger than a hypothesis.
    #[error("causal and root-cause assertions must remain hypotheses")]
    UnsupportedCausalCertainty,
}

macro_rules! digest_identifier {
    ($name:ident, $field:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            #[doc = concat!("Parse a hexadecimal ", $field, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error unless the value is exactly 32 hexadecimal bytes.
            pub fn parse_hex(value: &str) -> Result<Self, EnrichmentGraphError> {
                let mut bytes = [0_u8; 32];
                hex::decode_to_slice(value, &mut bytes)
                    .map_err(|_| EnrichmentGraphError::InvalidDigest { field: $field })?;
                Ok(Self(bytes))
            }

            /// Lowercase hexadecimal representation.
            #[must_use]
            pub fn to_hex(self) -> String {
                hex::encode(self.0)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_hex())
                    .finish()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.to_hex())
            }
        }
    };
}

digest_identifier!(
    EnrichmentRunId,
    "enrichment run identifier",
    "Content-addressed identity of one enrichment execution."
);
digest_identifier!(
    EnrichmentFingerprint,
    "enrichment fingerprint",
    "Identity operation over source, policies, provider, prompt, and schema."
);
digest_identifier!(
    EnrichmentChunkId,
    "enrichment chunk identifier",
    "Content-addressed identity of one bounded sanitized chunk."
);
digest_identifier!(
    EnrichmentOutputDigest,
    "enrichment output digest",
    "Digest of one citation-validated chunk result."
);
digest_identifier!(
    EnrichmentAuthorizationPolicyDigest,
    "enrichment authorization policy digest",
    "Digest of the exact operator-reviewed disclosure policy."
);
digest_identifier!(
    EnrichmentPromptDigest,
    "enrichment prompt digest",
    "Digest of the exact immutable foundation-model prompt body."
);

/// Opaque identity of one process-local enrichment invocation.
///
/// The value carries no hostname, process identifier, path, or credential
/// material. It exists only to bind a paid-call lease to the invocation that
/// acquired it.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EnrichmentInvocationOwner([u8; 16]);

impl EnrichmentInvocationOwner {
    /// Construct an owner from opaque random UUID bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Parse a hexadecimal opaque owner identity.
    ///
    /// # Errors
    ///
    /// Returns an error unless the value is exactly 16 hexadecimal bytes.
    pub fn parse_hex(value: &str) -> Result<Self, EnrichmentGraphError> {
        let mut bytes = [0_u8; 16];
        hex::decode_to_slice(value, &mut bytes)
            .map_err(|_| EnrichmentGraphError::InvalidInvocationOwner)?;
        Ok(Self(bytes))
    }

    /// Lowercase hexadecimal database representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Debug for EnrichmentInvocationOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EnrichmentInvocationOwner([opaque])")
    }
}

impl Serialize for EnrichmentInvocationOwner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}
digest_identifier!(
    TranscriptSpanId,
    "transcript span identifier",
    "Opaque identity of one source-anchored sanitized transcript span."
);
digest_identifier!(
    NarrativeEpisodeId,
    "narrative episode identifier",
    "Content-addressed identity of one narrative episode."
);
digest_identifier!(
    KnowledgeEntityId,
    "knowledge entity identifier",
    "Content-addressed identity of one enrichment-only entity."
);
digest_identifier!(
    KnowledgeClaimId,
    "knowledge claim identifier",
    "Content-addressed identity of one enrichment-only claim."
);
digest_identifier!(
    KnowledgeRelationId,
    "knowledge relation identifier",
    "Content-addressed identity of one reified enrichment relation."
);

macro_rules! safe_identifier {
    ($name:ident, $field:literal, $maximum:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Validate a ", $field, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error for empty, oversized, or unsafe identifiers.
            pub fn new(value: impl Into<String>) -> Result<Self, EnrichmentGraphError> {
                let value = value.into();
                let value = value.trim();
                if value.is_empty()
                    || value.chars().count() > $maximum
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
                {
                    return Err(EnrichmentGraphError::InvalidIdentifier {
                        field: $field,
                        maximum: $maximum,
                    });
                }
                Ok(Self(value.to_owned()))
            }

            /// Borrow the stable identifier.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

safe_identifier!(
    EnrichmentModelName,
    "enrichment model name",
    128,
    "Validated model identifier retained as enrichment provenance."
);
safe_identifier!(
    PromptVersion,
    "prompt version",
    64,
    "Versioned enrichment prompt."
);
safe_identifier!(
    EnrichmentSchemaVersion,
    "enrichment schema version",
    64,
    "Versioned structured-output schema."
);
safe_identifier!(
    RedactionPolicyVersion,
    "redaction policy version",
    64,
    "Versioned mandatory local redaction policy."
);
safe_identifier!(
    ChunkingPolicyVersion,
    "chunking policy version",
    64,
    "Versioned deterministic chunking policy."
);

macro_rules! display_text {
    ($name:ident, $field:literal, $maximum:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Validate source-safe ", $field, ".")]
            ///
            /// The caller must run mandatory local redaction before construction.
            ///
            /// # Errors
            ///
            /// Returns an error for empty, oversized, or control-bearing values.
            pub fn new(value: impl Into<String>) -> Result<Self, EnrichmentGraphError> {
                let value = value.into();
                let value = value.trim();
                if value.is_empty()
                    || value.chars().count() > $maximum
                    || value.chars().any(|character| {
                        character.is_control() && !matches!(character, '\n' | '\t')
                    })
                {
                    return Err(EnrichmentGraphError::InvalidDisplayText {
                        field: $field,
                        maximum: $maximum,
                    });
                }
                Ok(Self(value.to_owned()))
            }

            /// Borrow the validated display value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

display_text!(
    NarrativeTitle,
    "narrative title",
    160,
    "Sanitized narrative title."
);
display_text!(
    NarrativeSummary,
    "narrative summary",
    2_000,
    "Sanitized narrative episode summary."
);
display_text!(
    KnowledgeEntityName,
    "entity name",
    240,
    "Sanitized entity name."
);
display_text!(
    KnowledgeClaimTitle,
    "knowledge claim title",
    160,
    "Sanitized claim display title."
);
display_text!(
    KnowledgeStatement,
    "knowledge statement",
    2_000,
    "Sanitized semantic claim statement."
);

/// Only supported foundation-model provider for enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentProvider {
    /// Mistral through the isolated infrastructure adapter.
    Mistral,
}

impl EnrichmentProvider {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        "mistral"
    }
}

/// Closed transcript classes authorized for one enrichment run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentDisclosureScope {
    /// User, agent, collaborator, and completion messages only.
    ConversationOnly,
    /// Conversation plus textual tool requests, results, patches, and errors.
    ConversationAndExecution,
}

impl EnrichmentDisclosureScope {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConversationOnly => "conversation_only",
            Self::ConversationAndExecution => "conversation_and_execution",
        }
    }

    /// Parse the closed graph representation at a persistence boundary.
    ///
    /// # Errors
    ///
    /// Returns a source-safe error when persisted data is outside the closed vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "conversation_only" => Ok(Self::ConversationOnly),
            "conversation_and_execution" => Ok(Self::ConversationAndExecution),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "enrichment disclosure scope",
                maximum: 26,
            }),
        }
    }
}

/// Source-safe audit product retained with every immutable enrichment run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EnrichmentRunAuditProvenance {
    disclosure_scope: EnrichmentDisclosureScope,
    authorization_policy_digest: EnrichmentAuthorizationPolicyDigest,
    prompt_digest: EnrichmentPromptDigest,
}

impl EnrichmentRunAuditProvenance {
    /// Construct exact authorization and prompt provenance.
    #[must_use]
    pub const fn new(
        disclosure_scope: EnrichmentDisclosureScope,
        authorization_policy_digest: EnrichmentAuthorizationPolicyDigest,
        prompt_digest: EnrichmentPromptDigest,
    ) -> Self {
        Self {
            disclosure_scope,
            authorization_policy_digest,
            prompt_digest,
        }
    }

    /// Exact externally authorized transcript classes.
    #[must_use]
    pub const fn disclosure_scope(self) -> EnrichmentDisclosureScope {
        self.disclosure_scope
    }

    /// Digest of the operator-reviewed disclosure policy.
    #[must_use]
    pub const fn authorization_policy_digest(self) -> EnrichmentAuthorizationPolicyDigest {
        self.authorization_policy_digest
    }

    /// Digest of the exact immutable prompt body.
    #[must_use]
    pub const fn prompt_digest(self) -> EnrichmentPromptDigest {
        self.prompt_digest
    }
}

/// Closed source-safe failure class persisted without provider or transcript text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentFailureClass {
    /// Mistral rate-limited a cost-bearing request.
    RateLimited,
    /// Mistral or an upstream dependency was temporarily unavailable.
    TemporarilyUnavailable,
    /// A bounded provider operation timed out.
    Timeout,
    /// Another live invocation owns the paid-call lease for this chunk.
    LeaseBusy,
    /// Transport failed without a trustworthy provider response.
    Transport,
    /// Provider authentication or authorization failed.
    Authentication,
    /// Mistral rejected the request as non-retryable.
    ProviderRejected,
    /// Provider output violated the structured-output contract.
    InvalidStructuredOutput,
    /// The bounded provider concurrency gate was unavailable.
    ConcurrencyUnavailable,
    /// A provider retry delay exceeded the configured safety bound.
    RetryAfterExceedsBound,
    /// Local disclosure, integrity, scanner, or resource policy blocked processing.
    PolicyBlocked,
    /// Provider output repeated secret-shaped material.
    SecretEcho,
    /// A citation or semantic invariant rejected provider output.
    CitationValidation,
    /// Additive graph projection rejected validated enrichment.
    Projection,
}

impl EnrichmentFailureClass {
    /// Stable source-safe graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RateLimited => "rate_limited",
            Self::TemporarilyUnavailable => "temporarily_unavailable",
            Self::Timeout => "timeout",
            Self::LeaseBusy => "lease_busy",
            Self::Transport => "transport",
            Self::Authentication => "authentication",
            Self::ProviderRejected => "provider_rejected",
            Self::InvalidStructuredOutput => "invalid_structured_output",
            Self::ConcurrencyUnavailable => "concurrency_unavailable",
            Self::RetryAfterExceedsBound => "retry_after_exceeds_bound",
            Self::PolicyBlocked => "policy_blocked",
            Self::SecretEcho => "secret_echo",
            Self::CitationValidation => "citation_validation",
            Self::Projection => "projection",
        }
    }

    /// Lifecycle state derived from the failure class rather than caller input.
    #[must_use]
    pub const fn status(self) -> EnrichmentFailureStatus {
        match self {
            Self::RateLimited
            | Self::TemporarilyUnavailable
            | Self::Timeout
            | Self::LeaseBusy
            | Self::Transport => EnrichmentFailureStatus::RetryableFailed,
            Self::Authentication
            | Self::ProviderRejected
            | Self::InvalidStructuredOutput
            | Self::ConcurrencyUnavailable
            | Self::RetryAfterExceedsBound
            | Self::PolicyBlocked
            | Self::SecretEcho
            | Self::CitationValidation
            | Self::Projection => EnrichmentFailureStatus::TerminalFailed,
        }
    }
}

/// Closed failed-run terminal shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentFailureStatus {
    /// A later bounded retry may create or resume the same run.
    RetryableFailed,
    /// The run is closed and requires a changed input or policy decision.
    TerminalFailed,
}

impl EnrichmentFailureStatus {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RetryableFailed => "retryable_failed",
            Self::TerminalFailed => "terminal_failed",
        }
    }
}

/// Closed entity/claim semantic vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeKind {
    /// Stated execution goal.
    Goal,
    /// Decision made during execution.
    Decision,
    /// Constraint on execution.
    Constraint,
    /// Produced or referenced artifact.
    Artifact,
    /// External or internal dependency.
    Dependency,
    /// Observed failure.
    Failure,
    /// Non-authoritative root-cause hypothesis.
    RootCauseHypothesis,
    /// Repair action or strategy.
    Repair,
    /// Verification action or evidence.
    Verification,
    /// Potential risk.
    Risk,
    /// Reusable lesson.
    Lesson,
    /// Unresolved question.
    OpenQuestion,
}

impl KnowledgeKind {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Goal => "goal",
            Self::Decision => "decision",
            Self::Constraint => "constraint",
            Self::Artifact => "artifact",
            Self::Dependency => "dependency",
            Self::Failure => "failure",
            Self::RootCauseHypothesis => "root_cause_hypothesis",
            Self::Repair => "repair",
            Self::Verification => "verification",
            Self::Risk => "risk",
            Self::Lesson => "lesson",
            Self::OpenQuestion => "open_question",
        }
    }

    /// Parse a persisted closed vocabulary value.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the closed vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "goal" => Ok(Self::Goal),
            "decision" => Ok(Self::Decision),
            "constraint" => Ok(Self::Constraint),
            "artifact" => Ok(Self::Artifact),
            "dependency" => Ok(Self::Dependency),
            "failure" => Ok(Self::Failure),
            "root_cause_hypothesis" => Ok(Self::RootCauseHypothesis),
            "repair" => Ok(Self::Repair),
            "verification" => Ok(Self::Verification),
            "risk" => Ok(Self::Risk),
            "lesson" => Ok(Self::Lesson),
            "open_question" => Ok(Self::OpenQuestion),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "knowledge kind",
                maximum: 64,
            }),
        }
    }
}

/// Closed entity vocabulary preserved from transcript enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeEntityKind {
    /// Project or product.
    Project,
    /// Version-control repository.
    Repository,
    /// Code module, crate, or package.
    Module,
    /// File or path-like artifact.
    File,
    /// Shell or application command.
    Command,
    /// Agent tool.
    Tool,
    /// Software, service, or data dependency.
    Dependency,
    /// Configuration or policy.
    Configuration,
    /// Runtime or deployment environment.
    Environment,
    /// Error class or failure signature.
    Error,
    /// Domain or engineering concept.
    Concept,
    /// Produced document, binary, patch, or result.
    Artifact,
    /// Closed fallback when no narrower class is supported.
    Other,
}

impl KnowledgeEntityKind {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::Repository => "repository",
            Self::Module => "module",
            Self::File => "file",
            Self::Command => "command",
            Self::Tool => "tool",
            Self::Dependency => "dependency",
            Self::Configuration => "configuration",
            Self::Environment => "environment",
            Self::Error => "error",
            Self::Concept => "concept",
            Self::Artifact => "artifact",
            Self::Other => "other",
        }
    }

    /// Parse a persisted closed entity kind.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the transcript entity vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "project" => Ok(Self::Project),
            "repository" => Ok(Self::Repository),
            "module" => Ok(Self::Module),
            "file" => Ok(Self::File),
            "command" => Ok(Self::Command),
            "tool" => Ok(Self::Tool),
            "dependency" => Ok(Self::Dependency),
            "configuration" => Ok(Self::Configuration),
            "environment" => Ok(Self::Environment),
            "error" => Ok(Self::Error),
            "concept" => Ok(Self::Concept),
            "artifact" => Ok(Self::Artifact),
            "other" => Ok(Self::Other),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "knowledge entity kind",
                maximum: 32,
            }),
        }
    }
}

/// Coarse confidence without false numeric precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeConfidence {
    /// Weak or ambiguous evidence.
    Low,
    /// Material but incomplete evidence.
    Medium,
    /// Directly supported by cited text.
    High,
}

impl KnowledgeConfidence {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    /// Parse a persisted confidence.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the closed confidence vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "knowledge confidence",
                maximum: 16,
            }),
        }
    }
}

/// Epistemic status preventing model inference from becoming deterministic fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EpistemicStatus {
    /// Direct transcript assertion.
    Explicit,
    /// Model interpretation that remains non-authoritative.
    Inferred,
    /// Causal or root-cause hypothesis requiring deterministic corroboration.
    Hypothesis,
}

impl EpistemicStatus {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Inferred => "inferred",
            Self::Hypothesis => "hypothesis",
        }
    }

    /// Parse a persisted epistemic status.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the closed epistemic vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "explicit" => Ok(Self::Explicit),
            "inferred" => Ok(Self::Inferred),
            "hypothesis" => Ok(Self::Hypothesis),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "epistemic status",
                maximum: 32,
            }),
        }
    }
}

/// Closed reified relation predicate vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgePredicate {
    /// Subject uses object.
    Uses,
    /// Subject modifies object.
    Modifies,
    /// Subject depends on object.
    DependsOn,
    /// Subject is inferred to cause object.
    Causes,
    /// Subject is blocked by object.
    BlockedBy,
    /// Subject resolves object.
    Resolves,
    /// Subject verifies object.
    Verifies,
    /// Subject produces object.
    Produces,
    /// Subject contributes to object.
    ContributesTo,
    /// Subject contradicts object.
    Contradicts,
    /// Weak non-causal association.
    RelatedTo,
}

impl KnowledgePredicate {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uses => "uses",
            Self::Modifies => "modifies",
            Self::DependsOn => "depends_on",
            Self::Causes => "causes",
            Self::BlockedBy => "blocked_by",
            Self::Resolves => "resolves",
            Self::Verifies => "verifies",
            Self::Produces => "produces",
            Self::ContributesTo => "contributes_to",
            Self::Contradicts => "contradicts",
            Self::RelatedTo => "related_to",
        }
    }

    /// Parse a persisted closed predicate.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the closed predicate vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "uses" => Ok(Self::Uses),
            "modifies" => Ok(Self::Modifies),
            "depends_on" => Ok(Self::DependsOn),
            "causes" => Ok(Self::Causes),
            "blocked_by" => Ok(Self::BlockedBy),
            "resolves" => Ok(Self::Resolves),
            "verifies" => Ok(Self::Verifies),
            "produces" => Ok(Self::Produces),
            "contributes_to" => Ok(Self::ContributesTo),
            "contradicts" => Ok(Self::Contradicts),
            "related_to" => Ok(Self::RelatedTo),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "knowledge predicate",
                maximum: 32,
            }),
        }
    }
}

/// Closed transcript role retained without transcript content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptRole {
    /// Human operator.
    User,
    /// Coding agent.
    Agent,
    /// Collaborating agent.
    Collaborator,
    /// Tool or runtime boundary.
    Tool,
}

impl TranscriptRole {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Collaborator => "collaborator",
            Self::Tool => "tool",
        }
    }

    /// Parse a persisted role.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the closed role vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "user" => Ok(Self::User),
            "agent" => Ok(Self::Agent),
            "collaborator" => Ok(Self::Collaborator),
            "tool" => Ok(Self::Tool),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "transcript role",
                maximum: 16,
            }),
        }
    }
}

/// Closed allowlisted transcript field anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptField {
    /// Message field.
    Message,
    /// Textual content member.
    ContentText,
    /// Tool arguments.
    Arguments,
    /// Tool input.
    Input,
    /// Tool output.
    Output,
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
    /// Combined output.
    AggregatedOutput,
    /// Final completion message.
    LastAgentMessage,
    /// Search query.
    Query,
    /// Structured action.
    Action,
    /// Structured invocation.
    Invocation,
    /// Structured result.
    Result,
    /// Patch changes.
    Changes,
    /// Tool execution metadata.
    Execution,
    /// Tool-search results.
    Tools,
}

impl TranscriptField {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::ContentText => "content_text",
            Self::Arguments => "arguments",
            Self::Input => "input",
            Self::Output => "output",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
            Self::AggregatedOutput => "aggregated_output",
            Self::LastAgentMessage => "last_agent_message",
            Self::Query => "query",
            Self::Action => "action",
            Self::Invocation => "invocation",
            Self::Result => "result",
            Self::Changes => "changes",
            Self::Execution => "execution",
            Self::Tools => "tools",
        }
    }

    /// Parse a persisted field anchor.
    ///
    /// # Errors
    ///
    /// Returns an error for values outside the allowlisted field vocabulary.
    pub fn parse(value: &str) -> Result<Self, EnrichmentGraphError> {
        match value {
            "message" => Ok(Self::Message),
            "content_text" => Ok(Self::ContentText),
            "arguments" => Ok(Self::Arguments),
            "input" => Ok(Self::Input),
            "output" => Ok(Self::Output),
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            "aggregated_output" => Ok(Self::AggregatedOutput),
            "last_agent_message" => Ok(Self::LastAgentMessage),
            "query" => Ok(Self::Query),
            "action" => Ok(Self::Action),
            "invocation" => Ok(Self::Invocation),
            "result" => Ok(Self::Result),
            "changes" => Ok(Self::Changes),
            "execution" => Ok(Self::Execution),
            "tools" => Ok(Self::Tools),
            _ => Err(EnrichmentGraphError::InvalidIdentifier {
                field: "transcript field",
                maximum: 32,
            }),
        }
    }
}

/// Validated expected chunk count for a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EnrichmentChunkCount(u64);

impl EnrichmentChunkCount {
    /// Validate the bounded non-zero chunk count.
    ///
    /// # Errors
    ///
    /// Returns an error outside `1..=10_000`.
    pub fn new(value: u64) -> Result<Self, EnrichmentGraphError> {
        if (1..=10_000).contains(&value) {
            Ok(Self(value))
        } else {
            Err(EnrichmentGraphError::InvalidChunkCount)
        }
    }

    /// Numeric chunk count for adapter conversion.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// One-based narrative episode position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct EpisodeOrdinal(u64);

impl EpisodeOrdinal {
    /// Validate a one-based position.
    ///
    /// # Errors
    ///
    /// Returns an error for zero.
    pub fn new(value: u64) -> Result<Self, EnrichmentGraphError> {
        if value == 0 {
            Err(EnrichmentGraphError::InvalidOrdinal {
                field: "episode ordinal",
            })
        } else {
            Ok(Self(value))
        }
    }

    /// Numeric position.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Zero-based occurrence of an allowlisted field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TranscriptFieldOrdinal(u32);

impl TranscriptFieldOrdinal {
    /// Construct a typed zero-based field occurrence.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Numeric occurrence.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Zero-based split part within one allowlisted transcript field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct TranscriptPartIndex(u32);

impl TranscriptPartIndex {
    /// Construct a typed zero-based split-part index.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Numeric split-part index.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Number of bytes in a sanitized span, never its contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct TranscriptByteCount(u64);

impl TranscriptByteCount {
    /// Construct a typed byte count.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric byte count.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Exact immutable identity and provenance of one enrichment run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnrichmentRunSpec {
    namespace: GraphNamespace,
    session_id: SessionId,
    source_digest: SourceDigest,
    run_id: EnrichmentRunId,
    fingerprint: EnrichmentFingerprint,
    provider: EnrichmentProvider,
    model: EnrichmentModelName,
    prompt_version: PromptVersion,
    audit_provenance: EnrichmentRunAuditProvenance,
    schema_version: EnrichmentSchemaVersion,
    redaction_version: RedactionPolicyVersion,
    chunking_version: ChunkingPolicyVersion,
    expected_chunks: EnrichmentChunkCount,
}

impl EnrichmentRunSpec {
    /// Construct immutable run provenance.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        namespace: GraphNamespace,
        session_id: SessionId,
        source_digest: SourceDigest,
        run_id: EnrichmentRunId,
        fingerprint: EnrichmentFingerprint,
        provider: EnrichmentProvider,
        model: EnrichmentModelName,
        prompt_version: PromptVersion,
        audit_provenance: EnrichmentRunAuditProvenance,
        schema_version: EnrichmentSchemaVersion,
        redaction_version: RedactionPolicyVersion,
        chunking_version: ChunkingPolicyVersion,
        expected_chunks: EnrichmentChunkCount,
    ) -> Self {
        Self {
            namespace,
            session_id,
            source_digest,
            run_id,
            fingerprint,
            provider,
            model,
            prompt_version,
            audit_provenance,
            schema_version,
            redaction_version,
            chunking_version,
            expected_chunks,
        }
    }

    /// Graph namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }
    /// Session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }
    /// Verified source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }
    /// Run identity.
    #[must_use]
    pub const fn run_id(&self) -> EnrichmentRunId {
        self.run_id
    }
    /// Complete input fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> EnrichmentFingerprint {
        self.fingerprint
    }
    /// Foundation-model provider.
    #[must_use]
    pub const fn provider(&self) -> EnrichmentProvider {
        self.provider
    }
    /// Model identifier.
    #[must_use]
    pub const fn model(&self) -> &EnrichmentModelName {
        &self.model
    }
    /// Prompt version.
    #[must_use]
    pub const fn prompt_version(&self) -> &PromptVersion {
        &self.prompt_version
    }
    /// Exact source-safe authorization and prompt audit provenance.
    #[must_use]
    pub const fn audit_provenance(&self) -> EnrichmentRunAuditProvenance {
        self.audit_provenance
    }
    /// Output schema version.
    #[must_use]
    pub const fn schema_version(&self) -> &EnrichmentSchemaVersion {
        &self.schema_version
    }
    /// Redaction policy version.
    #[must_use]
    pub const fn redaction_version(&self) -> &RedactionPolicyVersion {
        &self.redaction_version
    }
    /// Chunking policy version.
    #[must_use]
    pub const fn chunking_version(&self) -> &ChunkingPolicyVersion {
        &self.chunking_version
    }
    /// Expected chunks.
    #[must_use]
    pub const fn expected_chunks(&self) -> EnrichmentChunkCount {
        self.expected_chunks
    }
}

/// Stable reference to an existing run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnrichmentRunRef {
    namespace: GraphNamespace,
    source_digest: SourceDigest,
    fingerprint: EnrichmentFingerprint,
}

impl EnrichmentRunRef {
    /// Construct a source-bound run reference.
    #[must_use]
    pub const fn new(
        namespace: GraphNamespace,
        source_digest: SourceDigest,
        fingerprint: EnrichmentFingerprint,
    ) -> Self {
        Self {
            namespace,
            source_digest,
            fingerprint,
        }
    }
    /// Graph namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }
    /// Source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }
    /// Run fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> EnrichmentFingerprint {
        self.fingerprint
    }
}

/// Source-only metadata for one transcript span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TranscriptSpanProjection {
    id: TranscriptSpanId,
    observation_id: ObservationId,
    sequence: RecordSequence,
    field: TranscriptField,
    field_ordinal: TranscriptFieldOrdinal,
    part_index: TranscriptPartIndex,
    role: TranscriptRole,
    byte_count: TranscriptByteCount,
    token_count: TokenCount,
    content_digest: PayloadDigest,
}

impl TranscriptSpanProjection {
    /// Construct a text-free transcript span projection.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        id: TranscriptSpanId,
        observation_id: ObservationId,
        sequence: RecordSequence,
        field: TranscriptField,
        field_ordinal: TranscriptFieldOrdinal,
        part_index: TranscriptPartIndex,
        role: TranscriptRole,
        byte_count: TranscriptByteCount,
        token_count: TokenCount,
        content_digest: PayloadDigest,
    ) -> Self {
        Self {
            id,
            observation_id,
            sequence,
            field,
            field_ordinal,
            part_index,
            role,
            byte_count,
            token_count,
            content_digest,
        }
    }
    /// Span identity.
    #[must_use]
    pub const fn id(&self) -> TranscriptSpanId {
        self.id
    }
    /// Deterministic observation anchor.
    #[must_use]
    pub const fn observation_id(&self) -> &ObservationId {
        &self.observation_id
    }
    /// Source sequence.
    #[must_use]
    pub const fn sequence(&self) -> RecordSequence {
        self.sequence
    }
    /// Allowlisted field.
    #[must_use]
    pub const fn field(&self) -> TranscriptField {
        self.field
    }
    /// Field occurrence.
    #[must_use]
    pub const fn field_ordinal(&self) -> TranscriptFieldOrdinal {
        self.field_ordinal
    }
    /// Split part within the allowlisted field occurrence.
    #[must_use]
    pub const fn part_index(&self) -> TranscriptPartIndex {
        self.part_index
    }
    /// Fragment role.
    #[must_use]
    pub const fn role(&self) -> TranscriptRole {
        self.role
    }
    /// Sanitized byte count.
    #[must_use]
    pub const fn byte_count(&self) -> TranscriptByteCount {
        self.byte_count
    }
    /// Estimated token count.
    #[must_use]
    pub const fn token_count(&self) -> TokenCount {
        self.token_count
    }
    /// Sanitized content digest.
    #[must_use]
    pub const fn content_digest(&self) -> PayloadDigest {
        self.content_digest
    }
}

macro_rules! non_empty_unique_refs {
    ($name:ident, $item:ty, $field:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub struct $name(Vec<$item>);

        impl $name {
            #[doc = concat!("Validate non-empty unique ", $field, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error for an empty collection or duplicate identity.
            pub fn new(
                values: impl IntoIterator<Item = $item>,
            ) -> Result<Self, EnrichmentGraphError> {
                let values: Vec<_> = values.into_iter().collect();
                if values.is_empty() {
                    return Err(EnrichmentGraphError::EmptyCollection { field: $field });
                }
                let unique: HashSet<_> = values.iter().cloned().collect();
                if unique.len() != values.len() {
                    return Err(EnrichmentGraphError::DuplicateIdentity { field: $field });
                }
                Ok(Self(values))
            }

            /// Iterate over references.
            pub fn iter(&self) -> impl Iterator<Item = &$item> {
                self.0.iter()
            }
            /// Typed reference count.
            #[must_use]
            pub fn count(&self) -> RecordCount {
                RecordCount::new(self.0.len() as u64)
            }
        }
    };
}

non_empty_unique_refs!(
    SpanCitations,
    TranscriptSpanId,
    "span citations",
    "Evidence spans supporting a semantic assertion."
);

/// Optional deterministic corroboration represented without `Option`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "availability",
    content = "observations",
    rename_all = "snake_case"
)]
pub enum ObservationCorroboration {
    /// No deterministic corroboration is claimed.
    Unavailable,
    /// One or more deterministic observations corroborate the claim.
    Available(Vec<ObservationId>),
}

impl ObservationCorroboration {
    /// Validate a non-empty, duplicate-free available state.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty collection or duplicate observation.
    pub fn available(
        values: impl IntoIterator<Item = ObservationId>,
    ) -> Result<Self, EnrichmentGraphError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            return Err(EnrichmentGraphError::EmptyCollection {
                field: "observation corroboration",
            });
        }
        let unique: HashSet<_> = values.iter().cloned().collect();
        if unique.len() != values.len() {
            return Err(EnrichmentGraphError::DuplicateIdentity {
                field: "observation corroboration",
            });
        }
        Ok(Self::Available(values))
    }

    /// Iterate over available observation identities.
    pub fn iter(&self) -> impl Iterator<Item = &ObservationId> {
        let slice: &[ObservationId] = match self {
            Self::Unavailable => &[],
            Self::Available(values) => values,
        };
        slice.iter()
    }
}

/// One narrative overlay episode with exact transcript evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NarrativeEpisodeProjection {
    id: NarrativeEpisodeId,
    ordinal: EpisodeOrdinal,
    title: NarrativeTitle,
    summary: NarrativeSummary,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    spans: SpanCitations,
}

impl NarrativeEpisodeProjection {
    /// Construct a citation-backed episode.
    #[must_use]
    pub const fn new(
        id: NarrativeEpisodeId,
        ordinal: EpisodeOrdinal,
        title: NarrativeTitle,
        summary: NarrativeSummary,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        spans: SpanCitations,
    ) -> Self {
        Self {
            id,
            ordinal,
            title,
            summary,
            confidence,
            epistemic_status,
            spans,
        }
    }
    /// Episode identity.
    #[must_use]
    pub const fn id(&self) -> NarrativeEpisodeId {
        self.id
    }
    /// Canonical position.
    #[must_use]
    pub const fn ordinal(&self) -> EpisodeOrdinal {
        self.ordinal
    }
    /// Sanitized title.
    #[must_use]
    pub const fn title(&self) -> &NarrativeTitle {
        &self.title
    }
    /// Sanitized summary.
    #[must_use]
    pub const fn summary(&self) -> &NarrativeSummary {
        &self.summary
    }
    /// Coarse model confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }
    /// Explicit inference status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }
    /// Exact transcript evidence.
    #[must_use]
    pub const fn spans(&self) -> &SpanCitations {
        &self.spans
    }
}

/// One enrichment-only knowledge entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KnowledgeEntityProjection {
    id: KnowledgeEntityId,
    kind: KnowledgeEntityKind,
    name: KnowledgeEntityName,
}

impl KnowledgeEntityProjection {
    /// Construct an entity projection.
    #[must_use]
    pub const fn new(
        id: KnowledgeEntityId,
        kind: KnowledgeEntityKind,
        name: KnowledgeEntityName,
    ) -> Self {
        Self { id, kind, name }
    }
    /// Entity identity.
    #[must_use]
    pub const fn id(&self) -> KnowledgeEntityId {
        self.id
    }
    /// Entity kind.
    #[must_use]
    pub const fn kind(&self) -> KnowledgeEntityKind {
        self.kind
    }
    /// Sanitized entity name.
    #[must_use]
    pub const fn name(&self) -> &KnowledgeEntityName {
        &self.name
    }
}

/// Faithful claim-subject coproduct from transcript enrichment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "scope", content = "entities", rename_all = "snake_case")]
pub enum KnowledgeClaimSubjects {
    /// Claim applies to the session as a whole.
    SessionWide,
    /// Claim applies to one or more enrichment entity identities.
    Entities(Vec<KnowledgeEntityId>),
}

impl KnowledgeClaimSubjects {
    /// Validate a non-empty, duplicate-free entity subject set.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty set or duplicate identity.
    pub fn entities(
        values: impl IntoIterator<Item = KnowledgeEntityId>,
    ) -> Result<Self, EnrichmentGraphError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            return Err(EnrichmentGraphError::EmptyCollection {
                field: "knowledge claim subjects",
            });
        }
        let unique: HashSet<_> = values.iter().copied().collect();
        if unique.len() != values.len() {
            return Err(EnrichmentGraphError::DuplicateIdentity {
                field: "knowledge claim subjects",
            });
        }
        Ok(Self::Entities(values))
    }

    /// Stable graph scope representation.
    #[must_use]
    pub const fn scope(&self) -> &'static str {
        match self {
            Self::SessionWide => "session_wide",
            Self::Entities(_) => "entities",
        }
    }

    /// Iterate over entity subjects; session-wide claims yield no entity.
    pub fn iter(&self) -> impl Iterator<Item = &KnowledgeEntityId> {
        let values: &[KnowledgeEntityId] = match self {
            Self::SessionWide => &[],
            Self::Entities(values) => values,
        };
        values.iter()
    }
}

/// One evidence-linked non-authoritative knowledge claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KnowledgeClaimProjection {
    id: KnowledgeClaimId,
    kind: KnowledgeKind,
    title: KnowledgeClaimTitle,
    statement: KnowledgeStatement,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    subjects: KnowledgeClaimSubjects,
    spans: SpanCitations,
    corroboration: ObservationCorroboration,
}

impl KnowledgeClaimProjection {
    /// Construct a fully cited claim.
    ///
    /// # Errors
    ///
    /// Returns an error when a root-cause assertion is stronger than a hypothesis.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: KnowledgeClaimId,
        kind: KnowledgeKind,
        title: KnowledgeClaimTitle,
        statement: KnowledgeStatement,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        subjects: KnowledgeClaimSubjects,
        spans: SpanCitations,
        corroboration: ObservationCorroboration,
    ) -> Result<Self, EnrichmentGraphError> {
        if kind == KnowledgeKind::RootCauseHypothesis
            && epistemic_status != EpistemicStatus::Hypothesis
        {
            return Err(EnrichmentGraphError::UnsupportedCausalCertainty);
        }
        Ok(Self {
            id,
            kind,
            title,
            statement,
            confidence,
            epistemic_status,
            subjects,
            spans,
            corroboration,
        })
    }
    /// Claim identity.
    #[must_use]
    pub const fn id(&self) -> KnowledgeClaimId {
        self.id
    }
    /// Claim kind.
    #[must_use]
    pub const fn kind(&self) -> KnowledgeKind {
        self.kind
    }
    /// Sanitized display title.
    #[must_use]
    pub const fn title(&self) -> &KnowledgeClaimTitle {
        &self.title
    }
    /// Sanitized statement.
    #[must_use]
    pub const fn statement(&self) -> &KnowledgeStatement {
        &self.statement
    }
    /// Coarse confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }
    /// Epistemic status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }
    /// Session-wide or non-empty entity subject set.
    #[must_use]
    pub const fn subjects(&self) -> &KnowledgeClaimSubjects {
        &self.subjects
    }
    /// Transcript evidence.
    #[must_use]
    pub const fn spans(&self) -> &SpanCitations {
        &self.spans
    }
    /// Deterministic corroboration.
    #[must_use]
    pub const fn corroboration(&self) -> &ObservationCorroboration {
        &self.corroboration
    }
}

/// One reified, evidence-linked relation between enrichment entities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KnowledgeRelationProjection {
    id: KnowledgeRelationId,
    predicate: KnowledgePredicate,
    subject: KnowledgeEntityId,
    object: KnowledgeEntityId,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    spans: SpanCitations,
}

impl KnowledgeRelationProjection {
    /// Validate a relation projection.
    ///
    /// # Errors
    ///
    /// Returns an error when subject and object are the same entity.
    pub fn new(
        id: KnowledgeRelationId,
        predicate: KnowledgePredicate,
        subject: KnowledgeEntityId,
        object: KnowledgeEntityId,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        spans: SpanCitations,
    ) -> Result<Self, EnrichmentGraphError> {
        if subject == object {
            return Err(EnrichmentGraphError::SelfRelation);
        }
        if predicate == KnowledgePredicate::Causes
            && epistemic_status != EpistemicStatus::Hypothesis
        {
            return Err(EnrichmentGraphError::UnsupportedCausalCertainty);
        }
        Ok(Self {
            id,
            predicate,
            subject,
            object,
            confidence,
            epistemic_status,
            spans,
        })
    }
    /// Relation identity.
    #[must_use]
    pub const fn id(&self) -> KnowledgeRelationId {
        self.id
    }
    /// Closed predicate.
    #[must_use]
    pub const fn predicate(&self) -> KnowledgePredicate {
        self.predicate
    }
    /// Subject entity.
    #[must_use]
    pub const fn subject(&self) -> KnowledgeEntityId {
        self.subject
    }
    /// Object entity.
    #[must_use]
    pub const fn object(&self) -> KnowledgeEntityId {
        self.object
    }
    /// Coarse confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }
    /// Epistemic status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }
    /// Transcript evidence.
    #[must_use]
    pub const fn spans(&self) -> &SpanCitations {
        &self.spans
    }
}

macro_rules! projection_collection {
    ($name:ident, $item:ty, $id:expr, $field:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub struct $name(Vec<$item>);

        impl $name {
            /// Validate unique projection identities.
            ///
            /// # Errors
            ///
            /// Returns an error when an identity occurs more than once.
            pub fn new(
                values: impl IntoIterator<Item = $item>,
            ) -> Result<Self, EnrichmentGraphError> {
                let values: Vec<_> = values.into_iter().collect();
                let unique: HashSet<_> = values.iter().map($id).collect();
                if unique.len() != values.len() {
                    return Err(EnrichmentGraphError::DuplicateIdentity { field: $field });
                }
                Ok(Self(values))
            }
            /// Iterate over projections.
            pub fn iter(&self) -> impl Iterator<Item = &$item> {
                self.0.iter()
            }
            /// Typed projection count.
            #[must_use]
            pub fn count(&self) -> RecordCount {
                RecordCount::new(self.0.len() as u64)
            }
        }
    };
}

projection_collection!(
    TranscriptSpans,
    TranscriptSpanProjection,
    |value: &TranscriptSpanProjection| value.id(),
    "transcript spans",
    "Validated transcript span projections."
);
projection_collection!(
    NarrativeEpisodes,
    NarrativeEpisodeProjection,
    |value: &NarrativeEpisodeProjection| value.id(),
    "narrative episodes",
    "Validated narrative episode projections."
);
projection_collection!(
    KnowledgeEntities,
    KnowledgeEntityProjection,
    |value: &KnowledgeEntityProjection| value.id(),
    "knowledge entities",
    "Validated knowledge entity projections."
);
projection_collection!(
    KnowledgeClaims,
    KnowledgeClaimProjection,
    |value: &KnowledgeClaimProjection| value.id(),
    "knowledge claims",
    "Validated knowledge claim projections."
);
projection_collection!(
    KnowledgeRelations,
    KnowledgeRelationProjection,
    |value: &KnowledgeRelationProjection| value.id(),
    "knowledge relations",
    "Validated knowledge relation projections."
);

/// Atomic validated payload for one chunk receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnrichmentChunkProjection {
    chunk_id: EnrichmentChunkId,
    output_digest: EnrichmentOutputDigest,
    spans: TranscriptSpans,
    episodes: NarrativeEpisodes,
    entities: KnowledgeEntities,
    claims: KnowledgeClaims,
    relations: KnowledgeRelations,
    input_tokens: TokenCount,
    output_tokens: TokenCount,
}

impl EnrichmentChunkProjection {
    /// Validate all chunk-local span and entity references.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty span set or an unresolved span/entity citation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chunk_id: EnrichmentChunkId,
        output_digest: EnrichmentOutputDigest,
        spans: TranscriptSpans,
        episodes: NarrativeEpisodes,
        entities: KnowledgeEntities,
        claims: KnowledgeClaims,
        relations: KnowledgeRelations,
        input_tokens: TokenCount,
        output_tokens: TokenCount,
    ) -> Result<Self, EnrichmentGraphError> {
        if spans.count().value() == 0 {
            return Err(EnrichmentGraphError::EmptyCollection {
                field: "transcript spans",
            });
        }
        let span_ids: HashSet<_> = spans.iter().map(TranscriptSpanProjection::id).collect();
        let entity_ids: HashSet<_> = entities.iter().map(KnowledgeEntityProjection::id).collect();
        for episode in episodes.iter() {
            if episode.spans().iter().any(|id| !span_ids.contains(id)) {
                return Err(EnrichmentGraphError::UnresolvedCitation {
                    field: "narrative episodes",
                });
            }
        }
        for claim in claims.iter() {
            if claim
                .subjects()
                .iter()
                .any(|subject| !entity_ids.contains(subject))
                || claim.spans().iter().any(|id| !span_ids.contains(id))
            {
                return Err(EnrichmentGraphError::UnresolvedCitation {
                    field: "knowledge claims",
                });
            }
        }
        for relation in relations.iter() {
            if !entity_ids.contains(&relation.subject())
                || !entity_ids.contains(&relation.object())
                || relation.spans().iter().any(|id| !span_ids.contains(id))
            {
                return Err(EnrichmentGraphError::UnresolvedCitation {
                    field: "knowledge relations",
                });
            }
        }
        Ok(Self {
            chunk_id,
            output_digest,
            spans,
            episodes,
            entities,
            claims,
            relations,
            input_tokens,
            output_tokens,
        })
    }
    /// Chunk identity.
    #[must_use]
    pub const fn chunk_id(&self) -> EnrichmentChunkId {
        self.chunk_id
    }
    /// Validated output digest.
    #[must_use]
    pub const fn output_digest(&self) -> EnrichmentOutputDigest {
        self.output_digest
    }
    /// Transcript spans.
    #[must_use]
    pub const fn spans(&self) -> &TranscriptSpans {
        &self.spans
    }
    /// Narrative episodes.
    #[must_use]
    pub const fn episodes(&self) -> &NarrativeEpisodes {
        &self.episodes
    }
    /// Entities.
    #[must_use]
    pub const fn entities(&self) -> &KnowledgeEntities {
        &self.entities
    }
    /// Claims.
    #[must_use]
    pub const fn claims(&self) -> &KnowledgeClaims {
        &self.claims
    }
    /// Relations.
    #[must_use]
    pub const fn relations(&self) -> &KnowledgeRelations {
        &self.relations
    }
    /// Provider input usage.
    #[must_use]
    pub const fn input_tokens(&self) -> TokenCount {
        self.input_tokens
    }
    /// Provider output usage.
    #[must_use]
    pub const fn output_tokens(&self) -> TokenCount {
        self.output_tokens
    }
}

/// Start one immutable enrichment run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginEnrichmentRunCommand {
    run: EnrichmentRunSpec,
}

impl BeginEnrichmentRunCommand {
    /// Construct a begin command.
    #[must_use]
    pub const fn new(run: EnrichmentRunSpec) -> Self {
        Self { run }
    }
    /// Immutable run specification.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunSpec {
        &self.run
    }
}

/// Database-enforced lifetime of one cost-bearing chunk lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EnrichmentLeaseDuration(u32);

impl EnrichmentLeaseDuration {
    /// Default bound for one Mistral extraction including bounded retries.
    pub const PAID_CALL: Self = Self(900);

    /// Validate a lease lifetime between 30 seconds and one hour.
    ///
    /// # Errors
    ///
    /// Returns an error when recovery would be too eager or unbounded.
    pub const fn new(seconds: u32) -> Result<Self, EnrichmentGraphError> {
        if seconds < 30 || seconds > 3_600 {
            Err(EnrichmentGraphError::InvalidLeaseDuration)
        } else {
            Ok(Self(seconds))
        }
    }

    /// Duration in seconds for database parameter conversion.
    #[must_use]
    pub const fn seconds(self) -> u32 {
        self.0
    }
}

/// Atomically claim one missing chunk before any provider invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimEnrichmentChunkCommand {
    run: EnrichmentRunRef,
    chunk_id: EnrichmentChunkId,
    owner: EnrichmentInvocationOwner,
    duration: EnrichmentLeaseDuration,
}

impl ClaimEnrichmentChunkCommand {
    /// Construct a source-, fingerprint-, chunk-, and owner-bound claim.
    #[must_use]
    pub const fn new(
        run: EnrichmentRunRef,
        chunk_id: EnrichmentChunkId,
        owner: EnrichmentInvocationOwner,
        duration: EnrichmentLeaseDuration,
    ) -> Self {
        Self {
            run,
            chunk_id,
            owner,
            duration,
        }
    }

    /// Exact target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }

    /// Deterministic sanitized chunk identity.
    #[must_use]
    pub const fn chunk_id(&self) -> EnrichmentChunkId {
        self.chunk_id
    }

    /// Opaque invocation identity.
    #[must_use]
    pub const fn owner(&self) -> EnrichmentInvocationOwner {
        self.owner
    }

    /// Bounded database-enforced lease lifetime.
    #[must_use]
    pub const fn duration(&self) -> EnrichmentLeaseDuration {
        self.duration
    }
}

/// Owner token proving one invocation may perform a cost-bearing extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ClaimedEnrichmentChunk {
    chunk_id: EnrichmentChunkId,
    owner: EnrichmentInvocationOwner,
}

impl ClaimedEnrichmentChunk {
    /// Construct the exact token returned by an atomic claim transition.
    #[must_use]
    pub const fn new(chunk_id: EnrichmentChunkId, owner: EnrichmentInvocationOwner) -> Self {
        Self { chunk_id, owner }
    }

    /// Claimed deterministic chunk identity.
    #[must_use]
    pub const fn chunk_id(self) -> EnrichmentChunkId {
        self.chunk_id
    }

    /// Invocation that owns projection or release authority.
    #[must_use]
    pub const fn owner(self) -> EnrichmentInvocationOwner {
        self.owner
    }
}

/// Exact result of one atomic paid-call claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "claim", content = "receipt", rename_all = "snake_case")]
pub enum EnrichmentChunkClaim {
    /// A receipt already proves the provider call was committed.
    Committed(CommittedEnrichmentChunk),
    /// Another unexpired owner holds the lease; no provider call is allowed.
    Busy,
    /// This invocation owns the bounded lease and may call the provider once.
    Claimed(ClaimedEnrichmentChunk),
}

/// Atomically persist one validated chunk and its checkpoint receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectEnrichmentChunkCommand {
    run: EnrichmentRunRef,
    projection: EnrichmentChunkProjection,
}

impl ProjectEnrichmentChunkCommand {
    /// Construct a chunk projection command.
    #[must_use]
    pub const fn new(run: EnrichmentRunRef, projection: EnrichmentChunkProjection) -> Self {
        Self { run, projection }
    }
    /// Target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }
    /// Validated chunk payload.
    #[must_use]
    pub const fn projection(&self) -> &EnrichmentChunkProjection {
        &self.projection
    }
}

/// Owner-bound atomic projection of one claimed provider result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectClaimedEnrichmentChunkCommand {
    run: EnrichmentRunRef,
    lease: ClaimedEnrichmentChunk,
    projection: EnrichmentChunkProjection,
}

impl ProjectClaimedEnrichmentChunkCommand {
    /// Validate that the lease and projection describe the same chunk.
    ///
    /// # Errors
    ///
    /// Returns an error when a provider result is paired with another lease.
    pub fn new(
        run: EnrichmentRunRef,
        lease: ClaimedEnrichmentChunk,
        projection: EnrichmentChunkProjection,
    ) -> Result<Self, EnrichmentGraphError> {
        if lease.chunk_id() != projection.chunk_id() {
            return Err(EnrichmentGraphError::UnresolvedCitation {
                field: "claimed chunk projection",
            });
        }
        Ok(Self {
            run,
            lease,
            projection,
        })
    }

    /// Exact target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }

    /// Owner-bound lease authority.
    #[must_use]
    pub const fn lease(&self) -> ClaimedEnrichmentChunk {
        self.lease
    }

    /// Citation-validated enrichment payload.
    #[must_use]
    pub const fn projection(&self) -> &EnrichmentChunkProjection {
        &self.projection
    }
}

/// Release one claimed chunk after a provider or local validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseEnrichmentChunkLeaseCommand {
    run: EnrichmentRunRef,
    lease: ClaimedEnrichmentChunk,
}

impl ReleaseEnrichmentChunkLeaseCommand {
    /// Construct an owner-bound release transition.
    #[must_use]
    pub const fn new(run: EnrichmentRunRef, lease: ClaimedEnrichmentChunk) -> Self {
        Self { run, lease }
    }

    /// Exact target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }

    /// Owner and chunk that may be released.
    #[must_use]
    pub const fn lease(&self) -> ClaimedEnrichmentChunk {
        self.lease
    }
}

/// Owner-bound release outcome without nullable persistence state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "release", content = "receipt", rename_all = "snake_case")]
pub enum EnrichmentChunkLeaseRelease {
    /// The exact owner's active lease was removed.
    Released,
    /// The lease was absent, expired and reassigned, or owned by another invocation.
    NotOwned,
    /// Projection already committed; the lease is no longer needed.
    Committed(CommittedEnrichmentChunk),
}

/// Complete a fully checkpointed run and select it atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteEnrichmentRunCommand {
    run: EnrichmentRunRef,
}

impl CompleteEnrichmentRunCommand {
    /// Construct a completion command.
    #[must_use]
    pub const fn new(run: EnrichmentRunRef) -> Self {
        Self { run }
    }
    /// Target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }
}

/// Mark a non-completed enrichment run failed using source-safe classification only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkEnrichmentRunFailedCommand {
    run: EnrichmentRunRef,
    class: EnrichmentFailureClass,
}

impl MarkEnrichmentRunFailedCommand {
    /// Construct a failure transition.
    #[must_use]
    pub const fn new(run: EnrichmentRunRef, class: EnrichmentFailureClass) -> Self {
        Self { run, class }
    }
    /// Target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }
    /// Closed source-safe failure class.
    #[must_use]
    pub const fn class(&self) -> EnrichmentFailureClass {
        self.class
    }
    /// Failure lifecycle state derived from the class.
    #[must_use]
    pub const fn status(&self) -> EnrichmentFailureStatus {
        self.class.status()
    }
}

/// Separate enrichment-only graph mutation family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnrichmentGraphCommand {
    /// Create immutable planned-run provenance.
    BeginRun(BeginEnrichmentRunCommand),
    /// Project one validated chunk and checkpoint.
    ProjectChunk(ProjectEnrichmentChunkCommand),
    /// Complete and select a fully checkpointed run.
    CompleteRun(CompleteEnrichmentRunCommand),
    /// Close a partial run without changing the selected completed view.
    MarkRunFailed(MarkEnrichmentRunFailedCommand),
}

/// Whether an idempotent enrichment mutation changed the overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentProjectionDisposition {
    /// New overlay state was committed.
    Applied,
    /// The exact fingerprint/chunk/completion was already committed.
    Unchanged,
}

/// Receipt for one committed enrichment-only transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EnrichmentProjectionReceipt {
    disposition: EnrichmentProjectionDisposition,
}

impl EnrichmentProjectionReceipt {
    /// Construct a typed receipt.
    #[must_use]
    pub const fn new(disposition: EnrichmentProjectionDisposition) -> Self {
        Self { disposition }
    }
    /// Idempotent mutation disposition.
    #[must_use]
    pub const fn disposition(self) -> EnrichmentProjectionDisposition {
        self.disposition
    }
}

/// Read selector for one session's selected completed enrichment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentQuery {
    namespace: GraphNamespace,
    session_id: SessionId,
}

/// Read selector for one cost-bearing chunk checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentChunkCheckpointQuery {
    run: EnrichmentRunRef,
    chunk_id: EnrichmentChunkId,
}

/// Read selector for one exact immutable enrichment run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentRunLifecycleQuery {
    run: EnrichmentRunRef,
}

impl EnrichmentRunLifecycleQuery {
    /// Construct an exact source- and fingerprint-bound lifecycle query.
    #[must_use]
    pub const fn new(run: EnrichmentRunRef) -> Self {
        Self { run }
    }

    /// Exact run identity.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }
}

/// Paid-call safety state for one exact enrichment fingerprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentRunLifecycle {
    /// No run exists, so a begin command is required before provider work.
    Absent,
    /// Planned, projecting, or retryable-failed work may safely resume.
    Resumable,
    /// A terminal failure forbids repeating provider work for this fingerprint.
    TerminalFailed,
    /// The exact fingerprint already completed and is an identity/no-op.
    Completed,
}

impl EnrichmentChunkCheckpointQuery {
    /// Construct a source- and run-bound checkpoint query.
    #[must_use]
    pub const fn new(run: EnrichmentRunRef, chunk_id: EnrichmentChunkId) -> Self {
        Self { run, chunk_id }
    }
    /// Target run.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunRef {
        &self.run
    }
    /// Deterministic chunk identity.
    #[must_use]
    pub const fn chunk_id(&self) -> EnrichmentChunkId {
        self.chunk_id
    }
}

/// Committed receipt proving one validated cost-bearing chunk need not be repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CommittedEnrichmentChunk {
    chunk_id: EnrichmentChunkId,
    output_digest: EnrichmentOutputDigest,
    input_tokens: TokenCount,
    output_tokens: TokenCount,
}

impl CommittedEnrichmentChunk {
    /// Construct a validated committed chunk receipt.
    #[must_use]
    pub const fn new(
        chunk_id: EnrichmentChunkId,
        output_digest: EnrichmentOutputDigest,
        input_tokens: TokenCount,
        output_tokens: TokenCount,
    ) -> Self {
        Self {
            chunk_id,
            output_digest,
            input_tokens,
            output_tokens,
        }
    }
    /// Deterministic chunk identity.
    #[must_use]
    pub const fn chunk_id(self) -> EnrichmentChunkId {
        self.chunk_id
    }
    /// Digest of the citation-validated provider output.
    #[must_use]
    pub const fn output_digest(self) -> EnrichmentOutputDigest {
        self.output_digest
    }
    /// Committed provider input usage.
    #[must_use]
    pub const fn input_tokens(self) -> TokenCount {
        self.input_tokens
    }
    /// Committed provider output usage.
    #[must_use]
    pub const fn output_tokens(self) -> TokenCount {
        self.output_tokens
    }
}

/// Resume decision represented without nullable or raw persistence state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "checkpoint", content = "receipt", rename_all = "snake_case")]
pub enum EnrichmentChunkCheckpoint {
    /// No committed receipt exists; provider work is still required.
    Required,
    /// A committed receipt exists; the cost-bearing call must be skipped.
    Committed(CommittedEnrichmentChunk),
}

impl EnrichmentQuery {
    /// Construct a typed query.
    #[must_use]
    pub const fn new(namespace: GraphNamespace, session_id: SessionId) -> Self {
        Self {
            namespace,
            session_id,
        }
    }
    /// Graph namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }
    /// Session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }
}

/// Typed reason an enrichment overlay is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EnrichmentUnavailableReason {
    /// No deterministic session exists in the namespace.
    SessionNotFound,
    /// The session has no completed receipt-verified source snapshot.
    VerifiedSourceNotFound,
    /// No completed enrichment is selected for the latest verified source.
    NoCompletedSelection,
}

/// Completed enrichment selected for display and retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SelectedEnrichment {
    run: EnrichmentRunSpec,
    spans: TranscriptSpans,
    episodes: NarrativeEpisodes,
    entities: KnowledgeEntities,
    claims: KnowledgeClaims,
    relations: KnowledgeRelations,
}

impl SelectedEnrichment {
    /// Construct a selected completed overlay.
    #[must_use]
    pub const fn new(
        run: EnrichmentRunSpec,
        spans: TranscriptSpans,
        episodes: NarrativeEpisodes,
        entities: KnowledgeEntities,
        claims: KnowledgeClaims,
        relations: KnowledgeRelations,
    ) -> Self {
        Self {
            run,
            spans,
            episodes,
            entities,
            claims,
            relations,
        }
    }
    /// Selected run provenance.
    #[must_use]
    pub const fn run(&self) -> &EnrichmentRunSpec {
        &self.run
    }
    /// Text-free source spans.
    #[must_use]
    pub const fn spans(&self) -> &TranscriptSpans {
        &self.spans
    }
    /// Narrative episodes.
    #[must_use]
    pub const fn episodes(&self) -> &NarrativeEpisodes {
        &self.episodes
    }
    /// Knowledge entities.
    #[must_use]
    pub const fn entities(&self) -> &KnowledgeEntities {
        &self.entities
    }
    /// Knowledge claims.
    #[must_use]
    pub const fn claims(&self) -> &KnowledgeClaims {
        &self.claims
    }
    /// Reified relations.
    #[must_use]
    pub const fn relations(&self) -> &KnowledgeRelations {
        &self.relations
    }
}

/// Completed-only lookup without nullable domain state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "availability", content = "value", rename_all = "snake_case")]
pub enum EnrichmentLookup {
    /// A selected completed overlay exists.
    Selected(Box<SelectedEnrichment>),
    /// No query-visible completed overlay exists.
    Unavailable(EnrichmentUnavailableReason),
}

/// Provider-independent additive enrichment projection capability.
#[async_trait]
pub trait EnrichmentProjector: Send + Sync {
    /// Concrete adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Create only enrichment-overlay constraints and indexes.
    async fn ensure_enrichment_schema(&self) -> Result<(), Self::Error>;

    /// Atomically claim one missing chunk before any cost-bearing provider call.
    async fn claim_enrichment_chunk(
        &self,
        command: &ClaimEnrichmentChunkCommand,
    ) -> Result<EnrichmentChunkClaim, Self::Error>;

    /// Commit a validated provider result only while the exact owner holds the lease.
    async fn project_claimed_enrichment_chunk(
        &self,
        command: ProjectClaimedEnrichmentChunkCommand,
    ) -> Result<EnrichmentProjectionReceipt, Self::Error>;

    /// Release only the exact owner's lease after work settles unsuccessfully.
    async fn release_enrichment_chunk_lease(
        &self,
        command: &ReleaseEnrichmentChunkLeaseCommand,
    ) -> Result<EnrichmentChunkLeaseRelease, Self::Error>;

    /// Project one enrichment-only command atomically.
    async fn project_enrichment(
        &self,
        command: EnrichmentGraphCommand,
    ) -> Result<EnrichmentProjectionReceipt, Self::Error>;
}

/// Provider-independent completed enrichment read capability.
#[async_trait]
pub trait EnrichmentReader: Send + Sync {
    /// Concrete adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Read only the selected completed overlay for the latest verified source.
    async fn selected_enrichment(
        &self,
        query: &EnrichmentQuery,
    ) -> Result<EnrichmentLookup, Self::Error>;

    /// Read the exact run lifecycle before any cost-bearing provider call.
    async fn enrichment_run_lifecycle(
        &self,
        query: &EnrichmentRunLifecycleQuery,
    ) -> Result<EnrichmentRunLifecycle, Self::Error>;

    /// Read one committed chunk receipt before invoking the provider.
    async fn enrichment_chunk_checkpoint(
        &self,
        query: &EnrichmentChunkCheckpointQuery,
    ) -> Result<EnrichmentChunkCheckpoint, Self::Error>;
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{
        ObservationId, PayloadDigest, RecordSequence, SourceDigest, TokenCount,
    };

    use super::*;

    fn digest<T>(
        value: &str,
        parse: impl FnOnce(&str) -> Result<T, EnrichmentGraphError>,
    ) -> Result<T, EnrichmentGraphError> {
        parse(&value.repeat(64))
    }

    fn span(id: &str) -> Result<TranscriptSpanProjection, Box<dyn std::error::Error>> {
        let source = SourceDigest::hash(b"source");
        let sequence = RecordSequence::from_zero_based(0);
        Ok(TranscriptSpanProjection::new(
            digest(id, TranscriptSpanId::parse_hex)?,
            ObservationId::from_source(source, sequence),
            sequence,
            TranscriptField::Message,
            TranscriptFieldOrdinal::new(0),
            TranscriptPartIndex::new(0),
            TranscriptRole::User,
            TranscriptByteCount::new(12),
            TokenCount::new(3),
            PayloadDigest::hash(b"sanitized"),
        ))
    }

    #[test]
    fn chunk_rejects_claims_with_unresolved_span_citations()
    -> Result<(), Box<dyn std::error::Error>> {
        let entity_id = digest("2", KnowledgeEntityId::parse_hex)?;
        let entities = KnowledgeEntities::new([KnowledgeEntityProjection::new(
            entity_id,
            KnowledgeEntityKind::Artifact,
            KnowledgeEntityName::new("typed graph")?,
        )])?;
        let claim = KnowledgeClaimProjection::new(
            digest("3", KnowledgeClaimId::parse_hex)?,
            KnowledgeKind::Decision,
            KnowledgeClaimTitle::new("Additive overlay")?,
            KnowledgeStatement::new("Use an additive overlay")?,
            KnowledgeConfidence::High,
            EpistemicStatus::Explicit,
            KnowledgeClaimSubjects::entities([entity_id])?,
            SpanCitations::new([digest("9", TranscriptSpanId::parse_hex)?])?,
            ObservationCorroboration::Unavailable,
        )?;
        let result = EnrichmentChunkProjection::new(
            digest("4", EnrichmentChunkId::parse_hex)?,
            digest("5", EnrichmentOutputDigest::parse_hex)?,
            TranscriptSpans::new([span("1")?])?,
            NarrativeEpisodes::default(),
            entities,
            KnowledgeClaims::new([claim])?,
            KnowledgeRelations::default(),
            TokenCount::new(10),
            TokenCount::new(5),
        );
        assert!(matches!(
            result,
            Err(EnrichmentGraphError::UnresolvedCitation {
                field: "knowledge claims"
            })
        ));
        Ok(())
    }

    #[test]
    fn chunk_rejects_episodes_with_unresolved_span_citations()
    -> Result<(), Box<dyn std::error::Error>> {
        let episode = NarrativeEpisodeProjection::new(
            digest("2", NarrativeEpisodeId::parse_hex)?,
            EpisodeOrdinal::new(1)?,
            NarrativeTitle::new("Evidence-linked episode")?,
            NarrativeSummary::new("A bounded narrative must retain its exact transcript evidence")?,
            KnowledgeConfidence::High,
            EpistemicStatus::Explicit,
            SpanCitations::new([digest("9", TranscriptSpanId::parse_hex)?])?,
        );
        let result = EnrichmentChunkProjection::new(
            digest("3", EnrichmentChunkId::parse_hex)?,
            digest("4", EnrichmentOutputDigest::parse_hex)?,
            TranscriptSpans::new([span("1")?])?,
            NarrativeEpisodes::new([episode])?,
            KnowledgeEntities::default(),
            KnowledgeClaims::default(),
            KnowledgeRelations::default(),
            TokenCount::new(10),
            TokenCount::new(5),
        );
        assert!(matches!(
            result,
            Err(EnrichmentGraphError::UnresolvedCitation {
                field: "narrative episodes"
            })
        ));
        Ok(())
    }

    #[test]
    fn claim_subjects_preserve_session_wide_and_multi_entity_states()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = digest("d", KnowledgeEntityId::parse_hex)?;
        let second = digest("e", KnowledgeEntityId::parse_hex)?;
        let subjects = KnowledgeClaimSubjects::entities([first, second])?;
        assert_eq!(
            subjects.iter().copied().collect::<Vec<_>>(),
            [first, second]
        );
        assert_eq!(subjects.scope(), "entities");
        assert_eq!(KnowledgeClaimSubjects::SessionWide.scope(), "session_wide");
        assert_eq!(KnowledgeClaimSubjects::SessionWide.iter().count(), 0);
        assert!(KnowledgeClaimSubjects::entities([]).is_err());
        assert!(KnowledgeClaimSubjects::entities([first, first]).is_err());
        Ok(())
    }

    #[test]
    fn causal_assertions_cannot_claim_explicit_certainty() -> Result<(), Box<dyn std::error::Error>>
    {
        let span_id = digest("1", TranscriptSpanId::parse_hex)?;
        let root_cause = KnowledgeClaimProjection::new(
            digest("2", KnowledgeClaimId::parse_hex)?,
            KnowledgeKind::RootCauseHypothesis,
            KnowledgeClaimTitle::new("Possible cause")?,
            KnowledgeStatement::new("A dependency may have caused the failure")?,
            KnowledgeConfidence::Medium,
            EpistemicStatus::Explicit,
            KnowledgeClaimSubjects::SessionWide,
            SpanCitations::new([span_id])?,
            ObservationCorroboration::Unavailable,
        );
        assert!(matches!(
            root_cause,
            Err(EnrichmentGraphError::UnsupportedCausalCertainty)
        ));

        let relation = KnowledgeRelationProjection::new(
            digest("3", KnowledgeRelationId::parse_hex)?,
            KnowledgePredicate::Causes,
            digest("4", KnowledgeEntityId::parse_hex)?,
            digest("5", KnowledgeEntityId::parse_hex)?,
            KnowledgeConfidence::Medium,
            EpistemicStatus::Explicit,
            SpanCitations::new([span_id])?,
        );
        assert!(matches!(
            relation,
            Err(EnrichmentGraphError::UnsupportedCausalCertainty)
        ));
        Ok(())
    }

    #[test]
    fn chunk_accepts_closed_citation_graph() -> Result<(), Box<dyn std::error::Error>> {
        let span = span("1")?;
        let span_id = span.id();
        let subject = digest("2", KnowledgeEntityId::parse_hex)?;
        let object = digest("3", KnowledgeEntityId::parse_hex)?;
        let entities = KnowledgeEntities::new([
            KnowledgeEntityProjection::new(
                subject,
                KnowledgeEntityKind::Configuration,
                KnowledgeEntityName::new("overlay")?,
            ),
            KnowledgeEntityProjection::new(
                object,
                KnowledgeEntityKind::Artifact,
                KnowledgeEntityName::new("base graph")?,
            ),
        ])?;
        let relation = KnowledgeRelationProjection::new(
            digest("4", KnowledgeRelationId::parse_hex)?,
            KnowledgePredicate::RelatedTo,
            subject,
            object,
            KnowledgeConfidence::High,
            EpistemicStatus::Explicit,
            SpanCitations::new([span_id])?,
        )?;
        let projection = EnrichmentChunkProjection::new(
            digest("5", EnrichmentChunkId::parse_hex)?,
            digest("6", EnrichmentOutputDigest::parse_hex)?,
            TranscriptSpans::new([span])?,
            NarrativeEpisodes::default(),
            entities,
            KnowledgeClaims::default(),
            KnowledgeRelations::new([relation])?,
            TokenCount::new(10),
            TokenCount::new(5),
        )?;
        assert_eq!(projection.relations().count(), RecordCount::new(1));
        Ok(())
    }

    #[test]
    fn validated_values_serialize_without_internal_byte_arrays()
    -> Result<(), Box<dyn std::error::Error>> {
        let id = digest("a", EnrichmentFingerprint::parse_hex)?;
        assert_eq!(
            serde_json::to_string(&id)?,
            format!("\"{}\"", "a".repeat(64))
        );
        assert!(NarrativeTitle::new("unsafe\u{0}title").is_err());
        Ok(())
    }

    #[test]
    fn run_audit_provenance_is_closed_content_addressed_and_source_safe()
    -> Result<(), Box<dyn std::error::Error>> {
        let authorization = digest("b", EnrichmentAuthorizationPolicyDigest::parse_hex)?;
        let prompt = digest("c", EnrichmentPromptDigest::parse_hex)?;
        let provenance = EnrichmentRunAuditProvenance::new(
            EnrichmentDisclosureScope::ConversationAndExecution,
            authorization,
            prompt,
        );
        let value = serde_json::to_value(provenance)?;
        assert_eq!(value["disclosure_scope"], "conversation_and_execution");
        assert_eq!(value["authorization_policy_digest"], authorization.to_hex());
        assert_eq!(value["prompt_digest"], prompt.to_hex());
        assert!(EnrichmentDisclosureScope::parse("conversation_only").is_ok());
        assert!(EnrichmentDisclosureScope::parse("unbounded_scope").is_err());
        Ok(())
    }

    #[test]
    fn failure_classes_determine_retryability_without_free_text() {
        assert_eq!(
            EnrichmentFailureClass::RateLimited.status(),
            EnrichmentFailureStatus::RetryableFailed
        );
        assert_eq!(
            EnrichmentFailureClass::SecretEcho.status(),
            EnrichmentFailureStatus::TerminalFailed
        );
        assert_eq!(EnrichmentFailureClass::SecretEcho.as_str(), "secret_echo");
        assert_eq!(
            EnrichmentFailureClass::LeaseBusy.status(),
            EnrichmentFailureStatus::RetryableFailed
        );
    }

    #[test]
    fn paid_call_lease_values_are_opaque_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let owner = EnrichmentInvocationOwner::parse_hex(&"a".repeat(32))?;
        assert_eq!(owner.to_hex(), "a".repeat(32));
        assert_eq!(format!("{owner:?}"), "EnrichmentInvocationOwner([opaque])");
        assert!(EnrichmentInvocationOwner::parse_hex("not-an-owner").is_err());
        assert!(EnrichmentLeaseDuration::new(29).is_err());
        assert_eq!(EnrichmentLeaseDuration::new(30)?.seconds(), 30);
        assert_eq!(EnrichmentLeaseDuration::new(3_600)?.seconds(), 3_600);
        assert!(EnrichmentLeaseDuration::new(3_601).is_err());
        Ok(())
    }

    #[test]
    fn claimed_projection_requires_the_exact_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let projection = EnrichmentChunkProjection::new(
            digest("5", EnrichmentChunkId::parse_hex)?,
            digest("6", EnrichmentOutputDigest::parse_hex)?,
            TranscriptSpans::new([span("1")?])?,
            NarrativeEpisodes::default(),
            KnowledgeEntities::default(),
            KnowledgeClaims::default(),
            KnowledgeRelations::default(),
            TokenCount::new(10),
            TokenCount::new(5),
        )?;
        let owner = EnrichmentInvocationOwner::from_bytes([7; 16]);
        let wrong_lease =
            ClaimedEnrichmentChunk::new(digest("8", EnrichmentChunkId::parse_hex)?, owner);
        let run = EnrichmentRunRef::new(
            GraphNamespace::new("lease-contract")?,
            SourceDigest::hash(b"source"),
            digest("9", EnrichmentFingerprint::parse_hex)?,
        );
        assert!(matches!(
            ProjectClaimedEnrichmentChunkCommand::new(run, wrong_lease, projection),
            Err(EnrichmentGraphError::UnresolvedCitation {
                field: "claimed chunk projection"
            })
        ));
        Ok(())
    }

    #[test]
    fn committed_checkpoint_carries_cost_safe_resume_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let chunk_id = digest("b", EnrichmentChunkId::parse_hex)?;
        let output_digest = digest("c", EnrichmentOutputDigest::parse_hex)?;
        let checkpoint = EnrichmentChunkCheckpoint::Committed(CommittedEnrichmentChunk::new(
            chunk_id,
            output_digest,
            TokenCount::new(21),
            TokenCount::new(8),
        ));
        let EnrichmentChunkCheckpoint::Committed(receipt) = checkpoint else {
            return Err("committed checkpoint unexpectedly became required".into());
        };
        assert_eq!(receipt.chunk_id(), chunk_id);
        assert_eq!(receipt.output_digest(), output_digest);
        assert_eq!(receipt.input_tokens(), TokenCount::new(21));
        assert_eq!(receipt.output_tokens(), TokenCount::new(8));
        Ok(())
    }
}
