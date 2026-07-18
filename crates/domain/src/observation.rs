//! Canonical observations and partial lifecycle states.

use serde::{Deserialize, Serialize};

use crate::{
    ContextDigest, NativeCallId, NativeRecordKind, OccurredAt, PayloadDigest, RecordSequence,
    SessionId, SourceDigest, ToolName, TurnId,
};

/// Stable provenance for one line in a verified canonical rollout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRecordRef {
    session_id: SessionId,
    source_digest: SourceDigest,
    sequence: RecordSequence,
}

impl SourceRecordRef {
    /// Create source provenance.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        source_digest: SourceDigest,
        sequence: RecordSequence,
    ) -> Self {
        Self {
            session_id,
            source_digest,
            sequence,
        }
    }

    /// Session containing the record.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Verified source snapshot digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Record sequence.
    #[must_use]
    pub const fn sequence(&self) -> RecordSequence {
        self.sequence
    }
}

/// Canonical semantic category assigned deterministically at the protocol edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationKind {
    /// Session metadata assertion.
    SessionMetadataAsserted,
    /// Execution context assertion.
    ContextAsserted,
    /// Task lifecycle start.
    TaskStarted,
    /// Turn lifecycle start.
    TurnStarted,
    /// Turn was aborted.
    TurnAborted,
    /// Turn completed.
    TurnCompleted,
    /// User message.
    UserMessageReceived,
    /// Agent message.
    AgentMessageReceived,
    /// Tool invocation request.
    ToolRequested,
    /// Tool invocation result.
    ToolCompleted,
    /// Structured command completion.
    CommandCompleted,
    /// Patch application.
    PatchApplied,
    /// Token accounting observation.
    TokenUsageObserved,
    /// Thread settings assertion.
    ThreadSettingsApplied,
    /// Goal update.
    GoalUpdated,
    /// Context compaction boundary.
    ContextCompacted,
    /// Thread rollback boundary.
    ThreadRolledBack,
    /// World-state assertion.
    WorldStateAsserted,
    /// Sub-agent lifecycle observation.
    SubAgentActivityObserved,
    /// Inter-agent message.
    InterAgentMessageObserved,
    /// Explicit error event.
    ErrorObserved,
    /// Verification evidence.
    VerificationCompleted,
    /// Task lifecycle completion.
    TaskCompleted,
}

impl ObservationKind {
    /// Stable graph/property representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SessionMetadataAsserted => "session_metadata_asserted",
            Self::ContextAsserted => "context_asserted",
            Self::TaskStarted => "task_started",
            Self::TurnStarted => "turn_started",
            Self::TurnAborted => "turn_aborted",
            Self::TurnCompleted => "turn_completed",
            Self::UserMessageReceived => "user_message_received",
            Self::AgentMessageReceived => "agent_message_received",
            Self::ToolRequested => "tool_requested",
            Self::ToolCompleted => "tool_completed",
            Self::CommandCompleted => "command_completed",
            Self::PatchApplied => "patch_applied",
            Self::TokenUsageObserved => "token_usage_observed",
            Self::ThreadSettingsApplied => "thread_settings_applied",
            Self::GoalUpdated => "goal_updated",
            Self::ContextCompacted => "context_compacted",
            Self::ThreadRolledBack => "thread_rolled_back",
            Self::WorldStateAsserted => "world_state_asserted",
            Self::SubAgentActivityObserved => "sub_agent_activity_observed",
            Self::InterAgentMessageObserved => "inter_agent_message_observed",
            Self::ErrorObserved => "error_observed",
            Self::VerificationCompleted => "verification_completed",
            Self::TaskCompleted => "task_completed",
        }
    }
}

/// Relationship between an observation and semantic execution context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "context", content = "digest", rename_all = "snake_case")]
pub enum ContextAssociation {
    /// The record does not assert context.
    NotApplicable,
    /// The record asserts this stable semantic context.
    Asserted(ContextDigest),
}

/// Relationship between an observation and a native turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "turn", content = "id", rename_all = "snake_case")]
pub enum TurnAssociation {
    /// The record is session-scoped.
    SessionScoped,
    /// The record belongs to a native turn.
    Turn(TurnId),
}

/// Relationship between an observation and a native call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "call", content = "id", rename_all = "snake_case")]
pub enum CallAssociation {
    /// The record has no call identity.
    NotApplicable,
    /// The record references a native call.
    Call(NativeCallId),
}

/// Relationship between an observation and a tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "tool", content = "name", rename_all = "snake_case")]
pub enum ToolAssociation {
    /// The record does not identify a tool.
    NotApplicable,
    /// The record identifies a tool.
    Tool(ToolName),
}

/// One typed native observation stripped of sensitive payload content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    source: SourceRecordRef,
    occurred_at: OccurredAt,
    kind: ObservationKind,
    payload_digest: PayloadDigest,
    context: ContextAssociation,
    turn: TurnAssociation,
    call: CallAssociation,
    tool: ToolAssociation,
}

impl Observation {
    /// Construct a canonical observation at the protocol boundary.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        source: SourceRecordRef,
        occurred_at: OccurredAt,
        kind: ObservationKind,
        payload_digest: PayloadDigest,
        context: ContextAssociation,
        turn: TurnAssociation,
        call: CallAssociation,
        tool: ToolAssociation,
    ) -> Self {
        Self {
            source,
            occurred_at,
            kind,
            payload_digest,
            context,
            turn,
            call,
            tool,
        }
    }

    /// Record provenance.
    #[must_use]
    pub const fn source(&self) -> &SourceRecordRef {
        &self.source
    }

    /// Occurrence timestamp.
    #[must_use]
    pub const fn occurred_at(&self) -> OccurredAt {
        self.occurred_at
    }

    /// Canonical kind.
    #[must_use]
    pub const fn kind(&self) -> ObservationKind {
        self.kind
    }

    /// Redacted payload digest.
    #[must_use]
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }

    /// Stable semantic context digest, when the record asserts context.
    #[must_use]
    pub const fn context(&self) -> ContextAssociation {
        self.context
    }

    /// Native turn identity, when present.
    #[must_use]
    pub const fn turn(&self) -> &TurnAssociation {
        &self.turn
    }

    /// Native call identity, when present.
    #[must_use]
    pub const fn call(&self) -> &CallAssociation {
        &self.call
    }

    /// Tool name, when present.
    #[must_use]
    pub const fn tool(&self) -> &ToolAssociation {
        &self.tool
    }
}

/// A fully decoded record that can enter application logic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnownNativeRecord {
    observation: Observation,
}

impl KnownNativeRecord {
    /// Wrap a canonical observation.
    #[must_use]
    pub const fn new(observation: Observation) -> Self {
        Self { observation }
    }

    /// Borrow the canonical observation.
    #[must_use]
    pub const fn observation(&self) -> &Observation {
        &self.observation
    }
}

/// A forward-compatible native record retained as typed quarantine metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedNativeRecord {
    source: SourceRecordRef,
    occurred_at: OccurredAt,
    native_kind: NativeRecordKind,
    payload_digest: PayloadDigest,
}

impl UnsupportedNativeRecord {
    /// Construct quarantine metadata.
    #[must_use]
    pub const fn new(
        source: SourceRecordRef,
        occurred_at: OccurredAt,
        native_kind: NativeRecordKind,
        payload_digest: PayloadDigest,
    ) -> Self {
        Self {
            source,
            occurred_at,
            native_kind,
            payload_digest,
        }
    }

    /// Record provenance.
    #[must_use]
    pub const fn source(&self) -> &SourceRecordRef {
        &self.source
    }

    /// Native record kind.
    #[must_use]
    pub const fn native_kind(&self) -> &NativeRecordKind {
        &self.native_kind
    }

    /// Occurrence timestamp.
    #[must_use]
    pub const fn occurred_at(&self) -> OccurredAt {
        self.occurred_at
    }

    /// Redacted payload digest.
    #[must_use]
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }
}

/// Result of decoding one native record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "support", rename_all = "snake_case")]
pub enum DecodedNativeRecord {
    /// Supported record mapped to a canonical observation.
    Known(KnownNativeRecord),
    /// Forward-compatible quarantine record.
    Unsupported(UnsupportedNativeRecord),
}

/// Correlation state for a native tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ToolCallLifecycle {
    /// A request exists without a result in the current source snapshot.
    Pending {
        /// Native call identity.
        call_id: NativeCallId,
    },
    /// A native request/result pair exists.
    Completed {
        /// Native call identity.
        call_id: NativeCallId,
    },
    /// The containing turn was interrupted before a result arrived.
    Interrupted {
        /// Native call identity.
        call_id: NativeCallId,
    },
    /// A result exists without an observed request.
    OrphanedResult {
        /// Native call identity.
        call_id: NativeCallId,
    },
}
