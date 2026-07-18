//! Typed domain objects for `HarnessGraph`.

mod digest;
mod error;
mod observation;
mod semantics;
mod value;

pub use digest::{
    ActivityId, ContextDigest, InvocationDigest, PathSignature, PayloadDigest, RiskId, SourceDigest,
};
pub use error::DomainError;
pub use observation::{
    CallAssociation, ContextAssociation, DecodedNativeRecord, KnownNativeRecord, Observation,
    ObservationKind, SourceRecordRef, ToolAssociation, ToolCallLifecycle, TurnAssociation,
    UnsupportedNativeRecord,
};
pub use semantics::{
    ActivityInvocation, ActivityKind, ActivityStatus, AnalysisReport, CorrelatedInvocation,
    CorrelatedOutcome, CorrelatedPurpose, CorrelatedTool, CorrelatedToolCall, EvidenceRefs,
    ExecutionPath, HazardKind, InvocationAssociation, OutcomeAssociation, OutcomeClass, PathStep,
    PathSteps, RiskExposure, RiskExposures, RiskSeverity, RunOutcome, SemanticActivities,
    SemanticActivity, ToolCallCorrelations, ToolOutcome, ToolPurpose, VerificationStatus,
};
pub use value::{
    GraphNamespace, NativeCallId, NativeRecordKind, ObservationId, OccurredAt, RecordCount,
    RecordSequence, SessionId, TokenCount, ToolName, TurnId,
};
