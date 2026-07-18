//! Deterministic semantic, assurance, risk, and path objects.

use serde::{Deserialize, Serialize};

use crate::{
    ActivityId, DomainError, InvocationDigest, NativeCallId, PathSignature, RiskId,
    SourceRecordRef, ToolCallLifecycle, ToolName,
};

/// Deterministic purpose inferred at the protocol boundary without retaining
/// raw command text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPurpose {
    /// Read, list, or inspect state.
    Inspect,
    /// Search local or remote information.
    Search,
    /// Change source or filesystem state.
    Modify,
    /// Establish verification evidence.
    Verify,
    /// Install a dependency or runtime.
    Install,
    /// Execute behavior with no narrower deterministic classification.
    Execute,
    /// Request or use elevated permissions.
    PermissionEscalation,
    /// Access a network boundary.
    NetworkAccess,
    /// Perform a potentially destructive operation.
    Destructive,
    /// Typed evidence is insufficient for a deterministic purpose.
    Ambiguous,
}

impl ToolPurpose {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Search => "search",
            Self::Modify => "modify",
            Self::Verify => "verify",
            Self::Install => "install",
            Self::Execute => "execute",
            Self::PermissionEscalation => "permission_escalation",
            Self::NetworkAccess => "network_access",
            Self::Destructive => "destructive",
            Self::Ambiguous => "ambiguous",
        }
    }
}

/// Structured outcome of a tool result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    /// The provider contract reported success.
    Succeeded,
    /// The provider contract reported failure.
    Failed,
    /// A result exists but its success semantics are unavailable.
    Indeterminate,
}

impl ToolOutcome {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// Tool-invocation semantics associated with an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "invocation", rename_all = "snake_case")]
pub enum InvocationAssociation {
    /// The observation is not a tool request.
    NotApplicable,
    /// A source-safe invocation fingerprint and deterministic purpose exist.
    Classified {
        /// Hash of tool name plus raw invocation input, computed before the raw
        /// input is discarded.
        digest: InvocationDigest,
        /// Deterministic purpose.
        purpose: ToolPurpose,
    },
}

/// Structured result semantics associated with an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", content = "outcome", rename_all = "snake_case")]
pub enum OutcomeAssociation {
    /// The observation is not a tool result.
    NotApplicable,
    /// The observation carries a structured or contract-derived outcome.
    Tool(ToolOutcome),
}

/// A correlated tool may be unnamed when only a result was observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "tool", content = "name", rename_all = "snake_case")]
pub enum CorrelatedTool {
    /// No trusted tool name was present.
    Unnamed,
    /// Trusted tool name from a request.
    Named(ToolName),
}

/// Purpose state for a correlated tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "purpose_state", content = "purpose", rename_all = "snake_case")]
pub enum CorrelatedPurpose {
    /// No request carrying purpose evidence was observed.
    Unknown,
    /// Deterministic purpose from the request boundary.
    Known(ToolPurpose),
}

/// Invocation-fingerprint state for a correlated tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "invocation_state",
    content = "digest",
    rename_all = "snake_case"
)]
pub enum CorrelatedInvocation {
    /// No request carrying invocation evidence was observed.
    Unknown,
    /// Content-addressed request fingerprint.
    Known(InvocationDigest),
}

/// Outcome state for a correlated tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome_state", content = "outcome", rename_all = "snake_case")]
pub enum CorrelatedOutcome {
    /// No result was observed.
    Missing,
    /// A result was observed.
    Known(ToolOutcome),
}

/// One native-ID correlation with source-safe provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrelatedToolCall {
    call_id: NativeCallId,
    lifecycle: ToolCallLifecycle,
    tool: CorrelatedTool,
    purpose: CorrelatedPurpose,
    invocation: CorrelatedInvocation,
    outcome: CorrelatedOutcome,
    evidence: EvidenceRefs,
}

impl CorrelatedToolCall {
    /// Construct a fully typed correlation.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        call_id: NativeCallId,
        lifecycle: ToolCallLifecycle,
        tool: CorrelatedTool,
        purpose: CorrelatedPurpose,
        invocation: CorrelatedInvocation,
        outcome: CorrelatedOutcome,
        evidence: EvidenceRefs,
    ) -> Self {
        Self {
            call_id,
            lifecycle,
            tool,
            purpose,
            invocation,
            outcome,
            evidence,
        }
    }

    /// Native call identity.
    #[must_use]
    pub const fn call_id(&self) -> &NativeCallId {
        &self.call_id
    }

    /// Partial or completed lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &ToolCallLifecycle {
        &self.lifecycle
    }

    /// Tool identity.
    #[must_use]
    pub const fn tool(&self) -> &CorrelatedTool {
        &self.tool
    }

    /// Deterministic purpose state.
    #[must_use]
    pub const fn purpose(&self) -> CorrelatedPurpose {
        self.purpose
    }

    /// Invocation fingerprint state.
    #[must_use]
    pub const fn invocation(&self) -> CorrelatedInvocation {
        self.invocation
    }

    /// Result state.
    #[must_use]
    pub const fn outcome(&self) -> CorrelatedOutcome {
        self.outcome
    }

    /// Supporting source references.
    #[must_use]
    pub const fn evidence(&self) -> &EvidenceRefs {
        &self.evidence
    }
}

/// Non-empty source evidence supporting a derived object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EvidenceRefs(Vec<SourceRecordRef>);

impl EvidenceRefs {
    /// Validate a non-empty evidence set.
    ///
    /// # Errors
    ///
    /// Returns an error when no source reference is supplied.
    pub fn new(values: impl IntoIterator<Item = SourceRecordRef>) -> Result<Self, DomainError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            return Err(DomainError::EmptyCollection { field: "evidence" });
        }
        Ok(Self(values))
    }

    /// Iterate over supporting source references.
    pub fn iter(&self) -> impl Iterator<Item = &SourceRecordRef> {
        self.0.iter()
    }

    /// Typed number of evidence references.
    #[must_use]
    pub fn count(&self) -> crate::RecordCount {
        crate::RecordCount::new(self.0.len() as u64)
    }

    /// First source reference in canonical order.
    #[must_use]
    pub fn first(&self) -> &SourceRecordRef {
        &self.0[0]
    }

    /// Last source reference in canonical order.
    #[must_use]
    pub fn last(&self) -> &SourceRecordRef {
        &self.0[self.0.len() - 1]
    }

    /// Merge ordered evidence while preserving the non-empty invariant.
    #[must_use]
    pub fn merge(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        Self(self.0)
    }
}

/// Correlations for one source snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallCorrelations(Vec<CorrelatedToolCall>);

impl ToolCallCorrelations {
    /// Construct a correlation collection.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = CorrelatedToolCall>) -> Self {
        Self(values.into_iter().collect())
    }

    /// Iterate over correlations.
    pub fn iter(&self) -> impl Iterator<Item = &CorrelatedToolCall> {
        self.0.iter()
    }

    /// Typed number of correlations.
    #[must_use]
    pub fn count(&self) -> crate::RecordCount {
        crate::RecordCount::new(self.0.len() as u64)
    }
}

/// Normalized semantic activity kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    /// Start a task or run.
    Start,
    /// Receive a task request.
    Request,
    /// Inspect local state.
    Inspect,
    /// Search for information.
    Search,
    /// Modify state.
    Modify,
    /// Modify state in response to observed failure evidence.
    Repair,
    /// Verify behavior.
    Verify,
    /// Install a dependency or runtime.
    Install,
    /// Execute uncategorized behavior.
    Execute,
    /// Diagnose a failure.
    Diagnose,
    /// Request or use elevated permissions.
    RequestPermission,
    /// Access a network boundary.
    NetworkAccess,
    /// Execute a destructive operation.
    Destructive,
    /// Compact or otherwise change context.
    ManageContext,
    /// Roll back state.
    Rollback,
    /// Complete the task.
    Complete,
}

impl ActivityKind {
    /// Stable graph/path representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Request => "request",
            Self::Inspect => "inspect",
            Self::Search => "search",
            Self::Modify => "modify",
            Self::Repair => "repair",
            Self::Verify => "verify",
            Self::Install => "install",
            Self::Execute => "execute",
            Self::Diagnose => "diagnose",
            Self::RequestPermission => "request_permission",
            Self::NetworkAccess => "network_access",
            Self::Destructive => "destructive",
            Self::ManageContext => "manage_context",
            Self::Rollback => "rollback",
            Self::Complete => "complete",
        }
    }
}

/// Semantic activity completion state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityStatus {
    /// Activity began but no result exists.
    Pending,
    /// Activity completed successfully.
    Succeeded,
    /// Activity completed unsuccessfully.
    Failed,
    /// Activity was interrupted.
    Interrupted,
    /// Completion exists without trustworthy success semantics.
    Indeterminate,
}

impl ActivityStatus {
    /// Stable graph/path representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::Indeterminate => "indeterminate",
        }
    }
}

/// Invocation fingerprint attached to an activity when applicable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "invocation", content = "digest", rename_all = "snake_case")]
pub enum ActivityInvocation {
    /// Activity is not backed by a tool invocation.
    NotApplicable,
    /// Tool result was observed without a request fingerprint.
    Unknown,
    /// Content-addressed invocation fingerprint.
    Known(InvocationDigest),
}

/// One normalized semantic activity or adjacent episode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticActivity {
    id: ActivityId,
    kind: ActivityKind,
    status: ActivityStatus,
    invocation: ActivityInvocation,
    evidence: EvidenceRefs,
}

impl SemanticActivity {
    /// Construct a typed semantic activity.
    #[must_use]
    pub const fn new(
        id: ActivityId,
        kind: ActivityKind,
        status: ActivityStatus,
        invocation: ActivityInvocation,
        evidence: EvidenceRefs,
    ) -> Self {
        Self {
            id,
            kind,
            status,
            invocation,
            evidence,
        }
    }

    /// Stable identity.
    #[must_use]
    pub const fn id(&self) -> ActivityId {
        self.id
    }

    /// Normalized kind.
    #[must_use]
    pub const fn kind(&self) -> ActivityKind {
        self.kind
    }

    /// Completion state.
    #[must_use]
    pub const fn status(&self) -> ActivityStatus {
        self.status
    }

    /// Invocation fingerprint state.
    #[must_use]
    pub const fn invocation(&self) -> ActivityInvocation {
        self.invocation
    }

    /// Supporting source evidence.
    #[must_use]
    pub const fn evidence(&self) -> &EvidenceRefs {
        &self.evidence
    }

    /// Merge an adjacent activity of the same kind and status into an episode.
    #[must_use]
    pub fn merge_evidence(self, evidence: EvidenceRefs) -> Self {
        Self {
            evidence: self.evidence.merge(evidence),
            ..self
        }
    }
}

/// Ordered semantic activities for a source snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SemanticActivities(Vec<SemanticActivity>);

impl SemanticActivities {
    /// Construct an ordered activity collection.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = SemanticActivity>) -> Self {
        Self(values.into_iter().collect())
    }

    /// Iterate in canonical source order.
    #[must_use]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &SemanticActivity> {
        self.0.iter()
    }

    /// Typed activity count.
    #[must_use]
    pub fn count(&self) -> crate::RecordCount {
        crate::RecordCount::new(self.0.len() as u64)
    }
}

/// Evidence-derived run outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeClass {
    /// A fresh successful verification supports completion.
    VerifiedSuccess,
    /// Completion was observed without fresh verification.
    UnverifiedCompletion,
    /// Explicit failure evidence exists.
    Failed,
    /// Execution ended without enough evidence for another class.
    Inconclusive,
    /// Execution was cancelled or interrupted.
    Cancelled,
}

impl OutcomeClass {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VerifiedSuccess => "verified_success",
            Self::UnverifiedCompletion => "unverified_completion",
            Self::Failed => "failed",
            Self::Inconclusive => "inconclusive",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Freshness and success state of verification evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    /// Successful verification occurred after the final modification.
    Fresh,
    /// Successful verification exists but predates the final modification.
    Stale,
    /// A verification activity failed.
    Failed,
    /// No verification activity exists.
    Missing,
}

impl VerificationStatus {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Failed => "failed",
            Self::Missing => "missing",
        }
    }
}

/// Deterministic run outcome with supporting evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOutcome {
    class: OutcomeClass,
    verification: VerificationStatus,
    evidence: EvidenceRefs,
}

impl RunOutcome {
    /// Construct an evidence-derived outcome.
    #[must_use]
    pub const fn new(
        class: OutcomeClass,
        verification: VerificationStatus,
        evidence: EvidenceRefs,
    ) -> Self {
        Self {
            class,
            verification,
            evidence,
        }
    }

    /// Outcome class.
    #[must_use]
    pub const fn class(&self) -> OutcomeClass {
        self.class
    }

    /// Verification freshness.
    #[must_use]
    pub const fn verification(&self) -> VerificationStatus {
        self.verification
    }

    /// Supporting evidence.
    #[must_use]
    pub const fn evidence(&self) -> &EvidenceRefs {
        &self.evidence
    }
}

/// Initial deterministic hazard vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HazardKind {
    /// A modification followed the latest successful verification.
    UnverifiedFinalEdit,
    /// The same invocation failed repeatedly.
    RepeatedFailingCommand,
    /// The same invocation repeated enough to indicate a loop.
    ToolCallLoop,
    /// A pending, interrupted, or orphaned call remains.
    IncompleteObservationStream,
    /// An elevated-permission operation was observed.
    PermissionEscalation,
    /// A destructive operation was observed.
    DestructiveCommand,
}

impl HazardKind {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnverifiedFinalEdit => "unverified_final_edit",
            Self::RepeatedFailingCommand => "repeated_failing_command",
            Self::ToolCallLoop => "tool_call_loop",
            Self::IncompleteObservationStream => "incomplete_observation_stream",
            Self::PermissionEscalation => "permission_escalation",
            Self::DestructiveCommand => "destructive_command",
        }
    }
}

/// Ordinal risk severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskSeverity {
    /// Low operational impact.
    Low,
    /// Material but bounded impact.
    Medium,
    /// High impact requiring attention.
    High,
    /// Potentially catastrophic impact.
    Critical,
}

impl RiskSeverity {
    /// Stable graph representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

/// One evidence-linked deterministic risk exposure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskExposure {
    id: RiskId,
    hazard: HazardKind,
    severity: RiskSeverity,
    evidence: EvidenceRefs,
}

impl RiskExposure {
    /// Construct a risk exposure.
    #[must_use]
    pub const fn new(
        id: RiskId,
        hazard: HazardKind,
        severity: RiskSeverity,
        evidence: EvidenceRefs,
    ) -> Self {
        Self {
            id,
            hazard,
            severity,
            evidence,
        }
    }

    /// Stable identity.
    #[must_use]
    pub const fn id(&self) -> RiskId {
        self.id
    }

    /// Hazard category.
    #[must_use]
    pub const fn hazard(&self) -> HazardKind {
        self.hazard
    }

    /// Severity.
    #[must_use]
    pub const fn severity(&self) -> RiskSeverity {
        self.severity
    }

    /// Supporting evidence.
    #[must_use]
    pub const fn evidence(&self) -> &EvidenceRefs {
        &self.evidence
    }
}

/// Deterministic risks for one source snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RiskExposures(Vec<RiskExposure>);

impl RiskExposures {
    /// Construct a risk collection.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = RiskExposure>) -> Self {
        Self(values.into_iter().collect())
    }

    /// Iterate over risks.
    pub fn iter(&self) -> impl Iterator<Item = &RiskExposure> {
        self.0.iter()
    }

    /// Typed risk count.
    #[must_use]
    pub fn count(&self) -> crate::RecordCount {
        crate::RecordCount::new(self.0.len() as u64)
    }
}

/// One normalized path step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PathStep {
    kind: ActivityKind,
    status: ActivityStatus,
}

impl PathStep {
    /// Construct a normalized step.
    #[must_use]
    pub const fn new(kind: ActivityKind, status: ActivityStatus) -> Self {
        Self { kind, status }
    }

    /// Step kind.
    #[must_use]
    pub const fn kind(self) -> ActivityKind {
        self.kind
    }

    /// Step status.
    #[must_use]
    pub const fn status(self) -> ActivityStatus {
        self.status
    }
}

/// Non-empty normalized path steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PathSteps(Vec<PathStep>);

impl PathSteps {
    /// Validate non-empty normalized steps.
    ///
    /// # Errors
    ///
    /// Returns an error when no semantic activity exists.
    pub fn new(values: impl IntoIterator<Item = PathStep>) -> Result<Self, DomainError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            return Err(DomainError::EmptyCollection {
                field: "execution path",
            });
        }
        Ok(Self(values))
    }

    /// Iterate over normalized steps.
    pub fn iter(&self) -> impl Iterator<Item = &PathStep> {
        self.0.iter()
    }

    /// Typed number of normalized steps.
    #[must_use]
    pub fn count(&self) -> crate::RecordCount {
        crate::RecordCount::new(self.0.len() as u64)
    }
}

/// Content-addressed normalized execution path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPath {
    signature: PathSignature,
    steps: PathSteps,
}

impl ExecutionPath {
    /// Construct an execution path.
    #[must_use]
    pub const fn new(signature: PathSignature, steps: PathSteps) -> Self {
        Self { signature, steps }
    }

    /// Stable path signature.
    #[must_use]
    pub const fn signature(&self) -> PathSignature {
        self.signature
    }

    /// Ordered normalized steps.
    #[must_use]
    pub const fn steps(&self) -> &PathSteps {
        &self.steps
    }
}

/// Complete deterministic analysis result ready for projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalysisReport {
    correlations: ToolCallCorrelations,
    activities: SemanticActivities,
    outcome: RunOutcome,
    risks: RiskExposures,
    path: ExecutionPath,
}

impl AnalysisReport {
    /// Construct a complete report from independently derived components.
    #[must_use]
    pub const fn new(
        correlations: ToolCallCorrelations,
        activities: SemanticActivities,
        outcome: RunOutcome,
        risks: RiskExposures,
        path: ExecutionPath,
    ) -> Self {
        Self {
            correlations,
            activities,
            outcome,
            risks,
            path,
        }
    }

    /// Tool-call correlations.
    #[must_use]
    pub const fn correlations(&self) -> &ToolCallCorrelations {
        &self.correlations
    }

    /// Semantic activities.
    #[must_use]
    pub const fn activities(&self) -> &SemanticActivities {
        &self.activities
    }

    /// Evidence-derived outcome.
    #[must_use]
    pub const fn outcome(&self) -> &RunOutcome {
        &self.outcome
    }

    /// Risk exposures.
    #[must_use]
    pub const fn risks(&self) -> &RiskExposures {
        &self.risks
    }

    /// Normalized execution path.
    #[must_use]
    pub const fn path(&self) -> &ExecutionPath {
        &self.path
    }
}
