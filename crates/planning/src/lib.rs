//! Provider-agnostic narrative interpretation and precedent planning contracts.

use std::collections::HashSet;

use async_trait::async_trait;
use harness_graph_domain::{
    ActivityId, ActivityKind, ActivityStatus, GraphNamespace, PathSignature, RecordCount,
    SemanticActivities, SessionId, SourceDigest, TokenCount,
};
use serde::{Deserialize, Serialize};

/// Invalid interpretation or planning data.
#[derive(Debug, thiserror::Error)]
pub enum PlanningError {
    /// A required collection was empty.
    #[error("{field} must contain at least one item")]
    EmptyCollection {
        /// Typed field name.
        field: &'static str,
    },

    /// A bounded text value was empty or too long.
    #[error("{field} must contain between 1 and {maximum} characters")]
    InvalidText {
        /// Typed field name.
        field: &'static str,
        /// Maximum accepted Unicode scalar count.
        maximum: usize,
    },

    /// A precedent query limit was outside its safety bound.
    #[error("precedent limit must be between 1 and 10")]
    InvalidPrecedentLimit,

    /// A model-produced activity citation was not present in its input.
    #[error("model output cited an activity outside the supplied evidence")]
    UnknownActivityCitation,

    /// A model-produced precedent citation was not present in its input.
    #[error("model output cited a precedent outside the supplied evidence")]
    UnknownPrecedentCitation,

    /// Too many narrative activities escaped the bounded interpretation layer.
    #[error("narrative summary must contain at most 25 activities")]
    NarrativeTooLarge,

    /// A large deterministic sequence was compressed below the narrative floor.
    #[error("narrative summary must contain at least 15 activities for this source")]
    NarrativeTooSmall,

    /// At least one deterministic activity was omitted from the narrative map.
    #[error("narrative summary must cite every deterministic activity")]
    IncompleteNarrativeCoverage,

    /// A deterministic activity appeared in more than one macro-activity.
    #[error("narrative summary must cite each deterministic activity exactly once")]
    DuplicateNarrativeCitation,

    /// A candidate plan exceeded its execution safety bound.
    #[error("candidate plan must contain at most 20 steps")]
    PlanTooLarge,
}

macro_rules! bounded_text {
    ($name:ident, $field:literal, $maximum:literal) => {
        #[doc = concat!("Validated ", $field, ".")]
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Validate a ", $field, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error when the value is empty or exceeds its bound.
            pub fn new(value: impl Into<String>) -> Result<Self, PlanningError> {
                let value = value.into();
                let value = value.trim();
                if value.is_empty() || value.chars().count() > $maximum {
                    return Err(PlanningError::InvalidText {
                        field: $field,
                        maximum: $maximum,
                    });
                }
                Ok(Self(value.to_owned()))
            }

            /// Borrow the validated value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

bounded_text!(TaskBrief, "source-safe task brief", 2_000);
bounded_text!(NarrativeTitle, "narrative title", 120);
bounded_text!(PlanRationale, "plan rationale", 500);
bounded_text!(ClassificationExplanation, "classification explanation", 300);

/// Closed task category returned by the ambiguous model boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskCategory {
    /// Diagnose and repair incorrect behavior.
    BugFix,
    /// Add user-visible or system behavior.
    Feature,
    /// Restructure code while preserving behavior.
    Refactor,
    /// Gather evidence or investigate an unknown.
    Research,
    /// Operate, deploy, recover, or configure a system.
    Operations,
    /// Create or improve documentation.
    Documentation,
    /// Add, execute, or repair verification coverage.
    Testing,
    /// Inspect, transform, or explain data.
    DataAnalysis,
    /// The source-safe brief does not support a narrower category.
    Other,
}

impl TaskCategory {
    /// Stable API representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BugFix => "bug_fix",
            Self::Feature => "feature",
            Self::Refactor => "refactor",
            Self::Research => "research",
            Self::Operations => "operations",
            Self::Documentation => "documentation",
            Self::Testing => "testing",
            Self::DataAnalysis => "data_analysis",
            Self::Other => "other",
        }
    }
}

/// Coarse, non-numeric confidence that avoids false precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassificationConfidence {
    /// The brief is ambiguous or spans several categories.
    Low,
    /// The category is supported but not uniquely determined.
    Medium,
    /// The category follows directly from the brief.
    High,
}

impl ClassificationConfidence {
    /// Stable API representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Validated Mistral classification of a source-safe task brief.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassifiedTask {
    category: TaskCategory,
    confidence: ClassificationConfidence,
    explanation: ClassificationExplanation,
}

impl ClassifiedTask {
    /// Construct a typed classification.
    #[must_use]
    pub const fn new(
        category: TaskCategory,
        confidence: ClassificationConfidence,
        explanation: ClassificationExplanation,
    ) -> Self {
        Self {
            category,
            confidence,
            explanation,
        }
    }

    /// Closed task category.
    #[must_use]
    pub const fn category(&self) -> TaskCategory {
        self.category
    }

    /// Coarse model confidence.
    #[must_use]
    pub const fn confidence(&self) -> ClassificationConfidence {
        self.confidence
    }

    /// Bounded model explanation.
    #[must_use]
    pub const fn explanation(&self) -> &ClassificationExplanation {
        &self.explanation
    }
}

/// Source-safe task classification request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskClassificationRequest {
    task: TaskBrief,
}

impl TaskClassificationRequest {
    /// Construct a task classification request.
    #[must_use]
    pub const fn new(task: TaskBrief) -> Self {
        Self { task }
    }

    /// Validated source-safe brief.
    #[must_use]
    pub const fn task(&self) -> &TaskBrief {
        &self.task
    }
}

/// Validated maximum number of precedents returned by a graph query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrecedentLimit(usize);

impl PrecedentLimit {
    /// Validate a bounded precedent count.
    ///
    /// # Errors
    ///
    /// Returns an error outside `1..=10`.
    pub fn new(value: usize) -> Result<Self, PlanningError> {
        if (1..=10).contains(&value) {
            Ok(Self(value))
        } else {
            Err(PlanningError::InvalidPrecedentLimit)
        }
    }

    /// Numeric query limit.
    #[must_use]
    pub const fn value(self) -> usize {
        self.0
    }
}

/// One activity in a verified precedent path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrecedentStep {
    activity_id: ActivityId,
    kind: ActivityKind,
    status: ActivityStatus,
}

impl PrecedentStep {
    /// Construct a source-referenced precedent step.
    #[must_use]
    pub const fn new(activity_id: ActivityId, kind: ActivityKind, status: ActivityStatus) -> Self {
        Self {
            activity_id,
            kind,
            status,
        }
    }

    /// Stable activity identity.
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id
    }

    /// Semantic activity kind.
    #[must_use]
    pub const fn kind(&self) -> ActivityKind {
        self.kind
    }

    /// Evidence-derived activity status.
    #[must_use]
    pub const fn status(&self) -> ActivityStatus {
        self.status
    }
}

/// Non-empty steps from one verified precedent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrecedentSteps(Vec<PrecedentStep>);

impl PrecedentSteps {
    /// Validate non-empty precedent steps.
    ///
    /// # Errors
    ///
    /// Returns an error when no step is supplied.
    pub fn new(values: impl IntoIterator<Item = PrecedentStep>) -> Result<Self, PlanningError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            Err(PlanningError::EmptyCollection {
                field: "precedent steps",
            })
        } else {
            Ok(Self(values))
        }
    }

    /// Iterate in source order.
    pub fn iter(&self) -> impl Iterator<Item = &PrecedentStep> {
        self.0.iter()
    }

    /// Typed step count.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(self.0.len() as u64)
    }
}

/// One evidence-backed verified-success path available to Pathfinder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrecedentPath {
    session_id: SessionId,
    source_digest: SourceDigest,
    path_signature: PathSignature,
    steps: PrecedentSteps,
}

impl PrecedentPath {
    /// Construct a verified precedent path.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        source_digest: SourceDigest,
        path_signature: PathSignature,
        steps: PrecedentSteps,
    ) -> Self {
        Self {
            session_id,
            source_digest,
            path_signature,
            steps,
        }
    }

    /// Source session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Immutable source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Normalized path signature.
    #[must_use]
    pub const fn path_signature(&self) -> PathSignature {
        self.path_signature
    }

    /// Ordered source activities.
    #[must_use]
    pub const fn steps(&self) -> &PrecedentSteps {
        &self.steps
    }
}

/// Non-empty verified precedents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrecedentPaths(Vec<PrecedentPath>);

impl PrecedentPaths {
    /// Validate at least one verified precedent.
    ///
    /// # Errors
    ///
    /// Returns an error when no precedent is supplied.
    pub fn new(values: impl IntoIterator<Item = PrecedentPath>) -> Result<Self, PlanningError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            Err(PlanningError::EmptyCollection {
                field: "verified precedents",
            })
        } else {
            Ok(Self(values))
        }
    }

    /// Iterate over verified precedents.
    pub fn iter(&self) -> impl Iterator<Item = &PrecedentPath> {
        self.0.iter()
    }

    /// Typed precedent count.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(self.0.len() as u64)
    }

    fn activity_ids(&self) -> HashSet<ActivityId> {
        self.iter()
            .flat_map(|path| path.steps().iter().map(PrecedentStep::activity_id))
            .collect()
    }

    fn session_ids(&self) -> HashSet<SessionId> {
        self.iter().map(PrecedentPath::session_id).collect()
    }
}

/// Non-empty citations to deterministic activities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActivityCitations(Vec<ActivityId>);

impl ActivityCitations {
    /// Validate non-empty activity citations.
    ///
    /// # Errors
    ///
    /// Returns an error when no citation is supplied.
    pub fn new(values: impl IntoIterator<Item = ActivityId>) -> Result<Self, PlanningError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            Err(PlanningError::EmptyCollection {
                field: "activity citations",
            })
        } else {
            Ok(Self(values))
        }
    }

    /// Iterate over cited activity IDs.
    pub fn iter(&self) -> impl Iterator<Item = &ActivityId> {
        self.0.iter()
    }
}

/// One bounded narrative macro-activity produced from deterministic episodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NarrativeActivity {
    title: NarrativeTitle,
    kind: ActivityKind,
    origin: NarrativeOrigin,
    citations: ActivityCitations,
}

/// Provenance of a narrative label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NarrativeOrigin {
    /// Label returned by the validated Mistral structured-output boundary.
    Mistral,
    /// Kind-only label used when Mistral omitted a deterministic group.
    DeterministicFallback,
}

impl NarrativeOrigin {
    /// Stable output representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mistral => "mistral",
            Self::DeterministicFallback => "deterministic_fallback",
        }
    }
}

impl NarrativeActivity {
    /// Construct one narrative activity.
    #[must_use]
    pub const fn new(
        title: NarrativeTitle,
        kind: ActivityKind,
        origin: NarrativeOrigin,
        citations: ActivityCitations,
    ) -> Self {
        Self {
            title,
            kind,
            origin,
            citations,
        }
    }

    /// Short source-safe title.
    #[must_use]
    pub const fn title(&self) -> &NarrativeTitle {
        &self.title
    }

    /// Semantic kind.
    #[must_use]
    pub const fn kind(&self) -> ActivityKind {
        self.kind
    }

    /// Interpretation provenance.
    #[must_use]
    pub const fn origin(&self) -> NarrativeOrigin {
        self.origin
    }

    /// Supporting deterministic activities.
    #[must_use]
    pub const fn citations(&self) -> &ActivityCitations {
        &self.citations
    }
}

/// Bounded narrative summary that cannot replace its deterministic input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NarrativeSummary(Vec<NarrativeActivity>);

impl NarrativeSummary {
    /// Validate a model-produced summary against its deterministic source.
    ///
    /// # Errors
    ///
    /// Returns an error for empty/oversized summaries or unknown citations.
    pub fn new(
        values: impl IntoIterator<Item = NarrativeActivity>,
        source: &SemanticActivities,
    ) -> Result<Self, PlanningError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            return Err(PlanningError::EmptyCollection {
                field: "narrative activities",
            });
        }
        if values.len() > 25 {
            return Err(PlanningError::NarrativeTooLarge);
        }
        if source.count().value() >= 15 && values.len() < 15 {
            return Err(PlanningError::NarrativeTooSmall);
        }
        let known: HashSet<_> = source
            .iter()
            .map(harness_graph_domain::SemanticActivity::id)
            .collect();
        let mut cited = HashSet::new();
        for citation in values
            .iter()
            .flat_map(|activity| activity.citations().iter())
        {
            if !known.contains(citation) {
                return Err(PlanningError::UnknownActivityCitation);
            }
            if !cited.insert(*citation) {
                return Err(PlanningError::DuplicateNarrativeCitation);
            }
        }
        if cited != known {
            return Err(PlanningError::IncompleteNarrativeCoverage);
        }
        Ok(Self(values))
    }

    /// Iterate over narrative macro-activities.
    pub fn iter(&self) -> impl Iterator<Item = &NarrativeActivity> {
        self.0.iter()
    }

    /// Typed summary size.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(self.0.len() as u64)
    }
}

/// Source-safe deterministic activities awaiting ambiguous interpretation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarrativeRequest {
    activities: SemanticActivities,
}

impl NarrativeRequest {
    /// Construct a narrative request without raw payload data.
    ///
    /// # Errors
    ///
    /// Returns an error when no deterministic activity is available to cite.
    pub fn new(activities: SemanticActivities) -> Result<Self, PlanningError> {
        if activities.count().value() == 0 {
            Err(PlanningError::EmptyCollection {
                field: "deterministic activities",
            })
        } else {
            Ok(Self { activities })
        }
    }

    /// Deterministic evidence layer.
    #[must_use]
    pub const fn activities(&self) -> &SemanticActivities {
        &self.activities
    }
}

/// One source-cited future step proposed by Pathfinder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedStep {
    kind: ActivityKind,
    rationale: PlanRationale,
    citations: ActivityCitations,
}

impl PlannedStep {
    /// Construct a cited future step.
    #[must_use]
    pub const fn new(
        kind: ActivityKind,
        rationale: PlanRationale,
        citations: ActivityCitations,
    ) -> Self {
        Self {
            kind,
            rationale,
            citations,
        }
    }

    /// Proposed activity kind.
    #[must_use]
    pub const fn kind(&self) -> ActivityKind {
        self.kind
    }

    /// Bounded explanation.
    #[must_use]
    pub const fn rationale(&self) -> &PlanRationale {
        &self.rationale
    }

    /// Supporting precedent activities.
    #[must_use]
    pub const fn citations(&self) -> &ActivityCitations {
        &self.citations
    }
}

/// Non-empty future steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlannedSteps(Vec<PlannedStep>);

impl PlannedSteps {
    /// Validate at least one planned step.
    ///
    /// # Errors
    ///
    /// Returns an error when no step is supplied.
    pub fn new(values: impl IntoIterator<Item = PlannedStep>) -> Result<Self, PlanningError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            Err(PlanningError::EmptyCollection {
                field: "planned steps",
            })
        } else if values.len() > 20 {
            Err(PlanningError::PlanTooLarge)
        } else {
            Ok(Self(values))
        }
    }

    /// Iterate in proposed execution order.
    pub fn iter(&self) -> impl Iterator<Item = &PlannedStep> {
        self.0.iter()
    }

    /// Typed step count.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(self.0.len() as u64)
    }
}

/// Non-empty cited source sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrecedentCitations(Vec<SessionId>);

impl PrecedentCitations {
    /// Validate non-empty precedent citations.
    ///
    /// # Errors
    ///
    /// Returns an error when no precedent is cited.
    pub fn new(values: impl IntoIterator<Item = SessionId>) -> Result<Self, PlanningError> {
        let values: Vec<_> = values.into_iter().collect();
        if values.is_empty() {
            Err(PlanningError::EmptyCollection {
                field: "precedent citations",
            })
        } else {
            Ok(Self(values))
        }
    }

    /// Iterate over cited sessions.
    pub fn iter(&self) -> impl Iterator<Item = &SessionId> {
        self.0.iter()
    }
}

/// Fully validated candidate plan with source-backed citations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidatePlan {
    steps: PlannedSteps,
    precedents: PrecedentCitations,
}

impl CandidatePlan {
    /// Validate all model-produced citations against retrieved precedents.
    ///
    /// # Errors
    ///
    /// Returns an error when the model cites evidence outside its input.
    pub fn new(
        steps: PlannedSteps,
        precedents: PrecedentCitations,
        source: &PrecedentPaths,
    ) -> Result<Self, PlanningError> {
        let known_activities = source.activity_ids();
        if steps
            .iter()
            .flat_map(|step| step.citations().iter())
            .any(|citation| !known_activities.contains(citation))
        {
            return Err(PlanningError::UnknownActivityCitation);
        }
        let known_sessions = source.session_ids();
        if precedents
            .iter()
            .any(|citation| !known_sessions.contains(citation))
        {
            return Err(PlanningError::UnknownPrecedentCitation);
        }
        Ok(Self { steps, precedents })
    }

    /// Ordered future steps.
    #[must_use]
    pub const fn steps(&self) -> &PlannedSteps {
        &self.steps
    }

    /// Cited source sessions.
    #[must_use]
    pub const fn precedents(&self) -> &PrecedentCitations {
        &self.precedents
    }
}

/// Typed Pathfinder request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanningContext {
    task: TaskBrief,
}

/// Provider-reported usage for one successfully completed model operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsage {
    input: TokenCount,
    output: TokenCount,
    total: TokenCount,
}

impl ModelUsage {
    /// Construct typed provider usage.
    #[must_use]
    pub const fn new(input: TokenCount, output: TokenCount, total: TokenCount) -> Self {
        Self {
            input,
            output,
            total,
        }
    }

    /// Input tokens.
    #[must_use]
    pub const fn input(self) -> TokenCount {
        self.input
    }

    /// Output tokens.
    #[must_use]
    pub const fn output(self) -> TokenCount {
        self.output
    }

    /// Total tokens.
    #[must_use]
    pub const fn total(self) -> TokenCount {
        self.total
    }
}

/// Typed model result with provider usage retained for cost evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelResult<T> {
    value: T,
    usage: ModelUsage,
}

/// Atomic synchronization point for independent classification and extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SynchronizedInterpretation {
    classification: ModelResult<ClassifiedTask>,
    extraction: ModelResult<NarrativeSummary>,
}

impl SynchronizedInterpretation {
    /// Join two independently validated Mistral results.
    #[must_use]
    pub const fn new(
        classification: ModelResult<ClassifiedTask>,
        extraction: ModelResult<NarrativeSummary>,
    ) -> Self {
        Self {
            classification,
            extraction,
        }
    }

    /// Validated task classification and its usage.
    #[must_use]
    pub const fn classification(&self) -> &ModelResult<ClassifiedTask> {
        &self.classification
    }

    /// Citation-complete narrative extraction and its usage.
    #[must_use]
    pub const fn extraction(&self) -> &ModelResult<NarrativeSummary> {
        &self.extraction
    }
}

impl<T> ModelResult<T> {
    /// Construct a model result.
    #[must_use]
    pub const fn new(value: T, usage: ModelUsage) -> Self {
        Self { value, usage }
    }

    /// Validated model output.
    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Provider token evidence.
    #[must_use]
    pub const fn usage(&self) -> ModelUsage {
        self.usage
    }
}

impl PlanningContext {
    /// Construct a planning context.
    #[must_use]
    pub const fn new(task: TaskBrief) -> Self {
        Self { task }
    }

    /// Source-safe task brief.
    #[must_use]
    pub const fn task(&self) -> &TaskBrief {
        &self.task
    }
}

/// Read-only graph contract for verified-success precedents.
#[async_trait]
pub trait PrecedentReader: Send + Sync {
    /// Concrete graph error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Retrieve bounded verified-success paths.
    async fn verified_precedents(
        &self,
        namespace: &GraphNamespace,
        limit: PrecedentLimit,
    ) -> Result<PrecedentPaths, Self::Error>;
}

/// Provider-agnostic ambiguous narrative interpreter.
#[async_trait]
pub trait NarrativeInterpreter: Send + Sync {
    /// Concrete model adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Summarize deterministic episodes without deciding assurance.
    async fn summarize(
        &self,
        request: NarrativeRequest,
    ) -> Result<ModelResult<NarrativeSummary>, Self::Error>;
}

/// Provider-agnostic classifier for an ambiguous source-safe task brief.
#[async_trait]
pub trait TaskClassifier: Send + Sync {
    /// Concrete model adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Classify a source-safe brief into a closed category.
    async fn classify(
        &self,
        request: TaskClassificationRequest,
    ) -> Result<ModelResult<ClassifiedTask>, Self::Error>;
}

/// Provider-agnostic precedent-backed path planner.
#[async_trait]
pub trait Pathfinder: Send + Sync {
    /// Concrete model adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Propose a bounded plan whose citations validate against `precedents`.
    async fn propose(
        &self,
        context: PlanningContext,
        precedents: PrecedentPaths,
    ) -> Result<ModelResult<CandidatePlan>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{
        ActivityId, ActivityKind, ActivityStatus, PathSignature, SemanticActivities, SessionId,
        SourceDigest,
    };

    use super::{
        ActivityCitations, CandidatePlan, NarrativeRequest, PlanRationale, PlannedStep,
        PlannedSteps, PlanningError, PrecedentCitations, PrecedentPath, PrecedentPaths,
        PrecedentStep, PrecedentSteps,
    };

    #[test]
    fn narrative_request_rejects_an_uncitable_empty_source() {
        assert!(matches!(
            NarrativeRequest::new(SemanticActivities::default()),
            Err(PlanningError::EmptyCollection {
                field: "deterministic activities"
            })
        ));
    }

    #[test]
    fn candidate_plan_rejects_activity_outside_verified_precedents()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = SessionId::parse("019c8b3b-2aa8-7183-ba61-379f5b0af31c")?;
        let known_activity = ActivityId::hash(b"known activity");
        let precedents = PrecedentPaths::new([PrecedentPath::new(
            session_id,
            SourceDigest::hash(b"source"),
            PathSignature::hash(b"path"),
            PrecedentSteps::new([PrecedentStep::new(
                known_activity,
                ActivityKind::Inspect,
                ActivityStatus::Succeeded,
            )])?,
        )])?;
        let steps = PlannedSteps::new([PlannedStep::new(
            ActivityKind::Verify,
            PlanRationale::new("Verify after applying the precedent")?,
            ActivityCitations::new([ActivityId::hash(b"invented activity")])?,
        )])?;
        let citations = PrecedentCitations::new([session_id])?;

        assert!(matches!(
            CandidatePlan::new(steps, citations, &precedents),
            Err(PlanningError::UnknownActivityCitation)
        ));
        Ok(())
    }
}
