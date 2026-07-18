//! Source-safe, read-only experience projections for HTTP and UI consumers.

use std::collections::HashSet;

use async_trait::async_trait;
use harness_graph_domain::{
    ActivityId, ActivityKind, ActivityStatus, GraphNamespace, OutcomeClass, PayloadDigest,
    RecordCount, RecordSequence, SessionId,
};
use serde::{Serialize, Serializer};

use crate::{
    EnrichmentAuthorizationPolicyDigest, EnrichmentDisclosureScope, EnrichmentModelName,
    EnrichmentPromptDigest, EnrichmentProvider, EnrichmentRunId, EnrichmentSchemaVersion,
    EpistemicStatus, KnowledgeClaimProjection, KnowledgeClaimSubjects, KnowledgeConfidence,
    KnowledgeEntityId, KnowledgeEntityProjection, KnowledgePredicate, KnowledgeRelationProjection,
    NarrativeEpisodeProjection, PromptVersion, SelectedEnrichment, TranscriptSpanId,
};

const MAX_SESSIONS: usize = 100_000;
const MAX_ACTIVITIES: usize = 1_000_000;
const MAX_SOURCE_ANCHORS: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExperienceActivityId(ActivityId);

impl ExperienceActivityId {
    const fn new(value: ActivityId) -> Self {
        Self(value)
    }

    const fn domain(self) -> ActivityId {
        self.0
    }
}

impl Serialize for ExperienceActivityId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_hex())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExperienceContentDigest(PayloadDigest);

impl ExperienceContentDigest {
    const fn new(value: PayloadDigest) -> Self {
        Self(value)
    }
}

impl Serialize for ExperienceContentDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_hex())
    }
}

/// Construction failure at the source-safe experience boundary.
#[derive(Debug, thiserror::Error)]
pub enum ExperienceGraphError {
    /// A model-derived display value was empty, unsafe, or oversized.
    #[error("invalid source-safe {field}: expected 1 to {maximum} display characters")]
    InvalidDisplayText {
        /// Closed display field name.
        field: &'static str,
        /// Maximum number of Unicode scalar values.
        maximum: usize,
    },
    /// A bounded collection exceeded the public response safety limit.
    #[error("{field} exceeds the response limit of {maximum} items")]
    CollectionLimit {
        /// Closed collection name.
        field: &'static str,
        /// Maximum public item count.
        maximum: usize,
    },
    /// A set-like response collection contained a duplicate identity.
    #[error("{field} contains a duplicate identity")]
    DuplicateIdentity {
        /// Closed collection name.
        field: &'static str,
    },
    /// A selected completed enrichment had no displayable narrative episode.
    #[error("selected enrichment has no evidence-cited narrative episode")]
    MissingNarrativeEpisode,
    /// A completed enrichment referenced an object absent from the response.
    #[error("{field} contains an unresolved source-safe reference")]
    UnresolvedReference {
        /// Closed reference collection name.
        field: &'static str,
    },
}

macro_rules! source_safe_text {
    ($name:ident, $field:literal, $maximum:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            fn bounded(value: &str) -> Result<Self, ExperienceGraphError> {
                let value = bounded_text(value, $field, $maximum)?;
                Ok(Self(value))
            }

            /// Borrow the validated display value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

source_safe_text!(
    ExperienceTitle,
    "experience title",
    160,
    "Bounded source-safe title shown to an experience consumer."
);
source_safe_text!(
    ExperienceSummary,
    "experience summary",
    1_000,
    "Bounded source-safe summary shown to an experience consumer."
);
source_safe_text!(
    ExperienceActivityLabel,
    "activity label",
    160,
    "Deterministic kind-and-status activity label."
);
source_safe_text!(
    ExperienceAnchorLabel,
    "source anchor label",
    160,
    "Content-free source anchor label."
);
source_safe_text!(
    ExperienceEntityName,
    "knowledge entity name",
    160,
    "Bounded source-safe entity display name."
);
source_safe_text!(
    ExperienceClaimTitle,
    "knowledge claim title",
    160,
    "Bounded source-safe knowledge claim title."
);
source_safe_text!(
    ExperienceStatement,
    "knowledge statement",
    1_000,
    "Bounded source-safe knowledge statement."
);

fn bounded_text(
    value: &str,
    field: &'static str,
    maximum: usize,
) -> Result<String, ExperienceGraphError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || contains_forbidden_display_value(trimmed) {
        return Err(ExperienceGraphError::InvalidDisplayText { field, maximum });
    }
    if trimmed.chars().count() > maximum
        || trimmed
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\t'))
    {
        return Err(ExperienceGraphError::InvalidDisplayText { field, maximum });
    }
    Ok(trimmed.to_owned())
}

fn contains_forbidden_display_value(value: &str) -> bool {
    let lowercase = value.to_ascii_lowercase();
    let private_key = lowercase.contains("-----begin ") && lowercase.contains(" private key-----");
    let local_path = lowercase.contains("/users/")
        || lowercase.contains("/home/")
        || lowercase.contains("file://")
        || lowercase.contains(":\\users\\");
    let bearer = lowercase.split_whitespace().any(|part| part == "bearer")
        && lowercase
            .split_whitespace()
            .any(|part| part.len() >= 12 && part != "bearer");
    let assignment = [
        "api_key",
        "api-key",
        "access_token",
        "access-token",
        "password",
        "secret",
    ]
    .iter()
    .any(|marker| {
        [
            format!("{marker}="),
            format!("{marker} ="),
            format!("{marker}:"),
            format!("{marker} :"),
        ]
        .iter()
        .any(|candidate| lowercase.contains(candidate))
    });
    let jwt = value
        .split(|character: char| character.is_whitespace() || matches!(character, ',' | ';'))
        .any(|part| part.starts_with("eyJ") && part.matches('.').count() == 2 && part.len() >= 32);
    private_key || local_path || bearer || assignment || jwt
}

/// Whether human-readable display text came from enrichment or deterministic state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceDisplaySource {
    /// A selected completed Mistral enrichment supplied the display text.
    Enrichment,
    /// Closed deterministic graph values supplied the display text.
    DeterministicFallback,
}

/// Source-safe title and summary with explicit provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExperienceDisplay {
    source: ExperienceDisplaySource,
    title: ExperienceTitle,
    summary: ExperienceSummary,
}

impl ExperienceDisplay {
    fn deterministic(outcome: OutcomeClass, activity_count: RecordCount) -> Self {
        let (title, summary) = deterministic_display(outcome, activity_count);
        Self {
            source: ExperienceDisplaySource::DeterministicFallback,
            title: ExperienceTitle(title.to_owned()),
            summary: ExperienceSummary(summary),
        }
    }

    fn enriched(title: &str, summary: &str) -> Result<Self, ExperienceGraphError> {
        Ok(Self {
            source: ExperienceDisplaySource::Enrichment,
            title: ExperienceTitle::bounded(title)?,
            summary: ExperienceSummary::bounded(summary)?,
        })
    }

    /// Display provenance.
    #[must_use]
    pub const fn source(&self) -> ExperienceDisplaySource {
        self.source
    }
}

fn deterministic_display(
    outcome: OutcomeClass,
    activity_count: RecordCount,
) -> (&'static str, String) {
    let count = activity_count.value();
    match outcome {
        OutcomeClass::VerifiedSuccess => (
            "Verified successful session",
            format!(
                "The authoritative graph contains {count} deterministic activities and fresh successful verification evidence."
            ),
        ),
        OutcomeClass::UnverifiedCompletion => (
            "Completed session awaiting verification",
            format!(
                "The authoritative graph contains {count} deterministic activities, but completion lacks fresh successful verification."
            ),
        ),
        OutcomeClass::Failed => (
            "Session with observed failure",
            format!(
                "The authoritative graph contains {count} deterministic activities and explicit failure evidence."
            ),
        ),
        OutcomeClass::Inconclusive => (
            "Inconclusive session",
            format!(
                "The verified source contains {count} deterministic activities without enough evidence for a conclusive outcome."
            ),
        ),
        OutcomeClass::Cancelled => (
            "Cancelled or interrupted session",
            format!(
                "The authoritative graph contains {count} deterministic activities before cancellation or interruption."
            ),
        ),
    }
}

/// Whether enrichment projections are visible to an experience consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExperienceEnrichmentVisibility {
    /// Read the latest selected completed enrichment.
    Enabled,
    /// Return deterministic projections and an explicit disabled reason only.
    Disabled,
}

/// Namespace-scoped read policy for the experience surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceScope {
    namespace: GraphNamespace,
    enrichment_visibility: ExperienceEnrichmentVisibility,
}

impl ExperienceScope {
    /// Construct a namespace-scoped experience policy.
    #[must_use]
    pub const fn new(
        namespace: GraphNamespace,
        enrichment_visibility: ExperienceEnrichmentVisibility,
    ) -> Self {
        Self {
            namespace,
            enrichment_visibility,
        }
    }

    /// Graph namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }

    /// Enrichment read policy.
    #[must_use]
    pub const fn enrichment_visibility(&self) -> ExperienceEnrichmentVisibility {
        self.enrichment_visibility
    }
}

/// One session-specific experience lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceSessionQuery {
    scope: ExperienceScope,
    session_id: SessionId,
}

impl ExperienceSessionQuery {
    /// Construct one typed detail lookup.
    #[must_use]
    pub const fn new(scope: ExperienceScope, session_id: SessionId) -> Self {
        Self { scope, session_id }
    }

    /// Namespace and visibility policy.
    #[must_use]
    pub const fn scope(&self) -> &ExperienceScope {
        &self.scope
    }

    /// Stable session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }
}

/// One normalized deterministic activity for display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExperienceActivity {
    activity_id: ExperienceActivityId,
    sequence: RecordSequence,
    label: ExperienceActivityLabel,
    status: ActivityStatus,
}

impl ExperienceActivity {
    /// Construct a source-safe activity from closed deterministic values.
    ///
    /// # Errors
    ///
    /// Returns an error only if the closed label mapping violates its display bound.
    pub fn new(
        activity_id: ActivityId,
        sequence: RecordSequence,
        kind: ActivityKind,
        status: ActivityStatus,
    ) -> Result<Self, ExperienceGraphError> {
        let label = format!(
            "{} — {}",
            activity_kind_label(kind),
            activity_status_label(status)
        );
        Ok(Self {
            activity_id: ExperienceActivityId::new(activity_id),
            sequence,
            label: ExperienceActivityLabel::bounded(&label)?,
            status,
        })
    }

    /// Stable content-addressed activity identity.
    #[must_use]
    pub const fn activity_id(&self) -> ActivityId {
        self.activity_id.domain()
    }

    /// First supporting source sequence.
    #[must_use]
    pub const fn sequence(&self) -> RecordSequence {
        self.sequence
    }
}

fn activity_kind_label(kind: ActivityKind) -> &'static str {
    match kind {
        ActivityKind::Start => "Task start",
        ActivityKind::Request => "Request",
        ActivityKind::Inspect => "Inspection",
        ActivityKind::Search => "Search",
        ActivityKind::Modify => "Modification",
        ActivityKind::Repair => "Repair",
        ActivityKind::Verify => "Verification",
        ActivityKind::Install => "Installation",
        ActivityKind::Execute => "Execution",
        ActivityKind::Diagnose => "Diagnosis",
        ActivityKind::RequestPermission => "Permission request",
        ActivityKind::NetworkAccess => "Network access",
        ActivityKind::Destructive => "Destructive operation",
        ActivityKind::ManageContext => "Context management",
        ActivityKind::Rollback => "Rollback",
        ActivityKind::Complete => "Completion",
    }
}

fn activity_status_label(status: ActivityStatus) -> &'static str {
    match status {
        ActivityStatus::Pending => "pending",
        ActivityStatus::Succeeded => "succeeded",
        ActivityStatus::Failed => "failed",
        ActivityStatus::Interrupted => "interrupted",
        ActivityStatus::Indeterminate => "indeterminate",
    }
}

/// Bounded deterministic activity sequence.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExperienceActivities(Vec<ExperienceActivity>);

impl ExperienceActivities {
    /// Validate response bounds and activity identity uniqueness.
    ///
    /// # Errors
    ///
    /// Returns an error for too many activities or duplicate identities.
    pub fn new(
        values: impl IntoIterator<Item = ExperienceActivity>,
    ) -> Result<Self, ExperienceGraphError> {
        let mut values: Vec<_> = values.into_iter().collect();
        if values.len() > MAX_ACTIVITIES {
            return Err(ExperienceGraphError::CollectionLimit {
                field: "experience activities",
                maximum: MAX_ACTIVITIES,
            });
        }
        let unique: HashSet<_> = values.iter().map(ExperienceActivity::activity_id).collect();
        if unique.len() != values.len() {
            return Err(ExperienceGraphError::DuplicateIdentity {
                field: "experience activities",
            });
        }
        values
            .sort_by_key(|activity| (activity.sequence().value(), activity.activity_id().to_hex()));
        Ok(Self(values))
    }

    /// Iterate in deterministic source order.
    pub fn iter(&self) -> impl Iterator<Item = &ExperienceActivity> {
        self.0.iter()
    }

    /// Typed activity count.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(self.0.len() as u64)
    }
}

/// Closed source category for a content-free evidence anchor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceSourceKind {
    /// Human, agent, or inter-agent conversation.
    Conversation,
    /// Tool invocation request.
    ToolRequest,
    /// Tool invocation result.
    ToolResult,
    /// Command, patch, error, or other execution evidence.
    Execution,
    /// Deterministically classified verification evidence.
    Verification,
}

impl ExperienceSourceKind {
    fn label(self) -> &'static str {
        match self {
            Self::Conversation => "Conversation",
            Self::ToolRequest => "Tool request",
            Self::ToolResult => "Tool result",
            Self::Execution => "Execution",
            Self::Verification => "Verification",
        }
    }
}

/// Content-free source evidence address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExperienceSourceAnchor {
    anchor_id: TranscriptSpanId,
    label: ExperienceAnchorLabel,
    source_kind: ExperienceSourceKind,
    record_sequence: RecordSequence,
    content_digest: ExperienceContentDigest,
}

impl ExperienceSourceAnchor {
    /// Construct a content-free source anchor from graph-only metadata.
    ///
    /// # Errors
    ///
    /// Returns an error only if the closed label mapping violates its display bound.
    pub fn new(
        anchor_id: TranscriptSpanId,
        source_kind: ExperienceSourceKind,
        record_sequence: RecordSequence,
        content_digest: PayloadDigest,
    ) -> Result<Self, ExperienceGraphError> {
        let label = format!(
            "{} evidence at record {}",
            source_kind.label(),
            record_sequence.value()
        );
        Ok(Self {
            anchor_id,
            label: ExperienceAnchorLabel::bounded(&label)?,
            source_kind,
            record_sequence,
            content_digest: ExperienceContentDigest::new(content_digest),
        })
    }

    /// Opaque content-addressed anchor identity.
    #[must_use]
    pub const fn anchor_id(&self) -> TranscriptSpanId {
        self.anchor_id
    }

    /// One-based source record sequence.
    #[must_use]
    pub const fn record_sequence(&self) -> RecordSequence {
        self.record_sequence
    }
}

/// Bounded unique source-anchor collection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExperienceSourceAnchors(Vec<ExperienceSourceAnchor>);

impl ExperienceSourceAnchors {
    /// Validate response bounds and anchor uniqueness.
    ///
    /// # Errors
    ///
    /// Returns an error for too many anchors or duplicate identities.
    pub fn new(
        values: impl IntoIterator<Item = ExperienceSourceAnchor>,
    ) -> Result<Self, ExperienceGraphError> {
        let mut values: Vec<_> = values.into_iter().collect();
        if values.len() > MAX_SOURCE_ANCHORS {
            return Err(ExperienceGraphError::CollectionLimit {
                field: "experience source anchors",
                maximum: MAX_SOURCE_ANCHORS,
            });
        }
        let unique: HashSet<_> = values
            .iter()
            .map(ExperienceSourceAnchor::anchor_id)
            .collect();
        if unique.len() != values.len() {
            return Err(ExperienceGraphError::DuplicateIdentity {
                field: "experience source anchors",
            });
        }
        values.sort_by_key(|anchor| {
            (
                anchor.record_sequence().value(),
                anchor.anchor_id().to_hex(),
            )
        });
        Ok(Self(values))
    }

    /// Iterate in source order.
    pub fn iter(&self) -> impl Iterator<Item = &ExperienceSourceAnchor> {
        self.0.iter()
    }
}

/// Citation containing no source content or local locator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
struct ExperienceCitation {
    anchor_id: TranscriptSpanId,
}

impl ExperienceCitation {
    fn new(anchor_id: TranscriptSpanId) -> Self {
        Self { anchor_id }
    }

    /// Opaque cited anchor identity.
    #[must_use]
    pub const fn anchor_id(self) -> TranscriptSpanId {
        self.anchor_id
    }
}

/// Non-empty unique citation set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct ExperienceCitations(Vec<ExperienceCitation>);

impl ExperienceCitations {
    fn from_span_ids(
        values: impl IntoIterator<Item = TranscriptSpanId>,
        field: &'static str,
    ) -> Result<Self, ExperienceGraphError> {
        let values: Vec<_> = values.into_iter().map(ExperienceCitation::new).collect();
        if values.is_empty() {
            return Err(ExperienceGraphError::UnresolvedReference { field });
        }
        let unique: HashSet<_> = values.iter().copied().collect();
        if unique.len() != values.len() {
            return Err(ExperienceGraphError::DuplicateIdentity { field });
        }
        Ok(Self(values))
    }

    fn iter(&self) -> impl Iterator<Item = &ExperienceCitation> {
        self.0.iter()
    }
}

/// One cited narrative episode from a selected Mistral enrichment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ExperienceEpisode {
    episode_id: crate::NarrativeEpisodeId,
    ordinal: crate::EpisodeOrdinal,
    title: ExperienceTitle,
    summary: ExperienceSummary,
    activity_ids: Vec<ExperienceActivityId>,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    citations: ExperienceCitations,
}

impl ExperienceEpisode {
    fn from_projection(
        value: &NarrativeEpisodeProjection,
        activity_ids: Vec<ActivityId>,
    ) -> Result<Self, ExperienceGraphError> {
        Ok(Self {
            episode_id: value.id(),
            ordinal: value.ordinal(),
            title: ExperienceTitle::bounded(value.title().as_str())?,
            summary: ExperienceSummary::bounded(value.summary().as_str())?,
            activity_ids: activity_ids
                .into_iter()
                .map(ExperienceActivityId::new)
                .collect(),
            confidence: value.confidence(),
            epistemic_status: value.epistemic_status(),
            citations: ExperienceCitations::from_span_ids(
                value.spans().iter().copied(),
                "episode citations",
            )?,
        })
    }

    fn activity_ids(&self) -> impl Iterator<Item = ActivityId> + '_ {
        self.activity_ids
            .iter()
            .copied()
            .map(ExperienceActivityId::domain)
    }

    fn citations(&self) -> &ExperienceCitations {
        &self.citations
    }
}

/// Exact deterministic activities supported by one episode's cited observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExperienceEpisodeActivityBinding {
    episode_id: crate::NarrativeEpisodeId,
    activity_ids: Vec<ActivityId>,
}

impl ExperienceEpisodeActivityBinding {
    /// Construct a duplicate-free exact evidence binding.
    ///
    /// Empty activity sets are valid because a transcript-backed episode can cite
    /// conversation evidence that is not classified as a deterministic activity.
    ///
    /// # Errors
    ///
    /// Returns an error when the same activity identity occurs more than once.
    pub fn new(
        episode_id: crate::NarrativeEpisodeId,
        activity_ids: impl IntoIterator<Item = ActivityId>,
    ) -> Result<Self, ExperienceGraphError> {
        let activity_ids: Vec<_> = activity_ids.into_iter().collect();
        let unique: HashSet<_> = activity_ids.iter().copied().collect();
        if unique.len() != activity_ids.len() {
            return Err(ExperienceGraphError::DuplicateIdentity {
                field: "episode activity bindings",
            });
        }
        Ok(Self {
            episode_id,
            activity_ids,
        })
    }
}

/// Unique exact activity bindings for selected narrative episodes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExperienceEpisodeActivityBindings(Vec<ExperienceEpisodeActivityBinding>);

impl ExperienceEpisodeActivityBindings {
    /// Validate unique episode identities.
    ///
    /// # Errors
    ///
    /// Returns an error when an episode is bound more than once.
    pub fn new(
        values: impl IntoIterator<Item = ExperienceEpisodeActivityBinding>,
    ) -> Result<Self, ExperienceGraphError> {
        let values: Vec<_> = values.into_iter().collect();
        let unique: HashSet<_> = values.iter().map(|value| value.episode_id).collect();
        if unique.len() != values.len() {
            return Err(ExperienceGraphError::DuplicateIdentity {
                field: "episode activity bindings",
            });
        }
        Ok(Self(values))
    }

    fn activities_for(
        &self,
        episode_id: crate::NarrativeEpisodeId,
    ) -> Result<Vec<ActivityId>, ExperienceGraphError> {
        self.0
            .iter()
            .find(|value| value.episode_id == episode_id)
            .map(|value| value.activity_ids.clone())
            .ok_or(ExperienceGraphError::UnresolvedReference {
                field: "episode activity bindings",
            })
    }
}

/// One source-safe knowledge entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ExperienceEntity {
    entity_id: KnowledgeEntityId,
    kind: crate::KnowledgeEntityKind,
    name: ExperienceEntityName,
}

impl ExperienceEntity {
    fn from_projection(value: &KnowledgeEntityProjection) -> Result<Self, ExperienceGraphError> {
        Ok(Self {
            entity_id: value.id(),
            kind: value.kind(),
            name: ExperienceEntityName::bounded(value.name().as_str())?,
        })
    }

    fn entity_id(&self) -> KnowledgeEntityId {
        self.entity_id
    }
}

/// Faithful session-wide or entity-scoped claim subject set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
enum ExperienceClaimSubjects {
    /// The claim concerns the session as a whole.
    SessionWide,
    /// The claim concerns one or more listed enrichment entities.
    Entities {
        /// Referenced enrichment entity identities.
        entity_ids: Vec<KnowledgeEntityId>,
    },
}

impl ExperienceClaimSubjects {
    fn from_projection(value: &KnowledgeClaimSubjects) -> Self {
        match value {
            KnowledgeClaimSubjects::SessionWide => Self::SessionWide,
            KnowledgeClaimSubjects::Entities(values) => Self::Entities {
                entity_ids: values.clone(),
            },
        }
    }

    fn entity_ids(&self) -> impl Iterator<Item = KnowledgeEntityId> + '_ {
        let values: &[KnowledgeEntityId] = match self {
            Self::SessionWide => &[],
            Self::Entities { entity_ids } => entity_ids,
        };
        values.iter().copied()
    }
}

/// One evidence-cited non-authoritative knowledge claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ExperienceClaim {
    claim_id: crate::KnowledgeClaimId,
    kind: crate::KnowledgeKind,
    title: ExperienceClaimTitle,
    statement: ExperienceStatement,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    subjects: ExperienceClaimSubjects,
    citations: ExperienceCitations,
}

impl ExperienceClaim {
    fn from_projection(value: &KnowledgeClaimProjection) -> Result<Self, ExperienceGraphError> {
        Ok(Self {
            claim_id: value.id(),
            kind: value.kind(),
            title: ExperienceClaimTitle::bounded(value.title().as_str())?,
            statement: ExperienceStatement::bounded(value.statement().as_str())?,
            confidence: value.confidence(),
            epistemic_status: value.epistemic_status(),
            subjects: ExperienceClaimSubjects::from_projection(value.subjects()),
            citations: ExperienceCitations::from_span_ids(
                value.spans().iter().copied(),
                "claim citations",
            )?,
        })
    }
}

/// One evidence-cited relation between selected enrichment entities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ExperienceRelation {
    relation_id: crate::KnowledgeRelationId,
    predicate: KnowledgePredicate,
    subject_entity_id: KnowledgeEntityId,
    object_entity_id: KnowledgeEntityId,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    citations: ExperienceCitations,
}

impl ExperienceRelation {
    fn from_projection(value: &KnowledgeRelationProjection) -> Result<Self, ExperienceGraphError> {
        Ok(Self {
            relation_id: value.id(),
            predicate: value.predicate(),
            subject_entity_id: value.subject(),
            object_entity_id: value.object(),
            confidence: value.confidence(),
            epistemic_status: value.epistemic_status(),
            citations: ExperienceCitations::from_span_ids(
                value.spans().iter().copied(),
                "relation citations",
            )?,
        })
    }
}

/// Typed reason the experience view uses deterministic fallback display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperienceEnrichmentUnavailableReason {
    /// Enrichment visibility is disabled by local configuration.
    Disabled,
    /// The verified source has no semantic activities eligible for enrichment.
    NotEligible,
    /// No completed run has been selected for the latest verified source.
    NoCompletedRun,
    /// Runs exist, but only failed, incomplete, or structurally partial data is available.
    FailedOrPartial,
}

/// List-view enrichment lifecycle and provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ExperienceEnrichmentSummary {
    /// One completed selected Mistral run supplies display text.
    Completed {
        /// Content-addressed enrichment run identity.
        run_id: EnrichmentRunId,
        /// Episode confidence preserved without numeric invention.
        confidence: KnowledgeConfidence,
        /// Episode epistemic status preserved exactly.
        epistemic_status: EpistemicStatus,
    },
    /// Deterministic fallback is active.
    Unavailable {
        /// Closed fallback reason.
        reason: ExperienceEnrichmentUnavailableReason,
    },
}

/// One stable session list item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExperienceSessionSummary {
    session_id: SessionId,
    display: ExperienceDisplay,
    outcome: OutcomeClass,
    activity_count: RecordCount,
    enrichment: ExperienceEnrichmentSummary,
}

impl ExperienceSessionSummary {
    /// Construct a deterministic fallback list item.
    #[must_use]
    pub fn unavailable(
        session_id: SessionId,
        outcome: OutcomeClass,
        activity_count: RecordCount,
        reason: ExperienceEnrichmentUnavailableReason,
    ) -> Self {
        Self {
            session_id,
            display: ExperienceDisplay::deterministic(outcome, activity_count),
            outcome,
            activity_count,
            enrichment: ExperienceEnrichmentSummary::Unavailable { reason },
        }
    }

    /// Construct a selected completed-enrichment list item.
    ///
    /// # Errors
    ///
    /// Returns an error if provider display text violates the source-safe boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn completed(
        session_id: SessionId,
        outcome: OutcomeClass,
        activity_count: RecordCount,
        run_id: EnrichmentRunId,
        title: &str,
        summary: &str,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
    ) -> Result<Self, ExperienceGraphError> {
        Ok(Self {
            session_id,
            display: ExperienceDisplay::enriched(title, summary)?,
            outcome,
            activity_count,
            enrichment: ExperienceEnrichmentSummary::Completed {
                run_id,
                confidence,
                epistemic_status,
            },
        })
    }

    /// Stable session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }
}

/// Bounded, API-stably ordered list response payload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExperienceSessionSummaries(Vec<ExperienceSessionSummary>);

impl ExperienceSessionSummaries {
    /// Validate response bounds and unique session identities.
    ///
    /// # Errors
    ///
    /// Returns an error for too many sessions or duplicate identities.
    pub fn new(
        values: impl IntoIterator<Item = ExperienceSessionSummary>,
    ) -> Result<Self, ExperienceGraphError> {
        let mut values: Vec<_> = values.into_iter().collect();
        if values.len() > MAX_SESSIONS {
            return Err(ExperienceGraphError::CollectionLimit {
                field: "experience sessions",
                maximum: MAX_SESSIONS,
            });
        }
        let unique: HashSet<_> = values
            .iter()
            .map(ExperienceSessionSummary::session_id)
            .collect();
        if unique.len() != values.len() {
            return Err(ExperienceGraphError::DuplicateIdentity {
                field: "experience sessions",
            });
        }
        values.sort_by_key(ExperienceSessionSummary::session_id);
        Ok(Self(values))
    }

    /// Iterate in stable session-identity order.
    pub fn iter(&self) -> impl Iterator<Item = &ExperienceSessionSummary> {
        self.0.iter()
    }
}

/// Cohesive payload of one selected completed Mistral enrichment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompletedExperienceEnrichment {
    run_id: EnrichmentRunId,
    provider: EnrichmentProvider,
    model: EnrichmentModelName,
    prompt_version: PromptVersion,
    disclosure_scope: EnrichmentDisclosureScope,
    authorization_policy_digest: EnrichmentAuthorizationPolicyDigest,
    prompt_digest: EnrichmentPromptDigest,
    schema_version: EnrichmentSchemaVersion,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    episodes: Vec<ExperienceEpisode>,
    entities: Vec<ExperienceEntity>,
    claims: Vec<ExperienceClaim>,
    relations: Vec<ExperienceRelation>,
}

/// Detail-view completed-enrichment or unavailable coproduct.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ExperienceEnrichment {
    /// Selected completed Mistral enrichment.
    Completed(Box<CompletedExperienceEnrichment>),
    /// No selected completed enrichment can be displayed.
    Unavailable {
        /// Closed deterministic-fallback reason.
        reason: ExperienceEnrichmentUnavailableReason,
    },
}

impl ExperienceEnrichment {
    /// Convert an already validated selected overlay into its source-safe public shape.
    ///
    /// # Errors
    ///
    /// Returns an error for missing narrative, unsafe display text, duplicate semantic
    /// identities, or unresolved entity references.
    pub fn from_selected(
        value: &SelectedEnrichment,
        activity_bindings: &ExperienceEpisodeActivityBindings,
    ) -> Result<Self, ExperienceGraphError> {
        let episodes = value
            .episodes()
            .iter()
            .map(|episode| {
                ExperienceEpisode::from_projection(
                    episode,
                    activity_bindings.activities_for(episode.id())?,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = episodes
            .first()
            .ok_or(ExperienceGraphError::MissingNarrativeEpisode)?;
        let entities = value
            .entities()
            .iter()
            .map(ExperienceEntity::from_projection)
            .collect::<Result<Vec<_>, _>>()?;
        let claims = value
            .claims()
            .iter()
            .map(ExperienceClaim::from_projection)
            .collect::<Result<Vec<_>, _>>()?;
        let relations = value
            .relations()
            .iter()
            .map(ExperienceRelation::from_projection)
            .collect::<Result<Vec<_>, _>>()?;
        validate_semantic_references(&entities, &claims, &relations)?;
        Ok(Self::Completed(Box::new(CompletedExperienceEnrichment {
            run_id: value.run().run_id(),
            provider: value.run().provider(),
            model: value.run().model().clone(),
            prompt_version: value.run().prompt_version().clone(),
            disclosure_scope: value.run().audit_provenance().disclosure_scope(),
            authorization_policy_digest: value
                .run()
                .audit_provenance()
                .authorization_policy_digest(),
            prompt_digest: value.run().audit_provenance().prompt_digest(),
            schema_version: value.run().schema_version().clone(),
            confidence: first.confidence,
            epistemic_status: first.epistemic_status,
            episodes,
            entities,
            claims,
            relations,
        })))
    }

    /// Construct an explicit deterministic-fallback state.
    #[must_use]
    pub const fn unavailable(reason: ExperienceEnrichmentUnavailableReason) -> Self {
        Self::Unavailable { reason }
    }

    fn display(
        &self,
        outcome: OutcomeClass,
        activity_count: RecordCount,
    ) -> Result<ExperienceDisplay, ExperienceGraphError> {
        match self {
            Self::Completed(value) => {
                let first = value
                    .episodes
                    .first()
                    .ok_or(ExperienceGraphError::MissingNarrativeEpisode)?;
                ExperienceDisplay::enriched(first.title.as_str(), first.summary.as_str())
            }
            Self::Unavailable { .. } => {
                Ok(ExperienceDisplay::deterministic(outcome, activity_count))
            }
        }
    }

    fn citations(&self) -> Vec<TranscriptSpanId> {
        match self {
            Self::Completed(value) => value
                .episodes
                .iter()
                .flat_map(|episode| episode.citations().iter())
                .chain(value.claims.iter().flat_map(|claim| claim.citations.iter()))
                .chain(
                    value
                        .relations
                        .iter()
                        .flat_map(|relation| relation.citations.iter()),
                )
                .map(|citation| citation.anchor_id())
                .collect(),
            Self::Unavailable { .. } => Vec::new(),
        }
    }

    fn episode_activity_ids(&self) -> Vec<ActivityId> {
        match self {
            Self::Completed(value) => value
                .episodes
                .iter()
                .flat_map(ExperienceEpisode::activity_ids)
                .collect(),
            Self::Unavailable { .. } => Vec::new(),
        }
    }
}

fn validate_semantic_references(
    entities: &[ExperienceEntity],
    claims: &[ExperienceClaim],
    relations: &[ExperienceRelation],
) -> Result<(), ExperienceGraphError> {
    let entity_ids: HashSet<_> = entities.iter().map(ExperienceEntity::entity_id).collect();
    if entity_ids.len() != entities.len() {
        return Err(ExperienceGraphError::DuplicateIdentity {
            field: "experience entities",
        });
    }
    if claims
        .iter()
        .flat_map(|claim| claim.subjects.entity_ids())
        .any(|entity| !entity_ids.contains(&entity))
    {
        return Err(ExperienceGraphError::UnresolvedReference {
            field: "experience claim subjects",
        });
    }
    if relations.iter().any(|relation| {
        relation.subject_entity_id == relation.object_entity_id
            || !entity_ids.contains(&relation.subject_entity_id)
            || !entity_ids.contains(&relation.object_entity_id)
    }) {
        return Err(ExperienceGraphError::UnresolvedReference {
            field: "experience relation endpoints",
        });
    }
    Ok(())
}

/// One source-safe session detail response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExperienceSessionDetail {
    session_id: SessionId,
    display: ExperienceDisplay,
    outcome: OutcomeClass,
    activities: ExperienceActivities,
    enrichment: ExperienceEnrichment,
    source_anchors: ExperienceSourceAnchors,
}

impl ExperienceSessionDetail {
    /// Construct and cross-validate one complete experience view.
    ///
    /// # Errors
    ///
    /// Returns an error when a citation or episode activity reference cannot be
    /// resolved inside this exact response.
    pub fn new(
        session_id: SessionId,
        outcome: OutcomeClass,
        activities: ExperienceActivities,
        enrichment: ExperienceEnrichment,
        source_anchors: ExperienceSourceAnchors,
    ) -> Result<Self, ExperienceGraphError> {
        let activity_ids: HashSet<_> = activities
            .iter()
            .map(ExperienceActivity::activity_id)
            .collect();
        if enrichment
            .episode_activity_ids()
            .iter()
            .any(|activity| !activity_ids.contains(activity))
        {
            return Err(ExperienceGraphError::UnresolvedReference {
                field: "experience episode activities",
            });
        }
        let anchor_ids: HashSet<_> = source_anchors
            .iter()
            .map(ExperienceSourceAnchor::anchor_id)
            .collect();
        if enrichment
            .citations()
            .iter()
            .any(|citation| !anchor_ids.contains(citation))
        {
            return Err(ExperienceGraphError::UnresolvedReference {
                field: "experience citations",
            });
        }
        let display = enrichment.display(outcome, activities.count())?;
        Ok(Self {
            session_id,
            display,
            outcome,
            activities,
            enrichment,
            source_anchors,
        })
    }

    /// Stable session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }
}

/// Session detail lookup without nullable response state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExperienceSessionLookup {
    /// A verified deterministic session exists.
    Found(Box<ExperienceSessionDetail>),
    /// No session exists in the requested namespace.
    NotFound,
}

/// Provider-independent source-safe experience read capability.
#[async_trait]
pub trait ExperienceReader: Send + Sync {
    /// Concrete infrastructure error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Read all verified sessions in API-stable identity order.
    async fn experience_sessions(
        &self,
        scope: &ExperienceScope,
    ) -> Result<ExperienceSessionSummaries, Self::Error>;

    /// Read one verified session with its latest selected completed enrichment.
    async fn experience_session(
        &self,
        query: &ExperienceSessionQuery,
    ) -> Result<ExperienceSessionLookup, Self::Error>;
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{ActivityKind, ActivityStatus, OutcomeClass, RecordSequence};

    use super::*;

    #[test]
    fn deterministic_fallback_serializes_without_internal_graph_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let session_id = SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?;
        let activity = ExperienceActivity::new(
            ActivityId::parse_hex(&"a".repeat(64))?,
            RecordSequence::from_zero_based(0),
            ActivityKind::Inspect,
            ActivityStatus::Succeeded,
        )?;
        let detail = ExperienceSessionDetail::new(
            session_id,
            OutcomeClass::VerifiedSuccess,
            ExperienceActivities::new([activity])?,
            ExperienceEnrichment::unavailable(
                ExperienceEnrichmentUnavailableReason::NoCompletedRun,
            ),
            ExperienceSourceAnchors::default(),
        )?;
        let value = serde_json::to_value(detail)?;
        assert_eq!(value["display"]["source"], "deterministic_fallback");
        assert_eq!(value["activities"][0]["label"], "Inspection — succeeded");
        assert_eq!(value["enrichment"]["reason"], "no_completed_run");
        let serialized = serde_json::to_string(&value)?;
        for forbidden in [
            "\"key\"",
            "field_path",
            "raw_transcript",
            "local_path",
            "provider_body",
        ] {
            assert!(!serialized.contains(forbidden));
        }
        Ok(())
    }

    #[test]
    fn display_boundary_rejects_secret_and_local_path_shapes() {
        for forbidden in [
            "password: source-safe-canary",
            "Bearer source-safe-canary-token",
            "Read /Users/example/private.txt",
            "-----BEGIN PRIVATE KEY-----",
        ] {
            assert!(ExperienceTitle::bounded(forbidden).is_err());
        }
    }

    #[test]
    fn display_boundary_rejects_oversized_semantic_output_without_truncation()
    -> Result<(), Box<dyn std::error::Error>> {
        let oversized = "a".repeat(161);
        let Some(error) = ExperienceTitle::bounded(&oversized).err() else {
            return Err("oversized semantic title was silently accepted".into());
        };
        assert!(matches!(
            error,
            ExperienceGraphError::InvalidDisplayText {
                field: "experience title",
                maximum: 160,
            }
        ));
        assert!(ExperienceTitle::bounded(&"é".repeat(160)).is_ok());
        assert!(ExperienceTitle::bounded(&"é".repeat(161)).is_err());
        Ok(())
    }
}
