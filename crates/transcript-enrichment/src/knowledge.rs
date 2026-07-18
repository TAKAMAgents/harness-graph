//! Provider-agnostic, citation-validated additive knowledge objects.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use harness_graph_domain::TokenCount;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BoundedTranscriptChunk, BoundedTranscriptChunks, TranscriptChunkId, TranscriptEnrichmentError,
    TranscriptSpanRef, TranscriptSpanToken,
};

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
            /// Returns an error when empty or longer than the Unicode bound.
            pub fn new(value: impl Into<String>) -> Result<Self, TranscriptEnrichmentError> {
                let value = value.into();
                let value = value.trim();
                if value.is_empty() || value.chars().count() > $maximum {
                    Err(TranscriptEnrichmentError::InvalidKnowledgeText {
                        field: $field,
                        maximum: $maximum,
                    })
                } else {
                    Ok(Self(value.to_owned()))
                }
            }

            /// Borrow validated text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

bounded_text!(KnowledgeTitle, "knowledge title", 160);
bounded_text!(KnowledgeStatement, "knowledge statement", 2_000);
bounded_text!(KnowledgeEntityLabel, "knowledge entity label", 240);
bounded_text!(NarrativeEpisodeTitle, "narrative episode title", 160);
bounded_text!(NarrativeEpisodeSummary, "narrative episode summary", 2_000);
bounded_text!(SessionKnowledgeTitle, "session knowledge title", 160);
bounded_text!(SessionKnowledgeSummary, "session knowledge summary", 4_000);

/// Closed semantic kind for an additive knowledge claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeKind {
    /// Intended outcome.
    Goal,
    /// Chosen course of action.
    Decision,
    /// Requirement or limitation.
    Constraint,
    /// Produced or inspected artifact.
    Artifact,
    /// Required software, service, or data dependency.
    Dependency,
    /// Observed failure.
    Failure,
    /// Model-inferred possible root cause.
    RootCauseHypothesis,
    /// Applied or proposed repair.
    Repair,
    /// Verification action or evidence statement.
    Verification,
    /// Potential harm or uncertainty.
    Risk,
    /// Reusable lesson.
    Lesson,
    /// Unresolved question.
    OpenQuestion,
}

impl KnowledgeKind {
    const fn as_str(self) -> &'static str {
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
}

/// Coarse confidence attached to a model-produced assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeConfidence {
    /// Weak or incomplete evidence.
    Low,
    /// Plausible evidence with remaining uncertainty.
    Medium,
    /// Strong transcript support without becoming an authoritative fact.
    High,
}

impl KnowledgeConfidence {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// Epistemic status that prevents model inference from masquerading as fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpistemicStatus {
    /// Directly stated in cited transcript text.
    Explicit,
    /// Reasonable semantic inference from cited text.
    Inferred,
    /// Causal or root-cause hypothesis requiring deterministic corroboration.
    Hypothesis,
}

impl EpistemicStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Inferred => "inferred",
            Self::Hypothesis => "hypothesis",
        }
    }
}

/// Closed entity class produced by transcript enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    const fn as_str(self) -> &'static str {
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
}

/// Closed predicate for a reified semantic relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgePredicate {
    /// Subject uses object.
    Uses,
    /// Subject modifies object.
    Modifies,
    /// Subject depends on object.
    DependsOn,
    /// Subject may cause object; always a hypothesis at this layer.
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
    const fn as_str(self) -> &'static str {
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
}

macro_rules! semantic_id {
    ($name:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            fn from_hasher(hasher: Sha256) -> Self {
                Self(hasher.finalize().into())
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
    };
}

semantic_id!(
    KnowledgeEntityId,
    "Deterministic identity of a semantic entity."
);
semantic_id!(
    KnowledgeClaimId,
    "Deterministic identity of a knowledge claim."
);
semantic_id!(
    KnowledgeRelationId,
    "Deterministic identity of a reified knowledge relation."
);
semantic_id!(
    NarrativeEpisodeId,
    "Deterministic identity of one evidence-cited narrative episode."
);
semantic_id!(
    SessionSynopsisId,
    "Deterministic identity of one evidence-cited session synopsis."
);

/// Lookup that resolves only citation tokens supplied in bounded input.
#[derive(Debug, Clone)]
pub struct CitationIndex(BTreeMap<TranscriptSpanToken, TranscriptSpanRef>);

impl CitationIndex {
    /// Build an exact token-to-source lookup from prepared chunks.
    ///
    /// # Errors
    ///
    /// Returns an error if a token collision maps to conflicting spans.
    pub fn from_chunks(
        chunks: &BoundedTranscriptChunks,
    ) -> Result<Self, TranscriptEnrichmentError> {
        let mut values = BTreeMap::new();
        for chunk in chunks.iter() {
            for segment in chunk.segments() {
                let token = segment.citation_token();
                let span = segment.span().clone();
                if let Some(previous) = values.insert(token, span.clone())
                    && previous != span
                {
                    return Err(TranscriptEnrichmentError::ConflictingKnowledgeIdentity);
                }
            }
        }
        Ok(Self(values))
    }

    /// Build a lookup for one map request.
    ///
    /// # Errors
    ///
    /// Returns an error if a token collision maps to conflicting spans.
    pub fn from_chunk(chunk: &BoundedTranscriptChunk) -> Result<Self, TranscriptEnrichmentError> {
        let mut values = BTreeMap::new();
        for segment in chunk.segments() {
            let token = segment.citation_token();
            let span = segment.span().clone();
            if let Some(previous) = values.insert(token, span.clone())
                && previous != span
            {
                return Err(TranscriptEnrichmentError::ConflictingKnowledgeIdentity);
            }
        }
        Ok(Self(values))
    }

    fn resolve(&self, token: TranscriptSpanToken) -> Option<TranscriptSpanRef> {
        self.0.get(&token).cloned()
    }
}

/// Resolved evidence citation retaining both opaque token and exact source span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceCitation {
    token: TranscriptSpanToken,
    span: TranscriptSpanRef,
}

impl EvidenceCitation {
    /// Opaque model-facing token.
    #[must_use]
    pub const fn token(&self) -> TranscriptSpanToken {
        self.token
    }

    /// Exact typed source span.
    #[must_use]
    pub const fn span(&self) -> &TranscriptSpanRef {
        &self.span
    }
}

/// Non-empty, unique, source-resolved citations for one assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceCitations(Vec<EvidenceCitation>);

impl EvidenceCitations {
    /// Resolve and validate model-returned tokens against bounded input.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, duplicate, or unknown tokens.
    pub fn resolve(
        tokens: impl IntoIterator<Item = TranscriptSpanToken>,
        index: &CitationIndex,
    ) -> Result<Self, TranscriptEnrichmentError> {
        let mut seen = BTreeSet::new();
        let mut citations = Vec::new();
        for token in tokens {
            if !seen.insert(token) {
                return Err(TranscriptEnrichmentError::DuplicateTranscriptCitation);
            }
            let span = index
                .resolve(token)
                .ok_or(TranscriptEnrichmentError::UnknownTranscriptCitation)?;
            citations.push(EvidenceCitation { token, span });
        }
        if citations.is_empty() {
            return Err(TranscriptEnrichmentError::EmptyCitations {
                field: "knowledge assertion",
            });
        }
        citations.sort_by_key(EvidenceCitation::token);
        Ok(Self(citations))
    }

    /// Iterate over resolved citations.
    pub fn iter(&self) -> impl Iterator<Item = &EvidenceCitation> {
        self.0.iter()
    }
}

/// One ordered, evidence-cited narrative episode extracted from a bounded chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NarrativeEpisode {
    id: NarrativeEpisodeId,
    title: NarrativeEpisodeTitle,
    summary: NarrativeEpisodeSummary,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    citations: EvidenceCitations,
}

impl NarrativeEpisode {
    /// Construct a content-addressed episode from validated cited text.
    #[must_use]
    pub fn new(
        title: NarrativeEpisodeTitle,
        summary: NarrativeEpisodeSummary,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        citations: EvidenceCitations,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"harness-graph-narrative-episode-v1\0");
        hasher.update(title.as_str().as_bytes());
        hasher.update(summary.as_str().as_bytes());
        hasher.update(confidence.as_str().as_bytes());
        hasher.update(epistemic_status.as_str().as_bytes());
        for citation in citations.iter() {
            hasher.update(citation.token().bytes());
        }
        Self {
            id: NarrativeEpisodeId::from_hasher(hasher),
            title,
            summary,
            confidence,
            epistemic_status,
            citations,
        }
    }

    /// Content-addressed episode identity.
    #[must_use]
    pub const fn id(&self) -> NarrativeEpisodeId {
        self.id
    }

    /// Human-meaningful bounded episode title.
    #[must_use]
    pub const fn title(&self) -> &NarrativeEpisodeTitle {
        &self.title
    }

    /// Human-meaningful bounded episode summary.
    #[must_use]
    pub const fn summary(&self) -> &NarrativeEpisodeSummary {
        &self.summary
    }

    /// Model confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }

    /// Explicit inference status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }

    /// Resolved source citations.
    #[must_use]
    pub const fn citations(&self) -> &EvidenceCitations {
        &self.citations
    }

    fn first_sequence(&self) -> u64 {
        self.citations
            .iter()
            .map(|citation| citation.span().source().sequence().value())
            .min()
            .unwrap_or(u64::MAX)
    }
}

/// Deterministic chronological collection of cited narrative episodes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NarrativeEpisodes(Vec<NarrativeEpisode>);

impl NarrativeEpisodes {
    /// Validate duplicate identities and order episodes by first cited record.
    ///
    /// # Errors
    ///
    /// Returns an error if a deterministic identity carries conflicting data.
    pub fn new(
        values: impl IntoIterator<Item = NarrativeEpisode>,
    ) -> Result<Self, TranscriptEnrichmentError> {
        let mut keyed = BTreeMap::new();
        for value in values {
            if let Some(previous) = keyed.insert(value.id(), value.clone())
                && previous != value
            {
                return Err(TranscriptEnrichmentError::ConflictingKnowledgeIdentity);
            }
        }
        let mut values: Vec<_> = keyed.into_values().collect();
        values.sort_by_key(|episode| (episode.first_sequence(), episode.id()));
        Ok(Self(values))
    }

    /// Iterate in deterministic source order.
    pub fn iter(&self) -> impl Iterator<Item = &NarrativeEpisode> {
        self.0.iter()
    }
}

/// Evidence-cited human title and summary for one complete session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSynopsis {
    id: SessionSynopsisId,
    title: SessionKnowledgeTitle,
    summary: SessionKnowledgeSummary,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    citations: EvidenceCitations,
}

impl SessionSynopsis {
    /// Construct a content-addressed synopsis from validated citations.
    #[must_use]
    pub fn new(
        title: SessionKnowledgeTitle,
        summary: SessionKnowledgeSummary,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        citations: EvidenceCitations,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"harness-graph-session-synopsis-v1\0");
        hasher.update(title.as_str().as_bytes());
        hasher.update(summary.as_str().as_bytes());
        hasher.update(confidence.as_str().as_bytes());
        hasher.update(epistemic_status.as_str().as_bytes());
        for citation in citations.iter() {
            hasher.update(citation.token().bytes());
        }
        Self {
            id: SessionSynopsisId::from_hasher(hasher),
            title,
            summary,
            confidence,
            epistemic_status,
            citations,
        }
    }

    /// Content-addressed synopsis identity.
    #[must_use]
    pub const fn id(&self) -> SessionSynopsisId {
        self.id
    }

    /// Human-meaningful bounded session title.
    #[must_use]
    pub const fn title(&self) -> &SessionKnowledgeTitle {
        &self.title
    }

    /// Human-meaningful bounded session summary.
    #[must_use]
    pub const fn summary(&self) -> &SessionKnowledgeSummary {
        &self.summary
    }

    /// Model confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }

    /// Explicit inference status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }

    /// Resolved source citations.
    #[must_use]
    pub const fn citations(&self) -> &EvidenceCitations {
        &self.citations
    }
}

/// Typed absence or cited presence of a session-level synopsis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionNarrative {
    /// No validated reduction was supplied; consumers use deterministic labels.
    Unavailable,
    /// A bounded, evidence-cited semantic title and summary is available.
    Cited(SessionSynopsis),
}

/// Entity references attached to a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimSubjects {
    /// Claim is session-wide rather than about one extracted entity.
    SessionWide,
    /// Claim names one or more deterministic entity identities.
    Entities(Vec<KnowledgeEntityId>),
}

impl ClaimSubjects {
    /// Validate a non-empty, unique entity-reference set.
    ///
    /// # Errors
    ///
    /// Returns an error when no identity is supplied.
    pub fn entities(
        values: impl IntoIterator<Item = KnowledgeEntityId>,
    ) -> Result<Self, TranscriptEnrichmentError> {
        let values: BTreeSet<_> = values.into_iter().collect();
        if values.is_empty() {
            return Err(TranscriptEnrichmentError::EmptyValue {
                field: "claim entity references",
            });
        }
        Ok(Self::Entities(values.into_iter().collect()))
    }

    fn iter(&self) -> impl Iterator<Item = &KnowledgeEntityId> {
        let values: &[KnowledgeEntityId] = match self {
            Self::SessionWide => &[],
            Self::Entities(values) => values,
        };
        values.iter()
    }
}

/// One deterministic semantic entity extracted from sanitized text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeEntity {
    id: KnowledgeEntityId,
    kind: KnowledgeEntityKind,
    label: KnowledgeEntityLabel,
}

impl KnowledgeEntity {
    /// Construct content-addressed entity identity.
    #[must_use]
    pub fn new(kind: KnowledgeEntityKind, label: KnowledgeEntityLabel) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"harness-graph-knowledge-entity-v1\0");
        hasher.update(kind.as_str().as_bytes());
        hasher.update(label.as_str().to_lowercase().as_bytes());
        Self {
            id: KnowledgeEntityId::from_hasher(hasher),
            kind,
            label,
        }
    }

    /// Deterministic entity identity.
    #[must_use]
    pub const fn id(&self) -> KnowledgeEntityId {
        self.id
    }

    /// Closed entity class.
    #[must_use]
    pub const fn kind(&self) -> KnowledgeEntityKind {
        self.kind
    }

    /// Validated display label.
    #[must_use]
    pub const fn label(&self) -> &KnowledgeEntityLabel {
        &self.label
    }
}

/// One additive, evidence-cited knowledge claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeClaim {
    id: KnowledgeClaimId,
    kind: KnowledgeKind,
    title: KnowledgeTitle,
    statement: KnowledgeStatement,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    subjects: ClaimSubjects,
    citations: EvidenceCitations,
}

impl KnowledgeClaim {
    /// Validate epistemic status and construct deterministic claim identity.
    ///
    /// # Errors
    ///
    /// Root-cause assertions must remain hypotheses.
    pub fn new(
        kind: KnowledgeKind,
        title: KnowledgeTitle,
        statement: KnowledgeStatement,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        subjects: ClaimSubjects,
        citations: EvidenceCitations,
    ) -> Result<Self, TranscriptEnrichmentError> {
        if kind == KnowledgeKind::RootCauseHypothesis
            && epistemic_status != EpistemicStatus::Hypothesis
        {
            return Err(TranscriptEnrichmentError::UnsupportedCausalCertainty);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"harness-graph-knowledge-claim-v1\0");
        hasher.update(kind.as_str().as_bytes());
        hasher.update(title.as_str().as_bytes());
        hasher.update(statement.as_str().as_bytes());
        hasher.update(confidence.as_str().as_bytes());
        hasher.update(epistemic_status.as_str().as_bytes());
        for subject in subjects.iter() {
            hasher.update(subject.0);
        }
        for citation in citations.iter() {
            hasher.update(citation.token().bytes());
        }
        Ok(Self {
            id: KnowledgeClaimId::from_hasher(hasher),
            kind,
            title,
            statement,
            confidence,
            epistemic_status,
            subjects,
            citations,
        })
    }

    /// Deterministic claim identity.
    #[must_use]
    pub const fn id(&self) -> KnowledgeClaimId {
        self.id
    }

    /// Closed knowledge kind.
    #[must_use]
    pub const fn kind(&self) -> KnowledgeKind {
        self.kind
    }

    /// Display title.
    #[must_use]
    pub const fn title(&self) -> &KnowledgeTitle {
        &self.title
    }

    /// Claim statement.
    #[must_use]
    pub const fn statement(&self) -> &KnowledgeStatement {
        &self.statement
    }

    /// Model confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }

    /// Explicit inference status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }

    /// Claim entity subjects.
    #[must_use]
    pub const fn subjects(&self) -> &ClaimSubjects {
        &self.subjects
    }

    /// Resolved source citations.
    #[must_use]
    pub const fn citations(&self) -> &EvidenceCitations {
        &self.citations
    }
}

/// One additive, reified, evidence-cited semantic relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRelation {
    id: KnowledgeRelationId,
    predicate: KnowledgePredicate,
    subject: KnowledgeEntityId,
    object: KnowledgeEntityId,
    confidence: KnowledgeConfidence,
    epistemic_status: EpistemicStatus,
    citations: EvidenceCitations,
}

impl KnowledgeRelation {
    /// Validate causal epistemic status and construct deterministic identity.
    ///
    /// # Errors
    ///
    /// `Causes` relations must remain hypotheses.
    pub fn new(
        predicate: KnowledgePredicate,
        subject: KnowledgeEntityId,
        object: KnowledgeEntityId,
        confidence: KnowledgeConfidence,
        epistemic_status: EpistemicStatus,
        citations: EvidenceCitations,
    ) -> Result<Self, TranscriptEnrichmentError> {
        if predicate == KnowledgePredicate::Causes
            && epistemic_status != EpistemicStatus::Hypothesis
        {
            return Err(TranscriptEnrichmentError::UnsupportedCausalCertainty);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"harness-graph-knowledge-relation-v1\0");
        hasher.update(predicate.as_str().as_bytes());
        hasher.update(subject.0);
        hasher.update(object.0);
        hasher.update(confidence.as_str().as_bytes());
        hasher.update(epistemic_status.as_str().as_bytes());
        for citation in citations.iter() {
            hasher.update(citation.token().bytes());
        }
        Ok(Self {
            id: KnowledgeRelationId::from_hasher(hasher),
            predicate,
            subject,
            object,
            confidence,
            epistemic_status,
            citations,
        })
    }

    /// Deterministic relation identity.
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

    /// Model confidence.
    #[must_use]
    pub const fn confidence(&self) -> KnowledgeConfidence {
        self.confidence
    }

    /// Explicit inference status.
    #[must_use]
    pub const fn epistemic_status(&self) -> EpistemicStatus {
        self.epistemic_status
    }

    /// Resolved source citations.
    #[must_use]
    pub const fn citations(&self) -> &EvidenceCitations {
        &self.citations
    }
}

macro_rules! typed_collection {
    ($name:ident, $key:ty, $item:ty, $id:ident) => {
        #[doc = concat!("Deterministically keyed collection of `", stringify!($item), "` values.")]
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(BTreeMap<$key, $item>);

        impl $name {
            /// Validate that duplicate identities carry identical content.
            ///
            /// # Errors
            ///
            /// Returns an error for conflicting content under one identity.
            pub fn new(
                values: impl IntoIterator<Item = $item>,
            ) -> Result<Self, TranscriptEnrichmentError> {
                let mut keyed = BTreeMap::new();
                for value in values {
                    let id = value.$id();
                    if let Some(previous) = keyed.insert(id, value.clone()) {
                        if previous != value {
                            return Err(TranscriptEnrichmentError::ConflictingKnowledgeIdentity);
                        }
                    }
                }
                Ok(Self(keyed))
            }

            /// Iterate in deterministic identity order.
            pub fn iter(&self) -> impl Iterator<Item = &$item> {
                self.0.values()
            }

            /// Whether this deterministic identity is present.
            #[must_use]
            pub fn contains(&self, id: &$key) -> bool {
                self.0.contains_key(id)
            }
        }
    };
}

typed_collection!(KnowledgeEntities, KnowledgeEntityId, KnowledgeEntity, id);
typed_collection!(KnowledgeClaims, KnowledgeClaimId, KnowledgeClaim, id);
typed_collection!(
    KnowledgeRelations,
    KnowledgeRelationId,
    KnowledgeRelation,
    id
);

/// Citation- and endpoint-validated semantic result for one bounded chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedChunkKnowledge {
    chunk_id: TranscriptChunkId,
    entities: KnowledgeEntities,
    claims: KnowledgeClaims,
    relations: KnowledgeRelations,
    episodes: NarrativeEpisodes,
}

impl ValidatedChunkKnowledge {
    /// Validate all claim subjects and relation endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error when any assertion references an absent entity.
    pub fn new(
        chunk_id: TranscriptChunkId,
        entities: KnowledgeEntities,
        claims: KnowledgeClaims,
        relations: KnowledgeRelations,
    ) -> Result<Self, TranscriptEnrichmentError> {
        Self::with_episodes(
            chunk_id,
            entities,
            claims,
            relations,
            NarrativeEpisodes::default(),
        )
    }

    /// Validate semantic collections plus cited narrative episodes.
    ///
    /// # Errors
    ///
    /// Returns an error when any assertion references an absent entity.
    pub fn with_episodes(
        chunk_id: TranscriptChunkId,
        entities: KnowledgeEntities,
        claims: KnowledgeClaims,
        relations: KnowledgeRelations,
        episodes: NarrativeEpisodes,
    ) -> Result<Self, TranscriptEnrichmentError> {
        for claim in claims.iter() {
            if claim
                .subjects()
                .iter()
                .any(|entity| !entities.contains(entity))
            {
                return Err(TranscriptEnrichmentError::UnknownKnowledgeEntity);
            }
        }
        for relation in relations.iter() {
            if !entities.contains(&relation.subject()) || !entities.contains(&relation.object()) {
                return Err(TranscriptEnrichmentError::UnknownKnowledgeEntity);
            }
        }
        Ok(Self {
            chunk_id,
            entities,
            claims,
            relations,
            episodes,
        })
    }

    /// Input chunk identity.
    #[must_use]
    pub const fn chunk_id(&self) -> TranscriptChunkId {
        self.chunk_id
    }

    /// Validated entities.
    #[must_use]
    pub const fn entities(&self) -> &KnowledgeEntities {
        &self.entities
    }

    /// Validated claims.
    #[must_use]
    pub const fn claims(&self) -> &KnowledgeClaims {
        &self.claims
    }

    /// Validated relations.
    #[must_use]
    pub const fn relations(&self) -> &KnowledgeRelations {
        &self.relations
    }

    /// Validated evidence-cited narrative episodes.
    #[must_use]
    pub const fn episodes(&self) -> &NarrativeEpisodes {
        &self.episodes
    }
}

/// Provider-reported usage for one structured chunk extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnowledgeExtractionUsage {
    input: TokenCount,
    output: TokenCount,
    total: TokenCount,
}

impl KnowledgeExtractionUsage {
    /// Construct provider-attributed token usage.
    #[must_use]
    pub const fn new(
        input_tokens: TokenCount,
        output_tokens: TokenCount,
        total_tokens: TokenCount,
    ) -> Self {
        Self {
            input: input_tokens,
            output: output_tokens,
            total: total_tokens,
        }
    }

    /// Provider-reported input usage.
    #[must_use]
    pub const fn input_tokens(self) -> TokenCount {
        self.input
    }

    /// Provider-reported output usage.
    #[must_use]
    pub const fn output_tokens(self) -> TokenCount {
        self.output
    }

    /// Provider-reported total usage.
    #[must_use]
    pub const fn total_tokens(self) -> TokenCount {
        self.total
    }
}

/// One validated semantic result plus separately attributable provider usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkKnowledgeExtraction {
    knowledge: ValidatedChunkKnowledge,
    usage: KnowledgeExtractionUsage,
}

impl ChunkKnowledgeExtraction {
    /// Join semantic output with usage after verifying input identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the result names another chunk.
    pub fn new(
        requested_chunk: TranscriptChunkId,
        knowledge: ValidatedChunkKnowledge,
        usage: KnowledgeExtractionUsage,
    ) -> Result<Self, TranscriptEnrichmentError> {
        if requested_chunk != knowledge.chunk_id() {
            return Err(TranscriptEnrichmentError::KnowledgeChunkMismatch);
        }
        Ok(Self { knowledge, usage })
    }

    /// Validated semantic output.
    #[must_use]
    pub const fn knowledge(&self) -> &ValidatedChunkKnowledge {
        &self.knowledge
    }

    /// Provider-attributed usage.
    #[must_use]
    pub const fn usage(&self) -> KnowledgeExtractionUsage {
        self.usage
    }
}

/// Foundation-model boundary for one bounded sanitized chunk.
#[async_trait]
pub trait TranscriptKnowledgeExtractor: Send + Sync {
    /// Concrete provider error, retained outside the semantic core.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Extract and validate cited knowledge from one bounded chunk.
    async fn extract_chunk(
        &self,
        chunk: &BoundedTranscriptChunk,
    ) -> Result<ChunkKnowledgeExtraction, Self::Error>;
}

/// Provider-independent merged session semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionKnowledge {
    entities: KnowledgeEntities,
    claims: KnowledgeClaims,
    relations: KnowledgeRelations,
    episodes: NarrativeEpisodes,
    narrative: SessionNarrative,
}

impl SessionKnowledge {
    /// Deterministically merge validated chunk knowledge by semantic identity.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting content under one deterministic ID or
    /// relation endpoints absent from the merged entity set.
    pub fn new(
        chunks: impl IntoIterator<Item = ValidatedChunkKnowledge>,
    ) -> Result<Self, TranscriptEnrichmentError> {
        Self::assemble(chunks, SessionNarrative::Unavailable)
    }

    /// Merge validated chunks and attach one separately reduced cited synopsis.
    ///
    /// # Errors
    ///
    /// Returns an error for conflicting semantic identities or unknown relation
    /// and claim endpoints.
    pub fn with_synopsis(
        chunks: impl IntoIterator<Item = ValidatedChunkKnowledge>,
        synopsis: SessionSynopsis,
    ) -> Result<Self, TranscriptEnrichmentError> {
        Self::assemble(chunks, SessionNarrative::Cited(synopsis))
    }

    fn assemble(
        chunks: impl IntoIterator<Item = ValidatedChunkKnowledge>,
        narrative: SessionNarrative,
    ) -> Result<Self, TranscriptEnrichmentError> {
        let chunks: Vec<_> = chunks.into_iter().collect();
        let entities = KnowledgeEntities::new(
            chunks
                .iter()
                .flat_map(|chunk| chunk.entities.iter().cloned()),
        )?;
        let claims =
            KnowledgeClaims::new(chunks.iter().flat_map(|chunk| chunk.claims.iter().cloned()))?;
        let relations = KnowledgeRelations::new(
            chunks
                .iter()
                .flat_map(|chunk| chunk.relations.iter().cloned()),
        )?;
        let episodes = NarrativeEpisodes::new(
            chunks
                .iter()
                .flat_map(|chunk| chunk.episodes.iter().cloned()),
        )?;
        for claim in claims.iter() {
            if claim
                .subjects()
                .iter()
                .any(|entity| !entities.contains(entity))
            {
                return Err(TranscriptEnrichmentError::UnknownKnowledgeEntity);
            }
        }
        for relation in relations.iter() {
            if !entities.contains(&relation.subject()) || !entities.contains(&relation.object()) {
                return Err(TranscriptEnrichmentError::UnknownKnowledgeEntity);
            }
        }
        Ok(Self {
            entities,
            claims,
            relations,
            episodes,
            narrative,
        })
    }

    /// Merged entities.
    #[must_use]
    pub const fn entities(&self) -> &KnowledgeEntities {
        &self.entities
    }

    /// Merged claims.
    #[must_use]
    pub const fn claims(&self) -> &KnowledgeClaims {
        &self.claims
    }

    /// Merged relations.
    #[must_use]
    pub const fn relations(&self) -> &KnowledgeRelations {
        &self.relations
    }

    /// Merged narrative episodes in deterministic source order.
    #[must_use]
    pub const fn episodes(&self) -> &NarrativeEpisodes {
        &self.episodes
    }

    /// Cited session synopsis or typed deterministic fallback state.
    #[must_use]
    pub const fn narrative(&self) -> &SessionNarrative {
        &self.narrative
    }
}

#[cfg(test)]
mod tests {
    use super::{EpistemicStatus, KnowledgeKind, KnowledgePredicate};

    #[test]
    fn causal_assertions_are_always_hypotheses() {
        assert_eq!(
            KnowledgeKind::RootCauseHypothesis.as_str(),
            "root_cause_hypothesis"
        );
        assert_eq!(KnowledgePredicate::Causes.as_str(), "causes");
        assert_eq!(EpistemicStatus::Hypothesis.as_str(), "hypothesis");
    }
}
