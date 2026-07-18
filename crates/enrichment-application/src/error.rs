//! Source-safe application failure vocabulary.

use harness_graph_domain::RecordCount;
pub use harness_graph_graph_port::{EnrichmentFailureClass, EnrichmentFailureStatus};

/// Semantic run field rejected before a provider or graph mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunConfigurationField {
    /// Authorized session identity.
    Session,
    /// Verified source snapshot.
    SourceDigest,
    /// Exact externally authorized transcript classes.
    DisclosureScope,
    /// Operator-reviewed authorization policy.
    AuthorizationPolicyDigest,
    /// Exact immutable foundation-model prompt body.
    PromptDigest,
    /// Mandatory local redaction policy.
    RedactionPolicyVersion,
    /// Deterministic chunking policy.
    ChunkingPolicyVersion,
    /// Number of bounded chunks.
    ExpectedChunks,
    /// Cost-bearing parallelism bound.
    ExtractionConcurrency,
    /// Complete semantic input identity.
    Fingerprint,
    /// Content-addressed execution identity.
    RunIdentity,
}

/// Closed graph operation that failed without retaining adapter text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOperation {
    /// Create overlay constraints and indexes.
    EnsureSchema,
    /// Read the currently selected completed overlay.
    ReadSelection,
    /// Read paid-call safety state for the exact fingerprint.
    ReadRunLifecycle,
    /// Create or resume immutable run provenance.
    BeginRun,
    /// Read a cost-bearing chunk checkpoint.
    ReadCheckpoint,
    /// Atomically acquire a paid-call chunk lease.
    ClaimChunkLease,
    /// Commit one validated chunk atomically.
    ProjectChunk,
    /// Release a paid-call chunk lease after unsuccessful work.
    ReleaseChunkLease,
    /// Select a fully checkpointed completed run.
    CompleteRun,
    /// Persist a source-safe failed-run transition.
    MarkRunFailed,
}

/// Closed local conversion stage that rejected provider output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionStage {
    /// Provider returned knowledge for another chunk.
    ChunkIdentity,
    /// Text-free transcript span projection.
    TranscriptSpan,
    /// Evidence-cited narrative projection.
    NarrativeEpisode,
    /// Enrichment-only entity projection.
    KnowledgeEntity,
    /// Evidence-cited claim projection.
    KnowledgeClaim,
    /// Evidence-cited relation projection.
    KnowledgeRelation,
    /// Atomic chunk payload validation.
    ChunkProjection,
}

/// Presence of a closed conversion location in a settled failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionFailureLocation {
    /// No missing chunk failed local conversion.
    NotApplicable,
    /// First canonical missing chunk rejected at this conversion stage.
    Stage(ConversionStage),
}

/// All-results-settle counts retained without provider or transcript content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailedEnrichmentSettlement {
    class: EnrichmentFailureClass,
    extraction_failures: RecordCount,
    conversion_failures: RecordCount,
    projection_failures: RecordCount,
    lease_busy_chunks: RecordCount,
    lease_boundary_failures: RecordCount,
    missing_checkpoints: RecordCount,
    conversion_location: ConversionFailureLocation,
}

/// Typed product of all-results-settle failure counts.
#[derive(Clone, Copy)]
pub(crate) struct SettlementFailureCounts {
    pub(crate) extraction: RecordCount,
    pub(crate) conversion: RecordCount,
    pub(crate) projection: RecordCount,
    pub(crate) lease_busy: RecordCount,
    pub(crate) lease_boundary: RecordCount,
}

impl FailedEnrichmentSettlement {
    pub(crate) const fn new(
        class: EnrichmentFailureClass,
        counts: SettlementFailureCounts,
        missing_checkpoints: RecordCount,
        conversion_location: ConversionFailureLocation,
    ) -> Self {
        Self {
            class,
            extraction_failures: counts.extraction,
            conversion_failures: counts.conversion,
            projection_failures: counts.projection,
            lease_busy_chunks: counts.lease_busy,
            lease_boundary_failures: counts.lease_boundary,
            missing_checkpoints,
            conversion_location,
        }
    }

    /// Deterministic primary failure class persisted on the run.
    #[must_use]
    pub const fn class(self) -> EnrichmentFailureClass {
        self.class
    }

    /// Provider calls that settled as errors.
    #[must_use]
    pub const fn extraction_failures(self) -> RecordCount {
        self.extraction_failures
    }

    /// Successful provider outputs rejected by local conversion.
    #[must_use]
    pub const fn conversion_failures(self) -> RecordCount {
        self.conversion_failures
    }

    /// Atomic graph chunk projections that did not confirm success.
    #[must_use]
    pub const fn projection_failures(self) -> RecordCount {
        self.projection_failures
    }

    /// Missing chunks skipped because another live invocation owns the lease.
    #[must_use]
    pub const fn lease_busy_chunks(self) -> RecordCount {
        self.lease_busy_chunks
    }

    /// Lease claim or release boundary operations that did not confirm success.
    #[must_use]
    pub const fn lease_boundary_failures(self) -> RecordCount {
        self.lease_boundary_failures
    }

    /// Expected chunks without a confirmed committed checkpoint.
    #[must_use]
    pub const fn missing_checkpoints(self) -> RecordCount {
        self.missing_checkpoints
    }

    /// First local conversion stage among canonical missing chunks.
    #[must_use]
    pub const fn conversion_location(self) -> ConversionFailureLocation {
        self.conversion_location
    }
}

/// Map a concrete boundary failure into a closed source-safe class.
///
/// Infrastructure adapters implement this trait next to their own error type;
/// the application never returns or formats the concrete error.
pub trait ClassifiedEnrichmentFailure {
    /// Closed source-safe classification suitable for persistence.
    fn enrichment_failure_class(&self) -> EnrichmentFailureClass;
}

/// Failure in additive transcript-enrichment composition.
#[derive(Debug, thiserror::Error)]
pub enum EnrichmentApplicationError {
    /// Immutable configuration was invalid.
    #[error("invalid enrichment run configuration field: {field:?}")]
    InvalidRunConfiguration {
        /// Closed semantic field.
        field: RunConfigurationField,
    },
    /// Prepared transcript provenance differed from the immutable run.
    #[error("prepared transcript does not match run field: {field:?}")]
    PreparationMismatch {
        /// Closed mismatched field.
        field: RunConfigurationField,
    },
    /// A graph boundary failed before settlement was available.
    #[error("enrichment graph operation {operation:?} failed as {class:?}")]
    GraphBoundary {
        /// Closed operation.
        operation: GraphOperation,
        /// Source-safe failure classification.
        class: EnrichmentFailureClass,
    },
    /// Completion failed and the selected view could not reconcile its outcome.
    #[error(
        "enrichment completion reconciliation unavailable: completion={completion:?}, selection={selection:?}"
    )]
    CompletionReconciliationUnavailable {
        /// Source-safe completion failure.
        completion: EnrichmentFailureClass,
        /// Source-safe selection-read failure.
        selection: EnrichmentFailureClass,
    },
    /// An unchanged terminal fingerprint cannot repeat cost-bearing work.
    #[error("terminal enrichment run cannot resume: {run_id:?}")]
    TerminalRunCannotResume {
        /// Content-addressed terminal run identity.
        run_id: harness_graph_graph_port::EnrichmentRunId,
    },
    /// Provider output could not be faithfully mapped to the graph contract.
    #[error("enrichment conversion failed at {stage:?}")]
    Conversion {
        /// Closed conversion stage.
        stage: ConversionStage,
    },
    /// All independent work settled, but the run remained incomplete.
    #[error("enrichment settlement failed as {settlement:?}")]
    SettlementFailed {
        /// Source-safe aggregate failure receipt.
        settlement: FailedEnrichmentSettlement,
    },
    /// Persisting the original failure transition also failed.
    #[error(
        "enrichment failure transition unavailable: original={original:?}, transition={transition:?}"
    )]
    FailureTransitionUnavailable {
        /// Original source-safe failure.
        original: EnrichmentFailureClass,
        /// Failure encountered while marking the run.
        transition: EnrichmentFailureClass,
    },
}
