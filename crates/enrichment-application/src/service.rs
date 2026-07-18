//! Resumable all-results-settle enrichment workflow.

use futures_util::{StreamExt, stream};
use harness_graph_domain::{RecordCount, TokenCount};
use harness_graph_graph_port::{
    BeginEnrichmentRunCommand, ClaimEnrichmentChunkCommand, ClaimedEnrichmentChunk,
    CommittedEnrichmentChunk, CompleteEnrichmentRunCommand, EnrichmentChunkCheckpoint,
    EnrichmentChunkCheckpointQuery, EnrichmentChunkClaim, EnrichmentChunkId,
    EnrichmentFailureClass, EnrichmentGraphCommand, EnrichmentInvocationOwner,
    EnrichmentLeaseDuration, EnrichmentLookup, EnrichmentProjectionDisposition,
    EnrichmentProjector, EnrichmentQuery, EnrichmentReader, EnrichmentRunId,
    EnrichmentRunLifecycle, EnrichmentRunLifecycleQuery, EnrichmentRunRef,
    MarkEnrichmentRunFailedCommand, ProjectClaimedEnrichmentChunkCommand,
    ReleaseEnrichmentChunkLeaseCommand,
};
use harness_graph_transcript_enrichment::{
    BoundedTranscriptChunk, ChunkKnowledgeExtraction, PreparedTranscript,
    TranscriptKnowledgeExtractor,
};

use crate::{
    ClassifiedEnrichmentFailure, ConversionFailureLocation, ConversionStage,
    EnrichmentApplicationError, EnrichmentRunConfiguration, ExtractionConcurrency,
    FailedEnrichmentSettlement, GraphOperation, PlannedEnrichmentRun, SettlementFailureCounts,
    conversion::convert_chunk, plan_enrichment_run,
};

/// Receipt for a newly completed, fully checkpointed enrichment run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletedEnrichmentRun {
    run_id: EnrichmentRunId,
    submitted_chunks: RecordCount,
    resumed_chunks: RecordCount,
    input_tokens: TokenCount,
    output_tokens: TokenCount,
    completion_disposition: EnrichmentProjectionDisposition,
}

impl CompletedEnrichmentRun {
    /// Content-addressed run identity.
    #[must_use]
    pub const fn run_id(self) -> EnrichmentRunId {
        self.run_id
    }

    /// Chunks sent to the provider during this invocation.
    #[must_use]
    pub const fn submitted_chunks(self) -> RecordCount {
        self.submitted_chunks
    }

    /// Existing committed chunks skipped during this invocation.
    #[must_use]
    pub const fn resumed_chunks(self) -> RecordCount {
        self.resumed_chunks
    }

    /// Provider-attributed input tokens across every committed run chunk.
    #[must_use]
    pub const fn input_tokens(self) -> TokenCount {
        self.input_tokens
    }

    /// Provider-attributed output tokens across every committed run chunk.
    #[must_use]
    pub const fn output_tokens(self) -> TokenCount {
        self.output_tokens
    }

    /// Whether the completion transition changed graph state.
    #[must_use]
    pub const fn completion_disposition(self) -> EnrichmentProjectionDisposition {
        self.completion_disposition
    }
}

/// Terminal source-safe outcome of one application invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnrichmentRunOutcome {
    /// The selected completed run already has the exact content fingerprint.
    ExactFingerprintUnchanged {
        /// Existing selected run identity.
        run_id: EnrichmentRunId,
    },
    /// Every expected chunk has a receipt and the run was selected atomically.
    Completed(CompletedEnrichmentRun),
}

/// Generic application service with no concrete model or database dependency.
pub struct TranscriptEnrichmentApplication<'a, Extractor, Reader, Projector> {
    extractor: &'a Extractor,
    reader: &'a Reader,
    projector: &'a Projector,
    concurrency: ExtractionConcurrency,
}

enum RunStart {
    Exact(EnrichmentRunOutcome),
    Active(Box<PlannedEnrichmentRun>),
}

struct MissingChunk<'prepared> {
    ordinal: usize,
    chunk: &'prepared BoundedTranscriptChunk,
    chunk_id: EnrichmentChunkId,
}

struct ResumePlan<'prepared> {
    missing: Vec<MissingChunk<'prepared>>,
    resumed_chunks: u64,
}

impl<'a, Extractor, Reader, Projector>
    TranscriptEnrichmentApplication<'a, Extractor, Reader, Projector>
where
    Extractor: TranscriptKnowledgeExtractor,
    Extractor::Error: ClassifiedEnrichmentFailure,
    Reader: EnrichmentReader,
    Reader::Error: ClassifiedEnrichmentFailure,
    Projector: EnrichmentProjector,
    Projector::Error: ClassifiedEnrichmentFailure,
{
    /// Compose provider and graph capabilities behind one bounded workflow.
    #[must_use]
    pub const fn new(
        extractor: &'a Extractor,
        reader: &'a Reader,
        projector: &'a Projector,
        concurrency: ExtractionConcurrency,
    ) -> Self {
        Self {
            extractor,
            reader,
            projector,
            concurrency,
        }
    }

    /// Enrich one locally prepared transcript without mutating base commands.
    ///
    /// Missing provider chunks run concurrently and all settle. Successful
    /// chunks project independently and atomically, so a later invocation can
    /// resume from committed receipts. Completion is attempted only after a
    /// second read proves every expected checkpoint exists.
    ///
    /// # Errors
    ///
    /// Returns only closed source-safe failures. Concrete provider, transcript,
    /// and database messages never cross this boundary.
    pub async fn enrich(
        &self,
        prepared: &PreparedTranscript,
        configuration: &EnrichmentRunConfiguration,
    ) -> Result<EnrichmentRunOutcome, EnrichmentApplicationError> {
        let planned = match self.start_run(prepared, configuration).await? {
            RunStart::Exact(outcome) => return Ok(outcome),
            RunStart::Active(planned) => *planned,
        };
        let resume = self.load_resume_plan(prepared, &planned).await?;
        let initially_resumed_chunks = resume.resumed_chunks;
        let owner = EnrichmentInvocationOwner::from_bytes(*uuid::Uuid::now_v7().as_bytes());
        let settlement = self
            .settle_missing(resume, planned.reference(), configuration, owner)
            .await;
        let checkpoints = self.confirm_checkpoints(prepared, &planned).await?;

        // Checkpoints form the authoritative receipt monoid. Another worker
        // may reconcile an ambiguous local error, but any missing receipt keeps
        // the run non-visible.
        if !checkpoints.missing.is_empty() {
            let failure = settlement.finish(&checkpoints.missing);
            let class = failure.class();
            return Err(self
                .terminated_error(
                    planned.reference(),
                    class,
                    EnrichmentApplicationError::SettlementFailed {
                        settlement: failure,
                    },
                )
                .await);
        }
        self.complete_run(
            &planned,
            configuration,
            settlement.submitted_chunks,
            initially_resumed_chunks.saturating_add(settlement.concurrently_committed_chunks),
            checkpoints,
        )
        .await
    }

    async fn start_run(
        &self,
        prepared: &PreparedTranscript,
        configuration: &EnrichmentRunConfiguration,
    ) -> Result<RunStart, EnrichmentApplicationError> {
        let planned = plan_enrichment_run(prepared, configuration)?;
        self.projector
            .ensure_enrichment_schema()
            .await
            .map_err(|error| EnrichmentApplicationError::GraphBoundary {
                operation: GraphOperation::EnsureSchema,
                class: error.enrichment_failure_class(),
            })?;
        if let Some(outcome) = self.exact_selected_outcome(configuration, &planned).await? {
            return Ok(RunStart::Exact(outcome));
        }
        let lifecycle = self
            .reader
            .enrichment_run_lifecycle(&EnrichmentRunLifecycleQuery::new(
                planned.reference().clone(),
            ))
            .await
            .map_err(|error| EnrichmentApplicationError::GraphBoundary {
                operation: GraphOperation::ReadRunLifecycle,
                class: error.enrichment_failure_class(),
            })?;
        match lifecycle {
            EnrichmentRunLifecycle::Completed => {
                return Ok(RunStart::Exact(
                    EnrichmentRunOutcome::ExactFingerprintUnchanged {
                        run_id: planned.specification().run_id(),
                    },
                ));
            }
            EnrichmentRunLifecycle::TerminalFailed => {
                return Err(EnrichmentApplicationError::TerminalRunCannotResume {
                    run_id: planned.specification().run_id(),
                });
            }
            EnrichmentRunLifecycle::Absent | EnrichmentRunLifecycle::Resumable => {}
        }
        self.projector
            .project_enrichment(EnrichmentGraphCommand::BeginRun(
                BeginEnrichmentRunCommand::new(planned.specification().clone()),
            ))
            .await
            .map_err(|error| EnrichmentApplicationError::GraphBoundary {
                operation: GraphOperation::BeginRun,
                class: error.enrichment_failure_class(),
            })?;
        Ok(RunStart::Active(Box::new(planned)))
    }

    async fn load_resume_plan<'prepared>(
        &self,
        prepared: &'prepared PreparedTranscript,
        planned: &PlannedEnrichmentRun,
    ) -> Result<ResumePlan<'prepared>, EnrichmentApplicationError> {
        let mut missing = Vec::new();
        let mut resumed_chunks = 0_u64;
        for (ordinal, chunk) in prepared.chunks().iter().enumerate() {
            let chunk_id = graph_chunk_id(chunk)?;
            let checkpoint = self
                .reader
                .enrichment_chunk_checkpoint(&EnrichmentChunkCheckpointQuery::new(
                    planned.reference().clone(),
                    chunk_id,
                ))
                .await
                .map_err(|error| error.enrichment_failure_class());
            let checkpoint = match checkpoint {
                Ok(checkpoint) => checkpoint,
                Err(class) => {
                    return Err(self
                        .terminated_error(
                            planned.reference(),
                            class,
                            EnrichmentApplicationError::GraphBoundary {
                                operation: GraphOperation::ReadCheckpoint,
                                class,
                            },
                        )
                        .await);
                }
            };
            match checkpoint {
                EnrichmentChunkCheckpoint::Required => missing.push(MissingChunk {
                    ordinal,
                    chunk,
                    chunk_id,
                }),
                EnrichmentChunkCheckpoint::Committed(receipt) => {
                    if receipt.chunk_id() != chunk_id {
                        let class = EnrichmentFailureClass::CitationValidation;
                        return Err(self
                            .terminated_error(
                                planned.reference(),
                                class,
                                EnrichmentApplicationError::GraphBoundary {
                                    operation: GraphOperation::ReadCheckpoint,
                                    class,
                                },
                            )
                            .await);
                    }
                    resumed_chunks = resumed_chunks.saturating_add(1);
                }
            }
        }
        Ok(ResumePlan {
            missing,
            resumed_chunks,
        })
    }

    async fn settle_missing(
        &self,
        resume: ResumePlan<'_>,
        run: &EnrichmentRunRef,
        configuration: &EnrichmentRunConfiguration,
        owner: EnrichmentInvocationOwner,
    ) -> SettlementAccumulator {
        let mut settlements = stream::iter(resume.missing)
            .map(|missing| async move {
                self.settle_one_chunk(missing, run, configuration, owner)
                    .await
            })
            .buffer_unordered(self.concurrency.value());
        let mut aggregate = SettlementAccumulator::default();
        while let Some(settlement) = settlements.next().await {
            aggregate.submitted_chunks = aggregate
                .submitted_chunks
                .saturating_add(settlement.submitted_chunks);
            aggregate.concurrently_committed_chunks = aggregate
                .concurrently_committed_chunks
                .saturating_add(settlement.concurrently_committed_chunks);
            for failure in settlement.failures {
                aggregate.record(failure);
            }
        }
        aggregate
    }

    async fn settle_one_chunk(
        &self,
        missing: MissingChunk<'_>,
        run: &EnrichmentRunRef,
        configuration: &EnrichmentRunConfiguration,
        owner: EnrichmentInvocationOwner,
    ) -> ChunkSettlement {
        let claim = self
            .projector
            .claim_enrichment_chunk(&ClaimEnrichmentChunkCommand::new(
                run.clone(),
                missing.chunk_id,
                owner,
                EnrichmentLeaseDuration::PAID_CALL,
            ))
            .await;
        let lease = match claim {
            Ok(EnrichmentChunkClaim::Committed(_)) => {
                return ChunkSettlement::concurrently_committed();
            }
            Ok(EnrichmentChunkClaim::Busy) => {
                return ChunkSettlement::failed(ChunkFailure::lease_busy(
                    missing.ordinal,
                    missing.chunk_id,
                ));
            }
            Ok(EnrichmentChunkClaim::Claimed(lease)) => lease,
            Err(error) => {
                return ChunkSettlement::failed(ChunkFailure::lease_claim(
                    missing.ordinal,
                    missing.chunk_id,
                    error.enrichment_failure_class(),
                ));
            }
        };
        match self.extractor.extract_chunk(missing.chunk).await {
            Ok(extraction) => {
                self.project_extraction(run, lease, missing, &extraction, configuration)
                    .await
            }
            Err(error) => {
                let failure = ChunkFailure::extraction(
                    missing.ordinal,
                    missing.chunk_id,
                    error.enrichment_failure_class(),
                );
                self.failed_with_release(run, lease, failure).await
            }
        }
    }

    async fn confirm_checkpoints(
        &self,
        prepared: &PreparedTranscript,
        planned: &PlannedEnrichmentRun,
    ) -> Result<CheckpointAggregate, EnrichmentApplicationError> {
        match self
            .read_all_checkpoints(prepared, planned.reference())
            .await
        {
            Ok(checkpoints) => Ok(checkpoints),
            Err(class) => Err(self
                .terminated_error(
                    planned.reference(),
                    class,
                    EnrichmentApplicationError::GraphBoundary {
                        operation: GraphOperation::ReadCheckpoint,
                        class,
                    },
                )
                .await),
        }
    }

    async fn complete_run(
        &self,
        planned: &PlannedEnrichmentRun,
        configuration: &EnrichmentRunConfiguration,
        submitted_chunks: u64,
        resumed_chunks: u64,
        checkpoints: CheckpointAggregate,
    ) -> Result<EnrichmentRunOutcome, EnrichmentApplicationError> {
        let completion = self
            .projector
            .project_enrichment(EnrichmentGraphCommand::CompleteRun(
                CompleteEnrichmentRunCommand::new(planned.reference().clone()),
            ))
            .await;
        let completion_disposition = match completion {
            Ok(receipt) => receipt.disposition(),
            Err(error) => {
                let class = error.enrichment_failure_class();
                match self.is_exact_selected(configuration, planned).await {
                    Ok(true) => EnrichmentProjectionDisposition::Unchanged,
                    Ok(false) => {
                        return Err(self
                            .terminated_error(
                                planned.reference(),
                                class,
                                EnrichmentApplicationError::GraphBoundary {
                                    operation: GraphOperation::CompleteRun,
                                    class,
                                },
                            )
                            .await);
                    }
                    Err(selection) => {
                        return Err(self
                            .terminated_error(
                                planned.reference(),
                                class,
                                EnrichmentApplicationError::CompletionReconciliationUnavailable {
                                    completion: class,
                                    selection,
                                },
                            )
                            .await);
                    }
                }
            }
        };
        Ok(EnrichmentRunOutcome::Completed(CompletedEnrichmentRun {
            run_id: planned.specification().run_id(),
            submitted_chunks: RecordCount::new(submitted_chunks),
            resumed_chunks: RecordCount::new(resumed_chunks),
            input_tokens: TokenCount::new(checkpoints.input_tokens),
            output_tokens: TokenCount::new(checkpoints.output_tokens),
            completion_disposition,
        }))
    }

    async fn exact_selected_outcome(
        &self,
        configuration: &EnrichmentRunConfiguration,
        planned: &PlannedEnrichmentRun,
    ) -> Result<Option<EnrichmentRunOutcome>, EnrichmentApplicationError> {
        let lookup = self
            .reader
            .selected_enrichment(&EnrichmentQuery::new(
                configuration.namespace().clone(),
                configuration.session_id(),
            ))
            .await
            .map_err(|error| EnrichmentApplicationError::GraphBoundary {
                operation: GraphOperation::ReadSelection,
                class: error.enrichment_failure_class(),
            })?;
        Ok(match lookup {
            EnrichmentLookup::Selected(selected)
                if selected.run().source_digest() == configuration.source_digest()
                    && selected.run().fingerprint() == planned.specification().fingerprint() =>
            {
                Some(EnrichmentRunOutcome::ExactFingerprintUnchanged {
                    run_id: selected.run().run_id(),
                })
            }
            EnrichmentLookup::Selected(_) | EnrichmentLookup::Unavailable(_) => None,
        })
    }

    async fn is_exact_selected(
        &self,
        configuration: &EnrichmentRunConfiguration,
        planned: &PlannedEnrichmentRun,
    ) -> Result<bool, EnrichmentFailureClass> {
        let lookup = self
            .reader
            .selected_enrichment(&EnrichmentQuery::new(
                configuration.namespace().clone(),
                configuration.session_id(),
            ))
            .await
            .map_err(|error| error.enrichment_failure_class())?;
        Ok(matches!(
            lookup,
            EnrichmentLookup::Selected(selected)
                if selected.run().source_digest() == configuration.source_digest()
                    && selected.run().fingerprint() == planned.specification().fingerprint()
        ))
    }

    async fn project_extraction(
        &self,
        run: &EnrichmentRunRef,
        lease: ClaimedEnrichmentChunk,
        missing: MissingChunk<'_>,
        extraction: &ChunkKnowledgeExtraction,
        configuration: &EnrichmentRunConfiguration,
    ) -> ChunkSettlement {
        let projection = match convert_chunk(missing.chunk, extraction, configuration) {
            Ok(projection) => projection,
            Err(EnrichmentApplicationError::Conversion { stage }) => {
                return self
                    .failed_with_release(
                        run,
                        lease,
                        ChunkFailure::conversion(
                            missing.ordinal,
                            missing.chunk_id,
                            EnrichmentFailureClass::CitationValidation,
                            stage,
                        ),
                    )
                    .await;
            }
            Err(_) => {
                return self
                    .failed_with_release(
                        run,
                        lease,
                        ChunkFailure::conversion(
                            missing.ordinal,
                            missing.chunk_id,
                            EnrichmentFailureClass::CitationValidation,
                            ConversionStage::ChunkProjection,
                        ),
                    )
                    .await;
            }
        };
        let Ok(command) = ProjectClaimedEnrichmentChunkCommand::new(run.clone(), lease, projection)
        else {
            return self
                .failed_with_release(
                    run,
                    lease,
                    ChunkFailure::conversion(
                        missing.ordinal,
                        missing.chunk_id,
                        EnrichmentFailureClass::CitationValidation,
                        ConversionStage::ChunkProjection,
                    ),
                )
                .await;
        };
        match self
            .projector
            .project_claimed_enrichment_chunk(command)
            .await
        {
            Ok(_) => ChunkSettlement::submitted(),
            Err(error) => {
                let failure = ChunkFailure::projection(
                    missing.ordinal,
                    missing.chunk_id,
                    error.enrichment_failure_class(),
                );
                self.failed_with_release(run, lease, failure).await
            }
        }
    }

    async fn failed_with_release(
        &self,
        run: &EnrichmentRunRef,
        lease: ClaimedEnrichmentChunk,
        failure: ChunkFailure,
    ) -> ChunkSettlement {
        let ordinal = failure.ordinal;
        let chunk_id = failure.chunk_id;
        let release = self
            .projector
            .release_enrichment_chunk_lease(&ReleaseEnrichmentChunkLeaseCommand::new(
                run.clone(),
                lease,
            ))
            .await;
        let mut settlement = ChunkSettlement::failed_after_submission(failure);
        if let Err(error) = release {
            settlement.failures.push(ChunkFailure::lease_release(
                ordinal,
                chunk_id,
                error.enrichment_failure_class(),
            ));
        }
        settlement
    }

    async fn read_all_checkpoints(
        &self,
        prepared: &PreparedTranscript,
        run: &EnrichmentRunRef,
    ) -> Result<CheckpointAggregate, EnrichmentFailureClass> {
        let mut aggregate = CheckpointAggregate::default();
        for chunk in prepared.chunks().iter() {
            let chunk_id =
                graph_chunk_id(chunk).map_err(|_| EnrichmentFailureClass::CitationValidation)?;
            let checkpoint = self
                .reader
                .enrichment_chunk_checkpoint(&EnrichmentChunkCheckpointQuery::new(
                    run.clone(),
                    chunk_id,
                ))
                .await
                .map_err(|error| error.enrichment_failure_class())?;
            match checkpoint {
                EnrichmentChunkCheckpoint::Required => {
                    aggregate.missing.push(chunk_id);
                }
                EnrichmentChunkCheckpoint::Committed(receipt) => {
                    if receipt.chunk_id() != chunk_id {
                        return Err(EnrichmentFailureClass::CitationValidation);
                    }
                    aggregate.add(receipt);
                }
            }
        }
        Ok(aggregate)
    }

    async fn terminated_error(
        &self,
        run: &EnrichmentRunRef,
        original: EnrichmentFailureClass,
        error: EnrichmentApplicationError,
    ) -> EnrichmentApplicationError {
        match self
            .projector
            .project_enrichment(EnrichmentGraphCommand::MarkRunFailed(
                MarkEnrichmentRunFailedCommand::new(run.clone(), original),
            ))
            .await
        {
            Ok(_) => error,
            Err(transition) => EnrichmentApplicationError::FailureTransitionUnavailable {
                original,
                transition: transition.enrichment_failure_class(),
            },
        }
    }
}

fn graph_chunk_id(
    chunk: &BoundedTranscriptChunk,
) -> Result<EnrichmentChunkId, EnrichmentApplicationError> {
    EnrichmentChunkId::parse_hex(&chunk.id().to_hex()).map_err(|_| {
        EnrichmentApplicationError::InvalidRunConfiguration {
            field: crate::RunConfigurationField::ExpectedChunks,
        }
    })
}

#[derive(Default)]
struct CheckpointAggregate {
    missing: Vec<EnrichmentChunkId>,
    input_tokens: u64,
    output_tokens: u64,
}

impl CheckpointAggregate {
    fn add(&mut self, receipt: CommittedEnrichmentChunk) {
        self.input_tokens = self
            .input_tokens
            .saturating_add(receipt.input_tokens().value());
        self.output_tokens = self
            .output_tokens
            .saturating_add(receipt.output_tokens().value());
    }
}

#[derive(Default)]
struct SettlementAccumulator {
    failures: Vec<ChunkFailure>,
    submitted_chunks: u64,
    concurrently_committed_chunks: u64,
}

impl SettlementAccumulator {
    fn record(&mut self, failure: ChunkFailure) {
        self.failures.push(failure);
    }

    #[cfg(test)]
    fn record_extraction(
        &mut self,
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
    ) {
        self.record(ChunkFailure::extraction(ordinal, chunk_id, class));
    }

    #[cfg(test)]
    fn record_conversion(
        &mut self,
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
        stage: ConversionStage,
    ) {
        self.record(ChunkFailure::conversion(ordinal, chunk_id, class, stage));
    }

    #[cfg(test)]
    fn record_projection(
        &mut self,
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
    ) {
        self.record(ChunkFailure::projection(ordinal, chunk_id, class));
    }

    #[cfg(test)]
    fn record_lease_busy(&mut self, ordinal: usize, chunk_id: EnrichmentChunkId) {
        self.record(ChunkFailure::lease_busy(ordinal, chunk_id));
    }

    fn finish(mut self, missing: &[EnrichmentChunkId]) -> FailedEnrichmentSettlement {
        self.failures.sort_by_key(|failure| failure.ordinal);
        let mut primary = None;
        let mut extraction_failures = 0_u64;
        let mut conversion_failures = 0_u64;
        let mut projection_failures = 0_u64;
        let mut lease_busy_chunks = 0_u64;
        let mut lease_boundary_failures = 0_u64;
        let mut conversion_location = ConversionFailureLocation::NotApplicable;
        for failure in self
            .failures
            .into_iter()
            .filter(|failure| missing.contains(&failure.chunk_id))
        {
            primary = Some(match primary {
                Some(current) => choose_primary(current, failure.class),
                None => failure.class,
            });
            match failure.kind {
                ChunkFailureKind::Extraction => {
                    extraction_failures = extraction_failures.saturating_add(1);
                }
                ChunkFailureKind::Conversion(stage) => {
                    conversion_failures = conversion_failures.saturating_add(1);
                    if conversion_location == ConversionFailureLocation::NotApplicable {
                        conversion_location = ConversionFailureLocation::Stage(stage);
                    }
                }
                ChunkFailureKind::Projection => {
                    projection_failures = projection_failures.saturating_add(1);
                }
                ChunkFailureKind::LeaseBusy => {
                    lease_busy_chunks = lease_busy_chunks.saturating_add(1);
                }
                ChunkFailureKind::LeaseClaim | ChunkFailureKind::LeaseRelease => {
                    lease_boundary_failures = lease_boundary_failures.saturating_add(1);
                }
            }
        }
        let primary = match primary {
            Some(primary) => primary,
            None => EnrichmentFailureClass::Projection,
        };
        FailedEnrichmentSettlement::new(
            primary,
            SettlementFailureCounts {
                extraction: RecordCount::new(extraction_failures),
                conversion: RecordCount::new(conversion_failures),
                projection: RecordCount::new(projection_failures),
                lease_busy: RecordCount::new(lease_busy_chunks),
                lease_boundary: RecordCount::new(lease_boundary_failures),
            },
            RecordCount::new(u64::try_from(missing.len()).map_or(u64::MAX, |value| value)),
            conversion_location,
        )
    }
}

struct ChunkSettlement {
    failures: Vec<ChunkFailure>,
    submitted_chunks: u64,
    concurrently_committed_chunks: u64,
}

impl ChunkSettlement {
    fn submitted() -> Self {
        Self {
            failures: Vec::new(),
            submitted_chunks: 1,
            concurrently_committed_chunks: 0,
        }
    }

    fn concurrently_committed() -> Self {
        Self {
            failures: Vec::new(),
            submitted_chunks: 0,
            concurrently_committed_chunks: 1,
        }
    }

    fn failed(failure: ChunkFailure) -> Self {
        Self {
            failures: vec![failure],
            submitted_chunks: 0,
            concurrently_committed_chunks: 0,
        }
    }

    fn failed_after_submission(failure: ChunkFailure) -> Self {
        Self {
            failures: vec![failure],
            submitted_chunks: 1,
            concurrently_committed_chunks: 0,
        }
    }
}

struct ChunkFailure {
    ordinal: usize,
    chunk_id: EnrichmentChunkId,
    class: EnrichmentFailureClass,
    kind: ChunkFailureKind,
}

impl ChunkFailure {
    const fn extraction(
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
    ) -> Self {
        Self {
            ordinal,
            chunk_id,
            class,
            kind: ChunkFailureKind::Extraction,
        }
    }

    const fn conversion(
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
        stage: ConversionStage,
    ) -> Self {
        Self {
            ordinal,
            chunk_id,
            class,
            kind: ChunkFailureKind::Conversion(stage),
        }
    }

    const fn projection(
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
    ) -> Self {
        Self {
            ordinal,
            chunk_id,
            class,
            kind: ChunkFailureKind::Projection,
        }
    }

    const fn lease_busy(ordinal: usize, chunk_id: EnrichmentChunkId) -> Self {
        Self {
            ordinal,
            chunk_id,
            class: EnrichmentFailureClass::LeaseBusy,
            kind: ChunkFailureKind::LeaseBusy,
        }
    }

    const fn lease_claim(
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
    ) -> Self {
        Self {
            ordinal,
            chunk_id,
            class,
            kind: ChunkFailureKind::LeaseClaim,
        }
    }

    const fn lease_release(
        ordinal: usize,
        chunk_id: EnrichmentChunkId,
        class: EnrichmentFailureClass,
    ) -> Self {
        Self {
            ordinal,
            chunk_id,
            class,
            kind: ChunkFailureKind::LeaseRelease,
        }
    }
}

enum ChunkFailureKind {
    Extraction,
    Conversion(ConversionStage),
    Projection,
    LeaseBusy,
    LeaseClaim,
    LeaseRelease,
}

fn choose_primary(
    current: EnrichmentFailureClass,
    candidate: EnrichmentFailureClass,
) -> EnrichmentFailureClass {
    match (current, candidate) {
        (current, candidate)
            if current.status()
                == harness_graph_graph_port::EnrichmentFailureStatus::RetryableFailed
                && candidate.status()
                    == harness_graph_graph_port::EnrichmentFailureStatus::TerminalFailed =>
        {
            candidate
        }
        (current, _) => current,
    }
}

#[cfg(test)]
mod tests {
    use harness_graph_graph_port::{EnrichmentChunkId, EnrichmentFailureClass};

    use crate::{ConversionFailureLocation, ConversionStage};

    use super::{SettlementAccumulator, choose_primary};

    fn chunk(value: char) -> Result<EnrichmentChunkId, Box<dyn std::error::Error>> {
        Ok(EnrichmentChunkId::parse_hex(&value.to_string().repeat(64))?)
    }

    #[test]
    fn terminal_failure_dominates_retryable_failure() {
        let primary = choose_primary(
            EnrichmentFailureClass::RateLimited,
            EnrichmentFailureClass::CitationValidation,
        );
        assert_eq!(primary, EnrichmentFailureClass::CitationValidation);
    }

    #[test]
    fn final_checkpoint_reconciliation_discards_ambiguous_committed_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        let reconciled = chunk('1')?;
        let missing = chunk('2')?;
        let mut failures = SettlementAccumulator::default();
        failures.record_projection(0, reconciled, EnrichmentFailureClass::Projection);
        failures.record_extraction(1, missing, EnrichmentFailureClass::RateLimited);

        let settlement = failures.finish(&[missing]);

        assert_eq!(settlement.class(), EnrichmentFailureClass::RateLimited);
        assert_eq!(settlement.extraction_failures().value(), 1);
        assert_eq!(settlement.projection_failures().value(), 0);
        assert_eq!(settlement.missing_checkpoints().value(), 1);
        Ok(())
    }

    #[test]
    fn busy_lease_is_retryable_and_not_counted_as_a_provider_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let missing = chunk('a')?;
        let mut failures = SettlementAccumulator::default();
        failures.record_lease_busy(0, missing);

        let settlement = failures.finish(&[missing]);

        assert_eq!(settlement.class(), EnrichmentFailureClass::LeaseBusy);
        assert_eq!(settlement.lease_busy_chunks().value(), 1);
        assert_eq!(settlement.extraction_failures().value(), 0);
        assert_eq!(settlement.lease_boundary_failures().value(), 0);
        Ok(())
    }

    #[test]
    fn settlement_reporting_is_independent_of_completion_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = chunk('3')?;
        let second = chunk('4')?;
        let mut forward = SettlementAccumulator::default();
        forward.record_conversion(
            0,
            first,
            EnrichmentFailureClass::CitationValidation,
            ConversionStage::NarrativeEpisode,
        );
        forward.record_conversion(
            1,
            second,
            EnrichmentFailureClass::SecretEcho,
            ConversionStage::KnowledgeClaim,
        );

        let mut reverse = SettlementAccumulator::default();
        reverse.record_conversion(
            1,
            second,
            EnrichmentFailureClass::SecretEcho,
            ConversionStage::KnowledgeClaim,
        );
        reverse.record_conversion(
            0,
            first,
            EnrichmentFailureClass::CitationValidation,
            ConversionStage::NarrativeEpisode,
        );

        let expected = forward.finish(&[first, second]);
        let actual = reverse.finish(&[second, first]);

        assert_eq!(actual, expected);
        assert_eq!(actual.class(), EnrichmentFailureClass::CitationValidation);
        assert_eq!(
            actual.conversion_location(),
            ConversionFailureLocation::Stage(ConversionStage::NarrativeEpisode)
        );
        Ok(())
    }
}
