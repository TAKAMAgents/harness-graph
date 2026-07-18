//! Cost-bearing transcript enrichment orchestration.
//!
//! This module is the CLI composition boundary for the additive enrichment
//! pipeline. A verified source is always projected through the deterministic
//! importer before its locally sanitized transcript can enter the Mistral-only
//! application workflow.

use std::sync::Arc;

use futures_util::{StreamExt, stream};
use harness_graph_domain::{GraphNamespace, OccurredAt, SessionId, SourceDigest};
use harness_graph_enrichment_application::{
    EnrichmentApplicationError, EnrichmentRunConfiguration, EnrichmentRunOutcome,
    ExtractionConcurrency, TranscriptEnrichmentApplication,
};
use harness_graph_graph_port::{
    EnrichmentFailureClass, EnrichmentProjectionDisposition, EnrichmentProjector, GraphProjector,
};
use harness_graph_ingestion::{
    MaxSourceRecordBytes, SessionBundle, SessionScope, VerifiedSessionBundle,
};
use harness_graph_mistral_adapter::{
    MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL, MistralConcurrencyLimit, MistralTranscriptPromptProvenance,
    RigMistralAdapter,
};
use harness_graph_neo4j_adapter::Neo4jAdapter;
use harness_graph_transcript_enrichment::{
    AuthorizationIdentity, AuthorizationPolicyDigest, DisclosureAuthorization,
    LocalTranscriptRedactor, PreparedTranscript, RedactionPolicyVersion, TranscriptChunkPolicy,
    TranscriptDisclosureScope, TranscriptPreparation, TranscriptPreparationBlockReason,
    TranscriptPreparationLimits, TranscriptTokenPricing, prepare_verified_transcript,
};
use serde::Serialize;

use crate::{
    AppConfig, CliError, MistralPrivacyControl, TranscriptApplyRequirement,
    TranscriptEnrichmentMode,
};

use super::{
    EligibleSessionLimit, EligibleSessionSelection, EnrichmentSessionConcurrencyLimit,
    ImportFailureClass, SessionImportResult, TRANSCRIPT_DISCLOSURE_POLICY,
    TRANSCRIPT_REDACTION_POLICY_VERSION, import_failure_class, import_verified_session,
    transcript_chunk_policy, write_json,
};

/// Apply one verified session through deterministic and additive graph layers.
pub(super) async fn enrich_transcripts_apply(
    config: &AppConfig,
    session_id: &str,
    authorization: String,
    disclosure_scope: TranscriptDisclosureScope,
) -> Result<(), CliError> {
    let authorization_identity = AuthorizationIdentity::new(authorization)?;
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root()?.discover(SessionScope::All)?;
    let bundle = catalog.require(session_id)?.clone();
    let runtime =
        TranscriptApplyRuntime::initialize(config, authorization_identity, disclosure_scope)
            .await?;
    let settlement = settle_complete_session(config, &runtime, bundle).await;
    let incomplete = settlement.output.is_blocked_or_failed();
    write_json(&TranscriptApplySingleOutput {
        execution_mode: TranscriptApplyExecutionMode::Apply,
        provenance: runtime.provenance_output(),
        session: settlement.output,
    })?;
    if incomplete {
        Err(CliError::TranscriptApplyIncomplete)
    } else {
        Ok(())
    }
}

/// Apply a stable, bounded, all-results-settle archive sweep.
pub(super) async fn enrich_all_transcripts_apply(
    config: &AppConfig,
    scope: SessionScope,
    authorization: String,
    disclosure_scope: TranscriptDisclosureScope,
    concurrency: EnrichmentSessionConcurrencyLimit,
    selection: EligibleSessionSelection,
) -> Result<(), CliError> {
    let authorization_identity = AuthorizationIdentity::new(authorization)?;
    let catalog = config.archive_root()?.discover(scope)?;
    let runtime =
        TranscriptApplyRuntime::initialize(config, authorization_identity, disclosure_scope)
            .await?;
    let discovered_sessions = saturating_usize_to_u64(catalog.len());
    let mut outputs = Vec::new();
    let mut scanned_sessions = 0_u64;
    let mut selected_eligible_sessions = 0_u64;

    if let EligibleSessionSelection::First(limit) = selection {
        let mut ready_batch = Vec::with_capacity(concurrency.value());
        for bundle in catalog.iter().cloned() {
            if selected_eligible_sessions >= saturating_usize_to_u64(limit.value()) {
                break;
            }
            scanned_sessions = scanned_sessions.saturating_add(1);
            match prepare_session_before_provider(config, &runtime, bundle).await {
                PreProviderSettlement::Ready(ready) => {
                    selected_eligible_sessions = selected_eligible_sessions.saturating_add(1);
                    ready_batch.push(*ready);
                    if ready_batch.len() == concurrency.value() {
                        settle_ready_batch(&runtime, concurrency, &mut ready_batch, &mut outputs)
                            .await;
                    }
                }
                PreProviderSettlement::Final(output) => outputs.push(output),
            }
        }
        settle_ready_batch(&runtime, concurrency, &mut ready_batch, &mut outputs).await;
    } else {
        let mut settlements = stream::iter(catalog.iter().cloned().map(|bundle| {
            let runtime = &runtime;
            async move { settle_complete_session(config, runtime, bundle).await }
        }))
        .buffer_unordered(concurrency.value());
        while let Some(settlement) = settlements.next().await {
            scanned_sessions = scanned_sessions.saturating_add(1);
            if settlement.was_eligible {
                selected_eligible_sessions = selected_eligible_sessions.saturating_add(1);
            }
            outputs.push(settlement.output);
        }
    }

    outputs.sort_by(|left, right| left.session_id().cmp(right.session_id()));
    let aggregate = TranscriptApplyAggregate::from_outputs(&outputs);
    let incomplete = aggregate.blocked_sessions > 0 || aggregate.failed_sessions > 0;
    write_json(&TranscriptApplyAllOutput {
        status: if incomplete {
            TranscriptApplyBulkStatus::CompletedWithBlocksOrFailures
        } else {
            TranscriptApplyBulkStatus::Completed
        },
        execution_mode: TranscriptApplyExecutionMode::Apply,
        synchronization: TranscriptApplySynchronization::AllResultsSettle,
        selection: match selection {
            EligibleSessionSelection::First(maximum) => {
                TranscriptApplySelection::FirstEligible { maximum }
            }
            EligibleSessionSelection::All => TranscriptApplySelection::AllEligible,
        },
        max_session_concurrency: concurrency,
        provider_concurrency: runtime.extraction_concurrency_value(),
        discovered_sessions,
        scanned_sessions,
        selected_eligible_sessions,
        aggregate,
        accounting_scope: TranscriptApplyAccountingScope::CompletedRunCheckpoints,
        provenance: runtime.provenance_output(),
        sessions: outputs,
    })?;
    if incomplete {
        Err(CliError::BulkTranscriptApplyIncomplete)
    } else {
        Ok(())
    }
}

async fn settle_ready_batch(
    runtime: &TranscriptApplyRuntime,
    concurrency: EnrichmentSessionConcurrencyLimit,
    ready_batch: &mut Vec<ReadyTranscriptSession>,
    outputs: &mut Vec<TranscriptApplySessionOutput>,
) {
    if ready_batch.is_empty() {
        return;
    }
    let batch = std::mem::take(ready_batch);
    let mut settlements = stream::iter(
        batch
            .into_iter()
            .map(|ready| async move { apply_prepared_session(runtime, ready).await }),
    )
    .buffer_unordered(concurrency.value());
    while let Some(output) = settlements.next().await {
        outputs.push(output);
    }
}

struct TranscriptApplyRuntime {
    neo4j: Neo4jAdapter,
    extractor: Arc<RigMistralAdapter>,
    namespace: GraphNamespace,
    redactor: Arc<LocalTranscriptRedactor>,
    chunk_policy: Arc<TranscriptChunkPolicy>,
    authorization_identity: AuthorizationIdentity,
    disclosure_scope: TranscriptDisclosureScope,
    authorization_policy_digest: AuthorizationPolicyDigest,
    prompt_provenance: MistralTranscriptPromptProvenance,
    extraction_concurrency: ExtractionConcurrency,
    provider_concurrency: MistralConcurrencyLimit,
    pricing: TranscriptTokenPricing,
}

impl TranscriptApplyRuntime {
    async fn initialize(
        config: &AppConfig,
        authorization_identity: AuthorizationIdentity,
        disclosure_scope: TranscriptDisclosureScope,
    ) -> Result<Self, CliError> {
        if config.transcript_enrichment_mode()? != TranscriptEnrichmentMode::Enabled {
            return Err(CliError::TranscriptApplyPrecondition {
                requirement: TranscriptApplyRequirement::EnrichmentEnabled,
            });
        }
        if config.mistral_privacy_control()? != MistralPrivacyControl::TrainingOptOutVerified {
            return Err(CliError::TranscriptApplyPrecondition {
                requirement: TranscriptApplyRequirement::TrainingOptOutVerified,
            });
        }
        let pseudonymization_key =
            config
                .pseudonymization_key()
                .map_err(|_| CliError::TranscriptApplyPrecondition {
                    requirement: TranscriptApplyRequirement::StablePseudonymizationKey,
                })?;
        let credential = config.transcript_mistral_credential()?;
        let model = config.transcript_mistral_model()?;
        if model.as_str() != MISTRAL_TRANSCRIPT_KNOWLEDGE_MODEL {
            return Err(CliError::TranscriptApplyPrecondition {
                requirement: TranscriptApplyRequirement::PinnedMistralModel,
            });
        }
        let prompt_provenance = MistralTranscriptPromptProvenance::pinned()?;
        let provider_concurrency = config.mistral_concurrency()?;
        let extraction_concurrency = ExtractionConcurrency::new(provider_concurrency.value())?;
        let sensitive_values = config.sensitive_values_for_redaction()?;
        let redactor = LocalTranscriptRedactor::new(
            RedactionPolicyVersion::new(TRANSCRIPT_REDACTION_POLICY_VERSION)?,
            pseudonymization_key,
            sensitive_values.clone(),
        )?;
        let extractor = RigMistralAdapter::with_concurrency_and_output_secrets(
            &credential,
            model,
            provider_concurrency,
            sensitive_values,
        )?;
        let namespace = config.graph_namespace()?;
        let chunk_policy = transcript_chunk_policy()?;
        let pricing = config.transcript_token_pricing()?;
        let neo4j = super::connect_neo4j(config).await?;
        neo4j.health().await?;
        neo4j.ensure_schema().await?;
        neo4j.ensure_enrichment_schema().await?;
        Ok(Self {
            neo4j,
            extractor: Arc::new(extractor),
            namespace,
            redactor: Arc::new(redactor),
            chunk_policy: Arc::new(chunk_policy),
            authorization_identity,
            disclosure_scope,
            authorization_policy_digest: AuthorizationPolicyDigest::hash(
                TRANSCRIPT_DISCLOSURE_POLICY,
            ),
            prompt_provenance,
            extraction_concurrency,
            provider_concurrency,
            pricing,
        })
    }

    fn run_configuration(
        &self,
        ready: &ReadyTranscriptSession,
    ) -> Result<EnrichmentRunConfiguration, EnrichmentApplicationError> {
        EnrichmentRunConfiguration::new(
            self.namespace.clone(),
            ready.session_id,
            ready.source_digest,
            self.disclosure_scope,
            self.authorization_policy_digest,
            self.redactor.policy_version().clone(),
            self.chunk_policy.version().clone(),
            self.prompt_provenance.model().clone(),
            self.prompt_provenance.prompt_version().clone(),
            self.prompt_provenance.prompt_digest(),
            self.prompt_provenance.schema_version().clone(),
        )
    }

    const fn extraction_concurrency_value(&self) -> usize {
        self.provider_concurrency.value()
    }

    fn provenance_output(&self) -> TranscriptApplyProvenanceOutput {
        TranscriptApplyProvenanceOutput {
            foundation_model_provider: "mistral",
            model: self.prompt_provenance.model().as_str().to_owned(),
            prompt_version: self.prompt_provenance.prompt_version().as_str().to_owned(),
            prompt_digest: self.prompt_provenance.prompt_digest().to_hex(),
            schema_version: self.prompt_provenance.schema_version().as_str().to_owned(),
            disclosure_scope: self.disclosure_scope,
            authorization_policy_digest: self.authorization_policy_digest.to_hex(),
            redaction_policy_version: self.redactor.policy_version().as_str().to_owned(),
            chunking_policy_version: self.chunk_policy.version().as_str().to_owned(),
        }
    }
}

struct ReadyTranscriptSession {
    session_id: SessionId,
    source_digest: SourceDigest,
    base_status: DeterministicBaseStatus,
    prepared: PreparedTranscript,
}

enum PreProviderSettlement {
    Ready(Box<ReadyTranscriptSession>),
    Final(TranscriptApplySessionOutput),
}

struct CompleteSessionSettlement {
    output: TranscriptApplySessionOutput,
    was_eligible: bool,
}

async fn settle_complete_session(
    config: &AppConfig,
    runtime: &TranscriptApplyRuntime,
    bundle: SessionBundle,
) -> CompleteSessionSettlement {
    match prepare_session_before_provider(config, runtime, bundle).await {
        PreProviderSettlement::Ready(ready) => CompleteSessionSettlement {
            output: apply_prepared_session(runtime, *ready).await,
            was_eligible: true,
        },
        PreProviderSettlement::Final(output) => CompleteSessionSettlement {
            output,
            was_eligible: false,
        },
    }
}

async fn prepare_session_before_provider(
    config: &AppConfig,
    runtime: &TranscriptApplyRuntime,
    bundle: SessionBundle,
) -> PreProviderSettlement {
    let session_id = bundle.session_id();
    let source_digest = bundle.source_digest();
    let verified = match tokio::task::spawn_blocking(move || bundle.verify()).await {
        Ok(Ok(verified)) => verified,
        Ok(Err(_)) => {
            return PreProviderSettlement::Final(TranscriptApplySessionOutput::Failed {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                failure: TranscriptApplyFailure::ArchiveIntegrity,
            });
        }
        Err(_) => {
            return PreProviderSettlement::Final(TranscriptApplySessionOutput::Failed {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                failure: TranscriptApplyFailure::WorkerJoin,
            });
        }
    };

    let base_status = match import_verified_session(config, &runtime.neo4j, verified.clone()).await
    {
        Ok(SessionImportResult::Imported(_)) => DeterministicBaseStatus::Imported,
        Ok(SessionImportResult::AlreadyComplete { .. }) => DeterministicBaseStatus::AlreadyComplete,
        Err(error) => {
            return PreProviderSettlement::Final(TranscriptApplySessionOutput::Failed {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                failure: TranscriptApplyFailure::DeterministicBase {
                    class: import_failure_class(&error),
                },
            });
        }
    };

    let redactor = Arc::clone(&runtime.redactor);
    let chunk_policy = Arc::clone(&runtime.chunk_policy);
    let authorization_identity = runtime.authorization_identity.clone();
    let disclosure_scope = runtime.disclosure_scope;
    let authorization_policy_digest = runtime.authorization_policy_digest;
    let preparation = tokio::task::spawn_blocking(move || {
        prepare_session(
            verified,
            &redactor,
            &chunk_policy,
            authorization_identity,
            disclosure_scope,
            authorization_policy_digest,
        )
    })
    .await;
    let preparation = match preparation {
        Ok(Ok(preparation)) => preparation,
        Ok(Err(_)) => {
            return PreProviderSettlement::Final(TranscriptApplySessionOutput::Failed {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                failure: TranscriptApplyFailure::Preparation,
            });
        }
        Err(_) => {
            return PreProviderSettlement::Final(TranscriptApplySessionOutput::Failed {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                failure: TranscriptApplyFailure::WorkerJoin,
            });
        }
    };
    match preparation {
        TranscriptPreparation::Prepared(prepared) => {
            PreProviderSettlement::Ready(Box::new(ReadyTranscriptSession {
                session_id,
                source_digest,
                base_status,
                prepared,
            }))
        }
        TranscriptPreparation::MetadataOnly(inventory) => {
            PreProviderSettlement::Final(TranscriptApplySessionOutput::MetadataOnly {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                deterministic_base: base_status,
                verified_records: inventory.total_records().value(),
            })
        }
        TranscriptPreparation::Blocked(blocked) => {
            PreProviderSettlement::Final(TranscriptApplySessionOutput::Blocked {
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                deterministic_base: base_status,
                reason: TranscriptApplyBlockReason::from(blocked.reason()),
                verified_records: blocked.inventory().total_records().value(),
            })
        }
    }
}

fn prepare_session(
    verified: VerifiedSessionBundle,
    redactor: &LocalTranscriptRedactor,
    chunk_policy: &TranscriptChunkPolicy,
    authorization_identity: AuthorizationIdentity,
    disclosure_scope: TranscriptDisclosureScope,
    authorization_policy_digest: AuthorizationPolicyDigest,
) -> Result<TranscriptPreparation, harness_graph_transcript_enrichment::TranscriptEnrichmentError> {
    let authorization = DisclosureAuthorization::new(
        verified.session_id(),
        verified.source_digest(),
        disclosure_scope,
        authorization_policy_digest,
        authorization_identity,
        OccurredAt::now_utc(),
    );
    prepare_verified_transcript(
        verified,
        &authorization,
        redactor,
        chunk_policy,
        MaxSourceRecordBytes::default(),
        TranscriptPreparationLimits::default(),
    )
}

async fn apply_prepared_session(
    runtime: &TranscriptApplyRuntime,
    ready: ReadyTranscriptSession,
) -> TranscriptApplySessionOutput {
    let configuration = match runtime.run_configuration(&ready) {
        Ok(configuration) => configuration,
        Err(error) => {
            return TranscriptApplySessionOutput::Failed {
                session_id: ready.session_id.to_string(),
                source_digest: ready.source_digest.to_hex(),
                failure: TranscriptApplyFailure::Enrichment {
                    class: enrichment_application_failure_class(&error),
                },
            };
        }
    };
    let application = TranscriptEnrichmentApplication::new(
        runtime.extractor.as_ref(),
        &runtime.neo4j,
        &runtime.neo4j,
        runtime.extraction_concurrency,
    );
    match application.enrich(&ready.prepared, &configuration).await {
        Ok(EnrichmentRunOutcome::ExactFingerprintUnchanged { run_id }) => {
            TranscriptApplySessionOutput::ExactFingerprintUnchanged {
                session_id: ready.session_id.to_string(),
                source_digest: ready.source_digest.to_hex(),
                deterministic_base: ready.base_status,
                run_id: run_id.to_hex(),
                submitted_chunks: 0,
                new_cost_microusd: 0,
            }
        }
        Ok(EnrichmentRunOutcome::Completed(completed)) => {
            let run_input_tokens = completed.input_tokens();
            let run_output_tokens = completed.output_tokens();
            TranscriptApplySessionOutput::Completed {
                session_id: ready.session_id.to_string(),
                source_digest: ready.source_digest.to_hex(),
                deterministic_base: ready.base_status,
                run_id: completed.run_id().to_hex(),
                submitted_chunks: completed.submitted_chunks().value(),
                resumed_chunks: completed.resumed_chunks().value(),
                run_input_tokens: run_input_tokens.value(),
                run_output_tokens: run_output_tokens.value(),
                run_cost_microusd: runtime
                    .pricing
                    .cost(run_input_tokens, run_output_tokens)
                    .value(),
                completion_disposition: completed.completion_disposition(),
            }
        }
        Err(error) => TranscriptApplySessionOutput::Failed {
            session_id: ready.session_id.to_string(),
            source_digest: ready.source_digest.to_hex(),
            failure: TranscriptApplyFailure::Enrichment {
                class: enrichment_application_failure_class(&error),
            },
        },
    }
}

const fn enrichment_application_failure_class(
    error: &EnrichmentApplicationError,
) -> EnrichmentFailureClass {
    match error {
        EnrichmentApplicationError::InvalidRunConfiguration { .. }
        | EnrichmentApplicationError::PreparationMismatch { .. }
        | EnrichmentApplicationError::TerminalRunCannotResume { .. } => {
            EnrichmentFailureClass::PolicyBlocked
        }
        EnrichmentApplicationError::GraphBoundary { class, .. } => *class,
        EnrichmentApplicationError::CompletionReconciliationUnavailable {
            completion,
            selection,
        } => dominant_failure_class(*completion, *selection),
        EnrichmentApplicationError::Conversion { .. } => EnrichmentFailureClass::CitationValidation,
        EnrichmentApplicationError::SettlementFailed { settlement } => settlement.class(),
        EnrichmentApplicationError::FailureTransitionUnavailable {
            original,
            transition,
        } => dominant_failure_class(*original, *transition),
    }
}

const fn dominant_failure_class(
    first: EnrichmentFailureClass,
    second: EnrichmentFailureClass,
) -> EnrichmentFailureClass {
    use harness_graph_graph_port::EnrichmentFailureStatus;

    match (first.status(), second.status()) {
        (EnrichmentFailureStatus::RetryableFailed, EnrichmentFailureStatus::TerminalFailed) => {
            second
        }
        _ => first,
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptApplyExecutionMode {
    Apply,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptApplySynchronization {
    AllResultsSettle,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptApplyBulkStatus {
    Completed,
    CompletedWithBlocksOrFailures,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum TranscriptApplySelection {
    AllEligible,
    FirstEligible { maximum: EligibleSessionLimit },
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptApplyAccountingScope {
    CompletedRunCheckpoints,
}

#[derive(Debug, Clone, Serialize)]
struct TranscriptApplyProvenanceOutput {
    foundation_model_provider: &'static str,
    model: String,
    prompt_version: String,
    prompt_digest: String,
    schema_version: String,
    disclosure_scope: TranscriptDisclosureScope,
    authorization_policy_digest: String,
    redaction_policy_version: String,
    chunking_policy_version: String,
}

#[derive(Serialize)]
struct TranscriptApplySingleOutput {
    execution_mode: TranscriptApplyExecutionMode,
    provenance: TranscriptApplyProvenanceOutput,
    #[serde(flatten)]
    session: TranscriptApplySessionOutput,
}

#[derive(Serialize)]
struct TranscriptApplyAllOutput {
    status: TranscriptApplyBulkStatus,
    execution_mode: TranscriptApplyExecutionMode,
    synchronization: TranscriptApplySynchronization,
    selection: TranscriptApplySelection,
    max_session_concurrency: EnrichmentSessionConcurrencyLimit,
    provider_concurrency: usize,
    discovered_sessions: u64,
    scanned_sessions: u64,
    selected_eligible_sessions: u64,
    #[serde(flatten)]
    aggregate: TranscriptApplyAggregate,
    accounting_scope: TranscriptApplyAccountingScope,
    provenance: TranscriptApplyProvenanceOutput,
    sessions: Vec<TranscriptApplySessionOutput>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeterministicBaseStatus {
    Imported,
    AlreadyComplete,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptApplyBlockReason {
    ScannerNonTextControlData,
    ScannerAssetOrBinaryData,
    ScannerSuspiciousEncodedBlob,
    SanitizedByteLimitExceeded,
    FragmentLimitExceeded,
    ChunkLimitExceeded,
}

impl From<TranscriptPreparationBlockReason> for TranscriptApplyBlockReason {
    fn from(value: TranscriptPreparationBlockReason) -> Self {
        use harness_graph_transcript_enrichment::ScannerBlockReason;

        match value {
            TranscriptPreparationBlockReason::ScannerRejected { reason, .. } => match reason {
                ScannerBlockReason::NonTextControlData => Self::ScannerNonTextControlData,
                ScannerBlockReason::AssetOrBinaryData => Self::ScannerAssetOrBinaryData,
                ScannerBlockReason::SuspiciousEncodedBlob => Self::ScannerSuspiciousEncodedBlob,
            },
            TranscriptPreparationBlockReason::SanitizedByteLimitExceeded => {
                Self::SanitizedByteLimitExceeded
            }
            TranscriptPreparationBlockReason::FragmentLimitExceeded => Self::FragmentLimitExceeded,
            TranscriptPreparationBlockReason::ChunkLimitExceeded => Self::ChunkLimitExceeded,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(tag = "stage", content = "class", rename_all = "snake_case")]
enum TranscriptApplyFailure {
    ArchiveIntegrity,
    WorkerJoin,
    DeterministicBase { class: ImportFailureClass },
    Preparation,
    Enrichment { class: EnrichmentFailureClass },
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum TranscriptApplySessionOutput {
    Completed {
        session_id: String,
        source_digest: String,
        deterministic_base: DeterministicBaseStatus,
        run_id: String,
        submitted_chunks: u64,
        resumed_chunks: u64,
        run_input_tokens: u64,
        run_output_tokens: u64,
        run_cost_microusd: u64,
        completion_disposition: EnrichmentProjectionDisposition,
    },
    ExactFingerprintUnchanged {
        session_id: String,
        source_digest: String,
        deterministic_base: DeterministicBaseStatus,
        run_id: String,
        submitted_chunks: u64,
        new_cost_microusd: u64,
    },
    MetadataOnly {
        session_id: String,
        source_digest: String,
        deterministic_base: DeterministicBaseStatus,
        verified_records: u64,
    },
    Blocked {
        session_id: String,
        source_digest: String,
        deterministic_base: DeterministicBaseStatus,
        reason: TranscriptApplyBlockReason,
        verified_records: u64,
    },
    Failed {
        session_id: String,
        source_digest: String,
        failure: TranscriptApplyFailure,
    },
}

impl TranscriptApplySessionOutput {
    fn session_id(&self) -> &str {
        match self {
            Self::Completed { session_id, .. }
            | Self::ExactFingerprintUnchanged { session_id, .. }
            | Self::MetadataOnly { session_id, .. }
            | Self::Blocked { session_id, .. }
            | Self::Failed { session_id, .. } => session_id,
        }
    }

    const fn is_blocked_or_failed(&self) -> bool {
        matches!(self, Self::Blocked { .. } | Self::Failed { .. })
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct TranscriptApplyAggregate {
    completed_sessions: u64,
    unchanged_sessions: u64,
    metadata_only_sessions: u64,
    blocked_sessions: u64,
    failed_sessions: u64,
    submitted_chunks: u64,
    resumed_chunks: u64,
    completed_run_input_tokens: u64,
    completed_run_output_tokens: u64,
    completed_run_cost_microusd: u64,
}

impl TranscriptApplyAggregate {
    fn from_outputs(outputs: &[TranscriptApplySessionOutput]) -> Self {
        let mut aggregate = Self::default();
        for output in outputs {
            match output {
                TranscriptApplySessionOutput::Completed {
                    submitted_chunks,
                    resumed_chunks,
                    run_input_tokens,
                    run_output_tokens,
                    run_cost_microusd,
                    ..
                } => {
                    aggregate.completed_sessions = aggregate.completed_sessions.saturating_add(1);
                    aggregate.submitted_chunks =
                        aggregate.submitted_chunks.saturating_add(*submitted_chunks);
                    aggregate.resumed_chunks =
                        aggregate.resumed_chunks.saturating_add(*resumed_chunks);
                    aggregate.completed_run_input_tokens = aggregate
                        .completed_run_input_tokens
                        .saturating_add(*run_input_tokens);
                    aggregate.completed_run_output_tokens = aggregate
                        .completed_run_output_tokens
                        .saturating_add(*run_output_tokens);
                    aggregate.completed_run_cost_microusd = aggregate
                        .completed_run_cost_microusd
                        .saturating_add(*run_cost_microusd);
                }
                TranscriptApplySessionOutput::ExactFingerprintUnchanged { .. } => {
                    aggregate.unchanged_sessions = aggregate.unchanged_sessions.saturating_add(1);
                }
                TranscriptApplySessionOutput::MetadataOnly { .. } => {
                    aggregate.metadata_only_sessions =
                        aggregate.metadata_only_sessions.saturating_add(1);
                }
                TranscriptApplySessionOutput::Blocked { .. } => {
                    aggregate.blocked_sessions = aggregate.blocked_sessions.saturating_add(1);
                }
                TranscriptApplySessionOutput::Failed { .. } => {
                    aggregate.failed_sessions = aggregate.failed_sessions.saturating_add(1);
                }
            }
        }
        aggregate
    }
}

fn saturating_usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use harness_graph_enrichment_application::{EnrichmentApplicationError, RunConfigurationField};
    use harness_graph_graph_port::EnrichmentFailureClass;

    use super::{
        TranscriptApplyBlockReason, TranscriptApplyFailure, TranscriptApplySessionOutput,
        dominant_failure_class, enrichment_application_failure_class, saturating_usize_to_u64,
    };

    #[test]
    fn local_application_failures_map_to_closed_source_safe_classes() {
        let invalid = EnrichmentApplicationError::InvalidRunConfiguration {
            field: RunConfigurationField::Fingerprint,
        };
        let conversion = EnrichmentApplicationError::Conversion {
            stage: harness_graph_enrichment_application::ConversionStage::KnowledgeClaim,
        };

        assert_eq!(
            enrichment_application_failure_class(&invalid),
            EnrichmentFailureClass::PolicyBlocked
        );
        assert_eq!(
            enrichment_application_failure_class(&conversion),
            EnrichmentFailureClass::CitationValidation
        );
    }

    #[test]
    fn preparation_block_reasons_remain_typed() {
        assert!(matches!(
            TranscriptApplyBlockReason::from(
                harness_graph_transcript_enrichment::TranscriptPreparationBlockReason::ChunkLimitExceeded
            ),
            TranscriptApplyBlockReason::ChunkLimitExceeded
        ));
    }

    #[test]
    fn platform_count_conversion_saturates_without_panicking() {
        assert_eq!(saturating_usize_to_u64(7), 7);
    }

    #[test]
    fn terminal_boundary_failure_dominates_retryable_reconciliation_failure() {
        assert_eq!(
            dominant_failure_class(
                EnrichmentFailureClass::Timeout,
                EnrichmentFailureClass::Projection,
            ),
            EnrichmentFailureClass::Projection
        );
        assert_eq!(
            dominant_failure_class(
                EnrichmentFailureClass::Projection,
                EnrichmentFailureClass::Timeout,
            ),
            EnrichmentFailureClass::Projection
        );
    }

    #[test]
    fn failed_session_output_serializes_only_closed_source_safe_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let value = serde_json::to_value(TranscriptApplySessionOutput::Failed {
            session_id: "019c63db-2995-74c3-b898-c1b92a8e1317".to_owned(),
            source_digest: "1".repeat(64),
            failure: TranscriptApplyFailure::Enrichment {
                class: EnrichmentFailureClass::RateLimited,
            },
        })?;

        assert_eq!(value["status"], "failed");
        assert_eq!(value["failure"]["stage"], "enrichment");
        assert_eq!(value["failure"]["class"]["class"], "rate_limited");
        let rendered = value.to_string();
        assert!(!rendered.contains("/Users/"));
        assert!(!rendered.contains("transcript"));
        Ok(())
    }
}
