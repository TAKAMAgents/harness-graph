//! CLI command parsing and orchestration.

mod transcript_apply;

use std::{fmt, str::FromStr};

use clap::{Args, Parser, Subcommand, ValueEnum};
use futures_util::{StreamExt, stream};
use harness_graph_assurance::assess_outcome;
use harness_graph_classification::ActivityBuilder;
use harness_graph_correlation::CorrelationEngine;
use harness_graph_domain::{
    AnalysisReport, DecodedNativeRecord, RecordCount, SemanticActivities, SessionId,
    ToolCallCorrelations, ToolCallLifecycle,
};
use harness_graph_event_journal::AppendOnlyJournal;
use harness_graph_graph_port::{
    AnalysisProjectionCommand, ExperienceScope, FinalizeIngestionCommand, GraphBatch, GraphCommand,
    GraphProjector, SourceSnapshotCommand,
};
use harness_graph_ingestion::{
    DecodedRecordStream, IngestionError, SessionBundle, SessionScope, SourceKind,
    VerifiedSessionBundle, inspect_bundle,
};
use harness_graph_mistral_adapter::RigMistralAdapter;
use harness_graph_neo4j_adapter::{Neo4jAdapter, SourceIngestionStatus};
use harness_graph_path_analysis::derive_path;
use harness_graph_planning::{
    ModelUsage, NarrativeInterpreter, NarrativeOrigin, NarrativeRequest, NarrativeSummary,
    Pathfinder, PlanningContext, PrecedentLimit, PrecedentReader, TaskBrief,
    TaskClassificationRequest,
};
use harness_graph_risk::RiskEngine;
use harness_graph_transcript_enrichment::{
    AuthorizationIdentity, AuthorizationPolicyDigest, ChunkByteLimit, ChunkingPolicyVersion,
    DisclosureAuthorization, EstimatedTokenLimit, FragmentByteLimit, LocalTranscriptRedactor,
    PseudonymizationKey, RedactionCategory, RedactionCounts, RedactionPolicyVersion,
    ScannerBlockReason, TranscriptChunkPolicy, TranscriptDisclosureScope,
    TranscriptEnrichmentError, TranscriptInventoryEstimator, TranscriptPreparation,
    TranscriptPreparationBlockReason, TranscriptPreparationLimits, prepare_verified_transcript,
};
use secrecy::SecretString;
use serde::Serialize;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{AppConfig, CliError};

use self::transcript_apply::{enrich_all_transcripts_apply, enrich_transcripts_apply};

/// Run the command selected by process arguments.
///
/// # Errors
///
/// Returns an error when configuration, ingestion, output encoding, or logging
/// initialization fails.
pub async fn run() -> Result<(), CliError> {
    initialize_logging()?;
    let cli = Cli::parse();
    let config = AppConfig::load()?;
    match cli.command {
        Command::Doctor => doctor(&config),
        Command::Discover { scope, limit } => discover(&config, scope.into(), limit),
        Command::Verify { session_id } => verify(&config, &session_id),
        Command::Inspect { session_id } => inspect(&config, &session_id),
        Command::Analyze { session_id } => analyze(&config, &session_id),
        Command::MistralHealth => mistral_health(&config).await,
        Command::Summarize { session_id } => summarize(&config, &session_id).await,
        Command::Interpret { session_id, task } => interpret(&config, &session_id, task).await,
        Command::Pathfinder { task, precedents } => pathfinder(&config, task, precedents).await,
        Command::Import { session_id } => import(&config, &session_id).await,
        Command::ImportAll { scope, concurrency } => import_all(&config, scope, concurrency).await,
        Command::EnrichTranscripts {
            session_id,
            authorization,
            disclosure_scope,
            execution,
        } => match execution.resolve()? {
            TranscriptExecutionMode::DryRun => enrich_transcripts_dry_run(
                &config,
                &session_id,
                authorization,
                disclosure_scope.into(),
            ),
            TranscriptExecutionMode::Apply => {
                enrich_transcripts_apply(
                    &config,
                    &session_id,
                    authorization,
                    disclosure_scope.into(),
                )
                .await
            }
        },
        Command::EnrichAllTranscripts {
            scope,
            authorization,
            disclosure_scope,
            concurrency,
            limit,
            execution,
        } => match execution.resolve()? {
            TranscriptExecutionMode::DryRun => enrich_all_transcripts_dry_run(
                &config,
                scope.into(),
                authorization,
                disclosure_scope.into(),
            ),
            TranscriptExecutionMode::Apply => {
                enrich_all_transcripts_apply(
                    &config,
                    scope.into(),
                    authorization,
                    disclosure_scope.into(),
                    concurrency,
                    limit.into(),
                )
                .await
            }
        },
        Command::Serve => serve(&config).await,
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "harness-graph",
    version,
    about = "Typed coding-agent execution graph"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate configuration without exposing or contacting credentials.
    Doctor,
    /// Discover unique published session bundles.
    Discover {
        /// Archive scope.
        #[arg(long, value_enum, default_value_t = ScopeArgument::All)]
        scope: ScopeArgument,
        /// Maximum session summaries to print.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Verify one session's complete checksum manifest.
    Verify {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
    },
    /// Stream and type every canonical record in one verified session.
    Inspect {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
    },
    /// Derive source-safe correlations, activities, outcome, risks, and path.
    Analyze {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
    },
    /// Verify the configured Rig-backed Mistral provider against its real API.
    MistralHealth,
    /// Ask Mistral to macro-summarize deterministic activities with citations.
    Summarize {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
    },
    /// Classify a task and extract its session narrative concurrently with Mistral.
    Interpret {
        /// Stable session UUID whose deterministic activities support extraction.
        #[arg(long)]
        session_id: String,
        /// Source-safe task brief. Do not include secrets or raw payloads.
        #[arg(long)]
        task: String,
    },
    /// Retrieve verified Neo4j precedents and ask Mistral for a cited plan.
    Pathfinder {
        /// Source-safe task brief. Do not include secrets or raw payloads.
        #[arg(long)]
        task: String,
        /// Maximum verified precedents to retrieve.
        #[arg(long, default_value_t = 3)]
        precedents: usize,
    },
    /// Verify, stream, and atomically upsert one session into Neo4j.
    Import {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
    },
    /// Import every verified session in a selected archive scope into Neo4j.
    ImportAll {
        /// Archive scope.
        #[arg(long, value_enum, default_value_t = ScopeArgument::All)]
        scope: ScopeArgument,
        /// Maximum simultaneous session imports.
        #[arg(long, default_value_t = ImportConcurrencyLimit::default())]
        concurrency: ImportConcurrencyLimit,
    },
    /// Inventory one authorized transcript without contacting Mistral or Neo4j.
    EnrichTranscripts {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
        /// Source-safe identity for the explicit disclosure authorization.
        #[arg(long)]
        authorization: String,
        /// Closed transcript disclosure scope.
        #[arg(long, value_enum, default_value_t = DisclosureScopeArgument::ConversationAndExecution)]
        disclosure_scope: DisclosureScopeArgument,
        #[command(flatten)]
        execution: TranscriptExecutionArguments,
    },
    /// Inventory every transcript in an archive scope without external mutation.
    EnrichAllTranscripts {
        /// Archive scope.
        #[arg(long, value_enum, default_value_t = ScopeArgument::All)]
        scope: ScopeArgument,
        /// Source-safe identity for the explicit disclosure authorization.
        #[arg(long)]
        authorization: String,
        /// Closed transcript disclosure scope.
        #[arg(long, value_enum, default_value_t = DisclosureScopeArgument::ConversationAndExecution)]
        disclosure_scope: DisclosureScopeArgument,
        /// Maximum simultaneous session workflows. Provider calls also share a stricter global gate.
        #[arg(long, default_value_t = EnrichmentSessionConcurrencyLimit::default())]
        concurrency: EnrichmentSessionConcurrencyLimit,
        /// Stop after the first N eligible sessions in stable catalog order (1-50).
        #[arg(long)]
        limit: Option<EligibleSessionLimit>,
        #[command(flatten)]
        execution: TranscriptExecutionArguments,
    },
    /// Serve durable live ingestion, replay, and server-sent events.
    Serve,
}

#[derive(Debug, Clone, Copy, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum ScopeArgument {
    Active,
    Archived,
    All,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DisclosureScopeArgument {
    ConversationOnly,
    ConversationAndExecution,
}

/// Raw CLI flags immediately converted into a semantic execution mode.
#[derive(Debug, Clone, Copy, Args)]
#[group(required = true, multiple = false)]
struct TranscriptExecutionArguments {
    /// Perform only local verification, scanning, chunk estimation, and reporting.
    #[arg(long)]
    dry_run: bool,
    /// Import the deterministic base, call Mistral on locally sanitized chunks, and project the additive overlay.
    #[arg(long)]
    apply: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TranscriptExecutionMode {
    DryRun,
    Apply,
}

impl TranscriptExecutionArguments {
    fn resolve(self) -> Result<TranscriptExecutionMode, CliError> {
        match (self.dry_run, self.apply) {
            (true, false) => Ok(TranscriptExecutionMode::DryRun),
            (false, true) => Ok(TranscriptExecutionMode::Apply),
            (false, false) | (true, true) => Err(CliError::TranscriptExecutionModeRequired),
        }
    }
}

impl From<DisclosureScopeArgument> for TranscriptDisclosureScope {
    fn from(value: DisclosureScopeArgument) -> Self {
        match value {
            DisclosureScopeArgument::ConversationOnly => Self::ConversationOnly,
            DisclosureScopeArgument::ConversationAndExecution => Self::ConversationAndExecution,
        }
    }
}

/// Bounded number of session projections that may advance concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct ImportConcurrencyLimit(usize);

impl ImportConcurrencyLimit {
    const DEFAULT: usize = 4;
    const MAX: usize = 8;

    const fn value(self) -> usize {
        self.0
    }
}

impl Default for ImportConcurrencyLimit {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl fmt::Display for ImportConcurrencyLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ImportConcurrencyLimit {
    type Err = ImportConcurrencyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value
            .parse::<usize>()
            .map_err(|_| ImportConcurrencyParseError)?;
        if (1..=Self::MAX).contains(&value) {
            Ok(Self(value))
        } else {
            Err(ImportConcurrencyParseError)
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("expected an integer between 1 and 8")]
struct ImportConcurrencyParseError;

/// Bounded number of end-to-end transcript session workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct EnrichmentSessionConcurrencyLimit(usize);

impl EnrichmentSessionConcurrencyLimit {
    const DEFAULT: usize = 2;
    const MAX: usize = 8;

    const fn value(self) -> usize {
        self.0
    }
}

impl Default for EnrichmentSessionConcurrencyLimit {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl fmt::Display for EnrichmentSessionConcurrencyLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for EnrichmentSessionConcurrencyLimit {
    type Err = EnrichmentSessionConcurrencyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value
            .parse::<usize>()
            .map_err(|_| EnrichmentSessionConcurrencyParseError)?;
        if (1..=Self::MAX).contains(&value) {
            Ok(Self(value))
        } else {
            Err(EnrichmentSessionConcurrencyParseError)
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("expected an integer between 1 and 8")]
struct EnrichmentSessionConcurrencyParseError;

/// Optional stable-order pilot bound over eligible sessions only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
struct EligibleSessionLimit(usize);

impl EligibleSessionLimit {
    const MAX: usize = 50;

    const fn value(self) -> usize {
        self.0
    }
}

impl fmt::Display for EligibleSessionLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for EligibleSessionLimit {
    type Err = EligibleSessionLimitParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value
            .parse::<usize>()
            .map_err(|_| EligibleSessionLimitParseError)?;
        if (1..=Self::MAX).contains(&value) {
            Ok(Self(value))
        } else {
            Err(EligibleSessionLimitParseError)
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("expected an integer between 1 and 50")]
struct EligibleSessionLimitParseError;

/// Stable provider-eligible selection policy for a bulk apply invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EligibleSessionSelection {
    All,
    First(EligibleSessionLimit),
}

impl From<Option<EligibleSessionLimit>> for EligibleSessionSelection {
    fn from(value: Option<EligibleSessionLimit>) -> Self {
        match value {
            Some(limit) => Self::First(limit),
            None => Self::All,
        }
    }
}

impl From<ScopeArgument> for SessionScope {
    fn from(value: ScopeArgument) -> Self {
        match value {
            ScopeArgument::Active => Self::Active,
            ScopeArgument::Archived => Self::Archived,
            ScopeArgument::All => Self::All,
        }
    }
}

#[derive(Serialize)]
struct DoctorOutput {
    status: &'static str,
    archive: &'static str,
    graph_provider: &'static str,
    foundation_model_provider: &'static str,
    credentials: CredentialStatus,
}

#[derive(Serialize)]
struct CredentialStatus {
    neo4j: &'static str,
    mistral: &'static str,
}

fn doctor(config: &AppConfig) -> Result<(), CliError> {
    let _archive = config.archive_root()?;
    let _neo4j = config.neo4j()?;
    let _mistral_credential = config.mistral_credential()?;
    let _journal_path = config.journal_path()?;
    write_json(&DoctorOutput {
        status: "ready",
        archive: "configured",
        graph_provider: "neo4j",
        foundation_model_provider: "mistral",
        credentials: CredentialStatus {
            neo4j: "configured",
            mistral: "configured",
        },
    })
}

async fn serve(config: &AppConfig) -> Result<(), CliError> {
    let adapter = connect_neo4j(config).await?;
    let experience_scope = ExperienceScope::new(
        config.graph_namespace()?,
        config.transcript_enrichment_mode()?.into(),
    );
    let journal_path = config.journal_path()?;
    let journal = AppendOnlyJournal::open(&journal_path)?;
    let bind_address = config.bind_address()?;
    let listener = tokio::net::TcpListener::bind(bind_address)
        .await
        .map_err(|source| CliError::Server { source })?;
    tracing::info!(address = %bind_address, "live API listening");
    axum::serve(
        listener,
        harness_graph_api::router_with_experience(journal, adapter, experience_scope),
    )
    .await
    .map_err(|source| CliError::Server { source })
}

#[derive(Serialize)]
struct DiscoveryOutput {
    unique_sessions: usize,
    sessions: Vec<SessionOutput>,
}

#[derive(Serialize)]
struct SessionOutput {
    session_id: String,
    source_kind: &'static str,
    expected_records: u64,
    source_digest: String,
}

fn discover(config: &AppConfig, scope: SessionScope, limit: usize) -> Result<(), CliError> {
    let catalog = config.archive_root()?.discover(scope)?;
    let sessions = catalog
        .iter()
        .take(limit)
        .map(|bundle| SessionOutput {
            session_id: bundle.session_id().to_string(),
            source_kind: source_kind_name(bundle.source_kind()),
            expected_records: bundle.expected_records().value(),
            source_digest: bundle.source_digest().to_hex(),
        })
        .collect();
    write_json(&DiscoveryOutput {
        unique_sessions: catalog.len(),
        sessions,
    })
}

#[derive(Serialize)]
struct VerificationOutput {
    status: &'static str,
    session_id: String,
    source_digest: String,
    expected_records: u64,
}

fn verify(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root()?.discover(SessionScope::All)?;
    let verified = catalog.require(session_id)?.verify()?;
    write_json(&VerificationOutput {
        status: "verified",
        session_id: verified.session_id().to_string(),
        source_digest: verified.source_digest().to_hex(),
        expected_records: verified.expected_records().value(),
    })
}

#[derive(Serialize)]
struct InspectionOutput {
    status: &'static str,
    session_id: String,
    known_records: u64,
    quarantined_records: u64,
    total_records: u64,
}

fn inspect(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root()?.discover(SessionScope::All)?;
    let verified = catalog.require(session_id)?.verify()?;
    let receipt = inspect_bundle(verified)?;
    write_json(&InspectionOutput {
        status: "inspected",
        session_id: session_id.to_string(),
        known_records: receipt.known_records.value(),
        quarantined_records: receipt.quarantined_records.value(),
        total_records: receipt.total_records.value(),
    })
}

const TRANSCRIPT_DISCLOSURE_POLICY: &[u8] =
    b"harness-graph/conversation-and-execution-disclosure/v1";
const TRANSCRIPT_REDACTION_POLICY_VERSION: &str = "redaction-v1";
const TRANSCRIPT_CHUNKING_POLICY_VERSION: &str = "chunking-v1";
const TRANSCRIPT_CHUNK_BYTES: usize = 24 * 1_024;
const TRANSCRIPT_FRAGMENT_BYTES: usize = 8 * 1_024;
const TRANSCRIPT_ESTIMATED_TOKENS: u64 = 8 * 1_024;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptDryRunSessionStatus {
    Eligible,
    MetadataOnly,
    Blocked,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum TranscriptDryRunBlockReasonOutput {
    None,
    ScannerNonTextControlData,
    ScannerAssetOrBinaryData,
    ScannerSuspiciousEncodedBlob,
    SanitizedByteLimitExceeded,
    FragmentLimitExceeded,
    ChunkLimitExceeded,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct RedactionCountOutput {
    known_secret: u64,
    private_key: u64,
    authentication_material: u64,
    credential_url: u64,
    provider_token: u64,
    high_entropy_assignment: u64,
    email: u64,
    phone: u64,
    ip_address: u64,
    home_path: u64,
}

impl RedactionCountOutput {
    fn from_counts(counts: &RedactionCounts) -> Self {
        Self {
            known_secret: counts.count(RedactionCategory::KnownSecret).value(),
            private_key: counts.count(RedactionCategory::PrivateKey).value(),
            authentication_material: counts
                .count(RedactionCategory::AuthenticationMaterial)
                .value(),
            credential_url: counts.count(RedactionCategory::CredentialUrl).value(),
            provider_token: counts.count(RedactionCategory::ProviderToken).value(),
            high_entropy_assignment: counts
                .count(RedactionCategory::HighEntropyAssignment)
                .value(),
            email: counts.count(RedactionCategory::Email).value(),
            phone: counts.count(RedactionCategory::Phone).value(),
            ip_address: counts.count(RedactionCategory::IpAddress).value(),
            home_path: counts.count(RedactionCategory::HomePath).value(),
        }
    }

    fn merge(&mut self, other: Self) {
        self.known_secret = self.known_secret.saturating_add(other.known_secret);
        self.private_key = self.private_key.saturating_add(other.private_key);
        self.authentication_material = self
            .authentication_material
            .saturating_add(other.authentication_material);
        self.credential_url = self.credential_url.saturating_add(other.credential_url);
        self.provider_token = self.provider_token.saturating_add(other.provider_token);
        self.high_entropy_assignment = self
            .high_entropy_assignment
            .saturating_add(other.high_entropy_assignment);
        self.email = self.email.saturating_add(other.email);
        self.phone = self.phone.saturating_add(other.phone);
        self.ip_address = self.ip_address.saturating_add(other.ip_address);
        self.home_path = self.home_path.saturating_add(other.home_path);
    }
}

#[derive(Serialize)]
struct TranscriptDryRunSessionOutput {
    status: TranscriptDryRunSessionStatus,
    block_reason: TranscriptDryRunBlockReasonOutput,
    session_id: String,
    source_digest: String,
    verified_records: u64,
    projected_fragments: u64,
    excluded_records: u64,
    scope_excluded_fragments: u64,
    sanitized_fragments: u64,
    sanitized_bytes: u64,
    expected_chunks: u64,
    estimated_input_tokens: u64,
    estimated_output_tokens: u64,
    estimated_cost_microusd: u64,
    expected_api_calls: u64,
    redactions: RedactionCountOutput,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct TranscriptDryRunScannerBlockCounts {
    non_text_control_data: u64,
    asset_or_binary_data: u64,
    suspicious_encoded_blob: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct TranscriptDryRunBlockCounts {
    integrity: u64,
    scanner: u64,
    scanner_reasons: TranscriptDryRunScannerBlockCounts,
    preparation: u64,
}

#[derive(Serialize)]
struct TranscriptDryRunAllOutput {
    status: &'static str,
    discovered_sessions: u64,
    eligible_sessions: u64,
    metadata_only_sessions: u64,
    blocked_sessions: u64,
    verified_records: u64,
    projected_fragments: u64,
    sanitized_fragments: u64,
    sanitized_bytes: u64,
    expected_chunks: u64,
    estimated_input_tokens: u64,
    estimated_output_tokens: u64,
    estimated_cost_microusd: u64,
    expected_api_calls: u64,
    blocks: TranscriptDryRunBlockCounts,
    redactions: RedactionCountOutput,
    external_provider_calls: u64,
    neo4j_writes: u64,
}

#[derive(Default)]
struct TranscriptDryRunAggregate {
    discovered_sessions: u64,
    eligible_sessions: u64,
    metadata_only_sessions: u64,
    verified_records: u64,
    projected_fragments: u64,
    sanitized_fragments: u64,
    sanitized_bytes: u64,
    expected_chunks: u64,
    estimated_input_tokens: u64,
    estimated_output_tokens: u64,
    estimated_cost_microusd: u64,
    expected_api_calls: u64,
    blocks: TranscriptDryRunBlockCounts,
    redactions: RedactionCountOutput,
}

impl TranscriptDryRunAggregate {
    fn include(&mut self, session: &TranscriptDryRunSessionOutput) {
        match session.status {
            TranscriptDryRunSessionStatus::Eligible => {
                self.eligible_sessions = self.eligible_sessions.saturating_add(1);
            }
            TranscriptDryRunSessionStatus::MetadataOnly => {
                self.metadata_only_sessions = self.metadata_only_sessions.saturating_add(1);
            }
            TranscriptDryRunSessionStatus::Blocked => match session.block_reason {
                TranscriptDryRunBlockReasonOutput::ScannerNonTextControlData => {
                    self.blocks.scanner = self.blocks.scanner.saturating_add(1);
                    self.blocks.scanner_reasons.non_text_control_data = self
                        .blocks
                        .scanner_reasons
                        .non_text_control_data
                        .saturating_add(1);
                }
                TranscriptDryRunBlockReasonOutput::ScannerAssetOrBinaryData => {
                    self.blocks.scanner = self.blocks.scanner.saturating_add(1);
                    self.blocks.scanner_reasons.asset_or_binary_data = self
                        .blocks
                        .scanner_reasons
                        .asset_or_binary_data
                        .saturating_add(1);
                }
                TranscriptDryRunBlockReasonOutput::ScannerSuspiciousEncodedBlob => {
                    self.blocks.scanner = self.blocks.scanner.saturating_add(1);
                    self.blocks.scanner_reasons.suspicious_encoded_blob = self
                        .blocks
                        .scanner_reasons
                        .suspicious_encoded_blob
                        .saturating_add(1);
                }
                TranscriptDryRunBlockReasonOutput::SanitizedByteLimitExceeded
                | TranscriptDryRunBlockReasonOutput::FragmentLimitExceeded
                | TranscriptDryRunBlockReasonOutput::ChunkLimitExceeded
                | TranscriptDryRunBlockReasonOutput::None => {
                    self.blocks.preparation = self.blocks.preparation.saturating_add(1);
                }
            },
        }
        self.verified_records = self
            .verified_records
            .saturating_add(session.verified_records);
        self.projected_fragments = self
            .projected_fragments
            .saturating_add(session.projected_fragments);
        self.sanitized_fragments = self
            .sanitized_fragments
            .saturating_add(session.sanitized_fragments);
        self.sanitized_bytes = self.sanitized_bytes.saturating_add(session.sanitized_bytes);
        self.expected_chunks = self.expected_chunks.saturating_add(session.expected_chunks);
        self.estimated_input_tokens = self
            .estimated_input_tokens
            .saturating_add(session.estimated_input_tokens);
        self.estimated_output_tokens = self
            .estimated_output_tokens
            .saturating_add(session.estimated_output_tokens);
        self.estimated_cost_microusd = self
            .estimated_cost_microusd
            .saturating_add(session.estimated_cost_microusd);
        self.expected_api_calls = self
            .expected_api_calls
            .saturating_add(session.expected_api_calls);
        self.redactions.merge(session.redactions);
    }

    const fn blocked_sessions(&self) -> u64 {
        self.blocks
            .integrity
            .saturating_add(self.blocks.scanner)
            .saturating_add(self.blocks.preparation)
    }
}

fn enrich_transcripts_dry_run(
    config: &AppConfig,
    session_id: &str,
    authorization: String,
    disclosure_scope: TranscriptDisclosureScope,
) -> Result<(), CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root()?.discover(SessionScope::All)?;
    let verified =
        catalog
            .require(session_id)?
            .verify()
            .map_err(|_| CliError::TranscriptDryRunBlocked {
                stage: "archive_integrity",
            })?;
    let redactor = dry_run_redactor(config)?;
    let policy = transcript_chunk_policy()?;
    let estimator = transcript_inventory_estimator(config)?;
    let authorization_identity = AuthorizationIdentity::new(authorization)?;
    let output = transcript_dry_run_session(
        verified,
        &authorization_identity,
        disclosure_scope,
        &redactor,
        &policy,
        &estimator,
    )
    .map_err(|error| CliError::TranscriptDryRunBlocked {
        stage: transcript_dry_run_stage(&error),
    })?;
    write_json(&output)
}

fn enrich_all_transcripts_dry_run(
    config: &AppConfig,
    scope: SessionScope,
    authorization: String,
    disclosure_scope: TranscriptDisclosureScope,
) -> Result<(), CliError> {
    let catalog = config.archive_root()?.discover(scope)?;
    let redactor = dry_run_redactor(config)?;
    let policy = transcript_chunk_policy()?;
    let estimator = transcript_inventory_estimator(config)?;
    let authorization_identity = AuthorizationIdentity::new(authorization)?;
    let mut aggregate = TranscriptDryRunAggregate {
        discovered_sessions: u64::try_from(catalog.len()).unwrap_or(u64::MAX),
        ..TranscriptDryRunAggregate::default()
    };
    for bundle in catalog.iter() {
        let Ok(verified) = bundle.verify() else {
            aggregate.blocks.integrity = aggregate.blocks.integrity.saturating_add(1);
            continue;
        };
        match transcript_dry_run_session(
            verified,
            &authorization_identity,
            disclosure_scope,
            &redactor,
            &policy,
            &estimator,
        ) {
            Ok(session) => aggregate.include(&session),
            Err(_) => {
                aggregate.blocks.preparation = aggregate.blocks.preparation.saturating_add(1);
            }
        }
    }
    write_json(&TranscriptDryRunAllOutput {
        status: "dry_run_complete",
        discovered_sessions: aggregate.discovered_sessions,
        eligible_sessions: aggregate.eligible_sessions,
        metadata_only_sessions: aggregate.metadata_only_sessions,
        blocked_sessions: aggregate.blocked_sessions(),
        verified_records: aggregate.verified_records,
        projected_fragments: aggregate.projected_fragments,
        sanitized_fragments: aggregate.sanitized_fragments,
        sanitized_bytes: aggregate.sanitized_bytes,
        expected_chunks: aggregate.expected_chunks,
        estimated_input_tokens: aggregate.estimated_input_tokens,
        estimated_output_tokens: aggregate.estimated_output_tokens,
        estimated_cost_microusd: aggregate.estimated_cost_microusd,
        expected_api_calls: aggregate.expected_api_calls,
        blocks: aggregate.blocks,
        redactions: aggregate.redactions,
        external_provider_calls: 0,
        neo4j_writes: 0,
    })
}

fn transcript_dry_run_session(
    verified: VerifiedSessionBundle,
    authorization_identity: &AuthorizationIdentity,
    disclosure_scope: TranscriptDisclosureScope,
    redactor: &LocalTranscriptRedactor,
    policy: &TranscriptChunkPolicy,
    estimator: &TranscriptInventoryEstimator,
) -> Result<TranscriptDryRunSessionOutput, TranscriptEnrichmentError> {
    let session_id = verified.session_id();
    let source_digest = verified.source_digest();
    let authorization = DisclosureAuthorization::new(
        session_id,
        source_digest,
        disclosure_scope,
        AuthorizationPolicyDigest::hash(TRANSCRIPT_DISCLOSURE_POLICY),
        authorization_identity.clone(),
        harness_graph_domain::OccurredAt::now_utc(),
    );
    let preparation = prepare_verified_transcript(
        verified,
        &authorization,
        redactor,
        policy,
        harness_graph_ingestion::MaxSourceRecordBytes::default(),
        TranscriptPreparationLimits::default(),
    )?;
    let estimate = estimator.estimate(&preparation);
    let (status, block_reason, inventory, redactions) = match &preparation {
        TranscriptPreparation::Prepared(prepared) => (
            TranscriptDryRunSessionStatus::Eligible,
            TranscriptDryRunBlockReasonOutput::None,
            prepared.inventory(),
            RedactionCountOutput::from_counts(prepared.inventory().redaction_counts()),
        ),
        TranscriptPreparation::MetadataOnly(inventory) => (
            TranscriptDryRunSessionStatus::MetadataOnly,
            TranscriptDryRunBlockReasonOutput::None,
            inventory,
            RedactionCountOutput::from_counts(inventory.redaction_counts()),
        ),
        TranscriptPreparation::Blocked(blocked) => (
            TranscriptDryRunSessionStatus::Blocked,
            match blocked.reason() {
                TranscriptPreparationBlockReason::ScannerRejected { reason, .. } => match reason {
                    ScannerBlockReason::NonTextControlData => {
                        TranscriptDryRunBlockReasonOutput::ScannerNonTextControlData
                    }
                    ScannerBlockReason::AssetOrBinaryData => {
                        TranscriptDryRunBlockReasonOutput::ScannerAssetOrBinaryData
                    }
                    ScannerBlockReason::SuspiciousEncodedBlob => {
                        TranscriptDryRunBlockReasonOutput::ScannerSuspiciousEncodedBlob
                    }
                },
                TranscriptPreparationBlockReason::SanitizedByteLimitExceeded => {
                    TranscriptDryRunBlockReasonOutput::SanitizedByteLimitExceeded
                }
                TranscriptPreparationBlockReason::FragmentLimitExceeded => {
                    TranscriptDryRunBlockReasonOutput::FragmentLimitExceeded
                }
                TranscriptPreparationBlockReason::ChunkLimitExceeded => {
                    TranscriptDryRunBlockReasonOutput::ChunkLimitExceeded
                }
            },
            blocked.inventory(),
            RedactionCountOutput::from_counts(blocked.inventory().redaction_counts()),
        ),
    };
    Ok(TranscriptDryRunSessionOutput {
        status,
        block_reason,
        session_id: session_id.to_string(),
        source_digest: source_digest.to_hex(),
        verified_records: inventory.total_records().value(),
        projected_fragments: inventory.projected_fragments().value(),
        excluded_records: inventory.excluded_records().value(),
        scope_excluded_fragments: inventory.scope_excluded_fragments().value(),
        sanitized_fragments: inventory.sanitized_fragments().value(),
        sanitized_bytes: inventory.sanitized_bytes().value(),
        expected_chunks: estimate.chunk_count().value(),
        estimated_input_tokens: estimate.estimated_input_tokens().value(),
        estimated_output_tokens: estimate.estimated_output_tokens().value(),
        estimated_cost_microusd: estimate.estimated_cost().value(),
        expected_api_calls: estimate.request_count().value(),
        redactions,
    })
}

fn dry_run_redactor(config: &AppConfig) -> Result<LocalTranscriptRedactor, CliError> {
    let ephemeral_key = format!(
        "dry-run-{}{}",
        uuid::Uuid::now_v7().simple(),
        uuid::Uuid::now_v7().simple()
    );
    Ok(LocalTranscriptRedactor::new(
        RedactionPolicyVersion::new(TRANSCRIPT_REDACTION_POLICY_VERSION)?,
        PseudonymizationKey::new(ephemeral_key)?,
        config.sensitive_values_for_redaction()?,
    )?)
}

fn transcript_chunk_policy() -> Result<TranscriptChunkPolicy, CliError> {
    Ok(TranscriptChunkPolicy::new(
        ChunkByteLimit::new(TRANSCRIPT_CHUNK_BYTES)?,
        EstimatedTokenLimit::new(TRANSCRIPT_ESTIMATED_TOKENS)?,
        FragmentByteLimit::new(TRANSCRIPT_FRAGMENT_BYTES)?,
        ChunkingPolicyVersion::new(TRANSCRIPT_CHUNKING_POLICY_VERSION)?,
    )?)
}

fn transcript_inventory_estimator(
    config: &AppConfig,
) -> Result<TranscriptInventoryEstimator, CliError> {
    Ok(TranscriptInventoryEstimator::new(
        config.transcript_token_pricing()?,
        config.transcript_estimated_output_tokens_per_request()?,
    ))
}

const fn transcript_dry_run_stage(error: &TranscriptEnrichmentError) -> &'static str {
    match error {
        TranscriptEnrichmentError::ScannerBlocked { .. } => "local_scanner",
        TranscriptEnrichmentError::Ingestion(_) => "canonical_transcript_projection",
        _ => "transcript_preparation",
    }
}

#[derive(Serialize)]
struct ImportOutput {
    status: &'static str,
    session_id: String,
    source_digest: String,
    known_records: u64,
    quarantined_records: u64,
    total_records: u64,
    observations_in_namespace: u64,
    analysis: ImportAnalysisOutput,
}

#[derive(Serialize)]
struct AlreadyCompleteImportOutput {
    status: &'static str,
    session_id: String,
    source_digest: String,
    expected_records: u64,
    observations_in_namespace: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(transparent)]
struct SessionCount(u64);

impl SessionCount {
    fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BulkImportStatus {
    Completed,
    CompletedWithFailures,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ImportFailureClass {
    ArchiveIntegrity,
    DomainValidation,
    Correlation,
    Classification,
    Assurance,
    RiskAnalysis,
    PathAnalysis,
    GraphProjection,
    WorkerJoin,
    RuntimeConfiguration,
    UnexpectedSubsystem,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum BulkSessionOutput {
    Imported {
        session_id: String,
        source_digest: String,
        known_records: u64,
        quarantined_records: u64,
        total_records: u64,
        analysis: ImportAnalysisOutput,
    },
    AlreadyComplete {
        session_id: String,
        source_digest: String,
        expected_records: u64,
    },
    Failed {
        session_id: String,
        source_digest: String,
        failure_class: ImportFailureClass,
    },
}

impl BulkSessionOutput {
    fn session_id(&self) -> &str {
        match self {
            Self::Imported { session_id, .. }
            | Self::AlreadyComplete { session_id, .. }
            | Self::Failed { session_id, .. } => session_id,
        }
    }
}

#[derive(Serialize)]
struct BulkImportOutput {
    status: BulkImportStatus,
    scope: ScopeArgument,
    execution_mode: &'static str,
    synchronization: &'static str,
    max_concurrency: ImportConcurrencyLimit,
    discovered_sessions: SessionCount,
    imported_sessions: SessionCount,
    already_complete_sessions: SessionCount,
    failed_sessions: SessionCount,
    observations_in_namespace: u64,
    sessions: Vec<BulkSessionOutput>,
}

#[derive(Serialize)]
struct ImportProgressOutput<'a> {
    event: &'static str,
    #[serde(flatten)]
    session: &'a BulkSessionOutput,
}

#[derive(Serialize)]
struct AnalysisOutput {
    tool_calls: u64,
    completed_tool_calls: u64,
    pending_tool_calls: u64,
    interrupted_tool_calls: u64,
    orphaned_tool_results: u64,
    semantic_activities: u64,
    outcome_class: &'static str,
    verification_status: &'static str,
    risk_exposures: u64,
    path_signature: String,
    path_steps: u64,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ImportAnalysisOutput {
    Projected {
        #[serde(flatten)]
        analysis: AnalysisOutput,
    },
    InsufficientSemanticEvidence {
        semantic_activities: RecordCount,
    },
}

#[derive(Serialize)]
struct AnalyzeOutput {
    status: &'static str,
    session_id: String,
    source_digest: String,
    analysis: AnalysisOutput,
}

struct SessionAnalysis {
    session_id: SessionId,
    source_digest: harness_graph_domain::SourceDigest,
    report: AnalysisReport,
}

#[derive(Debug, Default)]
struct AnalysisAccumulator {
    correlations: CorrelationEngine,
    activities: ActivityBuilder,
    risks: RiskEngine,
}

struct AnalysisComponents {
    correlations: ToolCallCorrelations,
    activities: SemanticActivities,
    risks: RiskEngine,
}

enum ImportAnalysis {
    Projected(AnalysisReport),
    InsufficientSemanticEvidence,
}

impl AnalysisAccumulator {
    fn observe(&mut self, record: &DecodedNativeRecord) -> Result<(), CliError> {
        self.risks.observe(record);
        if let DecodedNativeRecord::Known(known) = record {
            self.correlations.observe(known.observation())?;
            self.activities.observe(known.observation())?;
        }
        Ok(())
    }

    fn into_components(self) -> Result<AnalysisComponents, CliError> {
        let correlations = self.correlations.finish()?;
        let activities = self.activities.finish(&correlations)?;
        Ok(AnalysisComponents {
            correlations,
            activities,
            risks: self.risks,
        })
    }

    fn finish(self) -> Result<AnalysisReport, CliError> {
        finalize_analysis(self.into_components()?)
    }

    fn finish_for_import(self) -> Result<ImportAnalysis, CliError> {
        let components = self.into_components()?;
        if components.activities.count().value() == 0 {
            Ok(ImportAnalysis::InsufficientSemanticEvidence)
        } else {
            Ok(ImportAnalysis::Projected(finalize_analysis(components)?))
        }
    }
}

fn finalize_analysis(components: AnalysisComponents) -> Result<AnalysisReport, CliError> {
    let outcome = assess_outcome(&components.activities)?;
    let risks =
        components
            .risks
            .finish(&components.activities, &components.correlations, &outcome)?;
    let path = derive_path(&components.activities)?;
    Ok(AnalysisReport::new(
        components.correlations,
        components.activities,
        outcome,
        risks,
        path,
    ))
}

fn analyze(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let analyzed = analyze_session(config, session_id)?;
    write_json(&AnalyzeOutput {
        status: "analyzed",
        session_id: analyzed.session_id.to_string(),
        source_digest: analyzed.source_digest.to_hex(),
        analysis: summarize_analysis(&analyzed.report),
    })
}

fn analyze_session(config: &AppConfig, session_id: &str) -> Result<SessionAnalysis, CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root()?.discover(SessionScope::All)?;
    let verified = catalog.require(session_id)?.verify()?;
    let source_digest = verified.source_digest();
    let expected_records = verified.expected_records();
    let mut total_records = RecordCount::default();
    let mut accumulator = AnalysisAccumulator::default();
    for record in DecodedRecordStream::open(verified)? {
        let record = record?;
        accumulator.observe(&record)?;
        total_records.increment();
    }
    validate_record_count(expected_records, total_records)?;
    let report = accumulator.finish()?;
    Ok(SessionAnalysis {
        session_id,
        source_digest,
        report,
    })
}

#[derive(Serialize)]
struct MistralHealthOutput {
    status: &'static str,
    provider: &'static str,
    model: String,
}

async fn mistral_health(config: &AppConfig) -> Result<(), CliError> {
    let adapter = mistral_adapter(config)?;
    adapter.health().await?;
    write_json(&MistralHealthOutput {
        status: "ready",
        provider: "mistral",
        model: adapter.model().as_str().to_owned(),
    })
}

#[derive(Serialize)]
struct ModelUsageOutput {
    #[serde(rename = "input_tokens")]
    input: u64,
    #[serde(rename = "output_tokens")]
    output: u64,
    #[serde(rename = "total_tokens")]
    total: u64,
}

impl From<ModelUsage> for ModelUsageOutput {
    fn from(value: ModelUsage) -> Self {
        Self {
            input: value.input().value(),
            output: value.output().value(),
            total: value.total().value(),
        }
    }
}

#[derive(Serialize)]
struct NarrativeActivityOutput {
    title: String,
    kind: &'static str,
    origin: &'static str,
    cited_activity_ids: Vec<String>,
}

#[derive(Serialize)]
struct SummarizeOutput {
    status: &'static str,
    provider: &'static str,
    model: String,
    session_id: String,
    #[serde(flatten)]
    narrative: NarrativePayload,
    usage: ModelUsageOutput,
}

#[derive(Serialize)]
struct NarrativePayload {
    deterministic_activities: u64,
    narrative_activity_count: u64,
    mistral_labeled: u64,
    deterministic_fallbacks: u64,
    narrative_activities: Vec<NarrativeActivityOutput>,
}

async fn summarize(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let analyzed = analyze_session(config, session_id)?;
    let deterministic_activities = analyzed.report.activities().count().value();
    let adapter = mistral_adapter(config)?;
    let result = adapter
        .summarize(NarrativeRequest::new(analyzed.report.activities().clone())?)
        .await?;
    let narrative = narrative_payload(result.value(), deterministic_activities);
    write_json(&SummarizeOutput {
        status: "summarized",
        provider: "mistral",
        model: adapter.model().as_str().to_owned(),
        session_id: analyzed.session_id.to_string(),
        narrative,
        usage: result.usage().into(),
    })
}

fn narrative_payload(
    summary: &NarrativeSummary,
    deterministic_activities: u64,
) -> NarrativePayload {
    let narrative_activity_count = summary.count().value();
    let mut mistral_labeled = RecordCount::default();
    let mut deterministic_fallbacks = RecordCount::default();
    for activity in summary.iter() {
        match activity.origin() {
            NarrativeOrigin::Mistral => mistral_labeled.increment(),
            NarrativeOrigin::DeterministicFallback => deterministic_fallbacks.increment(),
        }
    }
    let narrative_activities = summary
        .iter()
        .map(|activity| NarrativeActivityOutput {
            title: activity.title().as_str().to_owned(),
            kind: activity.kind().as_str(),
            origin: activity.origin().as_str(),
            cited_activity_ids: activity
                .citations()
                .iter()
                .map(|citation| citation.to_hex())
                .collect(),
        })
        .collect();
    NarrativePayload {
        deterministic_activities,
        narrative_activity_count,
        mistral_labeled: mistral_labeled.value(),
        deterministic_fallbacks: deterministic_fallbacks.value(),
        narrative_activities,
    }
}

#[derive(Serialize)]
struct InterpretationOutput {
    status: &'static str,
    provider: &'static str,
    model: String,
    execution_mode: &'static str,
    synchronization: &'static str,
    max_concurrency: usize,
    session_id: String,
    classification: ClassificationOutput,
    extraction: ExtractionOutput,
}

#[derive(Serialize)]
struct ClassificationOutput {
    category: &'static str,
    confidence: &'static str,
    explanation: String,
    usage: ModelUsageOutput,
}

#[derive(Serialize)]
struct ExtractionOutput {
    #[serde(flatten)]
    narrative: NarrativePayload,
    usage: ModelUsageOutput,
}

async fn interpret(config: &AppConfig, session_id: &str, task: String) -> Result<(), CliError> {
    let analyzed = analyze_session(config, session_id)?;
    let deterministic_activities = analyzed.report.activities().count().value();
    let classification = TaskClassificationRequest::new(TaskBrief::new(task)?);
    let extraction = NarrativeRequest::new(analyzed.report.activities().clone())?;
    let adapter = mistral_adapter(config)?;
    let interpretation = adapter
        .classify_and_extract(classification, extraction)
        .await?;
    let classified = interpretation.classification();
    let extracted = interpretation.extraction();
    write_json(&InterpretationOutput {
        status: "interpreted",
        provider: "mistral",
        model: adapter.model().as_str().to_owned(),
        execution_mode: "concurrent",
        synchronization: "all_results_settle",
        max_concurrency: adapter.concurrency().value(),
        session_id: analyzed.session_id.to_string(),
        classification: ClassificationOutput {
            category: classified.value().category().as_str(),
            confidence: classified.value().confidence().as_str(),
            explanation: classified.value().explanation().as_str().to_owned(),
            usage: classified.usage().into(),
        },
        extraction: ExtractionOutput {
            narrative: narrative_payload(extracted.value(), deterministic_activities),
            usage: extracted.usage().into(),
        },
    })
}

#[derive(Serialize)]
struct PlannedStepOutput {
    kind: &'static str,
    rationale: String,
    cited_activity_ids: Vec<String>,
}

#[derive(Serialize)]
struct PathfinderOutput {
    status: &'static str,
    provider: &'static str,
    model: String,
    retrieved_precedents: u64,
    cited_session_ids: Vec<String>,
    steps: Vec<PlannedStepOutput>,
    usage: ModelUsageOutput,
}

async fn pathfinder(config: &AppConfig, task: String, precedents: usize) -> Result<(), CliError> {
    let context = PlanningContext::new(TaskBrief::new(task)?);
    let adapter = connect_neo4j(config).await?;
    adapter.health().await?;
    let namespace = config.graph_namespace()?;
    let precedents = adapter
        .verified_precedents(&namespace, PrecedentLimit::new(precedents)?)
        .await?;
    let retrieved_precedents = precedents.count().value();
    let mistral = mistral_adapter(config)?;
    let result = mistral.propose(context, precedents).await?;
    let cited_session_ids = result
        .value()
        .precedents()
        .iter()
        .map(ToString::to_string)
        .collect();
    let steps = result
        .value()
        .steps()
        .iter()
        .map(|step| PlannedStepOutput {
            kind: step.kind().as_str(),
            rationale: step.rationale().as_str().to_owned(),
            cited_activity_ids: step
                .citations()
                .iter()
                .map(|citation| citation.to_hex())
                .collect(),
        })
        .collect();
    write_json(&PathfinderOutput {
        status: "planned",
        provider: "mistral",
        model: mistral.model().as_str().to_owned(),
        retrieved_precedents,
        cited_session_ids,
        steps,
        usage: result.usage().into(),
    })
}

fn mistral_adapter(config: &AppConfig) -> Result<RigMistralAdapter, CliError> {
    let credential = config.mistral_credential()?;
    Ok(RigMistralAdapter::with_concurrency(
        &credential,
        config.mistral_model()?,
        config.mistral_concurrency()?,
    )?)
}

enum PendingBatch {
    Empty,
    Building(GraphBatch),
}

enum SessionImportResult {
    Imported(SessionImportReceipt),
    AlreadyComplete {
        session_id: SessionId,
        source_digest: harness_graph_domain::SourceDigest,
        expected_records: RecordCount,
    },
}

struct SessionImportReceipt {
    session_id: SessionId,
    source_digest: harness_graph_domain::SourceDigest,
    known_records: RecordCount,
    quarantined_records: RecordCount,
    total_records: RecordCount,
    analysis: ImportAnalysisOutput,
}

async fn import(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root()?.discover(SessionScope::All)?;
    let verified = catalog.require(session_id)?.verify()?;
    let adapter = connect_neo4j(config).await?;
    adapter.health().await?;
    adapter.ensure_schema().await?;
    match import_verified_session(config, &adapter, verified).await? {
        SessionImportResult::Imported(receipt) => {
            let namespace = config.graph_namespace()?;
            let observations_in_namespace = adapter.observation_count(&namespace).await?;
            write_json(&ImportOutput {
                status: "imported",
                session_id: receipt.session_id.to_string(),
                source_digest: receipt.source_digest.to_hex(),
                known_records: receipt.known_records.value(),
                quarantined_records: receipt.quarantined_records.value(),
                total_records: receipt.total_records.value(),
                observations_in_namespace: observations_in_namespace.value(),
                analysis: receipt.analysis,
            })
        }
        SessionImportResult::AlreadyComplete {
            session_id,
            source_digest,
            expected_records,
        } => {
            let namespace = config.graph_namespace()?;
            let observations_in_namespace = adapter.observation_count(&namespace).await?;
            write_json(&AlreadyCompleteImportOutput {
                status: "already_complete",
                session_id: session_id.to_string(),
                source_digest: source_digest.to_hex(),
                expected_records: expected_records.value(),
                observations_in_namespace: observations_in_namespace.value(),
            })
        }
    }
}

async fn import_all(
    config: &AppConfig,
    scope: ScopeArgument,
    concurrency: ImportConcurrencyLimit,
) -> Result<(), CliError> {
    let catalog = config.archive_root()?.discover(scope.into())?;
    let adapter = connect_neo4j(config).await?;
    adapter.health().await?;
    adapter.ensure_schema().await?;

    let mut settlements = stream::iter(catalog.iter().cloned().map(|bundle| {
        let adapter = adapter.clone();
        async move { settle_session_import(config, &adapter, bundle).await }
    }))
    .buffer_unordered(concurrency.value());

    let mut discovered_sessions = SessionCount::default();
    let mut imported_sessions = SessionCount::default();
    let mut already_complete_sessions = SessionCount::default();
    let mut failed_sessions = SessionCount::default();
    let mut progress_error = None;
    let mut sessions = Vec::with_capacity(catalog.len());
    while let Some(settlement) = settlements.next().await {
        discovered_sessions.increment();
        let output = match settlement.result {
            Ok(SessionImportResult::Imported(receipt)) => {
                imported_sessions.increment();
                BulkSessionOutput::Imported {
                    session_id: receipt.session_id.to_string(),
                    source_digest: receipt.source_digest.to_hex(),
                    known_records: receipt.known_records.value(),
                    quarantined_records: receipt.quarantined_records.value(),
                    total_records: receipt.total_records.value(),
                    analysis: receipt.analysis,
                }
            }
            Ok(SessionImportResult::AlreadyComplete {
                session_id,
                source_digest,
                expected_records,
            }) => {
                already_complete_sessions.increment();
                BulkSessionOutput::AlreadyComplete {
                    session_id: session_id.to_string(),
                    source_digest: source_digest.to_hex(),
                    expected_records: expected_records.value(),
                }
            }
            Err(error) => {
                failed_sessions.increment();
                BulkSessionOutput::Failed {
                    session_id: settlement.session_id.to_string(),
                    source_digest: settlement.source_digest.to_hex(),
                    failure_class: import_failure_class(&error),
                }
            }
        };
        let progress_result = write_progress_json(&ImportProgressOutput {
            event: "session_import_settled",
            session: &output,
        });
        if progress_error.is_none() {
            progress_error = progress_result.err();
        }
        sessions.push(output);
    }
    sessions.sort_by(|left, right| left.session_id().cmp(right.session_id()));

    let namespace = config.graph_namespace()?;
    let observations_in_namespace = adapter.observation_count(&namespace).await?;
    let has_failures = !failed_sessions.is_zero();
    let status = if has_failures {
        BulkImportStatus::CompletedWithFailures
    } else {
        BulkImportStatus::Completed
    };
    write_json(&BulkImportOutput {
        status,
        scope,
        execution_mode: "concurrent",
        synchronization: "all_results_settle",
        max_concurrency: concurrency,
        discovered_sessions,
        imported_sessions,
        already_complete_sessions,
        failed_sessions,
        observations_in_namespace: observations_in_namespace.value(),
        sessions,
    })?;
    if has_failures {
        Err(CliError::BulkImportIncomplete)
    } else {
        match progress_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

struct SessionImportSettlement {
    session_id: SessionId,
    source_digest: harness_graph_domain::SourceDigest,
    result: Result<SessionImportResult, CliError>,
}

async fn settle_session_import(
    config: &AppConfig,
    adapter: &Neo4jAdapter,
    bundle: SessionBundle,
) -> SessionImportSettlement {
    let session_id = bundle.session_id();
    let source_digest = bundle.source_digest();
    let result = match tokio::task::spawn_blocking(move || bundle.verify()).await {
        Ok(Ok(verified)) => import_verified_session(config, adapter, verified).await,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(CliError::ImportWorkerJoin),
    };
    SessionImportSettlement {
        session_id,
        source_digest,
        result,
    }
}

async fn import_verified_session(
    config: &AppConfig,
    adapter: &Neo4jAdapter,
    verified: VerifiedSessionBundle,
) -> Result<SessionImportResult, CliError> {
    let session_id = verified.session_id();
    let source_digest = verified.source_digest();
    let expected_records = verified.expected_records();
    let namespace = config.graph_namespace()?;
    let graph_batch_size = config.graph_batch_size()?;

    // Source receipts are content-addressed, while session provenance is not.
    // Always materialize the idempotent session-to-source edge before using a
    // completed receipt to skip record replay: distinct sessions may contain
    // the same exact source bytes.
    adapter
        .project(GraphBatch::first(GraphCommand::UpsertSourceSnapshot(
            SourceSnapshotCommand::new(
                namespace.clone(),
                session_id,
                source_digest,
                expected_records,
            ),
        )))
        .await?;

    if adapter
        .source_ingestion_status(&namespace, source_digest, expected_records)
        .await?
        == SourceIngestionStatus::Complete
    {
        return Ok(SessionImportResult::AlreadyComplete {
            session_id,
            source_digest,
            expected_records,
        });
    }

    let mut known_records = RecordCount::default();
    let mut quarantined_records = RecordCount::default();
    let mut total_records = RecordCount::default();
    let mut analysis = AnalysisAccumulator::default();
    let mut pending = PendingBatch::Empty;
    for record in DecodedRecordStream::open(verified)? {
        let record = record?;
        analysis.observe(&record)?;
        match &record {
            DecodedNativeRecord::Known(_) => known_records.increment(),
            DecodedNativeRecord::Unsupported(_) => quarantined_records.increment(),
        }
        total_records.increment();
        let command = GraphCommand::UpsertObservation {
            namespace: namespace.clone(),
            record,
        };
        pending = append_or_project(
            adapter,
            std::mem::replace(&mut pending, PendingBatch::Empty),
            command,
            graph_batch_size,
        )
        .await?;
    }
    if let PendingBatch::Building(batch) = pending {
        adapter.project(batch).await?;
    }
    validate_record_count(expected_records, total_records)?;

    let analysis_output = match analysis.finish_for_import()? {
        ImportAnalysis::Projected(report) => {
            let output = summarize_analysis(&report);
            adapter
                .project(GraphBatch::first(GraphCommand::UpsertAnalysis(
                    AnalysisProjectionCommand::new(
                        namespace.clone(),
                        session_id,
                        source_digest,
                        report,
                    ),
                )))
                .await?;
            ImportAnalysisOutput::Projected { analysis: output }
        }
        ImportAnalysis::InsufficientSemanticEvidence => {
            ImportAnalysisOutput::InsufficientSemanticEvidence {
                semantic_activities: RecordCount::default(),
            }
        }
    };

    adapter
        .project(GraphBatch::first(GraphCommand::FinalizeIngestion(
            FinalizeIngestionCommand::new(
                namespace.clone(),
                session_id,
                source_digest,
                known_records,
                quarantined_records,
                total_records,
            ),
        )))
        .await?;
    Ok(SessionImportResult::Imported(SessionImportReceipt {
        session_id,
        source_digest,
        known_records,
        quarantined_records,
        total_records,
        analysis: analysis_output,
    }))
}

async fn connect_neo4j(config: &AppConfig) -> Result<Neo4jAdapter, CliError> {
    let neo4j = config.neo4j()?;
    Ok(Neo4jAdapter::connect(
        &neo4j.bolt_address()?,
        neo4j.username(),
        SecretString::from(neo4j.expose_password().to_owned()),
    )
    .await?)
}

fn validate_record_count(expected: RecordCount, actual: RecordCount) -> Result<(), CliError> {
    if actual == expected {
        Ok(())
    } else {
        Err(IngestionError::RecordCountMismatch { expected, actual }.into())
    }
}

fn summarize_analysis(report: &AnalysisReport) -> AnalysisOutput {
    let mut completed = RecordCount::default();
    let mut pending = RecordCount::default();
    let mut interrupted = RecordCount::default();
    let mut orphaned = RecordCount::default();
    for correlation in report.correlations().iter() {
        match correlation.lifecycle() {
            ToolCallLifecycle::Completed { .. } => completed.increment(),
            ToolCallLifecycle::Pending { .. } => pending.increment(),
            ToolCallLifecycle::Interrupted { .. } => interrupted.increment(),
            ToolCallLifecycle::OrphanedResult { .. } => orphaned.increment(),
        }
    }
    AnalysisOutput {
        tool_calls: report.correlations().count().value(),
        completed_tool_calls: completed.value(),
        pending_tool_calls: pending.value(),
        interrupted_tool_calls: interrupted.value(),
        orphaned_tool_results: orphaned.value(),
        semantic_activities: report.activities().count().value(),
        outcome_class: report.outcome().class().as_str(),
        verification_status: report.outcome().verification().as_str(),
        risk_exposures: report.risks().count().value(),
        path_signature: report.path().signature().to_hex(),
        path_steps: report.path().steps().count().value(),
    }
}

async fn append_or_project(
    adapter: &Neo4jAdapter,
    pending: PendingBatch,
    command: GraphCommand,
    batch_size: harness_graph_graph_port::BatchSize,
) -> Result<PendingBatch, CliError> {
    let batch = match pending {
        PendingBatch::Empty => GraphBatch::first(command),
        PendingBatch::Building(mut batch) => {
            batch.push(command);
            batch
        }
    };
    if batch.is_full(batch_size) {
        adapter.project(batch).await?;
        Ok(PendingBatch::Empty)
    } else {
        Ok(PendingBatch::Building(batch))
    }
}

fn source_kind_name(source_kind: SourceKind) -> &'static str {
    match source_kind {
        SourceKind::Active => "active",
        SourceKind::Archived => "archived",
    }
}

const fn import_failure_class(error: &CliError) -> ImportFailureClass {
    match error {
        CliError::ConfigurationFile
        | CliError::MissingConfiguration { .. }
        | CliError::InvalidConfiguration { .. } => ImportFailureClass::RuntimeConfiguration,
        CliError::Domain(_) => ImportFailureClass::DomainValidation,
        CliError::Ingestion(_) => ImportFailureClass::ArchiveIntegrity,
        CliError::Correlation(_) => ImportFailureClass::Correlation,
        CliError::Classification(_) => ImportFailureClass::Classification,
        CliError::Assurance(_) => ImportFailureClass::Assurance,
        CliError::Risk(_) => ImportFailureClass::RiskAnalysis,
        CliError::PathAnalysis(_) => ImportFailureClass::PathAnalysis,
        CliError::GraphPort(_) | CliError::Neo4j(_) => ImportFailureClass::GraphProjection,
        CliError::ImportWorkerJoin => ImportFailureClass::WorkerJoin,
        CliError::BulkImportIncomplete
        | CliError::Mistral(_)
        | CliError::TranscriptEnrichment(_)
        | CliError::TranscriptExecutionModeRequired
        | CliError::TranscriptDryRunBlocked { .. }
        | CliError::TranscriptApplyPrecondition { .. }
        | CliError::TranscriptPromptProvenance(_)
        | CliError::EnrichmentApplication(_)
        | CliError::TranscriptApplyWorkerJoin
        | CliError::TranscriptApplyIncomplete
        | CliError::BulkTranscriptApplyIncomplete
        | CliError::Planning(_)
        | CliError::Journal(_)
        | CliError::Server { .. }
        | CliError::OutputEncoding { .. }
        | CliError::OutputWrite { .. }
        | CliError::Logging { .. } => ImportFailureClass::UnexpectedSubsystem,
    }
}

fn write_progress_json(value: &impl Serialize) -> Result<(), CliError> {
    use std::io::Write as _;

    let mut stderr = std::io::stderr().lock();
    serde_json::to_writer(&mut stderr, value)
        .map_err(|source| CliError::OutputEncoding { source })?;
    writeln!(stderr).map_err(|source| CliError::OutputWrite { source })
}

fn write_json(value: &impl Serialize) -> Result<(), CliError> {
    let output = serde_json::to_string_pretty(value)
        .map_err(|source| CliError::OutputEncoding { source })?;
    println!("{output}");
    Ok(())
}

fn initialize_logging() -> Result<(), CliError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .try_init()
        .map_err(|error| CliError::Logging {
            message: error.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use clap::{Parser, error::ErrorKind};

    use super::{Cli, Command, TranscriptExecutionMode};

    #[test]
    fn transcript_commands_require_exactly_one_execution_mode() {
        let missing = Cli::try_parse_from([
            "harness-graph",
            "enrich-transcripts",
            "--session-id",
            "019c63db-2995-74c3-b898-c1b92a8e1317",
            "--authorization",
            "operator-test",
        ]);
        assert_eq!(
            missing.err().map(|error| error.kind()),
            Some(ErrorKind::MissingRequiredArgument)
        );

        let conflicting = Cli::try_parse_from([
            "harness-graph",
            "enrich-transcripts",
            "--session-id",
            "019c63db-2995-74c3-b898-c1b92a8e1317",
            "--authorization",
            "operator-test",
            "--dry-run",
            "--apply",
        ]);
        assert_eq!(
            conflicting.err().map(|error| error.kind()),
            Some(ErrorKind::ArgumentConflict)
        );
    }

    #[test]
    fn apply_flag_is_immediately_resolved_to_semantic_mode()
    -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::try_parse_from([
            "harness-graph",
            "enrich-transcripts",
            "--session-id",
            "019c63db-2995-74c3-b898-c1b92a8e1317",
            "--authorization",
            "operator-test",
            "--apply",
        ])?;
        let Command::EnrichTranscripts { execution, .. } = cli.command else {
            return Err("unexpected parsed command".into());
        };
        assert_eq!(execution.resolve()?, TranscriptExecutionMode::Apply);
        Ok(())
    }

    #[test]
    fn eligible_pilot_limit_is_bounded_at_the_cli_boundary() {
        for invalid in ["0", "51"] {
            let parsed = Cli::try_parse_from([
                "harness-graph",
                "enrich-all-transcripts",
                "--authorization",
                "operator-test",
                "--apply",
                "--limit",
                invalid,
            ]);
            assert_eq!(
                parsed.err().map(|error| error.kind()),
                Some(ErrorKind::ValueValidation)
            );
        }
    }
}
