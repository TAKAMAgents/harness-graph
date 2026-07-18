//! CLI command parsing and orchestration.

use clap::{Parser, Subcommand, ValueEnum};
use harness_graph_assurance::assess_outcome;
use harness_graph_classification::ActivityBuilder;
use harness_graph_correlation::CorrelationEngine;
use harness_graph_domain::{
    AnalysisReport, DecodedNativeRecord, RecordCount, SessionId, ToolCallLifecycle,
};
use harness_graph_graph_port::{
    AnalysisProjectionCommand, FinalizeIngestionCommand, GraphBatch, GraphCommand, GraphProjector,
    SourceSnapshotCommand,
};
use harness_graph_ingestion::{
    DecodedRecordStream, IngestionError, SessionScope, SourceKind, inspect_bundle,
};
use harness_graph_neo4j_adapter::Neo4jAdapter;
use harness_graph_path_analysis::derive_path;
use harness_graph_risk::RiskEngine;
use secrecy::SecretString;
use serde::Serialize;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{AppConfig, CliError};

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
        Command::Import { session_id } => import(&config, &session_id).await,
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
    /// Verify, stream, and atomically upsert one session into Neo4j.
    Import {
        /// Stable session UUID.
        #[arg(long)]
        session_id: String,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ScopeArgument {
    Active,
    Archived,
    All,
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
    let _archive = config.archive_root();
    let _neo4j = config.neo4j();
    let _mistral_key = config.mistral_api_key();
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
    let catalog = config.archive_root().discover(scope)?;
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
    let catalog = config.archive_root().discover(SessionScope::All)?;
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
    let catalog = config.archive_root().discover(SessionScope::All)?;
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

#[derive(Serialize)]
struct ImportOutput {
    status: &'static str,
    session_id: String,
    source_digest: String,
    known_records: u64,
    quarantined_records: u64,
    total_records: u64,
    observations_in_namespace: u64,
    analysis: AnalysisOutput,
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
struct AnalyzeOutput {
    status: &'static str,
    session_id: String,
    source_digest: String,
    analysis: AnalysisOutput,
}

#[derive(Debug, Default)]
struct AnalysisAccumulator {
    correlations: CorrelationEngine,
    activities: ActivityBuilder,
    risks: RiskEngine,
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

    fn finish(self) -> Result<AnalysisReport, CliError> {
        let correlations = self.correlations.finish()?;
        let activities = self.activities.finish(&correlations)?;
        let outcome = assess_outcome(&activities)?;
        let risks = self.risks.finish(&activities, &correlations, &outcome)?;
        let path = derive_path(&activities)?;
        Ok(AnalysisReport::new(
            correlations,
            activities,
            outcome,
            risks,
            path,
        ))
    }
}

fn analyze(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root().discover(SessionScope::All)?;
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
    write_json(&AnalyzeOutput {
        status: "analyzed",
        session_id: session_id.to_string(),
        source_digest: source_digest.to_hex(),
        analysis: summarize_analysis(&report),
    })
}

enum PendingBatch {
    Empty,
    Building(GraphBatch),
}

async fn import(config: &AppConfig, session_id: &str) -> Result<(), CliError> {
    let session_id = SessionId::parse(session_id)?;
    let catalog = config.archive_root().discover(SessionScope::All)?;
    let verified = catalog.require(session_id)?.verify()?;
    let source_digest = verified.source_digest();
    let expected_records = verified.expected_records();
    let namespace = config.graph_namespace().clone();

    let neo4j = config.neo4j();
    let adapter = Neo4jAdapter::connect(
        &neo4j.bolt_address()?,
        neo4j.username(),
        SecretString::from(neo4j.expose_password().to_owned()),
    )
    .await?;
    adapter.health().await?;
    adapter.ensure_schema().await?;
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
            &adapter,
            std::mem::replace(&mut pending, PendingBatch::Empty),
            command,
            config.graph_batch_size(),
        )
        .await?;
    }
    if let PendingBatch::Building(batch) = pending {
        adapter.project(batch).await?;
    }
    validate_record_count(expected_records, total_records)?;

    let report = analysis.finish()?;
    let analysis_output = summarize_analysis(&report);
    adapter
        .project(GraphBatch::first(GraphCommand::UpsertAnalysis(
            AnalysisProjectionCommand::new(namespace.clone(), session_id, source_digest, report),
        )))
        .await?;

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
    let observations_in_namespace = adapter.observation_count(&namespace).await?;
    write_json(&ImportOutput {
        status: "imported",
        session_id: session_id.to_string(),
        source_digest: source_digest.to_hex(),
        known_records: known_records.value(),
        quarantined_records: quarantined_records.value(),
        total_records: total_records.value(),
        observations_in_namespace: observations_in_namespace.value(),
        analysis: analysis_output,
    })
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
