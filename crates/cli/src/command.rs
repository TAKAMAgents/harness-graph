//! CLI command parsing and orchestration.

use clap::{Parser, Subcommand, ValueEnum};
use harness_graph_domain::{DecodedNativeRecord, RecordCount, SessionId};
use harness_graph_graph_port::{
    FinalizeIngestionCommand, GraphBatch, GraphCommand, GraphProjector, SourceSnapshotCommand,
};
use harness_graph_ingestion::{
    DecodedRecordStream, IngestionError, SessionScope, SourceKind, inspect_bundle,
};
use harness_graph_neo4j_adapter::Neo4jAdapter;
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
    let mut pending = PendingBatch::Empty;
    for record in DecodedRecordStream::open(verified)? {
        let record = record?;
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
    if total_records != expected_records {
        return Err(IngestionError::RecordCountMismatch {
            expected: expected_records,
            actual: total_records,
        }
        .into());
    }

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
    })
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
