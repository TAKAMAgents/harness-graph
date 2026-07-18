//! CLI error boundary.

use harness_graph_assurance::AssuranceError;
use harness_graph_classification::ClassificationError;
use harness_graph_correlation::CorrelationError;
use harness_graph_domain::DomainError;
use harness_graph_event_journal::JournalError;
use harness_graph_graph_port::GraphPortError;
use harness_graph_ingestion::IngestionError;
use harness_graph_mistral_adapter::MistralAdapterError;
use harness_graph_neo4j_adapter::Neo4jAdapterError;
use harness_graph_path_analysis::PathAnalysisError;
use harness_graph_planning::PlanningError;
use harness_graph_risk::RiskError;

/// Top-level command failure.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// The selected `.env` file could not be read or parsed safely.
    #[error("project .env file is unreadable or malformed")]
    ConfigurationFile,

    /// Required configuration was absent.
    #[error("required configuration {canonical_name} is missing")]
    MissingConfiguration {
        /// Canonical environment variable name only; values are never included.
        canonical_name: &'static str,
    },

    /// Configuration value was invalid.
    #[error("configuration {canonical_name} is invalid: {reason}")]
    InvalidConfiguration {
        /// Canonical environment variable name.
        canonical_name: &'static str,
        /// Source-safe reason.
        reason: &'static str,
    },

    /// Domain construction failed.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// Archive ingestion failed.
    #[error(transparent)]
    Ingestion(#[from] IngestionError),

    /// Native tool-call correlation failed.
    #[error(transparent)]
    Correlation(#[from] CorrelationError),

    /// Semantic activity classification failed.
    #[error(transparent)]
    Classification(#[from] ClassificationError),

    /// Evidence assurance assessment failed.
    #[error(transparent)]
    Assurance(#[from] AssuranceError),

    /// Deterministic risk derivation failed.
    #[error(transparent)]
    Risk(#[from] RiskError),

    /// Normalized execution-path derivation failed.
    #[error(transparent)]
    PathAnalysis(#[from] PathAnalysisError),

    /// Mistral provider construction or invocation failed.
    #[error(transparent)]
    Mistral(#[from] MistralAdapterError),

    /// Typed planning input was invalid.
    #[error(transparent)]
    Planning(#[from] PlanningError),

    /// Graph projection configuration failed validation.
    #[error(transparent)]
    GraphPort(#[from] GraphPortError),

    /// Neo4j connectivity, schema, or projection failed.
    #[error(transparent)]
    Neo4j(#[from] Neo4jAdapterError),

    /// A blocking archive verification worker did not complete normally.
    #[error("archive verification worker did not complete")]
    ImportWorkerJoin,

    /// Bulk import settled every session but at least one session failed.
    #[error("bulk import completed with one or more failed sessions")]
    BulkImportIncomplete,

    /// Append-only live journal validation or durability failed.
    #[error(transparent)]
    Journal(#[from] JournalError),

    /// HTTP listener or server failed.
    #[error("live API server failed: {source}")]
    Server {
        /// Network listener or server error.
        #[source]
        source: std::io::Error,
    },

    /// Structured command output could not be encoded.
    #[error("failed to encode structured command output: {source}")]
    OutputEncoding {
        /// JSON encoder error.
        #[source]
        source: serde_json::Error,
    },

    /// Structured progress output could not be written to the process stream.
    #[error("failed to write structured command output: {source}")]
    OutputWrite {
        /// Output stream error.
        #[source]
        source: std::io::Error,
    },

    /// Logging initialization failed.
    #[error("failed to initialize structured logging: {message}")]
    Logging {
        /// Source-safe initialization message.
        message: String,
    },
}
