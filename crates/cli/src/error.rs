//! CLI error boundary.

use harness_graph_domain::DomainError;
use harness_graph_ingestion::IngestionError;

/// Top-level command failure.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
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

    /// Structured command output could not be encoded.
    #[error("failed to encode structured command output: {source}")]
    OutputEncoding {
        /// JSON encoder error.
        #[source]
        source: serde_json::Error,
    },

    /// Logging initialization failed.
    #[error("failed to initialize structured logging: {message}")]
    Logging {
        /// Source-safe initialization message.
        message: String,
    },
}
