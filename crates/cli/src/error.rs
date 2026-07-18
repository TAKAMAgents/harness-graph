//! CLI error boundary.

use harness_graph_assurance::AssuranceError;
use harness_graph_classification::ClassificationError;
use harness_graph_correlation::CorrelationError;
use harness_graph_domain::DomainError;
use harness_graph_enrichment_application::EnrichmentApplicationError;
use harness_graph_event_journal::JournalError;
use harness_graph_graph_port::GraphPortError;
use harness_graph_ingestion::IngestionError;
use harness_graph_mistral_adapter::{MistralAdapterError, TranscriptPromptProvenanceError};
use harness_graph_neo4j_adapter::Neo4jAdapterError;
use harness_graph_path_analysis::PathAnalysisError;
use harness_graph_planning::PlanningError;
use harness_graph_risk::RiskError;
use harness_graph_transcript_enrichment::TranscriptEnrichmentError;

/// Closed prerequisite for a cost-bearing transcript apply command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptApplyRequirement {
    /// Enrichment remains default-off until explicitly enabled.
    EnrichmentEnabled,
    /// Mistral account training/data-sharing controls were operator-verified.
    TrainingOptOutVerified,
    /// A persistent dedicated HMAC key is available for stable pseudonyms.
    StablePseudonymizationKey,
    /// Transcript extraction uses the source-controlled pinned Mistral model.
    PinnedMistralModel,
}

impl std::fmt::Display for TranscriptApplyRequirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::EnrichmentEnabled => "enrichment_enabled",
            Self::TrainingOptOutVerified => "training_opt_out_verified",
            Self::StablePseudonymizationKey => "stable_pseudonymization_key",
            Self::PinnedMistralModel => "pinned_mistral_model",
        })
    }
}

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

    /// Transcript authorization, redaction, chunking, or validation failed.
    #[error(transparent)]
    TranscriptEnrichment(#[from] TranscriptEnrichmentError),

    /// Transcript enrichment was invoked without selecting an explicit mode.
    #[error("transcript enrichment requires an explicit --dry-run or --apply mode")]
    TranscriptExecutionModeRequired,

    /// A dry-run stage failed without disclosing a source path or transcript.
    #[error("transcript dry run blocked at {stage}")]
    TranscriptDryRunBlocked {
        /// Closed source-safe stage name.
        stage: &'static str,
    },

    /// A cost-bearing apply precondition was not explicitly satisfied.
    #[error("transcript apply blocked by prerequisite {requirement}")]
    TranscriptApplyPrecondition {
        /// Closed requirement name; no configuration value is retained.
        requirement: TranscriptApplyRequirement,
    },

    /// Immutable prompt provenance could not be constructed locally.
    #[error(transparent)]
    TranscriptPromptProvenance(#[from] TranscriptPromptProvenanceError),

    /// Additive transcript workflow composition failed source-safely.
    #[error(transparent)]
    EnrichmentApplication(#[from] EnrichmentApplicationError),

    /// A blocking transcript preparation worker did not complete normally.
    #[error("transcript preparation worker did not complete")]
    TranscriptApplyWorkerJoin,

    /// One-session apply produced a typed blocked or failed settlement.
    #[error("transcript apply did not complete")]
    TranscriptApplyIncomplete,

    /// Bulk apply settled its selected sessions but retained failures or blocks.
    #[error("bulk transcript apply completed with blocked or failed sessions")]
    BulkTranscriptApplyIncomplete,

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
