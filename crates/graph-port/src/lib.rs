//! Provider-agnostic graph projection contract.

use async_trait::async_trait;
use harness_graph_domain::{
    DecodedNativeRecord, GraphNamespace, RecordCount, SessionId, SourceDigest,
};
use serde::{Deserialize, Serialize};

/// Invalid graph projection configuration.
#[derive(Debug, thiserror::Error)]
pub enum GraphPortError {
    /// Batch size was zero or exceeded the safety maximum.
    #[error("graph batch size must be between 1 and 10,000")]
    InvalidBatchSize,
}

/// Validated maximum number of logical commands in one transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchSize(usize);

impl BatchSize {
    /// Validate a projection batch size.
    ///
    /// # Errors
    ///
    /// Returns an error for zero or values above 10,000.
    pub fn new(value: usize) -> Result<Self, GraphPortError> {
        if !(1..=10_000).contains(&value) {
            return Err(GraphPortError::InvalidBatchSize);
        }
        Ok(Self(value))
    }

    /// Numeric batch size.
    #[must_use]
    pub const fn value(self) -> usize {
        self.0
    }
}

/// Metadata establishing a verified source snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshotCommand {
    namespace: GraphNamespace,
    session_id: SessionId,
    source_digest: SourceDigest,
    expected_records: RecordCount,
}

impl SourceSnapshotCommand {
    /// Construct a verified source snapshot command.
    #[must_use]
    pub const fn new(
        namespace: GraphNamespace,
        session_id: SessionId,
        source_digest: SourceDigest,
        expected_records: RecordCount,
    ) -> Self {
        Self {
            namespace,
            session_id,
            source_digest,
            expected_records,
        }
    }

    /// Graph namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }

    /// Session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Expected record count.
    #[must_use]
    pub const fn expected_records(&self) -> RecordCount {
        self.expected_records
    }
}

/// Final source-safe ingestion counts persisted after every record batch has
/// committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeIngestionCommand {
    namespace: GraphNamespace,
    session_id: SessionId,
    source_digest: SourceDigest,
    known_records: RecordCount,
    quarantined_records: RecordCount,
    total_records: RecordCount,
}

impl FinalizeIngestionCommand {
    /// Construct final ingestion counts.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        namespace: GraphNamespace,
        session_id: SessionId,
        source_digest: SourceDigest,
        known_records: RecordCount,
        quarantined_records: RecordCount,
        total_records: RecordCount,
    ) -> Self {
        Self {
            namespace,
            session_id,
            source_digest,
            known_records,
            quarantined_records,
            total_records,
        }
    }

    /// Graph namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }

    /// Session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Known record count.
    #[must_use]
    pub const fn known_records(&self) -> RecordCount {
        self.known_records
    }

    /// Quarantined record count.
    #[must_use]
    pub const fn quarantined_records(&self) -> RecordCount {
        self.quarantined_records
    }

    /// Total record count.
    #[must_use]
    pub const fn total_records(&self) -> RecordCount {
        self.total_records
    }
}

/// One provider-agnostic graph mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum GraphCommand {
    /// Establish the verified source and session nodes.
    UpsertSourceSnapshot(SourceSnapshotCommand),
    /// Project one known or quarantined native record.
    UpsertObservation {
        /// Graph namespace.
        namespace: GraphNamespace,
        /// Typed decoded record.
        record: DecodedNativeRecord,
    },
    /// Persist completion counts only after record projection succeeds.
    FinalizeIngestion(FinalizeIngestionCommand),
}

/// Non-empty bounded transaction command batch.
#[derive(Debug)]
pub struct GraphBatch {
    commands: Vec<GraphCommand>,
}

impl GraphBatch {
    /// Start a non-empty batch.
    #[must_use]
    pub fn first(command: GraphCommand) -> Self {
        Self {
            commands: vec![command],
        }
    }

    /// Append another command before projection.
    pub fn push(&mut self, command: GraphCommand) {
        self.commands.push(command);
    }

    /// Number of logical commands as a domain count.
    #[must_use]
    pub fn command_count(&self) -> RecordCount {
        RecordCount::new(self.commands.len() as u64)
    }

    /// Whether this batch has reached a validated capacity.
    #[must_use]
    pub fn is_full(&self, capacity: BatchSize) -> bool {
        self.commands.len() >= capacity.value()
    }

    /// Consume the batch as a command iterator.
    pub fn into_commands(self) -> impl Iterator<Item = GraphCommand> {
        self.commands.into_iter()
    }
}

/// Counts returned by a committed graph batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionReceipt {
    committed_commands: RecordCount,
}

impl ProjectionReceipt {
    /// Construct a receipt for committed commands.
    #[must_use]
    pub const fn new(committed_commands: RecordCount) -> Self {
        Self { committed_commands }
    }

    /// Number of committed logical commands.
    #[must_use]
    pub const fn committed_commands(self) -> RecordCount {
        self.committed_commands
    }
}

/// Concrete graph adapter capability required by the application.
#[async_trait]
pub trait GraphProjector: Send + Sync {
    /// Provider-specific typed error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Verify connectivity without mutating graph data.
    async fn health(&self) -> Result<(), Self::Error>;

    /// Create idempotent constraints required by projection.
    async fn ensure_schema(&self) -> Result<(), Self::Error>;

    /// Project one non-empty batch atomically.
    async fn project(&self, batch: GraphBatch) -> Result<ProjectionReceipt, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::BatchSize;

    #[test]
    fn batch_size_enforces_transaction_safety_bounds() -> Result<(), Box<dyn std::error::Error>> {
        assert!(BatchSize::new(0).is_err());
        assert_eq!(BatchSize::new(1)?.value(), 1);
        assert_eq!(BatchSize::new(10_000)?.value(), 10_000);
        assert!(BatchSize::new(10_001).is_err());
        Ok(())
    }
}
