//! Ingestion failures with source-safe context.

use std::path::PathBuf;

use harness_graph_domain::{DomainError, RecordCount, RecordSequence, SessionId};
use harness_graph_protocol::ProtocolError;

/// Failure while discovering, verifying, or streaming an export bundle.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    /// Archive root was absent or not a directory.
    #[error("export archive root is not a readable directory")]
    InvalidArchiveRoot,

    /// A filesystem operation failed.
    #[error("filesystem operation {operation} failed for {path}: {source}")]
    Filesystem {
        /// Safe operation name.
        operation: &'static str,
        /// Local path involved. This error must not be serialized to external clients.
        path: PathBuf,
        /// I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Metadata JSON was malformed.
    #[error("session metadata is invalid: {source}")]
    InvalidMetadata {
        /// JSON error.
        #[source]
        source: serde_json::Error,
    },

    /// Exporter metadata reports parse failures or an unstable source copy.
    #[error("session {session_id} is not publishable: {reason}")]
    UnpublishableBundle {
        /// Session identity.
        session_id: SessionId,
        /// Static safe reason.
        reason: &'static str,
    },

    /// Session directory identity disagrees with metadata.
    #[error("session directory identity does not match metadata")]
    SessionIdentityMismatch,

    /// Duplicate locations for the same session disagree on content.
    #[error("session {session_id} has conflicting source digests")]
    ConflictingSessionSnapshots {
        /// Conflicting session identity.
        session_id: SessionId,
    },

    /// A requested session was not present in the selected archive scope.
    #[error("session {session_id} was not found in the selected archive scope")]
    SessionNotFound {
        /// Requested session identity.
        session_id: SessionId,
    },

    /// A checksum manifest entry was malformed or unsafe.
    #[error("invalid checksum manifest entry on line {line_number}: {reason}")]
    InvalidChecksumEntry {
        /// One-based manifest line number.
        line_number: usize,
        /// Static safe reason.
        reason: &'static str,
    },

    /// A checksum entry points outside the session bundle or through a symlink.
    #[error("checksum manifest contains an unsafe path")]
    UnsafeChecksumPath,

    /// A file failed checksum verification.
    #[error("checksum verification failed for a declared bundle file")]
    ChecksumMismatch,

    /// Metadata source digest disagrees with the verified canonical raw file.
    #[error("metadata source digest does not match raw/rollout.jsonl")]
    RawDigestMismatch,

    /// A source line exceeded the expected record count.
    #[error("session record count mismatch: metadata={expected:?}, actual={actual:?}")]
    RecordCountMismatch {
        /// Metadata count.
        expected: RecordCount,
        /// Streamed count.
        actual: RecordCount,
    },

    /// A session or digest domain value failed validation.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// A canonical record failed protocol decoding.
    #[error("failed to decode canonical record {sequence:?}: {source}")]
    Protocol {
        /// Record sequence.
        sequence: RecordSequence,
        /// Protocol error.
        #[source]
        source: ProtocolError,
    },
}
