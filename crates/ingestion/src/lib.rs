//! Verified, streaming ingestion of Codex exporter archives.

mod archive;
mod error;
mod stream;

pub use archive::{
    ArchiveRoot, SessionBundle, SessionCatalog, SessionScope, SourceKind, VerifiedSessionBundle,
};
pub use error::IngestionError;
pub use stream::{DecodedRecordStream, IngestionReceipt, inspect_bundle};
