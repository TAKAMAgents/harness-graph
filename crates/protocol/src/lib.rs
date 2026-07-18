//! Codex JSONL protocol boundary.
//!
//! Raw JSON and stringly native variants are quarantined in this crate and are
//! converted immediately into validated domain objects.

mod codex;
mod error;
mod transcript;

pub use codex::decode_codex_line;
pub use error::ProtocolError;
pub use transcript::{
    SensitiveTranscriptFragment, SensitiveTranscriptFragments, TranscriptExclusionReason,
    TranscriptField, TranscriptFieldPath, TranscriptRecordClass, TranscriptRecordProjection,
    TranscriptRole, project_codex_transcript_line,
};
