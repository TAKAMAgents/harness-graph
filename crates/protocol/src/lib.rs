//! Codex JSONL protocol boundary.
//!
//! Raw JSON and stringly native variants are quarantined in this crate and are
//! converted immediately into validated domain objects.

mod codex;
mod error;

pub use codex::decode_codex_line;
pub use error::ProtocolError;
