//! Bounded-memory canonical record streaming.

use std::io::{BufRead, BufReader, Lines};

use harness_graph_domain::{DecodedNativeRecord, RecordCount, RecordSequence, SourceRecordRef};
use serde::Serialize;

use crate::{IngestionError, VerifiedSessionBundle};

/// Streaming decoder over a verified canonical rollout.
pub struct DecodedRecordStream {
    bundle: VerifiedSessionBundle,
    lines: Lines<BufReader<std::fs::File>>,
    offset: u64,
}

impl DecodedRecordStream {
    /// Open a verified session as a bounded-memory record stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the canonical rollout cannot be opened.
    pub fn open(bundle: VerifiedSessionBundle) -> Result<Self, IngestionError> {
        let file = bundle.open_raw()?;
        Ok(Self {
            bundle,
            lines: BufReader::new(file).lines(),
            offset: 0,
        })
    }
}

impl Iterator for DecodedRecordStream {
    type Item = Result<DecodedNativeRecord, IngestionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = self.lines.next()?;
        let sequence = RecordSequence::from_zero_based(self.offset);
        self.offset = self.offset.saturating_add(1);
        Some(line.map_or_else(
            |source| {
                Err(IngestionError::Filesystem {
                    operation: "read canonical rollout",
                    path: std::path::PathBuf::from("[verified canonical rollout]"),
                    source,
                })
            },
            |line| {
                let source = SourceRecordRef::new(
                    self.bundle.session_id(),
                    self.bundle.source_digest(),
                    sequence,
                );
                harness_graph_protocol::decode_codex_line(&line, source)
                    .map_err(|source| IngestionError::Protocol { sequence, source })
            },
        ))
    }
}

/// Source-safe result of a complete streaming inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IngestionReceipt {
    /// Parsed known records.
    pub known_records: RecordCount,
    /// Parsed but unsupported records retained as quarantine metadata.
    pub quarantined_records: RecordCount,
    /// Total streamed records.
    pub total_records: RecordCount,
}

/// Stream a verified bundle and count typed/quarantined records.
///
/// # Errors
///
/// Returns an error when file streaming or protocol decoding fails, or when the
/// actual record count disagrees with verified metadata.
pub fn inspect_bundle(bundle: VerifiedSessionBundle) -> Result<IngestionReceipt, IngestionError> {
    let expected = bundle.expected_records();
    let mut receipt = IngestionReceipt {
        known_records: RecordCount::default(),
        quarantined_records: RecordCount::default(),
        total_records: RecordCount::default(),
    };
    for record in DecodedRecordStream::open(bundle)? {
        match record? {
            DecodedNativeRecord::Known(_) => receipt.known_records.increment(),
            DecodedNativeRecord::Unsupported(_) => receipt.quarantined_records.increment(),
        }
        receipt.total_records.increment();
    }
    if receipt.total_records != expected {
        return Err(IngestionError::RecordCountMismatch {
            expected,
            actual: receipt.total_records,
        });
    }
    Ok(receipt)
}
