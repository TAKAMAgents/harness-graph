//! Bounded-memory sensitive transcript projection over a verified bundle.

use std::io::{BufRead, BufReader};

use harness_graph_domain::{RecordCount, RecordSequence, SourceRecordRef};
use harness_graph_protocol::TranscriptRecordProjection;

use crate::{IngestionError, VerifiedSessionBundle};

const MIN_RECORD_BYTES: usize = 256;
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// Hard upper bound for one canonical JSONL record in the sensitive reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaxSourceRecordBytes(usize);

impl MaxSourceRecordBytes {
    /// Validate a hard source-record limit.
    ///
    /// # Errors
    ///
    /// Returns an error outside the supported 256-byte through 64-MiB range.
    pub const fn new(value: usize) -> Result<Self, IngestionError> {
        if value < MIN_RECORD_BYTES || value > MAX_RECORD_BYTES {
            Err(IngestionError::InvalidSourceRecordByteLimit)
        } else {
            Ok(Self(value))
        }
    }

    const fn value(self) -> usize {
        self.0
    }
}

impl Default for MaxSourceRecordBytes {
    fn default() -> Self {
        Self(DEFAULT_RECORD_BYTES)
    }
}

/// Streaming transcript projector over a checksum-verified canonical rollout.
pub struct TranscriptProjectionStream {
    bundle: VerifiedSessionBundle,
    reader: BufReader<std::fs::File>,
    offset: u64,
    limit: MaxSourceRecordBytes,
}

impl TranscriptProjectionStream {
    /// Open a verified bundle with a hard per-record memory bound.
    ///
    /// # Errors
    ///
    /// Returns an error when the verified canonical rollout cannot be opened.
    pub fn open(
        bundle: VerifiedSessionBundle,
        limit: MaxSourceRecordBytes,
    ) -> Result<Self, IngestionError> {
        let file = bundle.open_raw()?;
        Ok(Self {
            bundle,
            reader: BufReader::new(file),
            offset: 0,
            limit,
        })
    }

    /// Validate that the stream consumed the exporter-declared record count.
    ///
    /// # Errors
    ///
    /// Returns an error when the actual and verified expected counts differ.
    pub fn finish(self) -> Result<RecordCount, IngestionError> {
        let actual = RecordCount::new(self.offset);
        let expected = self.bundle.expected_records();
        if actual == expected {
            Ok(actual)
        } else {
            Err(IngestionError::RecordCountMismatch { expected, actual })
        }
    }
}

impl Iterator for TranscriptProjectionStream {
    type Item = Result<TranscriptRecordProjection, IngestionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let sequence = RecordSequence::from_zero_based(self.offset);
        let bytes = match read_capped_line(&mut self.reader, self.limit.value()) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None,
            Err(CappedLineError::TooLarge) => {
                self.offset = self.offset.saturating_add(1);
                return Some(Err(IngestionError::SourceRecordTooLarge { sequence }));
            }
            Err(CappedLineError::Io(source)) => {
                return Some(Err(IngestionError::Filesystem {
                    operation: "read canonical transcript record",
                    path: std::path::PathBuf::from("[verified canonical rollout]"),
                    source,
                }));
            }
        };
        self.offset = self.offset.saturating_add(1);
        let Ok(line) = std::str::from_utf8(&bytes) else {
            return Some(Err(IngestionError::InvalidUtf8Record { sequence }));
        };
        let source = SourceRecordRef::new(
            self.bundle.session_id(),
            self.bundle.source_digest(),
            sequence,
        );
        Some(
            harness_graph_protocol::project_codex_transcript_line(line, source)
                .map_err(|source| IngestionError::Protocol { sequence, source }),
        )
    }
}

enum CappedLineError {
    TooLarge,
    Io(std::io::Error),
}

fn read_capped_line<R: BufRead>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<Vec<u8>>, CappedLineError> {
    let mut line = Vec::with_capacity(limit.min(8 * 1024));
    loop {
        let available = reader.fill_buf().map_err(CappedLineError::Io)?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(take) > limit {
            reader.consume(take);
            if newline.is_none() {
                drain_to_newline(reader)?;
            }
            return Err(CappedLineError::TooLarge);
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            return Ok(Some(line));
        }
    }
}

fn drain_to_newline<R: BufRead>(reader: &mut R) -> Result<(), CappedLineError> {
    loop {
        let available = reader.fill_buf().map_err(CappedLineError::Io)?;
        if available.is_empty() {
            return Ok(());
        }
        if let Some(position) = available.iter().position(|byte| *byte == b'\n') {
            reader.consume(position + 1);
            return Ok(());
        }
        let consumed = available.len();
        reader.consume(consumed);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{CappedLineError, read_capped_line};

    #[test]
    fn capped_reader_rejects_and_drains_an_oversized_record()
    -> Result<(), Box<dyn std::error::Error>> {
        let input = format!("{}\nnext\n", "x".repeat(300));
        let mut reader = Cursor::new(input.into_bytes());
        assert!(matches!(
            read_capped_line(&mut reader, 256),
            Err(CappedLineError::TooLarge)
        ));
        let next = read_capped_line(&mut reader, 256)
            .map_err(|_| "unexpected capped reader error")?
            .ok_or("missing next line")?;
        assert_eq!(next, b"next");
        Ok(())
    }

    #[test]
    fn capped_reader_preserves_unicode_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let mut reader = Cursor::new("grapheme: 🦀\n".as_bytes());
        let line = read_capped_line(&mut reader, 256)
            .map_err(|_| "unexpected capped reader error")?
            .ok_or("missing line")?;
        assert_eq!(std::str::from_utf8(&line)?, "grapheme: 🦀");
        Ok(())
    }
}
