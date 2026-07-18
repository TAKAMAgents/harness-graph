//! Durable append-only journal for source-safe live harness events.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use harness_graph_domain::{ActivityKind, ActivityStatus, PayloadDigest, VerificationStatus};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

/// Live-journal validation, durability, or replay failure.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// Journal path was empty or pointed to a directory.
    #[error("journal path must identify a file")]
    InvalidPath,

    /// Native live session identity was empty or unsafe.
    #[error("live session ID must contain 1 to 128 source-safe characters")]
    InvalidSessionId,

    /// Event timestamp was not RFC 3339.
    #[error("live event timestamp must be RFC 3339: {source}")]
    InvalidTimestamp {
        /// Timestamp parser error.
        #[source]
        source: time::error::Parse,
    },

    /// A parsed timestamp could not be normalized.
    #[error("failed to normalize live event timestamp: {source}")]
    TimestampFormat {
        /// Timestamp formatting error.
        #[source]
        source: time::error::Format,
    },

    /// Journal sequence zero is invalid.
    #[error("journal sequence must be greater than zero")]
    InvalidSequence,

    /// A sequence could not fit the journal's numeric contract.
    #[error("journal sequence exhausted its supported range")]
    SequenceOverflow,

    /// Filesystem operation failed.
    #[error("journal filesystem operation failed: {source}")]
    Io {
        /// Filesystem error without a potentially sensitive path.
        #[source]
        source: std::io::Error,
    },

    /// A journal record was not valid typed JSON.
    #[error("journal record failed typed JSON validation: {source}")]
    Json {
        /// JSON encoding or decoding failure.
        #[source]
        source: serde_json::Error,
    },

    /// The journal ended without a record delimiter.
    #[error("journal contains a torn final record")]
    TornFinalRecord,

    /// Replayed sequence numbers were not contiguous.
    #[error("journal sequence is not contiguous")]
    NonContiguousSequence,

    /// Replayed content did not match its recorded digest.
    #[error("journal record digest verification failed")]
    DigestMismatch,

    /// A durable journal contained the same event identity twice.
    #[error("journal contains a duplicate event identity")]
    DuplicateEventIdentity,

    /// One event identity was reused for different content.
    #[error("live event identity conflicts with already durable content")]
    EventIdentityConflict,
}

/// Validated journal file location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalPath(PathBuf);

impl JournalPath {
    /// Validate a file path for the append-only journal.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty path or an existing directory.
    pub fn new(value: impl Into<PathBuf>) -> Result<Self, JournalError> {
        let value = value.into();
        if value.as_os_str().is_empty() || value.is_dir() {
            Err(JournalError::InvalidPath)
        } else {
            Ok(Self(value))
        }
    }

    /// Borrow the filesystem path without exposing it through diagnostics.
    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

/// Globally unique idempotency key supplied by a live adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LiveEventId(Uuid);

impl LiveEventId {
    /// Parse a UUID event identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the input is not a UUID.
    pub fn parse(value: &str) -> Result<Self, uuid::Error> {
        Uuid::parse_str(value).map(Self)
    }
}

impl std::fmt::Display for LiveEventId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Validated native session identity from a live harness.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LiveSessionId(String);

impl LiveSessionId {
    /// Validate a source-safe native session identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the identifier is empty, too long, or unsafe.
    pub fn new(value: impl Into<String>) -> Result<Self, JournalError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(JournalError::InvalidSessionId);
        }
        Ok(Self(value.to_owned()))
    }

    /// Borrow the validated native identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LiveSessionId {
    type Error = JournalError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LiveSessionId> for String {
    fn from(value: LiveSessionId) -> Self {
        value.0
    }
}

/// Canonical RFC 3339 timestamp from a live harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LiveOccurredAt(String);

impl LiveOccurredAt {
    /// Parse and normalize an RFC 3339 timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when parsing or canonical formatting fails.
    pub fn parse(value: &str) -> Result<Self, JournalError> {
        let parsed = OffsetDateTime::parse(value, &Rfc3339)
            .map_err(|source| JournalError::InvalidTimestamp { source })?;
        let canonical = parsed
            .format(&Rfc3339)
            .map_err(|source| JournalError::TimestampFormat { source })?;
        Ok(Self(canonical))
    }

    /// Borrow the canonical timestamp.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LiveOccurredAt {
    type Error = JournalError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<LiveOccurredAt> for String {
    fn from(value: LiveOccurredAt) -> Self {
        value.0
    }
}

/// One source-safe event emitted by a live harness adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LiveEventPayload {
    /// A harness began a session.
    SessionStarted,
    /// The harness observed one normalized activity transition.
    ActivityObserved {
        /// Semantic activity category.
        kind: ActivityKind,
        /// Activity completion state.
        status: ActivityStatus,
    },
    /// A harness reported session completion with evidence status.
    SessionCompleted {
        /// Verification evidence known at completion time.
        verification: VerificationStatus,
    },
    /// A harness reported a terminal session error.
    SessionFailed,
}

/// Validated event before the journal assigns durable order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveEvent {
    event_id: LiveEventId,
    session_id: LiveSessionId,
    occurred_at: LiveOccurredAt,
    payload: LiveEventPayload,
}

impl LiveEvent {
    /// Construct a validated live event.
    #[must_use]
    pub const fn new(
        event_id: LiveEventId,
        session_id: LiveSessionId,
        occurred_at: LiveOccurredAt,
        payload: LiveEventPayload,
    ) -> Self {
        Self {
            event_id,
            session_id,
            occurred_at,
            payload,
        }
    }

    /// Idempotency identity.
    #[must_use]
    pub const fn event_id(&self) -> LiveEventId {
        self.event_id
    }

    /// Native live session identity.
    #[must_use]
    pub const fn session_id(&self) -> &LiveSessionId {
        &self.session_id
    }

    /// Event time.
    #[must_use]
    pub const fn occurred_at(&self) -> &LiveOccurredAt {
        &self.occurred_at
    }

    /// Typed event payload.
    #[must_use]
    pub const fn payload(&self) -> &LiveEventPayload {
        &self.payload
    }
}

/// One-based durable position in a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct JournalSequence(u64);

impl JournalSequence {
    /// Validate a non-zero sequence.
    ///
    /// # Errors
    ///
    /// Returns an error for zero.
    pub const fn new(value: u64) -> Result<Self, JournalError> {
        if value == 0 {
            Err(JournalError::InvalidSequence)
        } else {
            Ok(Self(value))
        }
    }

    /// Numeric one-based sequence.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for JournalSequence {
    type Error = JournalError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<JournalSequence> for u64 {
    fn from(value: JournalSequence) -> Self {
        value.0
    }
}

/// Durable, integrity-protected journal entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEntry {
    sequence: JournalSequence,
    content_digest: PayloadDigest,
    event: LiveEvent,
}

impl JournalEntry {
    /// Durable sequence.
    #[must_use]
    pub const fn sequence(&self) -> JournalSequence {
        self.sequence
    }

    /// Digest of the canonical typed event representation.
    #[must_use]
    pub const fn content_digest(&self) -> PayloadDigest {
        self.content_digest
    }

    /// Validated live event.
    #[must_use]
    pub const fn event(&self) -> &LiveEvent {
        &self.event
    }
}

/// Replay cursor preserving the difference between start and a concrete event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayCursor {
    /// Replay the complete journal.
    Beginning,
    /// Replay entries strictly after this durable sequence.
    After(JournalSequence),
}

/// Result of an idempotent durable append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    /// New content was flushed and synchronized.
    Appended(JournalEntry),
    /// The exact event was already durable.
    Duplicate(JournalEntry),
}

impl AppendOutcome {
    /// Durable entry for either outcome.
    #[must_use]
    pub const fn entry(&self) -> &JournalEntry {
        match self {
            Self::Appended(entry) | Self::Duplicate(entry) => entry,
        }
    }

    /// Whether this call created new durable content.
    #[must_use]
    pub const fn is_appended(&self) -> bool {
        matches!(self, Self::Appended(_))
    }
}

/// File-backed append-only journal with verified replay and idempotency.
pub struct AppendOnlyJournal {
    file: File,
    entries: Vec<JournalEntry>,
    event_positions: HashMap<LiveEventId, usize>,
}

impl AppendOnlyJournal {
    /// Open or create a journal and verify all existing records before use.
    ///
    /// # Errors
    ///
    /// Returns an error for filesystem failures, torn records, invalid JSON,
    /// non-contiguous sequences, digest mismatches, or duplicate identities.
    pub fn open(path: &JournalPath) -> Result<Self, JournalError> {
        if let Some(parent) = path.as_path().parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|source| JournalError::Io { source })?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.as_path())
            .map_err(|source| JournalError::Io { source })?;
        let mut bytes = Vec::new();
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.read_to_end(&mut bytes))
            .map_err(|source| JournalError::Io { source })?;
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            return Err(JournalError::TornFinalRecord);
        }

        let mut entries = Vec::new();
        let mut event_positions = HashMap::new();
        for (offset, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let entry: JournalEntry =
                serde_json::from_slice(line).map_err(|source| JournalError::Json { source })?;
            let expected = offset
                .checked_add(1)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or(JournalError::SequenceOverflow)?;
            if entry.sequence.value() != expected {
                return Err(JournalError::NonContiguousSequence);
            }
            if event_digest(&entry.event)? != entry.content_digest {
                return Err(JournalError::DigestMismatch);
            }
            if event_positions
                .insert(entry.event.event_id(), entries.len())
                .is_some()
            {
                return Err(JournalError::DuplicateEventIdentity);
            }
            entries.push(entry);
        }
        file.seek(SeekFrom::End(0))
            .map_err(|source| JournalError::Io { source })?;
        Ok(Self {
            file,
            entries,
            event_positions,
        })
    }

    /// Append, flush, and synchronize one event, or return its identity result.
    ///
    /// # Errors
    ///
    /// Returns an error for identity conflicts, serialization, or durability
    /// failures. No successful outcome is returned before `sync_data`.
    pub fn append(&mut self, event: LiveEvent) -> Result<AppendOutcome, JournalError> {
        if let Some(position) = self.event_positions.get(&event.event_id()) {
            let existing = self.entries[*position].clone();
            return if existing.event == event {
                Ok(AppendOutcome::Duplicate(existing))
            } else {
                Err(JournalError::EventIdentityConflict)
            };
        }
        let next = self
            .entries
            .len()
            .checked_add(1)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(JournalError::SequenceOverflow)?;
        let entry = JournalEntry {
            sequence: JournalSequence::new(next)?,
            content_digest: event_digest(&event)?,
            event,
        };
        let mut encoded =
            serde_json::to_vec(&entry).map_err(|source| JournalError::Json { source })?;
        encoded.push(b'\n');
        self.file
            .write_all(&encoded)
            .and_then(|()| self.file.flush())
            .and_then(|()| self.file.sync_data())
            .map_err(|source| JournalError::Io { source })?;
        self.event_positions
            .insert(entry.event.event_id(), self.entries.len());
        self.entries.push(entry.clone());
        Ok(AppendOutcome::Appended(entry))
    }

    /// Snapshot entries after a typed replay cursor.
    #[must_use]
    pub fn replay(&self, cursor: ReplayCursor) -> Vec<JournalEntry> {
        self.entries
            .iter()
            .filter(|entry| match cursor {
                ReplayCursor::Beginning => true,
                ReplayCursor::After(sequence) => entry.sequence > sequence,
            })
            .cloned()
            .collect()
    }
}

impl std::fmt::Debug for AppendOnlyJournal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppendOnlyJournal")
            .field("durable_entries", &self.entries.len())
            .finish_non_exhaustive()
    }
}

fn event_digest(event: &LiveEvent) -> Result<PayloadDigest, JournalError> {
    let encoded = serde_json::to_vec(event).map_err(|source| JournalError::Json { source })?;
    Ok(PayloadDigest::hash(&encoded))
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use harness_graph_domain::{ActivityKind, ActivityStatus};

    use super::{
        AppendOnlyJournal, AppendOutcome, JournalError, JournalPath, LiveEvent, LiveEventId,
        LiveEventPayload, LiveOccurredAt, LiveSessionId, ReplayCursor,
    };

    #[test]
    fn append_duplicate_and_reopen_preserve_one_logical_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = JournalPath::new(directory.path().join("live.jsonl"))?;
        let first = event(
            "019d2a40-7324-77a2-832c-f5f9f84473a0",
            LiveEventPayload::SessionStarted,
        )?;
        let second = event(
            "019d2a40-7324-77a2-832c-f5f9f84473a1",
            LiveEventPayload::ActivityObserved {
                kind: ActivityKind::Inspect,
                status: ActivityStatus::Succeeded,
            },
        )?;

        let mut journal = AppendOnlyJournal::open(&path)?;
        assert!(matches!(
            journal.append(first.clone())?,
            AppendOutcome::Appended(_)
        ));
        assert!(matches!(
            journal.append(first)?,
            AppendOutcome::Duplicate(_)
        ));
        let second_outcome = journal.append(second)?;
        assert_eq!(second_outcome.entry().sequence().value(), 2);
        drop(journal);

        let reopened = AppendOnlyJournal::open(&path)?;
        let replay = reopened.replay(ReplayCursor::Beginning);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].sequence().value(), 1);
        assert_eq!(replay[1].sequence().value(), 2);
        Ok(())
    }

    #[test]
    fn reused_event_identity_with_different_content_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = JournalPath::new(directory.path().join("live.jsonl"))?;
        let id = "019d2a40-7324-77a2-832c-f5f9f84473a2";
        let mut journal = AppendOnlyJournal::open(&path)?;
        journal.append(event(id, LiveEventPayload::SessionStarted)?)?;

        assert!(matches!(
            journal.append(event(id, LiveEventPayload::SessionFailed)?),
            Err(JournalError::EventIdentityConflict)
        ));
        Ok(())
    }

    #[test]
    fn torn_final_record_blocks_replay() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let raw_path = directory.path().join("live.jsonl");
        std::fs::File::create(&raw_path)?.write_all(b"{\"sequence\":1}")?;
        let path = JournalPath::new(raw_path)?;

        assert!(matches!(
            AppendOnlyJournal::open(&path),
            Err(JournalError::TornFinalRecord)
        ));
        Ok(())
    }

    fn event(id: &str, payload: LiveEventPayload) -> Result<LiveEvent, Box<dyn std::error::Error>> {
        Ok(LiveEvent::new(
            LiveEventId::parse(id)?,
            LiveSessionId::new("ses_harness_graph_e2e")?,
            LiveOccurredAt::parse("2026-07-18T12:00:00Z")?,
            payload,
        ))
    }
}
