//! Validated scalar domain values.

use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::DomainError;

/// Stable Codex session identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    /// Parse a UUID session identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the input is not a valid UUID.
    pub fn parse(value: &str) -> Result<Self, DomainError> {
        Uuid::parse_str(value)
            .map(Self)
            .map_err(|source| DomainError::InvalidSessionId { source })
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// One-based record sequence within a canonical source snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RecordSequence(u64);

impl RecordSequence {
    /// Construct a sequence from a zero-based iterator offset.
    #[must_use]
    pub const fn from_zero_based(offset: u64) -> Self {
        Self(offset + 1)
    }

    /// Return the one-based numeric sequence.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Count of records in an ingestion boundary or receipt.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RecordCount(u64);

impl RecordCount {
    /// Construct a record count.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Increment the count by one.
    pub fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    /// Return the numeric count.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Timestamp at which a native observation occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OccurredAt(OffsetDateTime);

impl OccurredAt {
    /// Parse an RFC 3339 timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when the timestamp does not conform to RFC 3339.
    pub fn parse(value: &str) -> Result<Self, DomainError> {
        OffsetDateTime::parse(value, &Rfc3339)
            .map(Self)
            .map_err(|source| DomainError::InvalidOccurredAt { source })
    }

    /// Format as RFC 3339.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal timestamp cannot be formatted.
    pub fn to_rfc3339(self) -> Result<String, time::error::Format> {
        self.0.format(&Rfc3339)
    }
}

macro_rules! non_empty_string_type {
    ($name:ident, $field:literal) => {
        #[doc = concat!("Validated ", $field, ".")]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Construct a validated ", $field, ".")]
            ///
            /// # Errors
            ///
            /// Returns an error when the input is empty after trimming.
            pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err(DomainError::EmptyValue { field: $field });
                }
                Ok(Self(trimmed.to_owned()))
            }

            /// Borrow the validated value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

non_empty_string_type!(NativeCallId, "native call identifier");
non_empty_string_type!(NativeRecordKind, "native record kind");
non_empty_string_type!(ToolName, "tool name");
non_empty_string_type!(TurnId, "turn identifier");
