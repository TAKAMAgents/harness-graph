//! Typed domain objects for `HarnessGraph`.

mod digest;
mod error;
mod observation;
mod value;

pub use digest::{ContextDigest, PayloadDigest, SourceDigest};
pub use error::DomainError;
pub use observation::{
    CallAssociation, ContextAssociation, DecodedNativeRecord, KnownNativeRecord, Observation,
    ObservationKind, SourceRecordRef, ToolAssociation, ToolCallLifecycle, TurnAssociation,
    UnsupportedNativeRecord,
};
pub use value::{
    NativeCallId, NativeRecordKind, OccurredAt, RecordCount, RecordSequence, SessionId, ToolName,
    TurnId,
};
