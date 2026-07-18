//! Deterministic transcript chunking and preparation.

use std::fmt;

use harness_graph_domain::{
    CallAssociation, RecordCount, SourceRecordRef, ToolAssociation, TurnAssociation,
};
use harness_graph_ingestion::{
    MaxSourceRecordBytes, TranscriptProjectionStream, VerifiedSessionBundle,
};
use harness_graph_protocol::{
    SensitiveTranscriptFragment, TranscriptFieldPath, TranscriptRecordClass,
    TranscriptRecordProjection, TranscriptRole,
};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};

use crate::{
    ChunkingPolicyVersion, DisclosureAuthorization, LocalTranscriptRedactor,
    LocallySanitizedFragment, RedactionCounts, RedactionOutcome, RedactionReceipt,
    SanitizedContentDigest, TranscriptEnrichmentError,
};

const MIN_CHUNK_BYTES: usize = 256;
const MAX_CHUNK_BYTES: usize = 1024 * 1024;
const MIN_ESTIMATED_TOKENS: u64 = 64;
const MAX_ESTIMATED_TOKENS: u64 = 256 * 1024;
const DEFAULT_SESSION_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_SESSION_FRAGMENTS: usize = 100_000;
const DEFAULT_SESSION_CHUNKS: usize = 4_096;

/// Bounded UTF-8 byte count for transcript material.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TranscriptByteCount(u64);

impl TranscriptByteCount {
    fn from_usize(value: usize) -> Self {
        Self(u64::try_from(value).unwrap_or(u64::MAX))
    }

    fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    pub(crate) const fn from_estimate(value: u64) -> Self {
        Self(value)
    }

    /// Numeric byte count for reporting and transport bounds.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Conservative token estimate used before a provider reports real usage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct EstimatedTokenCount(u64);

impl EstimatedTokenCount {
    fn from_bytes(bytes: usize) -> Self {
        Self(u64::try_from(bytes.div_ceil(3)).unwrap_or(u64::MAX))
    }

    pub(crate) fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    pub(crate) const fn from_estimate(value: u64) -> Self {
        Self(value)
    }

    /// Numeric estimate. This is never provider-reported usage.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Maximum sanitized UTF-8 bytes in one provider chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkByteLimit(usize);

impl ChunkByteLimit {
    /// Validate a provider chunk byte limit.
    ///
    /// # Errors
    ///
    /// Returns an error outside 256 bytes through 1 MiB.
    pub const fn new(value: usize) -> Result<Self, TranscriptEnrichmentError> {
        if value < MIN_CHUNK_BYTES || value > MAX_CHUNK_BYTES {
            Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "chunk byte limit",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Maximum conservative token estimate in one provider chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EstimatedTokenLimit(u64);

impl EstimatedTokenLimit {
    /// Validate a provider chunk token-estimate limit.
    ///
    /// # Errors
    ///
    /// Returns an error outside 64 through 256K estimated tokens.
    pub const fn new(value: u64) -> Result<Self, TranscriptEnrichmentError> {
        if value < MIN_ESTIMATED_TOKENS || value > MAX_ESTIMATED_TOKENS {
            Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "estimated token limit",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Maximum bytes in one split transcript segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentByteLimit(usize);

impl FragmentByteLimit {
    /// Validate a split-fragment bound.
    ///
    /// # Errors
    ///
    /// Returns an error outside 256 bytes through 1 MiB.
    pub const fn new(value: usize) -> Result<Self, TranscriptEnrichmentError> {
        if value < MIN_CHUNK_BYTES || value > MAX_CHUNK_BYTES {
            Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "fragment byte limit",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Hard cap for transient sanitized text retained for one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSanitizedByteLimit(usize);

impl SessionSanitizedByteLimit {
    /// Validate a session-level sanitized-text memory bound.
    ///
    /// # Errors
    ///
    /// Returns an error outside 256 bytes through 512 MiB.
    pub const fn new(value: usize) -> Result<Self, TranscriptEnrichmentError> {
        if value < MIN_CHUNK_BYTES || value > 512 * 1024 * 1024 {
            Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "session sanitized byte limit",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Hard cap for approved fragments retained for one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionFragmentLimit(usize);

impl SessionFragmentLimit {
    /// Validate a session-level fragment count bound.
    ///
    /// # Errors
    ///
    /// Returns an error outside one through one million fragments.
    pub const fn new(value: usize) -> Result<Self, TranscriptEnrichmentError> {
        if value == 0 || value > 1_000_000 {
            Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "session fragment limit",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Hard cap for materialized chunks in one prepared session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionChunkLimit(usize);

impl SessionChunkLimit {
    /// Validate a session-level chunk count bound.
    ///
    /// # Errors
    ///
    /// Returns an error outside one through 65,536 chunks.
    pub const fn new(value: usize) -> Result<Self, TranscriptEnrichmentError> {
        if value == 0 || value > 65_536 {
            Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "session chunk limit",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Explicit hard memory and cardinality bounds for one preparation run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptPreparationLimits {
    sanitized_bytes: SessionSanitizedByteLimit,
    fragments: SessionFragmentLimit,
    chunks: SessionChunkLimit,
}

impl TranscriptPreparationLimits {
    /// Construct exact session preparation limits.
    #[must_use]
    pub const fn new(
        max_sanitized_bytes: SessionSanitizedByteLimit,
        max_fragments: SessionFragmentLimit,
        max_chunks: SessionChunkLimit,
    ) -> Self {
        Self {
            sanitized_bytes: max_sanitized_bytes,
            fragments: max_fragments,
            chunks: max_chunks,
        }
    }
}

impl Default for TranscriptPreparationLimits {
    fn default() -> Self {
        Self {
            sanitized_bytes: SessionSanitizedByteLimit(DEFAULT_SESSION_BYTES),
            fragments: SessionFragmentLimit(DEFAULT_SESSION_FRAGMENTS),
            chunks: SessionChunkLimit(DEFAULT_SESSION_CHUNKS),
        }
    }
}

/// Versioned deterministic chunk policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptChunkPolicy {
    max_chunk_bytes: ChunkByteLimit,
    max_estimated_tokens: EstimatedTokenLimit,
    max_fragment_bytes: FragmentByteLimit,
    version: ChunkingPolicyVersion,
}

impl TranscriptChunkPolicy {
    /// Construct a fully validated policy.
    ///
    /// # Errors
    ///
    /// Returns an error when a fragment bound exceeds the chunk byte bound.
    pub fn new(
        max_chunk_bytes: ChunkByteLimit,
        max_estimated_tokens: EstimatedTokenLimit,
        max_fragment_bytes: FragmentByteLimit,
        version: ChunkingPolicyVersion,
    ) -> Result<Self, TranscriptEnrichmentError> {
        if max_fragment_bytes.0 > max_chunk_bytes.0 {
            return Err(TranscriptEnrichmentError::InvalidChunkBound {
                field: "fragment bound exceeds chunk bound",
            });
        }
        Ok(Self {
            max_chunk_bytes,
            max_estimated_tokens,
            max_fragment_bytes,
            version,
        })
    }

    /// Policy version included in content-addressed chunk identity.
    #[must_use]
    pub const fn version(&self) -> &ChunkingPolicyVersion {
        &self.version
    }
}

/// Zero-based split part within one allowlisted native field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TranscriptPartIndex(u32);

impl TranscriptPartIndex {
    fn from_offset(offset: usize) -> Self {
        Self(u32::try_from(offset).unwrap_or(u32::MAX))
    }

    /// Numeric part index for graph projection.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Exact source anchor cited by one chunk segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptSpanRef {
    source: SourceRecordRef,
    field_path: TranscriptFieldPath,
    part_index: TranscriptPartIndex,
}

impl TranscriptSpanRef {
    /// Source record.
    #[must_use]
    pub const fn source(&self) -> &SourceRecordRef {
        &self.source
    }

    /// Allowlisted source field.
    #[must_use]
    pub const fn field_path(&self) -> TranscriptFieldPath {
        self.field_path
    }

    /// Split part index.
    #[must_use]
    pub const fn part_index(&self) -> TranscriptPartIndex {
        self.part_index
    }
}

macro_rules! identifier_digest {
    ($name:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            fn from_hasher(hasher: Sha256) -> Self {
                Self(hasher.finalize().into())
            }

            /// Lowercase hexadecimal representation.
            #[must_use]
            pub fn to_hex(self) -> String {
                hex::encode(self.0)
            }

            /// Fixed digest bytes for deterministic composition.
            #[must_use]
            pub const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_hex())
                    .finish()
            }
        }
    };
}

identifier_digest!(
    TranscriptSpanToken,
    "Opaque model-facing citation token for one exact transcript span."
);
identifier_digest!(
    TranscriptChunkId,
    "Content-addressed identity of one bounded sanitized transcript chunk."
);
identifier_digest!(
    TranscriptProjectionDigest,
    "Digest of the versioned allowlisted transcript projection."
);

impl TranscriptSpanToken {
    /// Parse a model-returned citation token.
    ///
    /// # Errors
    ///
    /// Returns an error unless the value is exactly 32 hexadecimal bytes.
    pub fn parse_hex(value: &str) -> Result<Self, TranscriptEnrichmentError> {
        let mut bytes = [0_u8; 32];
        hex::decode_to_slice(value, &mut bytes)
            .map_err(|_| TranscriptEnrichmentError::InvalidCitationToken)?;
        Ok(Self(bytes))
    }
}

/// One sanitized segment and its exact model-facing citation token.
#[derive(Clone)]
pub struct TranscriptChunkSegment {
    span: TranscriptSpanRef,
    token: TranscriptSpanToken,
    class: TranscriptRecordClass,
    role: TranscriptRole,
    turn: TurnAssociation,
    call: CallAssociation,
    tool: ToolAssociation,
    text: SecretString,
    content_digest: SanitizedContentDigest,
    bytes: TranscriptByteCount,
    estimated_tokens: EstimatedTokenCount,
}

impl TranscriptChunkSegment {
    /// Exact source span.
    #[must_use]
    pub const fn span(&self) -> &TranscriptSpanRef {
        &self.span
    }

    /// Opaque citation token supplied to Mistral.
    #[must_use]
    pub const fn citation_token(&self) -> TranscriptSpanToken {
        self.token
    }

    /// Closed semantic class.
    #[must_use]
    pub const fn class(&self) -> TranscriptRecordClass {
        self.class
    }

    /// Closed producer role.
    #[must_use]
    pub const fn role(&self) -> TranscriptRole {
        self.role
    }

    /// Native turn anchor.
    #[must_use]
    pub const fn turn(&self) -> &TurnAssociation {
        &self.turn
    }

    /// Native call anchor.
    #[must_use]
    pub const fn call(&self) -> &CallAssociation {
        &self.call
    }

    /// Native tool anchor.
    #[must_use]
    pub const fn tool(&self) -> &ToolAssociation {
        &self.tool
    }

    /// Sanitized byte count.
    #[must_use]
    pub const fn byte_count(&self) -> TranscriptByteCount {
        self.bytes
    }

    /// Conservative pre-provider token estimate.
    #[must_use]
    pub const fn estimated_token_count(&self) -> EstimatedTokenCount {
        self.estimated_tokens
    }

    /// Digest of the complete sanitized source fragment.
    #[must_use]
    pub const fn sanitized_content_digest(&self) -> SanitizedContentDigest {
        self.content_digest
    }

    /// Explicitly expose already-sanitized text for provider request assembly.
    ///
    /// Provider adapters must never log or persist the returned text.
    #[must_use]
    pub fn expose_sanitized_text_for_provider(&self) -> &str {
        self.text.expose_secret()
    }
}

impl fmt::Debug for TranscriptChunkSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TranscriptChunkSegment")
            .field("span", &self.span)
            .field("token", &self.token)
            .field("class", &self.class)
            .field("role", &self.role)
            .field("bytes", &self.bytes)
            .field("estimated_tokens", &self.estimated_tokens)
            .field("text", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// One non-empty, deterministically bounded provider chunk.
#[derive(Clone)]
pub struct BoundedTranscriptChunk {
    id: TranscriptChunkId,
    segments: Vec<TranscriptChunkSegment>,
    bytes: TranscriptByteCount,
    estimated_tokens: EstimatedTokenCount,
}

impl BoundedTranscriptChunk {
    /// Content-addressed chunk identity.
    #[must_use]
    pub const fn id(&self) -> TranscriptChunkId {
        self.id
    }

    /// Ordered sanitized segments.
    pub fn segments(&self) -> impl Iterator<Item = &TranscriptChunkSegment> {
        self.segments.iter()
    }

    /// Number of segments.
    #[must_use]
    pub fn segment_count(&self) -> RecordCount {
        RecordCount::new(u64::try_from(self.segments.len()).unwrap_or(u64::MAX))
    }

    /// Sanitized chunk byte count.
    #[must_use]
    pub const fn byte_count(&self) -> TranscriptByteCount {
        self.bytes
    }

    /// Conservative token estimate.
    #[must_use]
    pub const fn estimated_token_count(&self) -> EstimatedTokenCount {
        self.estimated_tokens
    }
}

impl fmt::Debug for BoundedTranscriptChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedTranscriptChunk")
            .field("id", &self.id)
            .field("segment_count", &self.segments.len())
            .field("bytes", &self.bytes)
            .field("estimated_tokens", &self.estimated_tokens)
            .finish()
    }
}

/// Non-empty ordered chunks for one prepared session.
#[derive(Debug, Clone)]
pub struct BoundedTranscriptChunks(Vec<BoundedTranscriptChunk>);

impl BoundedTranscriptChunks {
    /// Iterate in canonical source order.
    pub fn iter(&self) -> impl Iterator<Item = &BoundedTranscriptChunk> {
        self.0.iter()
    }

    /// Typed number of map requests expected for this session.
    #[must_use]
    pub fn count(&self) -> RecordCount {
        RecordCount::new(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }

    /// Aggregate conservative token estimate.
    #[must_use]
    pub fn estimated_tokens(&self) -> EstimatedTokenCount {
        self.0
            .iter()
            .fold(EstimatedTokenCount::default(), |sum, chunk| {
                sum.saturating_add(chunk.estimated_token_count())
            })
    }
}

/// Source-safe per-session dry-run inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptInventory {
    total_records: RecordCount,
    projected_fragments: RecordCount,
    excluded_records: RecordCount,
    scope_excluded_fragments: RecordCount,
    sanitized_fragments: RecordCount,
    sanitized_bytes: TranscriptByteCount,
    redaction_counts: RedactionCounts,
}

impl TranscriptInventory {
    /// Verified source records consumed.
    #[must_use]
    pub const fn total_records(&self) -> RecordCount {
        self.total_records
    }

    /// Allowlisted fragments projected before scope selection.
    #[must_use]
    pub const fn projected_fragments(&self) -> RecordCount {
        self.projected_fragments
    }

    /// Native records with no allowlisted transcript content.
    #[must_use]
    pub const fn excluded_records(&self) -> RecordCount {
        self.excluded_records
    }

    /// Allowlisted fragments excluded by the exact authorization scope.
    #[must_use]
    pub const fn scope_excluded_fragments(&self) -> RecordCount {
        self.scope_excluded_fragments
    }

    /// Fragments approved by mandatory scanning.
    #[must_use]
    pub const fn sanitized_fragments(&self) -> RecordCount {
        self.sanitized_fragments
    }

    /// Total sanitized bytes retained transiently for chunking.
    #[must_use]
    pub const fn sanitized_bytes(&self) -> TranscriptByteCount {
        self.sanitized_bytes
    }

    /// Aggregate fixed-shape local redaction counts.
    #[must_use]
    pub const fn redaction_counts(&self) -> &RedactionCounts {
        &self.redaction_counts
    }
}

/// Fully prepared, provider-ready transcript for one immutable session source.
#[derive(Debug, Clone)]
pub struct PreparedTranscript {
    projection_digest: TranscriptProjectionDigest,
    chunks: BoundedTranscriptChunks,
    receipts: Vec<RedactionReceipt>,
    inventory: TranscriptInventory,
}

impl PreparedTranscript {
    /// Versioned allowlist projection digest.
    #[must_use]
    pub const fn projection_digest(&self) -> TranscriptProjectionDigest {
        self.projection_digest
    }

    /// Sanitized bounded chunks.
    #[must_use]
    pub const fn chunks(&self) -> &BoundedTranscriptChunks {
        &self.chunks
    }

    /// Per-fragment scanner receipts.
    pub fn redaction_receipts(&self) -> impl Iterator<Item = &RedactionReceipt> {
        self.receipts.iter()
    }

    /// Source-safe session inventory.
    #[must_use]
    pub const fn inventory(&self) -> &TranscriptInventory {
        &self.inventory
    }
}

/// Outcome of preparing one verified source snapshot.
#[derive(Debug, Clone)]
pub enum TranscriptPreparation {
    /// At least one sanitized provider chunk is available.
    Prepared(PreparedTranscript),
    /// Verified session has no content in the selected safe scope.
    MetadataOnly(TranscriptInventory),
    /// Verified session failed a typed local privacy or resource gate.
    Blocked(TranscriptBlocked),
}

/// Closed reason why a verified session cannot enter provider processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptPreparationBlockReason {
    /// Mandatory local scanner rejected one source record.
    ScannerRejected {
        /// Source record sequence.
        sequence: harness_graph_domain::RecordSequence,
        /// Closed scanner reason.
        reason: crate::ScannerBlockReason,
    },
    /// Approved sanitized bytes exceeded the exact session bound.
    SanitizedByteLimitExceeded,
    /// Approved fragment count exceeded the exact session bound.
    FragmentLimitExceeded,
    /// Deterministic chunk count exceeded the exact session bound.
    ChunkLimitExceeded,
}

/// Source-safe blocked preparation outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlocked {
    reason: TranscriptPreparationBlockReason,
    inventory: TranscriptInventory,
}

impl TranscriptBlocked {
    /// Closed block reason.
    #[must_use]
    pub const fn reason(&self) -> TranscriptPreparationBlockReason {
        self.reason
    }

    /// Source-safe inventory collected while completing the verified scan.
    #[must_use]
    pub const fn inventory(&self) -> &TranscriptInventory {
        &self.inventory
    }
}

struct PreparationAccumulator {
    total_records: RecordCount,
    projected_fragments: RecordCount,
    excluded_records: RecordCount,
    scope_excluded_fragments: RecordCount,
    sanitized_fragments: RecordCount,
    sanitized_bytes: TranscriptByteCount,
    redaction_counts: RedactionCounts,
    approved: Vec<LocallySanitizedFragment>,
    receipts: Vec<RedactionReceipt>,
    block_reason: Option<TranscriptPreparationBlockReason>,
    projection_hasher: Sha256,
}

impl PreparationAccumulator {
    fn new(authorization: &DisclosureAuthorization) -> Self {
        let mut projection_hasher = Sha256::new();
        projection_hasher.update(b"harness-graph-transcript-projection-v1\0");
        projection_hasher.update(authorization.source_digest().to_hex().as_bytes());
        projection_hasher.update(authorization.scope().as_str().as_bytes());
        projection_hasher.update(authorization.policy_digest().bytes());
        Self {
            total_records: RecordCount::default(),
            projected_fragments: RecordCount::default(),
            excluded_records: RecordCount::default(),
            scope_excluded_fragments: RecordCount::default(),
            sanitized_fragments: RecordCount::default(),
            sanitized_bytes: TranscriptByteCount::default(),
            redaction_counts: RedactionCounts::default(),
            approved: Vec::new(),
            receipts: Vec::new(),
            block_reason: None,
            projection_hasher,
        }
    }

    fn consume(
        &mut self,
        projection: TranscriptRecordProjection,
        redactor: &LocalTranscriptRedactor,
        authorization: &DisclosureAuthorization,
        limits: TranscriptPreparationLimits,
    ) -> Result<(), TranscriptEnrichmentError> {
        self.total_records.increment();
        match projection {
            TranscriptRecordProjection::Excluded(_) => self.excluded_records.increment(),
            TranscriptRecordProjection::Eligible(fragments) => {
                for fragment in fragments.into_fragments() {
                    self.consume_fragment(&fragment, redactor, authorization, limits)?;
                }
            }
        }
        Ok(())
    }

    fn consume_fragment(
        &mut self,
        fragment: &SensitiveTranscriptFragment,
        redactor: &LocalTranscriptRedactor,
        authorization: &DisclosureAuthorization,
        limits: TranscriptPreparationLimits,
    ) -> Result<(), TranscriptEnrichmentError> {
        self.projected_fragments.increment();
        update_projection_digest(&mut self.projection_hasher, fragment);
        if self.block_reason.is_some() {
            return Ok(());
        }
        match redactor.sanitize(fragment, authorization) {
            Err(TranscriptEnrichmentError::ScannerBlocked { sequence, reason }) => {
                self.block(TranscriptPreparationBlockReason::ScannerRejected { sequence, reason });
            }
            Err(error) => return Err(error),
            Ok(RedactionOutcome::ExcludedByScope) => self.scope_excluded_fragments.increment(),
            Ok(RedactionOutcome::Approved { fragment, receipt }) => {
                self.accept(*fragment, receipt, limits);
            }
        }
        Ok(())
    }

    fn accept(
        &mut self,
        fragment: LocallySanitizedFragment,
        receipt: RedactionReceipt,
        limits: TranscriptPreparationLimits,
    ) {
        let next_bytes = self
            .sanitized_bytes
            .saturating_add(TranscriptByteCount::from_usize(
                fragment.expose_for_chunking().len(),
            ));
        if next_bytes.value() > u64::try_from(limits.sanitized_bytes.0).unwrap_or(u64::MAX) {
            self.block(TranscriptPreparationBlockReason::SanitizedByteLimitExceeded);
        } else if self.approved.len() >= limits.fragments.0 {
            self.block(TranscriptPreparationBlockReason::FragmentLimitExceeded);
        } else {
            self.sanitized_fragments.increment();
            self.sanitized_bytes = next_bytes;
            self.redaction_counts.merge(receipt.counts());
            self.approved.push(fragment);
            self.receipts.push(receipt);
        }
    }

    fn block(&mut self, reason: TranscriptPreparationBlockReason) {
        self.block_reason = Some(reason);
        self.approved.clear();
        self.receipts.clear();
    }

    fn into_preparation(
        self,
        policy: &TranscriptChunkPolicy,
        limits: TranscriptPreparationLimits,
    ) -> Result<TranscriptPreparation, TranscriptEnrichmentError> {
        let inventory = TranscriptInventory {
            total_records: self.total_records,
            projected_fragments: self.projected_fragments,
            excluded_records: self.excluded_records,
            scope_excluded_fragments: self.scope_excluded_fragments,
            sanitized_fragments: self.sanitized_fragments,
            sanitized_bytes: self.sanitized_bytes,
            redaction_counts: self.redaction_counts,
        };
        if let Some(reason) = self.block_reason {
            return Ok(blocked(reason, inventory));
        }
        if self.approved.is_empty() {
            return Ok(TranscriptPreparation::MetadataOnly(inventory));
        }
        let chunks = TranscriptChunker::new(policy.clone()).chunk(&self.approved)?;
        if chunks.0.len() > limits.chunks.0 {
            return Ok(blocked(
                TranscriptPreparationBlockReason::ChunkLimitExceeded,
                inventory,
            ));
        }
        Ok(TranscriptPreparation::Prepared(PreparedTranscript {
            projection_digest: TranscriptProjectionDigest::from_hasher(self.projection_hasher),
            chunks,
            receipts: self.receipts,
            inventory,
        }))
    }
}

fn blocked(
    reason: TranscriptPreparationBlockReason,
    inventory: TranscriptInventory,
) -> TranscriptPreparation {
    TranscriptPreparation::Blocked(TranscriptBlocked { reason, inventory })
}

/// Prepare one checksum-verified session without provider or graph mutation.
///
/// # Errors
///
/// Returns a source-safe error for authorization mismatch, bounded streaming,
/// protocol projection, mandatory scanning, or chunk policy failures.
pub fn prepare_verified_transcript(
    bundle: VerifiedSessionBundle,
    authorization: &DisclosureAuthorization,
    redactor: &LocalTranscriptRedactor,
    policy: &TranscriptChunkPolicy,
    record_limit: MaxSourceRecordBytes,
    preparation_limits: TranscriptPreparationLimits,
) -> Result<TranscriptPreparation, TranscriptEnrichmentError> {
    authorization.verify_bundle(&bundle)?;
    let mut stream = TranscriptProjectionStream::open(bundle, record_limit)?;
    let mut accumulator = PreparationAccumulator::new(authorization);
    for record in stream.by_ref() {
        accumulator.consume(record?, redactor, authorization, preparation_limits)?;
    }
    stream.finish()?;
    accumulator.into_preparation(policy, preparation_limits)
}

/// Pure deterministic chunker over locally sanitized fragments.
#[derive(Debug, Clone)]
pub struct TranscriptChunker {
    policy: TranscriptChunkPolicy,
}

impl TranscriptChunker {
    /// Construct a chunker from a validated policy.
    #[must_use]
    pub const fn new(policy: TranscriptChunkPolicy) -> Self {
        Self { policy }
    }

    /// Split and pack sanitized fragments while preserving source order.
    ///
    /// # Errors
    ///
    /// Returns an error only when no fragment is supplied.
    pub fn chunk(
        &self,
        fragments: &[LocallySanitizedFragment],
    ) -> Result<BoundedTranscriptChunks, TranscriptEnrichmentError> {
        if fragments.is_empty() {
            return Err(TranscriptEnrichmentError::NoEligibleTranscript);
        }
        let segment_byte_limit = self
            .policy
            .max_fragment_bytes
            .0
            .min(self.policy.max_chunk_bytes.0)
            .min(
                usize::try_from(self.policy.max_estimated_tokens.0.saturating_mul(3))
                    .unwrap_or(usize::MAX),
            );
        let mut segments = Vec::new();
        for fragment in fragments {
            for (part_offset, part) in
                split_sanitized_text(fragment.expose_for_chunking(), segment_byte_limit)
                    .into_iter()
                    .enumerate()
            {
                segments.push(make_segment(
                    fragment,
                    TranscriptPartIndex::from_offset(part_offset),
                    part,
                ));
            }
        }
        let mut chunks = Vec::new();
        let mut current = Vec::new();
        let mut current_bytes = TranscriptByteCount::default();
        let mut current_tokens = EstimatedTokenCount::default();
        let mut current_turn: Option<TurnAssociation> = None;
        for segment in segments {
            let next_bytes = current_bytes.saturating_add(segment.byte_count());
            let next_tokens = current_tokens.saturating_add(segment.estimated_token_count());
            let changes_turn = current_turn
                .as_ref()
                .is_some_and(|turn| turn != segment.turn());
            let prefers_boundary = changes_turn
                && current_bytes.value()
                    >= u64::try_from(self.policy.max_chunk_bytes.0 / 2).unwrap_or(u64::MAX);
            let exceeds = next_bytes.value()
                > u64::try_from(self.policy.max_chunk_bytes.0).unwrap_or(u64::MAX)
                || next_tokens.value() > self.policy.max_estimated_tokens.0;
            if !current.is_empty() && (exceeds || prefers_boundary) {
                chunks.push(build_chunk(
                    std::mem::take(&mut current),
                    &self.policy.version,
                ));
                current_bytes = TranscriptByteCount::default();
                current_tokens = EstimatedTokenCount::default();
            }
            current_turn = Some(segment.turn().clone());
            current_bytes = current_bytes.saturating_add(segment.byte_count());
            current_tokens = current_tokens.saturating_add(segment.estimated_token_count());
            current.push(segment);
        }
        if !current.is_empty() {
            chunks.push(build_chunk(current, &self.policy.version));
        }
        Ok(BoundedTranscriptChunks(chunks))
    }
}

fn update_projection_digest(
    hasher: &mut Sha256,
    fragment: &harness_graph_protocol::SensitiveTranscriptFragment,
) {
    hasher.update(fragment.source().source_digest().to_hex().as_bytes());
    hasher.update(fragment.source().sequence().value().to_le_bytes());
    hasher.update(fragment.field_path().field().as_str().as_bytes());
    hasher.update(fragment.field_path().ordinal().to_le_bytes());
    hasher.update(fragment.class().as_str().as_bytes());
    hasher.update(fragment.role().as_str().as_bytes());
    hasher.update(fragment.expose_for_local_scanner().as_bytes());
}

fn make_segment(
    fragment: &LocallySanitizedFragment,
    part_index: TranscriptPartIndex,
    text: &str,
) -> TranscriptChunkSegment {
    let span = TranscriptSpanRef {
        source: fragment.source().clone(),
        field_path: fragment.field_path(),
        part_index,
    };
    let mut hasher = Sha256::new();
    hasher.update(b"harness-graph-transcript-span-v1\0");
    hasher.update(span.source.source_digest().to_hex().as_bytes());
    hasher.update(span.source.sequence().value().to_le_bytes());
    hasher.update(span.field_path.field().as_str().as_bytes());
    hasher.update(span.field_path.ordinal().to_le_bytes());
    hasher.update(span.part_index.value().to_le_bytes());
    let bytes = TranscriptByteCount::from_usize(text.len());
    TranscriptChunkSegment {
        span,
        token: TranscriptSpanToken::from_hasher(hasher),
        class: fragment.class(),
        role: fragment.role(),
        turn: fragment.turn().clone(),
        call: fragment.call().clone(),
        tool: fragment.tool().clone(),
        text: SecretString::from(text.to_owned()),
        content_digest: fragment.digest(),
        bytes,
        estimated_tokens: EstimatedTokenCount::from_bytes(text.len()),
    }
}

fn build_chunk(
    segments: Vec<TranscriptChunkSegment>,
    version: &ChunkingPolicyVersion,
) -> BoundedTranscriptChunk {
    let mut hasher = Sha256::new();
    hasher.update(b"harness-graph-transcript-chunk-v1\0");
    hasher.update(version.as_str().as_bytes());
    let mut bytes = TranscriptByteCount::default();
    let mut estimated_tokens = EstimatedTokenCount::default();
    for segment in &segments {
        hasher.update(segment.citation_token().0);
        hasher.update(segment.content_digest.bytes());
        bytes = bytes.saturating_add(segment.byte_count());
        estimated_tokens = estimated_tokens.saturating_add(segment.estimated_token_count());
    }
    BoundedTranscriptChunk {
        id: TranscriptChunkId::from_hasher(hasher),
        segments,
        bytes,
        estimated_tokens,
    }
}

fn split_sanitized_text(text: &str, limit: usize) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut remaining = text;
    while remaining.len() > limit {
        let candidate = &remaining[..floor_char_boundary(remaining, limit)];
        let boundary = candidate
            .rfind("\n\n")
            .map(|index| index + 2)
            .or_else(|| candidate.rfind('\n').map(|index| index + 1))
            .or_else(|| {
                candidate
                    .char_indices()
                    .rev()
                    .find(|(_, character)| character.is_whitespace())
                    .map(|(index, character)| index + character.len_utf8())
            })
            .filter(|boundary| *boundary != 0)
            .unwrap_or(candidate.len());
        let (part, rest) = remaining.split_at(boundary);
        parts.push(part);
        remaining = rest;
    }
    if !remaining.is_empty() {
        parts.push(remaining);
    }
    parts
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while !value.is_char_boundary(index) {
        index = index.saturating_sub(1);
    }
    index
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{RecordSequence, SessionId, SourceDigest, SourceRecordRef};
    use harness_graph_protocol::{TranscriptRecordProjection, project_codex_transcript_line};

    use super::{
        ChunkByteLimit, EstimatedTokenLimit, FragmentByteLimit, TranscriptChunkPolicy,
        TranscriptChunker,
    };
    use crate::{
        AuthorizationIdentity, AuthorizationPolicyDigest, ChunkingPolicyVersion,
        DisclosureAuthorization, LocalTranscriptRedactor, PseudonymizationKey, RedactionOutcome,
        RedactionPolicyVersion, SensitiveValueSet, TranscriptDisclosureScope,
    };

    #[test]
    fn unicode_chunking_is_bounded_deterministic_and_citation_complete()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = SourceRecordRef::new(
            SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?,
            SourceDigest::hash(b"fixture"),
            RecordSequence::from_zero_based(0),
        );
        let message = format!("paragraph 🦀\n\n{}", "semantic text ".repeat(80));
        let line = format!(
            r#"{{"timestamp":"2026-07-18T12:00:00Z","type":"event_msg","payload":{{"type":"user_message","message":{}}}}}"#,
            serde_json::to_string(&message)?
        );
        let TranscriptRecordProjection::Eligible(fragments) =
            project_codex_transcript_line(&line, source.clone())?
        else {
            return Err("fixture excluded".into());
        };
        let authorization = DisclosureAuthorization::new(
            source.session_id(),
            source.source_digest(),
            TranscriptDisclosureScope::ConversationAndExecution,
            AuthorizationPolicyDigest::hash(b"policy"),
            AuthorizationIdentity::new("test")?,
            harness_graph_domain::OccurredAt::parse("2026-07-18T12:00:00Z")?,
        );
        let redactor = LocalTranscriptRedactor::new(
            RedactionPolicyVersion::new("r1")?,
            PseudonymizationKey::new("0123456789abcdef0123456789abcdef")?,
            SensitiveValueSet::default(),
        )?;
        let mut approved = Vec::new();
        for fragment in fragments.into_fragments() {
            let RedactionOutcome::Approved { fragment, .. } =
                redactor.sanitize(&fragment, &authorization)?
            else {
                return Err("fragment outside scope".into());
            };
            approved.push(*fragment);
        }
        let policy = TranscriptChunkPolicy::new(
            ChunkByteLimit::new(512)?,
            EstimatedTokenLimit::new(256)?,
            FragmentByteLimit::new(384)?,
            ChunkingPolicyVersion::new("c1")?,
        )?;
        let chunker = TranscriptChunker::new(policy);
        let first = chunker.chunk(&approved)?;
        let second = chunker.chunk(&approved)?;
        assert_eq!(first.count(), second.count());
        let first_ids: Vec<_> = first
            .iter()
            .map(super::BoundedTranscriptChunk::id)
            .collect();
        let second_ids: Vec<_> = second
            .iter()
            .map(super::BoundedTranscriptChunk::id)
            .collect();
        assert_eq!(first_ids, second_ids);
        for chunk in first.iter() {
            assert!(chunk.byte_count().value() <= 512);
            assert!(chunk.estimated_token_count().value() <= 256);
            for segment in chunk.segments() {
                assert!(
                    std::str::from_utf8(segment.expose_sanitized_text_for_provider().as_bytes())
                        .is_ok()
                );
            }
        }
        Ok(())
    }
}
