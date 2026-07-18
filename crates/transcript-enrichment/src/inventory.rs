//! Typed dry-run estimation and cross-session inventory aggregation.

use harness_graph_domain::{RecordCount, TokenCount};

use crate::{
    EstimatedTokenCount, RedactionCounts, TranscriptByteCount, TranscriptInventory,
    TranscriptPreparation,
};

/// Count of verified sessions in a dry-run settlement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionCount(u64);

impl SessionCount {
    fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    /// Numeric session count.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Estimated number of stateless provider requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ModelRequestCount(u64);

impl ModelRequestCount {
    fn from_chunks(chunks: RecordCount) -> Self {
        Self(chunks.value())
    }

    fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    /// Numeric request count.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Integer millionths of one US dollar for source-safe deterministic costing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MicroUsd(u64);

impl MicroUsd {
    /// Construct an exact integer micro-USD amount.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    /// Numeric micro-USD amount.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Exact micro-USD rate per one million tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRatePerMillion(MicroUsd);

impl TokenRatePerMillion {
    /// Construct a pricing rate from integer micro-USD.
    #[must_use]
    pub const fn new(rate: MicroUsd) -> Self {
        Self(rate)
    }
}

/// Operator-supplied model pricing snapshot used only for dry-run estimates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptTokenPricing {
    input: TokenRatePerMillion,
    output: TokenRatePerMillion,
}

impl TranscriptTokenPricing {
    /// Construct exact input/output pricing rates.
    #[must_use]
    pub const fn new(input: TokenRatePerMillion, output: TokenRatePerMillion) -> Self {
        Self { input, output }
    }

    /// Calculate the exact rounded-up cost for provider-reported usage.
    #[must_use]
    pub fn cost(self, input: TokenCount, output: TokenCount) -> MicroUsd {
        cost_for_token_value(input.value(), self.input)
            .saturating_add(cost_for_token_value(output.value(), self.output))
    }
}

/// Bounded output-token estimate applied to each chunk extraction request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EstimatedOutputTokensPerRequest(u64);

impl EstimatedOutputTokensPerRequest {
    /// Construct an estimated provider output size.
    ///
    /// # Errors
    ///
    /// Returns an error outside one through 128K tokens.
    pub const fn new(value: u64) -> Result<Self, crate::TranscriptEnrichmentError> {
        if value == 0 || value > 128 * 1024 {
            Err(crate::TranscriptEnrichmentError::InvalidChunkBound {
                field: "estimated output tokens per request",
            })
        } else {
            Ok(Self(value))
        }
    }
}

/// Eligible, metadata-only, or blocked dry-run disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptInventoryDisposition {
    /// Provider calls would be allowed after all preflights.
    Eligible,
    /// No transcript text exists in the selected scope.
    MetadataOnly,
    /// A local scanner or resource gate blocked provider transfer.
    Blocked,
}

/// Source-safe estimate for one verified session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptSessionEstimate {
    disposition: TranscriptInventoryDisposition,
    inventory: TranscriptInventory,
    chunks: RecordCount,
    requests: ModelRequestCount,
    estimated_input_tokens: EstimatedTokenCount,
    estimated_output_tokens: EstimatedTokenCount,
    estimated_cost: MicroUsd,
}

impl TranscriptSessionEstimate {
    /// Disposition after local checks.
    #[must_use]
    pub const fn disposition(&self) -> TranscriptInventoryDisposition {
        self.disposition
    }

    /// Source-safe scan inventory.
    #[must_use]
    pub const fn inventory(&self) -> &TranscriptInventory {
        &self.inventory
    }

    /// Estimated bounded map chunks.
    #[must_use]
    pub const fn chunk_count(&self) -> RecordCount {
        self.chunks
    }

    /// Estimated chunk extraction requests.
    #[must_use]
    pub const fn request_count(&self) -> ModelRequestCount {
        self.requests
    }

    /// Estimated provider input tokens across chunk extraction requests.
    #[must_use]
    pub const fn estimated_input_tokens(&self) -> EstimatedTokenCount {
        self.estimated_input_tokens
    }

    /// Estimated provider output tokens.
    #[must_use]
    pub const fn estimated_output_tokens(&self) -> EstimatedTokenCount {
        self.estimated_output_tokens
    }

    /// Estimated total cost in integer micro-USD.
    #[must_use]
    pub const fn estimated_cost(&self) -> MicroUsd {
        self.estimated_cost
    }
}

/// Deterministic estimator parameterized by an explicit pricing snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptInventoryEstimator {
    pricing: TranscriptTokenPricing,
    output_per_request: EstimatedOutputTokensPerRequest,
}

impl TranscriptInventoryEstimator {
    /// Construct an estimator without hidden model pricing globals.
    #[must_use]
    pub const fn new(
        pricing: TranscriptTokenPricing,
        output_per_request: EstimatedOutputTokensPerRequest,
    ) -> Self {
        Self {
            pricing,
            output_per_request,
        }
    }

    /// Estimate calls, tokens, and cost for one prepared local outcome.
    #[must_use]
    pub fn estimate(&self, preparation: &TranscriptPreparation) -> TranscriptSessionEstimate {
        let (disposition, inventory, chunks, map_input) = match preparation {
            TranscriptPreparation::Prepared(prepared) => (
                TranscriptInventoryDisposition::Eligible,
                prepared.inventory().clone(),
                prepared.chunks().count(),
                prepared.chunks().estimated_tokens(),
            ),
            TranscriptPreparation::MetadataOnly(inventory) => (
                TranscriptInventoryDisposition::MetadataOnly,
                inventory.clone(),
                RecordCount::default(),
                EstimatedTokenCount::default(),
            ),
            TranscriptPreparation::Blocked(blocked) => (
                TranscriptInventoryDisposition::Blocked,
                blocked.inventory().clone(),
                RecordCount::default(),
                EstimatedTokenCount::default(),
            ),
        };
        let requests = if disposition == TranscriptInventoryDisposition::Eligible {
            ModelRequestCount::from_chunks(chunks)
        } else {
            ModelRequestCount::default()
        };
        let output_tokens = EstimatedTokenCount::from_estimate(
            requests.value().saturating_mul(self.output_per_request.0),
        );
        let input_tokens = map_input;
        let estimated_cost = cost_for_tokens(input_tokens, self.pricing.input)
            .saturating_add(cost_for_tokens(output_tokens, self.pricing.output));
        TranscriptSessionEstimate {
            disposition,
            inventory,
            chunks,
            requests,
            estimated_input_tokens: input_tokens,
            estimated_output_tokens: output_tokens,
            estimated_cost,
        }
    }
}

/// Associative source-safe inventory across an all-session dry run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TranscriptInventoryAggregate {
    sessions: SessionCount,
    eligible_sessions: SessionCount,
    metadata_only_sessions: SessionCount,
    blocked_sessions: SessionCount,
    total_records: RecordCount,
    sanitized_fragments: RecordCount,
    sanitized_bytes: TranscriptByteCount,
    redaction_counts: RedactionCounts,
    chunks: RecordCount,
    requests: ModelRequestCount,
    estimated_input_tokens: EstimatedTokenCount,
    estimated_output_tokens: EstimatedTokenCount,
    estimated_cost: MicroUsd,
}

impl TranscriptInventoryAggregate {
    /// Associatively add one per-session estimate.
    pub fn include(&mut self, estimate: &TranscriptSessionEstimate) {
        self.sessions.increment();
        match estimate.disposition {
            TranscriptInventoryDisposition::Eligible => self.eligible_sessions.increment(),
            TranscriptInventoryDisposition::MetadataOnly => {
                self.metadata_only_sessions.increment();
            }
            TranscriptInventoryDisposition::Blocked => self.blocked_sessions.increment(),
        }
        self.total_records = add_records(self.total_records, estimate.inventory.total_records());
        self.sanitized_fragments = add_records(
            self.sanitized_fragments,
            estimate.inventory.sanitized_fragments(),
        );
        self.sanitized_bytes = TranscriptByteCount::from_estimate(
            self.sanitized_bytes
                .value()
                .saturating_add(estimate.inventory.sanitized_bytes().value()),
        );
        self.redaction_counts
            .merge(estimate.inventory.redaction_counts());
        self.chunks = add_records(self.chunks, estimate.chunks);
        self.requests = self.requests.saturating_add(estimate.requests);
        self.estimated_input_tokens = self
            .estimated_input_tokens
            .saturating_add(estimate.estimated_input_tokens);
        self.estimated_output_tokens = self
            .estimated_output_tokens
            .saturating_add(estimate.estimated_output_tokens);
        self.estimated_cost = self.estimated_cost.saturating_add(estimate.estimated_cost);
    }

    /// All scanned sessions.
    #[must_use]
    pub const fn sessions(&self) -> SessionCount {
        self.sessions
    }

    /// Sessions eligible for provider calls.
    #[must_use]
    pub const fn eligible_sessions(&self) -> SessionCount {
        self.eligible_sessions
    }

    /// Sessions with no eligible transcript content.
    #[must_use]
    pub const fn metadata_only_sessions(&self) -> SessionCount {
        self.metadata_only_sessions
    }

    /// Sessions blocked by local gates.
    #[must_use]
    pub const fn blocked_sessions(&self) -> SessionCount {
        self.blocked_sessions
    }

    /// Total verified source records.
    #[must_use]
    pub const fn total_records(&self) -> RecordCount {
        self.total_records
    }

    /// Total approved fragments.
    #[must_use]
    pub const fn sanitized_fragments(&self) -> RecordCount {
        self.sanitized_fragments
    }

    /// Total approved transient bytes.
    #[must_use]
    pub const fn sanitized_bytes(&self) -> TranscriptByteCount {
        self.sanitized_bytes
    }

    /// Aggregate fixed-shape redaction counts.
    #[must_use]
    pub const fn redaction_counts(&self) -> &RedactionCounts {
        &self.redaction_counts
    }

    /// Estimated chunks.
    #[must_use]
    pub const fn chunks(&self) -> RecordCount {
        self.chunks
    }

    /// Estimated provider requests.
    #[must_use]
    pub const fn requests(&self) -> ModelRequestCount {
        self.requests
    }

    /// Estimated provider input tokens.
    #[must_use]
    pub const fn estimated_input_tokens(&self) -> EstimatedTokenCount {
        self.estimated_input_tokens
    }

    /// Estimated provider output tokens.
    #[must_use]
    pub const fn estimated_output_tokens(&self) -> EstimatedTokenCount {
        self.estimated_output_tokens
    }

    /// Estimated cost in integer micro-USD.
    #[must_use]
    pub const fn estimated_cost(&self) -> MicroUsd {
        self.estimated_cost
    }
}

fn cost_for_tokens(tokens: EstimatedTokenCount, rate: TokenRatePerMillion) -> MicroUsd {
    cost_for_token_value(tokens.value(), rate)
}

fn cost_for_token_value(tokens: u64, rate: TokenRatePerMillion) -> MicroUsd {
    let numerator = u128::from(tokens).saturating_mul(u128::from(rate.0.value()));
    let rounded = numerator.saturating_add(999_999) / 1_000_000;
    MicroUsd::new(u64::try_from(rounded).unwrap_or(u64::MAX))
}

fn add_records(left: RecordCount, right: RecordCount) -> RecordCount {
    RecordCount::new(left.value().saturating_add(right.value()))
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::RecordCount;

    use super::{MicroUsd, ModelRequestCount, TokenRatePerMillion, cost_for_tokens};
    use crate::EstimatedTokenCount;

    #[test]
    fn provider_request_count_equals_chunk_count_at_cardinality_boundaries() {
        for chunk_count in [0, 1, 2, u64::MAX] {
            assert_eq!(
                ModelRequestCount::from_chunks(RecordCount::new(chunk_count)).value(),
                chunk_count
            );
        }
    }

    #[test]
    fn integer_cost_rounds_up_without_floating_point() {
        let cost = cost_for_tokens(
            EstimatedTokenCount::from_estimate(500_001),
            TokenRatePerMillion::new(MicroUsd::new(2_000_000)),
        );
        assert_eq!(cost.value(), 1_000_002);
    }
}
