//! Immutable semantic run configuration and content-addressed planning.

use harness_graph_domain::{GraphNamespace, SessionId, SourceDigest};
use harness_graph_graph_port::{
    ChunkingPolicyVersion as GraphChunkingPolicyVersion, EnrichmentChunkCount,
    EnrichmentFingerprint, EnrichmentProvider, EnrichmentRunId, EnrichmentRunRef,
    EnrichmentRunSpec, RedactionPolicyVersion as GraphRedactionPolicyVersion,
};
pub use harness_graph_graph_port::{EnrichmentModelName, EnrichmentSchemaVersion, PromptVersion};
use harness_graph_transcript_enrichment::{
    AuthorizationPolicyDigest, ChunkingPolicyVersion, PreparedTranscript, RedactionPolicyVersion,
    TranscriptDisclosureScope,
};
use sha2::{Digest, Sha256};

use crate::{EnrichmentApplicationError, RunConfigurationField};

/// Digest of the exact prompt body named by [`PromptVersion`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EnrichmentPromptDigest([u8; 32]);

impl EnrichmentPromptDigest {
    /// Hash the immutable provider prompt body without retaining its text.
    #[must_use]
    pub fn hash(prompt: &[u8]) -> Self {
        Self(Sha256::digest(prompt).into())
    }

    /// Lowercase hexadecimal representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        encode_hex(self.0)
    }

    fn bytes(self) -> [u8; 32] {
        self.0
    }
}

impl std::fmt::Debug for EnrichmentPromptDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("EnrichmentPromptDigest")
            .field(&encode_hex(self.0))
            .finish()
    }
}

/// Validated maximum number of simultaneous cost-bearing chunk extractions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtractionConcurrency(usize);

impl ExtractionConcurrency {
    /// Validate the plan's bounded one-to-four provider concurrency.
    ///
    /// # Errors
    ///
    /// Returns an error outside `1..=4`.
    pub fn new(value: usize) -> Result<Self, EnrichmentApplicationError> {
        if (1..=4).contains(&value) {
            Ok(Self(value))
        } else {
            Err(EnrichmentApplicationError::InvalidRunConfiguration {
                field: RunConfigurationField::ExtractionConcurrency,
            })
        }
    }

    pub(crate) const fn value(self) -> usize {
        self.0
    }
}

/// Immutable semantic inputs that determine an enrichment run identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichmentRunConfiguration {
    namespace: GraphNamespace,
    session_id: SessionId,
    source_digest: SourceDigest,
    disclosure_scope: TranscriptDisclosureScope,
    authorization_policy_digest: AuthorizationPolicyDigest,
    redaction_version: RedactionPolicyVersion,
    graph_redaction_version: GraphRedactionPolicyVersion,
    chunking_version: ChunkingPolicyVersion,
    graph_chunking_version: GraphChunkingPolicyVersion,
    model: EnrichmentModelName,
    prompt_version: PromptVersion,
    prompt_digest: EnrichmentPromptDigest,
    schema_version: EnrichmentSchemaVersion,
}

impl EnrichmentRunConfiguration {
    /// Construct exact source, disclosure, policy, and model provenance.
    ///
    /// # Errors
    ///
    /// Returns a source-safe configuration error when transcript policy
    /// versions cannot be represented by the graph contract.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace: GraphNamespace,
        session_id: SessionId,
        source_digest: SourceDigest,
        disclosure_scope: TranscriptDisclosureScope,
        authorization_policy_digest: AuthorizationPolicyDigest,
        redaction_version: RedactionPolicyVersion,
        chunking_version: ChunkingPolicyVersion,
        model: EnrichmentModelName,
        prompt_version: PromptVersion,
        prompt_digest: EnrichmentPromptDigest,
        schema_version: EnrichmentSchemaVersion,
    ) -> Result<Self, EnrichmentApplicationError> {
        let graph_redaction_version = GraphRedactionPolicyVersion::new(
            redaction_version.as_str().to_owned(),
        )
        .map_err(|_| EnrichmentApplicationError::InvalidRunConfiguration {
            field: RunConfigurationField::RedactionPolicyVersion,
        })?;
        let graph_chunking_version = GraphChunkingPolicyVersion::new(
            chunking_version.as_str().to_owned(),
        )
        .map_err(|_| EnrichmentApplicationError::InvalidRunConfiguration {
            field: RunConfigurationField::ChunkingPolicyVersion,
        })?;
        Ok(Self {
            namespace,
            session_id,
            source_digest,
            disclosure_scope,
            authorization_policy_digest,
            redaction_version,
            graph_redaction_version,
            chunking_version,
            graph_chunking_version,
            model,
            prompt_version,
            prompt_digest,
            schema_version,
        })
    }

    /// Graph namespace isolating the run.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }

    /// Authorized source session.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Authorized verified source snapshot.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Exact disclosure scope included in the fingerprint.
    #[must_use]
    pub const fn disclosure_scope(&self) -> TranscriptDisclosureScope {
        self.disclosure_scope
    }

    /// Operator-reviewed policy digest included in the fingerprint.
    #[must_use]
    pub const fn authorization_policy_digest(&self) -> AuthorizationPolicyDigest {
        self.authorization_policy_digest
    }

    /// Mistral model provenance.
    #[must_use]
    pub const fn model(&self) -> &EnrichmentModelName {
        &self.model
    }

    /// Versioned prompt provenance.
    #[must_use]
    pub const fn prompt_version(&self) -> &PromptVersion {
        &self.prompt_version
    }

    /// Digest of the exact prompt body.
    #[must_use]
    pub const fn prompt_digest(&self) -> EnrichmentPromptDigest {
        self.prompt_digest
    }

    /// Structured-output schema provenance.
    #[must_use]
    pub const fn schema_version(&self) -> &EnrichmentSchemaVersion {
        &self.schema_version
    }
}

/// Content-addressed run specification plus its stable mutation reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedEnrichmentRun {
    specification: EnrichmentRunSpec,
    reference: EnrichmentRunRef,
}

impl PlannedEnrichmentRun {
    /// Immutable graph run specification.
    #[must_use]
    pub const fn specification(&self) -> &EnrichmentRunSpec {
        &self.specification
    }

    /// Source-bound stable run reference.
    #[must_use]
    pub const fn reference(&self) -> &EnrichmentRunRef {
        &self.reference
    }
}

/// Derive an exact run identity from prepared input and semantic configuration.
///
/// The ordered chunk identities are included in addition to the plan-required
/// projection and policy fields. This makes pseudonym-key rotation visible even
/// if an operator accidentally fails to bump the redaction policy version.
///
/// # Errors
///
/// Returns a source-safe error for source/policy mismatch or an invalid chunk
/// count.
pub fn plan_enrichment_run(
    prepared: &PreparedTranscript,
    configuration: &EnrichmentRunConfiguration,
) -> Result<PlannedEnrichmentRun, EnrichmentApplicationError> {
    if prepared.disclosure_scope() != configuration.disclosure_scope {
        return Err(EnrichmentApplicationError::PreparationMismatch {
            field: RunConfigurationField::DisclosureScope,
        });
    }
    if prepared.authorization_policy_digest() != configuration.authorization_policy_digest {
        return Err(EnrichmentApplicationError::PreparationMismatch {
            field: RunConfigurationField::AuthorizationPolicyDigest,
        });
    }
    if prepared.chunking_policy_version() != &configuration.chunking_version {
        return Err(EnrichmentApplicationError::PreparationMismatch {
            field: RunConfigurationField::ChunkingPolicyVersion,
        });
    }
    for receipt in prepared.redaction_receipts() {
        if receipt.source().session_id() != configuration.session_id {
            return Err(EnrichmentApplicationError::PreparationMismatch {
                field: RunConfigurationField::Session,
            });
        }
        if receipt.source().source_digest() != configuration.source_digest {
            return Err(EnrichmentApplicationError::PreparationMismatch {
                field: RunConfigurationField::SourceDigest,
            });
        }
        if receipt.policy_version() != &configuration.redaction_version {
            return Err(EnrichmentApplicationError::PreparationMismatch {
                field: RunConfigurationField::RedactionPolicyVersion,
            });
        }
    }

    let expected_chunks =
        EnrichmentChunkCount::new(prepared.chunks().count().value()).map_err(|_| {
            EnrichmentApplicationError::InvalidRunConfiguration {
                field: RunConfigurationField::ExpectedChunks,
            }
        })?;
    let fingerprint = derive_fingerprint(prepared, configuration, expected_chunks)?;
    let run_id = derive_run_id(fingerprint)?;
    let specification = EnrichmentRunSpec::new(
        configuration.namespace.clone(),
        configuration.session_id,
        configuration.source_digest,
        run_id,
        fingerprint,
        EnrichmentProvider::Mistral,
        configuration.model.clone(),
        configuration.prompt_version.clone(),
        configuration.schema_version.clone(),
        configuration.graph_redaction_version.clone(),
        configuration.graph_chunking_version.clone(),
        expected_chunks,
    );
    let reference = EnrichmentRunRef::new(
        configuration.namespace.clone(),
        configuration.source_digest,
        fingerprint,
    );
    Ok(PlannedEnrichmentRun {
        specification,
        reference,
    })
}

fn derive_fingerprint(
    prepared: &PreparedTranscript,
    configuration: &EnrichmentRunConfiguration,
    expected_chunks: EnrichmentChunkCount,
) -> Result<EnrichmentFingerprint, EnrichmentApplicationError> {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"harness-graph-enrichment-fingerprint-v1");
    append_field(&mut hasher, configuration.namespace.as_str().as_bytes());
    append_field(&mut hasher, configuration.session_id.to_string().as_bytes());
    append_field(&mut hasher, configuration.source_digest.to_hex().as_bytes());
    append_field(
        &mut hasher,
        prepared.projection_digest().to_hex().as_bytes(),
    );
    append_field(
        &mut hasher,
        configuration.disclosure_scope.as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        configuration
            .authorization_policy_digest
            .to_hex()
            .as_bytes(),
    );
    append_field(
        &mut hasher,
        configuration.redaction_version.as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        configuration.chunking_version.as_str().as_bytes(),
    );
    append_field(&mut hasher, EnrichmentProvider::Mistral.as_str().as_bytes());
    append_field(&mut hasher, configuration.model.as_str().as_bytes());
    append_field(
        &mut hasher,
        configuration.prompt_version.as_str().as_bytes(),
    );
    append_field(&mut hasher, &configuration.prompt_digest.bytes());
    append_field(
        &mut hasher,
        configuration.schema_version.as_str().as_bytes(),
    );
    append_field(&mut hasher, &expected_chunks.value().to_be_bytes());
    for chunk in prepared.chunks().iter() {
        append_field(&mut hasher, &chunk.id().bytes());
    }
    EnrichmentFingerprint::parse_hex(&encode_hex(hasher.finalize().into())).map_err(|_| {
        EnrichmentApplicationError::InvalidRunConfiguration {
            field: RunConfigurationField::Fingerprint,
        }
    })
}

fn derive_run_id(
    fingerprint: EnrichmentFingerprint,
) -> Result<EnrichmentRunId, EnrichmentApplicationError> {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"harness-graph-enrichment-run-v1");
    append_field(&mut hasher, fingerprint.to_hex().as_bytes());
    EnrichmentRunId::parse_hex(&encode_hex(hasher.finalize().into())).map_err(|_| {
        EnrichmentApplicationError::InvalidRunConfiguration {
            field: RunConfigurationField::RunIdentity,
        }
    })
}

fn append_field(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(TABLE[usize::from(byte >> 4)]));
        encoded.push(char::from(TABLE[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use harness_graph_graph_port::EnrichmentFingerprint;
    use sha2::{Digest, Sha256};

    use super::{
        EnrichmentPromptDigest, ExtractionConcurrency, append_field, derive_run_id, encode_hex,
    };

    #[test]
    fn length_framing_prevents_concatenation_collisions() {
        let mut left = Sha256::new();
        append_field(&mut left, b"ab");
        append_field(&mut left, b"c");

        let mut right = Sha256::new();
        append_field(&mut right, b"a");
        append_field(&mut right, b"bc");

        assert_ne!(
            encode_hex(left.finalize().into()),
            encode_hex(right.finalize().into())
        );
    }

    #[test]
    fn prompt_digest_is_content_addressed_and_source_safe() {
        let first = EnrichmentPromptDigest::hash(b"prompt-v1");
        let identical = EnrichmentPromptDigest::hash(b"prompt-v1");
        let changed = EnrichmentPromptDigest::hash(b"prompt-v2");

        assert_eq!(first, identical);
        assert_ne!(first, changed);
        assert!(!format!("{first:?}").contains("prompt-v1"));
    }

    #[test]
    fn run_identity_is_a_deterministic_morphism_of_the_fingerprint()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = EnrichmentFingerprint::parse_hex(&"1".repeat(64))?;
        let changed = EnrichmentFingerprint::parse_hex(&"2".repeat(64))?;

        assert_eq!(derive_run_id(first)?, derive_run_id(first)?);
        assert_ne!(derive_run_id(first)?, derive_run_id(changed)?);
        Ok(())
    }

    #[test]
    fn extraction_concurrency_is_bounded_by_provider_policy() {
        assert!(ExtractionConcurrency::new(1).is_ok());
        assert!(ExtractionConcurrency::new(4).is_ok());
        assert!(ExtractionConcurrency::new(0).is_err());
        assert!(ExtractionConcurrency::new(5).is_err());
    }
}
