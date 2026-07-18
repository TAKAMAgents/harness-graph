//! Typed transcript-disclosure authorization.

use harness_graph_domain::{OccurredAt, SessionId, SourceDigest};
use harness_graph_ingestion::VerifiedSessionBundle;
use harness_graph_protocol::TranscriptRecordClass;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::TranscriptEnrichmentError;

/// Scope of transcript content authorized for external semantic enrichment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptDisclosureScope {
    /// User, agent, collaborator, and completion messages only.
    ConversationOnly,
    /// Conversation plus textual tool requests, results, patches, and errors.
    ConversationAndExecution,
}

impl TranscriptDisclosureScope {
    /// Whether the closed record class is included in this scope.
    #[must_use]
    pub const fn allows(self, class: TranscriptRecordClass) -> bool {
        match self {
            Self::ConversationOnly => matches!(
                class,
                TranscriptRecordClass::UserMessage
                    | TranscriptRecordClass::AgentMessage
                    | TranscriptRecordClass::InterAgentMessage
                    | TranscriptRecordClass::CompletionSummary
            ),
            Self::ConversationAndExecution => true,
        }
    }

    /// Stable fingerprint representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConversationOnly => "conversation_only",
            Self::ConversationAndExecution => "conversation_and_execution",
        }
    }
}

macro_rules! digest_value {
    ($name:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Hash canonical policy material.
            #[must_use]
            pub fn hash(bytes: &[u8]) -> Self {
                Self(Sha256::digest(bytes).into())
            }

            /// Lowercase hexadecimal representation.
            #[must_use]
            pub fn to_hex(self) -> String {
                hex::encode(self.0)
            }

            pub(crate) const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.to_hex())
                    .finish()
            }
        }
    };
}

digest_value!(
    AuthorizationPolicyDigest,
    "Digest of the operator-reviewed disclosure policy."
);

/// Source-safe identity that authorized one disclosure run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationIdentity(String);

impl AuthorizationIdentity {
    /// Validate a non-empty authorization identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty identity.
    pub fn new(value: impl Into<String>) -> Result<Self, TranscriptEnrichmentError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty()
            || value.len() > 80
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            Err(TranscriptEnrichmentError::InvalidAuthorizationIdentity)
        } else {
            Ok(Self(value.to_owned()))
        }
    }

    /// Borrow the source-safe identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

macro_rules! version_value {
    ($name:ident, $field:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Validate a non-empty version identifier.
            ///
            /// # Errors
            ///
            /// Returns an error when the value is empty.
            pub fn new(value: impl Into<String>) -> Result<Self, TranscriptEnrichmentError> {
                let value = value.into();
                let value = value.trim();
                if value.is_empty() {
                    Err(TranscriptEnrichmentError::EmptyValue { field: $field })
                } else {
                    Ok(Self(value.to_owned()))
                }
            }

            /// Borrow the stable identifier.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

version_value!(
    RedactionPolicyVersion,
    "redaction policy version",
    "Version of the mandatory local redaction policy."
);
version_value!(
    ChunkingPolicyVersion,
    "chunking policy version",
    "Version of the deterministic chunking policy."
);

/// Explicit authorization bound to one immutable source snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisclosureAuthorization {
    session_id: SessionId,
    source_digest: SourceDigest,
    scope: TranscriptDisclosureScope,
    policy_digest: AuthorizationPolicyDigest,
    identity: AuthorizationIdentity,
    authorized_at: OccurredAt,
}

impl DisclosureAuthorization {
    /// Construct an exact source-bound authorization.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        source_digest: SourceDigest,
        scope: TranscriptDisclosureScope,
        policy_digest: AuthorizationPolicyDigest,
        identity: AuthorizationIdentity,
        authorized_at: OccurredAt,
    ) -> Self {
        Self {
            session_id,
            source_digest,
            scope,
            policy_digest,
            identity,
            authorized_at,
        }
    }

    /// Verify authorization against a checksum-verified bundle.
    ///
    /// # Errors
    ///
    /// Returns an error when the session or source digest differs.
    pub fn verify_bundle(
        &self,
        bundle: &VerifiedSessionBundle,
    ) -> Result<(), TranscriptEnrichmentError> {
        self.verify_source(bundle.session_id(), bundle.source_digest())
    }

    /// Verify authorization against one typed source reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the session or source digest differs.
    pub fn verify_source(
        &self,
        session_id: SessionId,
        source_digest: SourceDigest,
    ) -> Result<(), TranscriptEnrichmentError> {
        if session_id != self.session_id {
            return Err(TranscriptEnrichmentError::UnauthorizedSession { session_id });
        }
        if source_digest != self.source_digest {
            return Err(TranscriptEnrichmentError::UnauthorizedSourceSnapshot);
        }
        Ok(())
    }

    /// Authorized source session.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Authorized source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Authorized disclosure scope.
    #[must_use]
    pub const fn scope(&self) -> TranscriptDisclosureScope {
        self.scope
    }

    /// Disclosure policy digest.
    #[must_use]
    pub const fn policy_digest(&self) -> AuthorizationPolicyDigest {
        self.policy_digest
    }

    /// Operator or automation identity.
    #[must_use]
    pub const fn identity(&self) -> &AuthorizationIdentity {
        &self.identity
    }

    /// Authorization timestamp.
    #[must_use]
    pub const fn authorized_at(&self) -> OccurredAt {
        self.authorized_at
    }
}
