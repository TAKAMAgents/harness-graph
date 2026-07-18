//! Mandatory local transcript redaction.

use std::fmt;

use harness_graph_domain::{
    CallAssociation, OccurredAt, RecordCount, SourceRecordRef, ToolAssociation, TurnAssociation,
};
use harness_graph_protocol::{
    SensitiveTranscriptFragment, TranscriptFieldPath, TranscriptRecordClass, TranscriptRole,
};
use hmac::{Hmac, Mac};
use regex::Regex;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};

use crate::{
    DisclosureAuthorization, RedactionPolicyVersion, ScannerBlockReason, TranscriptEnrichmentError,
};

type HmacSha256 = Hmac<Sha256>;

/// Local HMAC key used for stable, non-reversible PII pseudonyms.
#[derive(Clone)]
pub struct PseudonymizationKey(SecretString);

impl PseudonymizationKey {
    /// Validate a dedicated pseudonymization key.
    ///
    /// # Errors
    ///
    /// Returns an error when fewer than 32 bytes are supplied.
    pub fn new(value: impl Into<String>) -> Result<Self, TranscriptEnrichmentError> {
        let value = value.into();
        if value.len() < 32 {
            Err(TranscriptEnrichmentError::WeakPseudonymizationKey)
        } else {
            Ok(Self(SecretString::from(value)))
        }
    }
}

impl fmt::Debug for PseudonymizationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PseudonymizationKey([redacted])")
    }
}

/// Exact loaded credential value that must never leave the machine.
#[derive(Clone)]
pub struct SensitiveValue(SecretString);

impl SensitiveValue {
    /// Validate an exact-match canary or loaded credential.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is too short for reliable matching.
    pub fn new(value: impl Into<String>) -> Result<Self, TranscriptEnrichmentError> {
        let value = value.into();
        if value.len() < 8 {
            Err(TranscriptEnrichmentError::SensitiveValueTooShort)
        } else {
            Ok(Self(SecretString::from(value)))
        }
    }
}

impl fmt::Debug for SensitiveValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitiveValue([redacted])")
    }
}

/// Typed collection of exact loaded secrets scanned before pattern matching.
#[derive(Clone, Default)]
pub struct SensitiveValueSet(Vec<SensitiveValue>);

impl SensitiveValueSet {
    /// Construct and longest-first order an exact-match secret set.
    #[must_use]
    pub fn new(values: impl IntoIterator<Item = SensitiveValue>) -> Self {
        let mut values: Vec<_> = values.into_iter().collect();
        values.sort_by(|left, right| {
            right
                .0
                .expose_secret()
                .len()
                .cmp(&left.0.expose_secret().len())
        });
        Self(values)
    }

    /// Check whether candidate output contains any locally loaded value.
    ///
    /// The comparison remains inside the secret boundary and never exposes or
    /// returns the matching value.
    #[must_use]
    pub fn contains(&self, candidate: &str) -> bool {
        self.0
            .iter()
            .any(|value| candidate.contains(value.0.expose_secret()))
    }
}

impl fmt::Debug for SensitiveValueSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SensitiveValueSet")
            .field("count", &self.0.len())
            .finish()
    }
}

/// Closed category of locally removed sensitive data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RedactionCategory {
    /// Exact loaded secret.
    KnownSecret,
    /// PEM-encoded private key.
    PrivateKey,
    /// Bearer, JWT, cookie, or authorization material.
    AuthenticationMaterial,
    /// URL carrying embedded user credentials.
    CredentialUrl,
    /// Recognizable provider token.
    ProviderToken,
    /// Secret-like high-entropy assignment.
    HighEntropyAssignment,
    /// Email address.
    Email,
    /// International phone number.
    Phone,
    /// IPv4 address.
    IpAddress,
    /// Absolute home-directory prefix.
    HomePath,
}

impl RedactionCategory {
    const COUNT: usize = 10;

    /// Every closed category in stable report order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::KnownSecret,
        Self::PrivateKey,
        Self::AuthenticationMaterial,
        Self::CredentialUrl,
        Self::ProviderToken,
        Self::HighEntropyAssignment,
        Self::Email,
        Self::Phone,
        Self::IpAddress,
        Self::HomePath,
    ];

    const fn index(self) -> usize {
        match self {
            Self::KnownSecret => 0,
            Self::PrivateKey => 1,
            Self::AuthenticationMaterial => 2,
            Self::CredentialUrl => 3,
            Self::ProviderToken => 4,
            Self::HighEntropyAssignment => 5,
            Self::Email => 6,
            Self::Phone => 7,
            Self::IpAddress => 8,
            Self::HomePath => 9,
        }
    }

    /// Stable source-safe category name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KnownSecret => "known_secret",
            Self::PrivateKey => "private_key",
            Self::AuthenticationMaterial => "authentication_material",
            Self::CredentialUrl => "credential_url",
            Self::ProviderToken => "provider_token",
            Self::HighEntropyAssignment => "high_entropy_assignment",
            Self::Email => "email",
            Self::Phone => "phone",
            Self::IpAddress => "ip_address",
            Self::HomePath => "home_path",
        }
    }
}

/// Fixed-shape redaction counters with no stringly category map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedactionCounts([u64; RedactionCategory::COUNT]);

impl RedactionCounts {
    fn empty() -> Self {
        Self::default()
    }

    fn add(&mut self, category: RedactionCategory, amount: usize) {
        let amount = u64::try_from(amount).unwrap_or(u64::MAX);
        let slot = &mut self.0[category.index()];
        *slot = slot.saturating_add(amount);
    }

    /// Count for one closed category.
    #[must_use]
    pub const fn count(&self, category: RedactionCategory) -> RecordCount {
        RecordCount::new(self.0[category.index()])
    }

    /// Total replacements in this receipt.
    #[must_use]
    pub fn total(&self) -> RecordCount {
        RecordCount::new(self.0.iter().copied().fold(0_u64, u64::saturating_add))
    }

    /// Associatively merge another receipt's fixed-shape counters.
    pub fn merge(&mut self, other: &Self) {
        for category in RedactionCategory::ALL {
            let slot = &mut self.0[category.index()];
            *slot = slot.saturating_add(other.0[category.index()]);
        }
    }
}

/// SHA-256 digest of locally sanitized content.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SanitizedContentDigest([u8; 32]);

impl SanitizedContentDigest {
    fn hash(bytes: &[u8]) -> Self {
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

impl fmt::Debug for SanitizedContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SanitizedContentDigest")
            .field(&self.to_hex())
            .finish()
    }
}

/// Source-safe proof that one fragment passed the mandatory local scanner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionReceipt {
    source: SourceRecordRef,
    sanitized_digest: SanitizedContentDigest,
    counts: RedactionCounts,
    policy_version: RedactionPolicyVersion,
}

impl RedactionReceipt {
    /// Source fragment that was scanned.
    #[must_use]
    pub const fn source(&self) -> &SourceRecordRef {
        &self.source
    }

    /// Digest of the approved sanitized text.
    #[must_use]
    pub const fn sanitized_digest(&self) -> SanitizedContentDigest {
        self.sanitized_digest
    }

    /// Typed redaction counts.
    #[must_use]
    pub const fn counts(&self) -> &RedactionCounts {
        &self.counts
    }

    /// Scanner policy version.
    #[must_use]
    pub const fn policy_version(&self) -> &RedactionPolicyVersion {
        &self.policy_version
    }
}

/// Transcript fragment approved for chunking after mandatory local scanning.
#[derive(Clone)]
pub struct LocallySanitizedFragment {
    source: SourceRecordRef,
    occurred_at: OccurredAt,
    class: TranscriptRecordClass,
    role: TranscriptRole,
    field_path: TranscriptFieldPath,
    turn: TurnAssociation,
    call: CallAssociation,
    tool: ToolAssociation,
    text: SecretString,
    digest: SanitizedContentDigest,
}

impl LocallySanitizedFragment {
    /// Source record anchor.
    #[must_use]
    pub const fn source(&self) -> &SourceRecordRef {
        &self.source
    }

    /// Closed class.
    #[must_use]
    pub const fn class(&self) -> TranscriptRecordClass {
        self.class
    }

    /// Closed producer role.
    #[must_use]
    pub const fn role(&self) -> TranscriptRole {
        self.role
    }

    /// Native field anchor.
    #[must_use]
    pub const fn field_path(&self) -> TranscriptFieldPath {
        self.field_path
    }

    /// Turn association.
    #[must_use]
    pub const fn turn(&self) -> &TurnAssociation {
        &self.turn
    }

    /// Call association.
    #[must_use]
    pub const fn call(&self) -> &CallAssociation {
        &self.call
    }

    /// Tool association.
    #[must_use]
    pub const fn tool(&self) -> &ToolAssociation {
        &self.tool
    }

    /// Occurrence timestamp.
    #[must_use]
    pub const fn occurred_at(&self) -> OccurredAt {
        self.occurred_at
    }

    /// Sanitized content digest.
    #[must_use]
    pub const fn digest(&self) -> SanitizedContentDigest {
        self.digest
    }

    pub(crate) fn expose_for_chunking(&self) -> &str {
        self.text.expose_secret()
    }
}

impl fmt::Debug for LocallySanitizedFragment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocallySanitizedFragment")
            .field("source", &self.source)
            .field("class", &self.class)
            .field("role", &self.role)
            .field("digest", &self.digest)
            .field("text", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// Result of applying scope and mandatory local scanning to one fragment.
#[derive(Debug, Clone)]
pub enum RedactionOutcome {
    /// Fragment is outside the exact authorized disclosure scope.
    ExcludedByScope,
    /// Sanitized fragment and its source-safe receipt.
    Approved {
        /// Approved text and provenance.
        fragment: Box<LocallySanitizedFragment>,
        /// Scanner receipt.
        receipt: RedactionReceipt,
    },
}

struct ScannerPatterns {
    private_key: Regex,
    authentication: Regex,
    credential_url: Regex,
    provider_token: Regex,
    high_entropy_assignment: Regex,
    email: Regex,
    phone: Regex,
    ip_address: Regex,
    home_path: Regex,
}

/// Mandatory deterministic scanner and redactor.
pub struct LocalTranscriptRedactor {
    policy_version: RedactionPolicyVersion,
    known_secrets: SensitiveValueSet,
    pseudonymizer: HmacSha256,
    patterns: ScannerPatterns,
}

impl LocalTranscriptRedactor {
    /// Construct and compile the mandatory scanner policy.
    ///
    /// # Errors
    ///
    /// Returns an error when a mandatory pattern cannot compile.
    pub fn new(
        policy_version: RedactionPolicyVersion,
        key: PseudonymizationKey,
        known_secrets: SensitiveValueSet,
    ) -> Result<Self, TranscriptEnrichmentError> {
        let PseudonymizationKey(secret_key) = key;
        let pseudonymizer = HmacSha256::new_from_slice(secret_key.expose_secret().as_bytes())
            .map_err(|_| TranscriptEnrichmentError::WeakPseudonymizationKey)?;
        Ok(Self {
            policy_version,
            known_secrets,
            pseudonymizer,
            patterns: ScannerPatterns::compile()?,
        })
    }

    /// Apply exact authorization, secret removal, PII pseudonymization, and
    /// encoded-content blocking to one fragment.
    ///
    /// # Errors
    ///
    /// Returns a source-safe error for authorization mismatch or blocked data.
    pub fn sanitize(
        &self,
        fragment: &SensitiveTranscriptFragment,
        authorization: &DisclosureAuthorization,
    ) -> Result<RedactionOutcome, TranscriptEnrichmentError> {
        authorization.verify_source(
            fragment.source().session_id(),
            fragment.source().source_digest(),
        )?;
        if !authorization.scope().allows(fragment.class()) {
            return Ok(RedactionOutcome::ExcludedByScope);
        }
        let raw = fragment.expose_for_local_scanner();
        let sequence = fragment.source().sequence();
        validate_scannable_text(raw, sequence)?;
        let (sanitized, counts) = self.redact_text(raw);
        if contains_suspicious_encoded_blob(&sanitized) {
            return Err(TranscriptEnrichmentError::ScannerBlocked {
                sequence,
                reason: ScannerBlockReason::SuspiciousEncodedBlob,
            });
        }

        let digest = SanitizedContentDigest::hash(sanitized.as_bytes());
        let receipt = RedactionReceipt {
            source: fragment.source().clone(),
            sanitized_digest: digest,
            counts,
            policy_version: self.policy_version.clone(),
        };
        let approved = LocallySanitizedFragment {
            source: fragment.source().clone(),
            occurred_at: fragment.occurred_at(),
            class: fragment.class(),
            role: fragment.role(),
            field_path: fragment.field_path(),
            turn: fragment.turn().clone(),
            call: fragment.call().clone(),
            tool: fragment.tool().clone(),
            text: SecretString::from(sanitized),
            digest,
        };
        Ok(RedactionOutcome::Approved {
            fragment: Box::new(approved),
            receipt,
        })
    }

    /// Active scanner policy version.
    #[must_use]
    pub const fn policy_version(&self) -> &RedactionPolicyVersion {
        &self.policy_version
    }

    fn redact_text(&self, raw: &str) -> (String, RedactionCounts) {
        let mut counts = RedactionCounts::empty();
        let mut sanitized = self.redact_known_secrets(raw, &mut counts);
        let fixed_patterns = [
            (
                &self.patterns.private_key,
                RedactionCategory::PrivateKey,
                "[REDACTED_PRIVATE_KEY]",
            ),
            (
                &self.patterns.authentication,
                RedactionCategory::AuthenticationMaterial,
                "[REDACTED_AUTH]",
            ),
            (
                &self.patterns.credential_url,
                RedactionCategory::CredentialUrl,
                "[REDACTED_CREDENTIAL_URL]",
            ),
            (
                &self.patterns.provider_token,
                RedactionCategory::ProviderToken,
                "[REDACTED_PROVIDER_TOKEN]",
            ),
            (
                &self.patterns.high_entropy_assignment,
                RedactionCategory::HighEntropyAssignment,
                "[REDACTED_SECRET_ASSIGNMENT]",
            ),
        ];
        for (pattern, category, replacement) in fixed_patterns {
            sanitized =
                Self::replace_generic(&sanitized, pattern, category, replacement, &mut counts);
        }
        let pseudonym_patterns = [
            (&self.patterns.email, RedactionCategory::Email),
            (&self.patterns.phone, RedactionCategory::Phone),
            (&self.patterns.ip_address, RedactionCategory::IpAddress),
            (&self.patterns.home_path, RedactionCategory::HomePath),
        ];
        for (pattern, category) in pseudonym_patterns {
            sanitized = self.replace_pseudonym(&sanitized, pattern, category, &mut counts);
        }
        (sanitized, counts)
    }

    fn redact_known_secrets(&self, raw: &str, counts: &mut RedactionCounts) -> String {
        let mut sanitized = raw.to_owned();
        for secret in &self.known_secrets.0 {
            let exposed = secret.0.expose_secret();
            let matches = sanitized.matches(exposed).count();
            if matches != 0 {
                counts.add(RedactionCategory::KnownSecret, matches);
                sanitized = sanitized.replace(exposed, "[REDACTED_SECRET]");
            }
        }
        sanitized
    }

    fn replace_generic(
        text: &str,
        pattern: &Regex,
        category: RedactionCategory,
        replacement: &str,
        counts: &mut RedactionCounts,
    ) -> String {
        replace_matches(text, pattern, counts, category, |_| replacement.to_owned())
    }

    fn replace_pseudonym(
        &self,
        text: &str,
        pattern: &Regex,
        category: RedactionCategory,
        counts: &mut RedactionCounts,
    ) -> String {
        replace_matches(text, pattern, counts, category, |matched| {
            let mut mac = self.pseudonymizer.clone();
            mac.update(matched.to_ascii_lowercase().as_bytes());
            let digest = mac.finalize().into_bytes();
            let encoded = hex::encode(digest);
            format!("[REDACTED_{}_{}]", category.as_str(), &encoded[..16])
        })
    }
}

impl fmt::Debug for LocalTranscriptRedactor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalTranscriptRedactor")
            .field("policy_version", &self.policy_version)
            .field("known_secrets", &self.known_secrets)
            .field("pseudonymizer", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl ScannerPatterns {
    fn compile() -> Result<Self, TranscriptEnrichmentError> {
        Ok(Self {
            private_key: compile(
                "private_key",
                r"(?s)-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----.*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
            )?,
            authentication: compile(
                "authentication",
                r"(?i)(?:authorization\s*[:=]\s*bearer|bearer|cookie\s*[:=])\s+[A-Za-z0-9._~+/=-]{8,}",
            )?,
            credential_url: compile(
                "credential_url",
                r"(?i)[a-z][a-z0-9+.-]*://[^\s/:@]+:[^\s/@]+@",
            )?,
            provider_token: compile(
                "provider_token",
                r"\b(?:sk-[A-Za-z0-9_-]{12,}|ghp_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]{12,}|xox[baprs]-[A-Za-z0-9-]{12,}|AKIA[A-Z0-9]{16}|eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,})\b",
            )?,
            high_entropy_assignment: compile(
                "high_entropy_assignment",
                r#"(?i)\b(?:api[_-]?key|token|secret|password|passwd|client_secret)\s*[:=]\s*["']?[A-Za-z0-9+/=_-]{12,}["']?"#,
            )?,
            email: compile(
                "email",
                r"\b[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+\b",
            )?,
            phone: compile("phone", r"\+\d[\d .()/-]{7,}\d")?,
            ip_address: compile("ip_address", r"\b(?:\d{1,3}\.){3}\d{1,3}\b")?,
            home_path: compile("home_path", r#"(?:/Users|/home)/[^/\s"']+"#)?,
        })
    }
}

fn compile(name: &'static str, pattern: &str) -> Result<Regex, TranscriptEnrichmentError> {
    Regex::new(pattern).map_err(|source| TranscriptEnrichmentError::ScannerPattern {
        pattern: name,
        source,
    })
}

fn validate_scannable_text(
    raw: &str,
    sequence: harness_graph_domain::RecordSequence,
) -> Result<(), TranscriptEnrichmentError> {
    if raw.chars().any(|character| {
        character == '\0' || (character.is_control() && !character.is_whitespace())
    }) {
        return Err(TranscriptEnrichmentError::ScannerBlocked {
            sequence,
            reason: ScannerBlockReason::NonTextControlData,
        });
    }
    let lowercase = raw.to_ascii_lowercase();
    if lowercase.contains("data:image/") || lowercase.contains("data:application/octet-stream") {
        return Err(TranscriptEnrichmentError::ScannerBlocked {
            sequence,
            reason: ScannerBlockReason::AssetOrBinaryData,
        });
    }
    Ok(())
}

fn replace_matches(
    text: &str,
    pattern: &Regex,
    counts: &mut RedactionCounts,
    category: RedactionCategory,
    replacement: impl Fn(&str) -> String,
) -> String {
    let matches: Vec<_> = pattern.find_iter(text).collect();
    if matches.is_empty() {
        return text.to_owned();
    }
    counts.add(category, matches.len());
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    for matched in matches {
        output.push_str(&text[cursor..matched.start()]);
        output.push_str(&replacement(matched.as_str()));
        cursor = matched.end();
    }
    output.push_str(&text[cursor..]);
    output
}

fn contains_suspicious_encoded_blob(text: &str) -> bool {
    text.split(|character: char| {
        !(character.is_ascii_alphanumeric() || matches!(character, '+' | '/' | '=' | '_' | '-'))
    })
    .filter(|token| token.len() >= 512)
    .any(|token| {
        let base64_like = token
            .bytes()
            .filter(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=' | b'_' | b'-')
            })
            .count();
        base64_like.saturating_mul(100) / token.len() >= 95
    })
}

#[cfg(test)]
mod tests {
    use harness_graph_domain::{RecordSequence, SessionId, SourceDigest, SourceRecordRef};
    use harness_graph_protocol::{TranscriptRecordProjection, project_codex_transcript_line};

    use super::{
        LocalTranscriptRedactor, PseudonymizationKey, RedactionCategory, RedactionOutcome,
        SensitiveValue, SensitiveValueSet,
    };
    use crate::{
        AuthorizationIdentity, AuthorizationPolicyDigest, DisclosureAuthorization,
        RedactionPolicyVersion, TranscriptDisclosureScope,
    };

    fn authorization(
        source: &SourceRecordRef,
    ) -> Result<DisclosureAuthorization, Box<dyn std::error::Error>> {
        Ok(DisclosureAuthorization::new(
            source.session_id(),
            source.source_digest(),
            TranscriptDisclosureScope::ConversationAndExecution,
            AuthorizationPolicyDigest::hash(b"test policy"),
            AuthorizationIdentity::new("test-operator")?,
            harness_graph_domain::OccurredAt::parse("2026-07-18T12:00:00Z")?,
        ))
    }

    #[test]
    fn mandatory_scanner_removes_secrets_and_stably_pseudonymizes_pii()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = SourceRecordRef::new(
            SessionId::parse("019c63db-2995-74c3-b898-c1b92a8e1317")?,
            SourceDigest::hash(b"fixture"),
            RecordSequence::from_zero_based(0),
        );
        let canary = "mistral-secret-canary-123456";
        let line = format!(
            r#"{{"timestamp":"2026-07-18T12:00:00Z","type":"event_msg","payload":{{"type":"user_message","message":"key {canary}; email person@example.com; path /Users/alice/project; ip 192.168.1.4"}}}}"#
        );
        let TranscriptRecordProjection::Eligible(fragments) =
            project_codex_transcript_line(&line, source.clone())?
        else {
            return Err("fixture was excluded".into());
        };
        let fragment = fragments
            .into_fragments()
            .next()
            .ok_or("missing fragment")?;
        let redactor = LocalTranscriptRedactor::new(
            RedactionPolicyVersion::new("redaction-v1")?,
            PseudonymizationKey::new("0123456789abcdef0123456789abcdef")?,
            SensitiveValueSet::new([SensitiveValue::new(canary)?]),
        )?;
        assert!(
            redactor
                .known_secrets
                .contains(&format!("provider echoed {canary} inside a longer field"))
        );
        assert!(!redactor.known_secrets.contains("source-safe output"));
        let RedactionOutcome::Approved { fragment, receipt } =
            redactor.sanitize(&fragment, &authorization(&source)?)?
        else {
            return Err("fragment was outside scope".into());
        };
        let sanitized = fragment.expose_for_chunking();
        assert!(!sanitized.contains(canary));
        assert!(!sanitized.contains("person@example.com"));
        assert!(!sanitized.contains("/Users/alice"));
        assert!(!sanitized.contains("192.168.1.4"));
        assert_eq!(
            receipt
                .counts()
                .count(RedactionCategory::KnownSecret)
                .value(),
            1
        );
        assert_eq!(receipt.counts().count(RedactionCategory::Email).value(), 1);
        assert!(!format!("{fragment:?}").contains("person@example.com"));
        assert!(!format!("{redactor:?}").contains(canary));
        Ok(())
    }
}
