//! Typed environment configuration with secret-safe debug behavior.

use std::{net::SocketAddr, path::PathBuf};

use harness_graph_domain::GraphNamespace;
use harness_graph_event_journal::JournalPath;
use harness_graph_graph_port::{BatchSize, ExperienceEnrichmentVisibility};
use harness_graph_ingestion::ArchiveRoot;
use harness_graph_mistral_adapter::{MistralConcurrencyLimit, MistralCredential, MistralModelName};
use harness_graph_transcript_enrichment::{
    EstimatedOutputTokensPerRequest, MicroUsd, PseudonymizationKey, SensitiveValue,
    SensitiveValueSet, TokenRatePerMillion, TranscriptTokenPricing,
};
use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::CliError;

/// Lazily validated runtime configuration source.
///
/// Commands resolve only the capabilities they actually use. In particular,
/// transcript inventory can inspect a verified archive without constructing a
/// Neo4j or Mistral client.
pub struct AppConfig {
    values: ConfigurationFile,
}

impl AppConfig {
    /// Load `.env` when present without eagerly constructing unrelated clients.
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration file is unreadable or malformed.
    pub fn load() -> Result<Self, CliError> {
        Ok(Self {
            values: ConfigurationFile::load_optional()?,
        })
    }

    /// Resolve the verified archive capability.
    ///
    /// # Errors
    ///
    /// Returns an error when the archive setting is absent or invalid.
    pub fn archive_root(&self) -> Result<ArchiveRoot, CliError> {
        let archive_path = required_setting(&self.values, "CODEX_SESSION_RAW_DATA_PATH", &[])?;
        Ok(ArchiveRoot::new(PathBuf::from(archive_path))?)
    }

    /// Resolve Neo4j connection settings.
    ///
    /// # Errors
    ///
    /// Returns an error when any required graph setting is absent or invalid.
    pub fn neo4j(&self) -> Result<Neo4jConnection, CliError> {
        let neo4j_url = project_file_preferred_setting(
            &self.values,
            "NEO4J_CONNECTION_URL",
            &["NEO4J_CONECTION_URL"],
            optional_process_value,
        )?;
        let neo4j_password = project_file_preferred_setting(
            &self.values,
            "NEO4J_PASSWORD",
            &["NEO4J_INATANSE_PASSWORD"],
            optional_process_value,
        )?;
        let neo4j_username = project_file_preferred_value(
            &self.values,
            "NEO4J_USERNAME",
            &[],
            optional_process_value,
        )
        .unwrap_or_else(|| "neo4j".to_owned());
        Neo4jConnection::new(&neo4j_url, &neo4j_username, neo4j_password)
    }

    /// Resolve the graph namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when the namespace violates its domain contract.
    pub fn graph_namespace(&self) -> Result<GraphNamespace, CliError> {
        let graph_namespace = GraphNamespace::new(
            optional_setting(&self.values, "HARNESS_GRAPH_NAMESPACE")
                .unwrap_or_else(|| "default".to_owned()),
        )?;
        Ok(graph_namespace)
    }

    /// Resolve the bounded graph transaction size.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured value is outside the graph port's
    /// safety range.
    pub fn graph_batch_size(&self) -> Result<BatchSize, CliError> {
        optional_setting(&self.values, "HARNESS_GRAPH_BATCH_SIZE")
            .unwrap_or_else(|| "250".to_owned())
            .parse::<usize>()
            .map_err(|_| CliError::InvalidConfiguration {
                canonical_name: "HARNESS_GRAPH_BATCH_SIZE",
                reason: "expected an integer between 1 and 10,000",
            })
            .and_then(|value| BatchSize::new(value).map_err(CliError::from))
    }

    /// Resolve the Mistral credential.
    ///
    /// # Errors
    ///
    /// Returns an error when the canonical project credential is absent.
    pub fn mistral_credential(&self) -> Result<MistralCredential, CliError> {
        let key = project_canonical_preferred_setting(
            &self.values,
            "MISTRAL_API_KEY",
            &["MISTARL_API_KEY"],
            optional_process_value,
        )?;
        Ok(MistralCredential::new(key)?)
    }

    /// Resolve the transcript credential from the canonical project file only.
    ///
    /// Transcript disclosure is intentionally stricter than source-safe model
    /// commands: neither inherited process values nor the historical misspelled
    /// alias may silently select a different Mistral account.
    ///
    /// # Errors
    ///
    /// Returns an error unless the project `.env` contains `MISTRAL_API_KEY`.
    pub fn transcript_mistral_credential(&self) -> Result<MistralCredential, CliError> {
        let key = self
            .values
            .value("MISTRAL_API_KEY")
            .ok_or(CliError::MissingConfiguration {
                canonical_name: "MISTRAL_API_KEY",
            })?;
        Ok(MistralCredential::new(key)?)
    }

    /// Resolve the validated Mistral model.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured model is not a Mistral family.
    pub fn mistral_model(&self) -> Result<MistralModelName, CliError> {
        let model = optional_setting(&self.values, "MISTRAL_MODEL")
            .unwrap_or_else(|| "mistral-small-latest".to_owned());
        Ok(MistralModelName::new(model)?)
    }

    /// Resolve the bounded Mistral concurrency.
    ///
    /// # Errors
    ///
    /// Returns an error outside the provider safety range.
    pub fn mistral_concurrency(&self) -> Result<MistralConcurrencyLimit, CliError> {
        optional_setting(&self.values, "MISTRAL_MAX_CONCURRENCY")
            .unwrap_or_else(|| "2".to_owned())
            .parse::<usize>()
            .map_err(|_| CliError::InvalidConfiguration {
                canonical_name: "MISTRAL_MAX_CONCURRENCY",
                reason: "expected an integer between 1 and 4",
            })
            .and_then(|value| MistralConcurrencyLimit::new(value).map_err(CliError::from))
    }

    /// Resolve the default-off transcript enrichment capability.
    ///
    /// # Errors
    ///
    /// Returns an error unless the value is the closed `disabled` or `enabled`
    /// state.
    pub fn transcript_enrichment_mode(&self) -> Result<TranscriptEnrichmentMode, CliError> {
        match optional_setting(&self.values, "HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE")
            .unwrap_or_else(|| "disabled".to_owned())
            .as_str()
        {
            "disabled" => Ok(TranscriptEnrichmentMode::Disabled),
            "enabled" => Ok(TranscriptEnrichmentMode::Enabled),
            _ => Err(CliError::InvalidConfiguration {
                canonical_name: "HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE",
                reason: "expected disabled or enabled",
            }),
        }
    }

    /// Resolve the fail-closed Mistral privacy-control attestation.
    ///
    /// # Errors
    ///
    /// Returns an error unless the value is a closed privacy state.
    pub fn mistral_privacy_control(&self) -> Result<MistralPrivacyControl, CliError> {
        match optional_setting(&self.values, "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL")
            .unwrap_or_else(|| "unverified".to_owned())
            .as_str()
        {
            "unverified" => Ok(MistralPrivacyControl::Unverified),
            "training_opt_out_verified" => Ok(MistralPrivacyControl::TrainingOptOutVerified),
            _ => Err(CliError::InvalidConfiguration {
                canonical_name: "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL",
                reason: "expected unverified or training_opt_out_verified",
            }),
        }
    }

    /// Resolve the dedicated local pseudonymization key.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is absent or weaker than the redaction
    /// boundary permits.
    pub fn pseudonymization_key(&self) -> Result<PseudonymizationKey, CliError> {
        let key = required_setting(&self.values, "HARNESS_GRAPH_REDACTION_HMAC_KEY", &[])?;
        validate_dedicated_pseudonymization_key(&self.values, &key, optional_process_value)?;
        Ok(PseudonymizationKey::new(key)?)
    }

    /// Collect configured credentials for exact local redaction without
    /// constructing any provider or database client.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured credential is too short to scan
    /// reliably as an exact secret.
    pub fn sensitive_values_for_redaction(&self) -> Result<SensitiveValueSet, CliError> {
        let names = [
            "MISTRAL_API_KEY",
            "MISTARL_API_KEY",
            "NEO4J_PASSWORD",
            "NEO4J_INATANSE_PASSWORD",
            "HARNESS_GRAPH_REDACTION_HMAC_KEY",
        ];
        let mut loaded_values = Vec::new();
        for name in names {
            for value in [optional_process_value(name), self.values.value(name)]
                .into_iter()
                .flatten()
            {
                if !loaded_values.contains(&value) {
                    loaded_values.push(value);
                }
            }
        }
        let values = loaded_values
            .into_iter()
            .map(SensitiveValue::new)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SensitiveValueSet::new(values))
    }

    /// Resolve the pinned transcript extraction model independently from the
    /// source-safe interpretation model.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured model is not a Mistral family.
    pub fn transcript_mistral_model(&self) -> Result<MistralModelName, CliError> {
        let model = optional_setting(&self.values, "MISTRAL_TRANSCRIPT_MODEL")
            .unwrap_or_else(|| "mistral-small-2603".to_owned());
        Ok(MistralModelName::new(model)?)
    }

    /// Resolve the explicit regional pricing snapshot used by dry-run cost
    /// estimation.
    ///
    /// # Errors
    ///
    /// Returns an error when either integer micro-USD rate is malformed.
    pub fn transcript_token_pricing(&self) -> Result<TranscriptTokenPricing, CliError> {
        let input = parse_u64_setting(
            &self.values,
            "HARNESS_GRAPH_TRANSCRIPT_INPUT_MICROUSD_PER_MILLION_TOKENS",
            "165000",
            "expected an integer micro-USD rate",
        )?;
        let output = parse_u64_setting(
            &self.values,
            "HARNESS_GRAPH_TRANSCRIPT_OUTPUT_MICROUSD_PER_MILLION_TOKENS",
            "660000",
            "expected an integer micro-USD rate",
        )?;
        Ok(TranscriptTokenPricing::new(
            TokenRatePerMillion::new(MicroUsd::new(input)),
            TokenRatePerMillion::new(MicroUsd::new(output)),
        ))
    }

    /// Resolve the bounded expected output size for each map/reduce call.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is malformed or outside the core bound.
    pub fn transcript_estimated_output_tokens_per_request(
        &self,
    ) -> Result<EstimatedOutputTokensPerRequest, CliError> {
        let value = parse_u64_setting(
            &self.values,
            "HARNESS_GRAPH_TRANSCRIPT_ESTIMATED_OUTPUT_TOKENS_PER_REQUEST",
            "1024",
            "expected an integer between 1 and 131072",
        )?;
        Ok(EstimatedOutputTokensPerRequest::new(value)?)
    }

    /// Resolve the durable live-event journal path.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured journal path is invalid.
    pub fn journal_path(&self) -> Result<JournalPath, CliError> {
        Ok(JournalPath::new(PathBuf::from(
            optional_setting(&self.values, "HARNESS_GRAPH_JOURNAL_PATH")
                .unwrap_or_else(|| "data/live-events.jsonl".to_owned()),
        ))?)
    }

    /// Resolve the HTTP bind address.
    ///
    /// # Errors
    ///
    /// Returns an error when the address is not an explicit IP socket.
    pub fn bind_address(&self) -> Result<SocketAddr, CliError> {
        optional_setting(&self.values, "HARNESS_GRAPH_BIND_ADDRESS")
            .unwrap_or_else(|| "127.0.0.1:3000".to_owned())
            .parse()
            .map_err(|_| CliError::InvalidConfiguration {
                canonical_name: "HARNESS_GRAPH_BIND_ADDRESS",
                reason: "expected an IP socket address such as 127.0.0.1:3000",
            })
    }
}

/// Closed operational state for paid transcript-provider work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscriptEnrichmentMode {
    /// Local inventory is allowed, but no provider or graph enrichment call is.
    Disabled,
    /// Explicitly enabled for an authorized enrichment run.
    Enabled,
}

impl From<TranscriptEnrichmentMode> for ExperienceEnrichmentVisibility {
    fn from(value: TranscriptEnrichmentMode) -> Self {
        match value {
            TranscriptEnrichmentMode::Disabled => Self::Disabled,
            TranscriptEnrichmentMode::Enabled => Self::Enabled,
        }
    }
}

/// Attested Mistral account data-training state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MistralPrivacyControl {
    /// Account privacy controls have not been verified; provider transfer blocks.
    Unverified,
    /// An operator verified that API training/data sharing is disabled.
    TrainingOptOutVerified,
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppConfig")
            .field("values", &"[configured; secrets redacted]")
            .finish()
    }
}

/// Validated Neo4j connection settings.
pub struct Neo4jConnection {
    url: Url,
    username: String,
    password: SecretString,
}

impl Neo4jConnection {
    fn new(url: &str, username: &str, password: String) -> Result<Self, CliError> {
        let url = Url::parse(url).map_err(|_| CliError::InvalidConfiguration {
            canonical_name: "NEO4J_CONNECTION_URL",
            reason: "expected a valid neo4j://, bolt://, or neo4j+s:// URL",
        })?;
        if !matches!(
            url.scheme(),
            "neo4j" | "neo4j+s" | "neo4j+ssc" | "bolt" | "bolt+s" | "bolt+ssc"
        ) {
            return Err(CliError::InvalidConfiguration {
                canonical_name: "NEO4J_CONNECTION_URL",
                reason: "unsupported URL scheme",
            });
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(CliError::InvalidConfiguration {
                canonical_name: "NEO4J_CONNECTION_URL",
                reason: "credentials must use dedicated environment variables",
            });
        }
        if username.trim().is_empty() {
            return Err(CliError::InvalidConfiguration {
                canonical_name: "NEO4J_USERNAME",
                reason: "username cannot be empty",
            });
        }
        if password.trim().is_empty() {
            return Err(CliError::InvalidConfiguration {
                canonical_name: "NEO4J_PASSWORD",
                reason: "password cannot be empty",
            });
        }
        Ok(Self {
            url,
            username: username.trim().to_owned(),
            password: SecretString::from(password),
        })
    }

    /// Neo4j URL without credentials.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Neo4j username.
    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Render the host and port shape expected by the Neo4j Bolt driver.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured URL has no host.
    pub fn bolt_address(&self) -> Result<String, CliError> {
        let host = self.url.host_str().ok_or(CliError::InvalidConfiguration {
            canonical_name: "NEO4J_CONNECTION_URL",
            reason: "URL requires a host",
        })?;
        Ok(format!("{host}:{}", self.url.port().unwrap_or(7687)))
    }

    /// Expose the password only to the concrete Neo4j adapter.
    #[must_use]
    pub fn expose_password(&self) -> &str {
        self.password.expose_secret()
    }
}

impl std::fmt::Debug for Neo4jConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Neo4jConnection")
            .field("scheme", &self.url.scheme())
            .field("host", &self.url.host_str())
            .field("username", &self.username)
            .field("password", &"[redacted]")
            .finish()
    }
}

struct ConfigurationFile(Vec<(String, String)>);

impl ConfigurationFile {
    fn load_optional() -> Result<Self, CliError> {
        let project_environment = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(".env");
        let iterator = match dotenvy::from_path_iter(project_environment) {
            Ok(iterator) => iterator,
            Err(error) if error.not_found() => return Ok(Self(Vec::new())),
            Err(_) => return Err(CliError::ConfigurationFile),
        };
        let mut values = Vec::new();
        for entry in iterator {
            values.push(entry.map_err(|_| CliError::ConfigurationFile)?);
        }
        Ok(Self(values))
    }

    fn value(&self, name: &str) -> Option<String> {
        self.0
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    }
}

fn project_canonical_preferred_setting<F>(
    file: &ConfigurationFile,
    canonical: &'static str,
    aliases: &[&str],
    process_value: F,
) -> Result<String, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    file.value(canonical)
        .or_else(|| process_value(canonical))
        .or_else(|| aliases.iter().find_map(|alias| file.value(alias)))
        .or_else(|| aliases.iter().find_map(|alias| process_value(alias)))
        .ok_or(CliError::MissingConfiguration {
            canonical_name: canonical,
        })
}

fn project_file_preferred_setting<F>(
    file: &ConfigurationFile,
    canonical: &'static str,
    aliases: &[&str],
    process_value: F,
) -> Result<String, CliError>
where
    F: Fn(&str) -> Option<String>,
{
    project_file_preferred_value(file, canonical, aliases, process_value).ok_or(
        CliError::MissingConfiguration {
            canonical_name: canonical,
        },
    )
}

fn project_file_preferred_value<F>(
    file: &ConfigurationFile,
    canonical: &str,
    aliases: &[&str],
    process_value: F,
) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    file.value(canonical)
        .or_else(|| aliases.iter().find_map(|alias| file.value(alias)))
        .or_else(|| process_value(canonical))
        .or_else(|| aliases.iter().find_map(|alias| process_value(alias)))
}

fn validate_dedicated_pseudonymization_key<F>(
    file: &ConfigurationFile,
    key: &str,
    process_value: F,
) -> Result<(), CliError>
where
    F: Fn(&str) -> Option<String>,
{
    let credential_names = [
        "MISTRAL_API_KEY",
        "MISTARL_API_KEY",
        "NEO4J_PASSWORD",
        "NEO4J_INATANSE_PASSWORD",
    ];
    if credential_names.iter().any(|name| {
        [process_value(name), file.value(name)]
            .into_iter()
            .flatten()
            .any(|credential| credential == key)
    }) {
        Err(CliError::InvalidConfiguration {
            canonical_name: "HARNESS_GRAPH_REDACTION_HMAC_KEY",
            reason: "must be a dedicated key distinct from provider and database credentials",
        })
    } else {
        Ok(())
    }
}

fn required_setting(
    file: &ConfigurationFile,
    canonical: &'static str,
    aliases: &[&str],
) -> Result<String, CliError> {
    optional_process_value(canonical)
        .or_else(|| file.value(canonical))
        .or_else(|| {
            aliases
                .iter()
                .find_map(|alias| optional_process_value(alias))
        })
        .or_else(|| aliases.iter().find_map(|alias| file.value(alias)))
        .ok_or(CliError::MissingConfiguration {
            canonical_name: canonical,
        })
}

fn optional_setting(file: &ConfigurationFile, name: &str) -> Option<String> {
    optional_setting_with_lookup(file, name, optional_process_value)
}

fn optional_setting_with_lookup<F>(
    file: &ConfigurationFile,
    name: &str,
    process_value: F,
) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    process_value(name).or_else(|| file.value(name))
}

fn optional_process_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn parse_u64_setting(
    file: &ConfigurationFile,
    canonical_name: &'static str,
    default_value: &'static str,
    reason: &'static str,
) -> Result<u64, CliError> {
    optional_setting(file, canonical_name)
        .unwrap_or_else(|| default_value.to_owned())
        .parse::<u64>()
        .map_err(|_| CliError::InvalidConfiguration {
            canonical_name,
            reason,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_capability_does_not_validate_unrelated_provider_settings() {
        let config = AppConfig {
            values: ConfigurationFile(vec![
                (
                    "CODEX_SESSION_RAW_DATA_PATH".to_owned(),
                    env!("CARGO_MANIFEST_DIR").to_owned(),
                ),
                (
                    "NEO4J_CONNECTION_URL".to_owned(),
                    "not-a-neo4j-url".to_owned(),
                ),
                ("NEO4J_PASSWORD".to_owned(), "not-used".to_owned()),
                ("MISTRAL_API_KEY".to_owned(), "not-used".to_owned()),
                (
                    "MISTRAL_MAX_CONCURRENCY".to_owned(),
                    "outside-the-domain".to_owned(),
                ),
            ]),
        };

        assert!(config.archive_root().is_ok());
        assert!(config.neo4j().is_err());
        assert!(config.mistral_concurrency().is_err());
    }

    #[test]
    fn debug_output_never_contains_configuration_values() {
        let config = AppConfig {
            values: ConfigurationFile(vec![(
                "MISTRAL_API_KEY".to_owned(),
                "must-never-be-rendered".to_owned(),
            )]),
        };

        let rendered = format!("{config:?}");
        assert!(!rendered.contains("must-never-be-rendered"));
        assert_eq!(
            rendered,
            "AppConfig { values: \"[configured; secrets redacted]\" }"
        );
    }

    #[test]
    fn transcript_provider_work_is_disabled_and_privacy_blocked_by_default() {
        let config = AppConfig {
            values: ConfigurationFile(Vec::new()),
        };

        assert!(matches!(
            config.transcript_enrichment_mode(),
            Ok(TranscriptEnrichmentMode::Disabled)
        ));
        assert!(matches!(
            config.mistral_privacy_control(),
            Ok(MistralPrivacyControl::Unverified)
        ));
    }

    #[test]
    fn transcript_provider_work_requires_closed_explicit_states() {
        let config = AppConfig {
            values: ConfigurationFile(vec![
                (
                    "HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE".to_owned(),
                    "enabled".to_owned(),
                ),
                (
                    "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL".to_owned(),
                    "training_opt_out_verified".to_owned(),
                ),
            ]),
        };

        assert!(matches!(
            config.transcript_enrichment_mode(),
            Ok(TranscriptEnrichmentMode::Enabled)
        ));
        assert!(matches!(
            config.mistral_privacy_control(),
            Ok(MistralPrivacyControl::TrainingOptOutVerified)
        ));
    }

    #[test]
    fn repository_canonical_mistral_key_precedes_inherited_and_alias_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let values = ConfigurationFile(vec![
            (
                "MISTRAL_API_KEY".to_owned(),
                "repository-canonical-canary".to_owned(),
            ),
            (
                "MISTARL_API_KEY".to_owned(),
                "repository-alias-canary".to_owned(),
            ),
        ]);
        let process_value = |name: &str| match name {
            "MISTRAL_API_KEY" => Some("process-canonical-canary".to_owned()),
            "MISTARL_API_KEY" => Some("process-alias-canary".to_owned()),
            _ => None,
        };

        let selected = project_canonical_preferred_setting(
            &values,
            "MISTRAL_API_KEY",
            &["MISTARL_API_KEY"],
            process_value,
        )?;

        assert_eq!(selected, "repository-canonical-canary");
        Ok(())
    }

    #[test]
    fn repository_neo4j_configuration_precedes_unrelated_inherited_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let values = ConfigurationFile(vec![
            (
                "NEO4J_CONECTION_URL".to_owned(),
                "neo4j://repository.example:7687".to_owned(),
            ),
            (
                "NEO4J_INATANSE_PASSWORD".to_owned(),
                "repository-password-canary".to_owned(),
            ),
        ]);
        let process_value = |name: &str| match name {
            "NEO4J_CONNECTION_URL" => Some("neo4j://unrelated.example:7687".to_owned()),
            "NEO4J_PASSWORD" => Some("unrelated-password-canary".to_owned()),
            _ => None,
        };

        let selected_url = project_file_preferred_setting(
            &values,
            "NEO4J_CONNECTION_URL",
            &["NEO4J_CONECTION_URL"],
            process_value,
        )?;
        let selected_password = project_file_preferred_setting(
            &values,
            "NEO4J_PASSWORD",
            &["NEO4J_INATANSE_PASSWORD"],
            process_value,
        )?;

        assert_eq!(selected_url, "neo4j://repository.example:7687");
        assert_eq!(selected_password, "repository-password-canary");
        Ok(())
    }

    #[test]
    fn transcript_credential_requires_the_canonical_project_file_key() {
        let alias_only = AppConfig {
            values: ConfigurationFile(vec![(
                "MISTARL_API_KEY".to_owned(),
                "source-safe-alias-canary".to_owned(),
            )]),
        };
        assert!(alias_only.transcript_mistral_credential().is_err());

        let canonical = AppConfig {
            values: ConfigurationFile(vec![(
                "MISTRAL_API_KEY".to_owned(),
                "source-safe-canonical-canary".to_owned(),
            )]),
        };
        assert!(canonical.transcript_mistral_credential().is_ok());
    }

    #[test]
    fn mistral_key_falls_back_to_process_canonical_only_when_project_key_is_absent()
    -> Result<(), Box<dyn std::error::Error>> {
        let values = ConfigurationFile(vec![(
            "MISTARL_API_KEY".to_owned(),
            "repository-alias-canary".to_owned(),
        )]);
        let process_value =
            |name: &str| (name == "MISTRAL_API_KEY").then(|| "process-canonical-canary".to_owned());

        let selected = project_canonical_preferred_setting(
            &values,
            "MISTRAL_API_KEY",
            &["MISTARL_API_KEY"],
            process_value,
        )?;

        assert_eq!(selected, "process-canonical-canary");
        Ok(())
    }

    #[test]
    fn pseudonymization_key_must_be_distinct_from_every_loaded_credential() {
        let shared = "shared-source-safe-key-material-000000000000";
        let values = ConfigurationFile(vec![("MISTRAL_API_KEY".to_owned(), shared.to_owned())]);
        let no_process_values = |_: &str| None;

        assert!(
            validate_dedicated_pseudonymization_key(&values, shared, no_process_values).is_err()
        );
        assert!(
            validate_dedicated_pseudonymization_key(
                &values,
                "dedicated-source-safe-hmac-key-000000000000",
                no_process_values,
            )
            .is_ok()
        );
    }

    #[test]
    fn runtime_transcript_controls_remain_process_overridable() {
        let file = ConfigurationFile(vec![
            (
                "HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE".to_owned(),
                "disabled".to_owned(),
            ),
            (
                "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL".to_owned(),
                "unverified".to_owned(),
            ),
            (
                "HARNESS_GRAPH_REDACTION_HMAC_KEY".to_owned(),
                "repository-hmac-canary".to_owned(),
            ),
        ]);
        let process_value = |name: &str| match name {
            "HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE" => Some("enabled".to_owned()),
            "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL" => Some("training_opt_out_verified".to_owned()),
            "HARNESS_GRAPH_REDACTION_HMAC_KEY" => Some("runtime-hmac-canary".to_owned()),
            _ => None,
        };

        assert_eq!(
            optional_setting_with_lookup(
                &file,
                "HARNESS_GRAPH_TRANSCRIPT_ENRICHMENT_MODE",
                process_value,
            ),
            Some("enabled".to_owned())
        );
        assert_eq!(
            optional_setting_with_lookup(
                &file,
                "HARNESS_GRAPH_MISTRAL_PRIVACY_CONTROL",
                process_value,
            ),
            Some("training_opt_out_verified".to_owned())
        );
        assert_eq!(
            optional_setting_with_lookup(&file, "HARNESS_GRAPH_REDACTION_HMAC_KEY", process_value,),
            Some("runtime-hmac-canary".to_owned())
        );
    }

    #[test]
    fn transcript_mode_maps_to_exact_experience_visibility() {
        assert_eq!(
            ExperienceEnrichmentVisibility::from(TranscriptEnrichmentMode::Disabled),
            ExperienceEnrichmentVisibility::Disabled
        );
        assert_eq!(
            ExperienceEnrichmentVisibility::from(TranscriptEnrichmentMode::Enabled),
            ExperienceEnrichmentVisibility::Enabled
        );
    }

    #[test]
    fn redaction_secret_collection_is_source_safe_and_client_free()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = AppConfig {
            values: ConfigurationFile(vec![
                (
                    "MISTRAL_API_KEY".to_owned(),
                    "provider-secret-canary".to_owned(),
                ),
                (
                    "NEO4J_PASSWORD".to_owned(),
                    "database-secret-canary".to_owned(),
                ),
            ]),
        };

        let rendered = format!("{:?}", config.sensitive_values_for_redaction()?);
        assert!(rendered.starts_with("SensitiveValueSet { count: "));
        assert!(!rendered.contains("canary"));
        Ok(())
    }
}
