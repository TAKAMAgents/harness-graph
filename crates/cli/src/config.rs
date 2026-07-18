//! Typed environment configuration with secret-safe debug behavior.

use std::{net::SocketAddr, path::PathBuf};

use harness_graph_domain::GraphNamespace;
use harness_graph_graph_port::BatchSize;
use harness_graph_ingestion::ArchiveRoot;
use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::CliError;

/// Complete validated runtime configuration.
pub struct AppConfig {
    archive_root: ArchiveRoot,
    neo4j: Neo4jConnection,
    graph_namespace: GraphNamespace,
    graph_batch_size: BatchSize,
    mistral_api_key: MistralApiKey,
    mistral_model: MistralModelName,
    bind_address: SocketAddr,
}

impl AppConfig {
    /// Load `.env` when present and validate all required settings.
    ///
    /// # Errors
    ///
    /// Returns an error when required configuration is absent or invalid, or
    /// when the configured archive root is not a directory.
    pub fn load() -> Result<Self, CliError> {
        let file_values = ConfigurationFile::load_optional()?;
        let archive_path = required_setting(&file_values, "CODEX_SESSION_RAW_DATA_PATH", &[])?;
        let archive_root = ArchiveRoot::new(PathBuf::from(archive_path))?;
        let neo4j_url = required_setting(
            &file_values,
            "NEO4J_CONNECTION_URL",
            &["NEO4J_CONECTION_URL"],
        )?;
        let neo4j_password =
            required_setting(&file_values, "NEO4J_PASSWORD", &["NEO4J_INATANSE_PASSWORD"])?;
        let neo4j_username =
            optional_setting(&file_values, "NEO4J_USERNAME").unwrap_or_else(|| "neo4j".to_owned());
        let graph_namespace = GraphNamespace::new(
            optional_setting(&file_values, "HARNESS_GRAPH_NAMESPACE")
                .unwrap_or_else(|| "default".to_owned()),
        )?;
        let graph_batch_size = optional_setting(&file_values, "HARNESS_GRAPH_BATCH_SIZE")
            .unwrap_or_else(|| "250".to_owned())
            .parse::<usize>()
            .map_err(|_| CliError::InvalidConfiguration {
                canonical_name: "HARNESS_GRAPH_BATCH_SIZE",
                reason: "expected an integer between 1 and 10,000",
            })
            .and_then(|value| BatchSize::new(value).map_err(CliError::from))?;
        let mistral_api_key =
            required_setting(&file_values, "MISTRAL_API_KEY", &["MISTARL_API_KEY"])?;
        let mistral_model = optional_setting(&file_values, "MISTRAL_MODEL")
            .unwrap_or_else(|| "mistral-small-latest".to_owned());
        let bind_address = optional_setting(&file_values, "HARNESS_GRAPH_BIND_ADDRESS")
            .unwrap_or_else(|| "127.0.0.1:3000".to_owned())
            .parse()
            .map_err(|_| CliError::InvalidConfiguration {
                canonical_name: "HARNESS_GRAPH_BIND_ADDRESS",
                reason: "expected an IP socket address such as 127.0.0.1:3000",
            })?;

        Ok(Self {
            archive_root,
            neo4j: Neo4jConnection::new(&neo4j_url, &neo4j_username, neo4j_password)?,
            graph_namespace,
            graph_batch_size,
            mistral_api_key: MistralApiKey::new(mistral_api_key)?,
            mistral_model: MistralModelName::new(&mistral_model)?,
            bind_address,
        })
    }

    /// Verified archive root.
    #[must_use]
    pub const fn archive_root(&self) -> &ArchiveRoot {
        &self.archive_root
    }

    /// Neo4j connection settings.
    #[must_use]
    pub const fn neo4j(&self) -> &Neo4jConnection {
        &self.neo4j
    }

    /// Namespace isolating this application's graph projection.
    #[must_use]
    pub const fn graph_namespace(&self) -> &GraphNamespace {
        &self.graph_namespace
    }

    /// Maximum number of logical graph commands per transaction.
    #[must_use]
    pub const fn graph_batch_size(&self) -> BatchSize {
        self.graph_batch_size
    }

    /// Mistral API key.
    #[must_use]
    pub const fn mistral_api_key(&self) -> &MistralApiKey {
        &self.mistral_api_key
    }

    /// Mistral model selection.
    #[must_use]
    pub const fn mistral_model(&self) -> &MistralModelName {
        &self.mistral_model
    }

    /// HTTP bind address.
    #[must_use]
    pub const fn bind_address(&self) -> SocketAddr {
        self.bind_address
    }
}

impl std::fmt::Debug for AppConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppConfig")
            .field("archive_root", &"[configured]")
            .field("neo4j", &self.neo4j)
            .field("graph_namespace", &self.graph_namespace)
            .field("graph_batch_size", &self.graph_batch_size)
            .field("mistral_api_key", &"[redacted]")
            .field("mistral_model", &self.mistral_model)
            .field("bind_address", &self.bind_address)
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

/// Secret Mistral API key with redacted debug output.
pub struct MistralApiKey(SecretString);

impl MistralApiKey {
    fn new(value: String) -> Result<Self, CliError> {
        if value.trim().is_empty() {
            return Err(CliError::InvalidConfiguration {
                canonical_name: "MISTRAL_API_KEY",
                reason: "API key cannot be empty",
            });
        }
        Ok(Self(SecretString::from(value)))
    }

    /// Expose the API key only to the concrete Mistral adapter.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl std::fmt::Debug for MistralApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MistralApiKey([redacted])")
    }
}

/// Validated Mistral model name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MistralModelName(String);

impl MistralModelName {
    fn new(value: &str) -> Result<Self, CliError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(CliError::InvalidConfiguration {
                canonical_name: "MISTRAL_MODEL",
                reason: "model name cannot be empty",
            });
        }
        Ok(Self(value.to_owned()))
    }

    /// Borrow the provider model name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

struct ConfigurationFile(Vec<(String, String)>);

impl ConfigurationFile {
    fn load_optional() -> Result<Self, CliError> {
        let iterator = match dotenvy::dotenv_iter() {
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

fn required_setting(
    file: &ConfigurationFile,
    canonical: &'static str,
    aliases: &[&str],
) -> Result<String, CliError> {
    file.value(canonical)
        .or_else(|| aliases.iter().find_map(|alias| file.value(alias)))
        .or_else(|| optional_process_value(canonical))
        .or_else(|| {
            aliases
                .iter()
                .find_map(|alias| optional_process_value(alias))
        })
        .ok_or(CliError::MissingConfiguration {
            canonical_name: canonical,
        })
}

fn optional_setting(file: &ConfigurationFile, name: &str) -> Option<String> {
    file.value(name).or_else(|| optional_process_value(name))
}

fn optional_process_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
