//! Export archive discovery and integrity verification.

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufRead, BufReader, Read},
    path::{Component, Path, PathBuf},
};

use harness_graph_domain::{RecordCount, SessionId, SourceDigest};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::IngestionError;

/// Validated root of a sensitive Codex exporter archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRoot(PathBuf);

impl ArchiveRoot {
    /// Validate an archive root directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is not a readable directory.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, IngestionError> {
        let path = path.into();
        if !path.is_dir() {
            return Err(IngestionError::InvalidArchiveRoot);
        }
        Ok(Self(path))
    }

    /// Discover published session bundles under active and/or archived scopes.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable directories, malformed metadata,
    /// invalid identities, or conflicting snapshots.
    pub fn discover(&self, scope: SessionScope) -> Result<SessionCatalog, IngestionError> {
        let mut sessions = BTreeMap::<SessionId, SessionBundle>::new();
        for source_kind in scope.source_kinds() {
            self.discover_source_kind(*source_kind, &mut sessions)?;
        }
        Ok(SessionCatalog(sessions.into_values().collect()))
    }

    fn discover_source_kind(
        &self,
        source_kind: SourceKind,
        sessions: &mut BTreeMap<SessionId, SessionBundle>,
    ) -> Result<(), IngestionError> {
        let scope_root = self.0.join(source_kind.directory_name());
        if !scope_root.exists() {
            return Ok(());
        }
        let date_entries = read_directory(&scope_root)?;
        for date_entry in date_entries {
            let date_entry = date_entry.map_err(|source| IngestionError::Filesystem {
                operation: "read archive scope entry",
                path: scope_root.clone(),
                source,
            })?;
            let date_path = date_entry.path();
            if !date_path.is_dir() {
                continue;
            }
            for session_entry in read_directory(&date_path)? {
                let session_entry = session_entry.map_err(|source| IngestionError::Filesystem {
                    operation: "read archive date entry",
                    path: date_path.clone(),
                    source,
                })?;
                let session_path = session_entry.path();
                if !session_path.is_dir() {
                    continue;
                }
                let bundle = SessionBundle::from_directory(session_path, source_kind)?;
                match sessions.get(&bundle.session_id) {
                    Some(existing) if existing.source_digest != bundle.source_digest => {
                        return Err(IngestionError::ConflictingSessionSnapshots {
                            session_id: bundle.session_id,
                        });
                    }
                    Some(_) => {}
                    None => {
                        sessions.insert(bundle.session_id, bundle);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Archive scope requested by an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionScope {
    /// Published active sessions only.
    Active,
    /// Published archived sessions only.
    Archived,
    /// Both published scopes, deduplicated by session identity.
    All,
}

impl SessionScope {
    fn source_kinds(self) -> &'static [SourceKind] {
        match self {
            Self::Active => &[SourceKind::Active],
            Self::Archived => &[SourceKind::Archived],
            Self::All => &[SourceKind::Active, SourceKind::Archived],
        }
    }
}

/// Exporter location class for a session bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    /// Session remains active.
    Active,
    /// Session was moved to the archive.
    Archived,
}

impl SourceKind {
    const fn directory_name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }
}

/// Discovered session bundles, ordered by session identity.
#[derive(Debug, Clone)]
pub struct SessionCatalog(Vec<SessionBundle>);

impl SessionCatalog {
    /// Number of discovered unique sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no sessions were discovered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate over discovered sessions.
    pub fn iter(&self) -> impl Iterator<Item = &SessionBundle> {
        self.0.iter()
    }

    /// Find a session by stable identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested session is absent.
    pub fn require(&self, session_id: SessionId) -> Result<&SessionBundle, IngestionError> {
        self.0
            .iter()
            .find(|bundle| bundle.session_id == session_id)
            .ok_or(IngestionError::SessionNotFound { session_id })
    }
}

/// A discovered session whose metadata has been parsed but whose checksums are
/// not yet trusted.
#[derive(Debug, Clone)]
pub struct SessionBundle {
    path: PathBuf,
    session_id: SessionId,
    source_kind: SourceKind,
    source_digest: SourceDigest,
    expected_records: RecordCount,
    raw_relative_path: PathBuf,
}

impl SessionBundle {
    fn from_directory(path: PathBuf, source_kind: SourceKind) -> Result<Self, IngestionError> {
        let directory_name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or(IngestionError::SessionIdentityMismatch)?;
        let directory_session_id = SessionId::parse(directory_name)?;
        let metadata_path = path.join("metadata.json");
        let metadata_file =
            File::open(&metadata_path).map_err(|source| IngestionError::Filesystem {
                operation: "open metadata",
                path: metadata_path,
                source,
            })?;
        let metadata: RawMetadata = serde_json::from_reader(BufReader::new(metadata_file))
            .map_err(|source| IngestionError::InvalidMetadata { source })?;
        let metadata_session_id = SessionId::parse(&metadata.session_id)?;
        if metadata_session_id != directory_session_id {
            return Err(IngestionError::SessionIdentityMismatch);
        }
        if metadata.parse_error_count != 0 {
            return Err(IngestionError::UnpublishableBundle {
                session_id: metadata_session_id,
                reason: "exporter reported parse errors",
            });
        }
        if !metadata.source_stable_during_copy {
            return Err(IngestionError::UnpublishableBundle {
                session_id: metadata_session_id,
                reason: "source changed during exporter copy",
            });
        }
        let raw_relative_path = PathBuf::from(metadata.raw_relative_path);
        if raw_relative_path != Path::new("raw/rollout.jsonl")
            || !is_safe_relative_path(&raw_relative_path)
        {
            return Err(IngestionError::UnsafeChecksumPath);
        }
        Ok(Self {
            path,
            session_id: metadata_session_id,
            source_kind,
            source_digest: SourceDigest::parse_hex(&metadata.raw_sha256)?,
            expected_records: RecordCount::new(metadata.record_count),
            raw_relative_path,
        })
    }

    /// Stable session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Location class.
    #[must_use]
    pub const fn source_kind(&self) -> SourceKind {
        self.source_kind
    }

    /// Metadata-declared source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Metadata-declared record count.
    #[must_use]
    pub const fn expected_records(&self) -> RecordCount {
        self.expected_records
    }

    /// Verify all declared checksums and return a trusted bundle.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest is malformed, a declared path is
    /// unsafe, a file cannot be read, or a digest does not match.
    pub fn verify(&self) -> Result<VerifiedSessionBundle, IngestionError> {
        let manifest_path = self.path.join("checksums.sha256");
        let manifest = File::open(&manifest_path).map_err(|source| IngestionError::Filesystem {
            operation: "open checksum manifest",
            path: manifest_path,
            source,
        })?;
        for (offset, line) in BufReader::new(manifest).lines().enumerate() {
            let line = line.map_err(|source| IngestionError::Filesystem {
                operation: "read checksum manifest",
                path: self.path.join("checksums.sha256"),
                source,
            })?;
            verify_manifest_line(&self.path, offset + 1, &line)?;
        }
        let raw_path = self.path.join(&self.raw_relative_path);
        let actual_digest = hash_file(&raw_path)?;
        if actual_digest != self.source_digest {
            return Err(IngestionError::RawDigestMismatch);
        }
        Ok(VerifiedSessionBundle {
            raw_path,
            session_id: self.session_id,
            source_kind: self.source_kind,
            source_digest: self.source_digest,
            expected_records: self.expected_records,
        })
    }
}

/// A session bundle whose full integrity manifest has passed.
#[derive(Debug, Clone)]
pub struct VerifiedSessionBundle {
    raw_path: PathBuf,
    session_id: SessionId,
    source_kind: SourceKind,
    source_digest: SourceDigest,
    expected_records: RecordCount,
}

impl VerifiedSessionBundle {
    /// Stable session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Verified source digest.
    #[must_use]
    pub const fn source_digest(&self) -> SourceDigest {
        self.source_digest
    }

    /// Expected record count.
    #[must_use]
    pub const fn expected_records(&self) -> RecordCount {
        self.expected_records
    }

    /// Source location class.
    #[must_use]
    pub const fn source_kind(&self) -> SourceKind {
        self.source_kind
    }

    pub(crate) fn open_raw(&self) -> Result<File, IngestionError> {
        File::open(&self.raw_path).map_err(|source| IngestionError::Filesystem {
            operation: "open canonical rollout",
            path: self.raw_path.clone(),
            source,
        })
    }
}

#[derive(Deserialize)]
struct RawMetadata {
    session_id: String,
    raw_relative_path: String,
    raw_sha256: String,
    record_count: u64,
    parse_error_count: u64,
    source_stable_during_copy: bool,
}

fn read_directory(path: &Path) -> Result<fs::ReadDir, IngestionError> {
    fs::read_dir(path).map_err(|source| IngestionError::Filesystem {
        operation: "read directory",
        path: path.to_path_buf(),
        source,
    })
}

fn verify_manifest_line(
    bundle_root: &Path,
    line_number: usize,
    line: &str,
) -> Result<(), IngestionError> {
    let (digest, relative_path) =
        line.split_once("  ")
            .ok_or(IngestionError::InvalidChecksumEntry {
                line_number,
                reason: "expected SHA-256 followed by two spaces and a relative path",
            })?;
    let expected =
        SourceDigest::parse_hex(digest).map_err(|_| IngestionError::InvalidChecksumEntry {
            line_number,
            reason: "invalid SHA-256 digest",
        })?;
    let relative_path = PathBuf::from(relative_path);
    if !is_safe_relative_path(&relative_path) {
        return Err(IngestionError::UnsafeChecksumPath);
    }
    let declared_path = bundle_root.join(relative_path);
    let metadata =
        fs::symlink_metadata(&declared_path).map_err(|source| IngestionError::Filesystem {
            operation: "inspect declared file",
            path: declared_path.clone(),
            source,
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(IngestionError::UnsafeChecksumPath);
    }
    if hash_file(&declared_path)? != expected {
        return Err(IngestionError::ChecksumMismatch);
    }
    Ok(())
}

fn is_safe_relative_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn hash_file(path: &Path) -> Result<SourceDigest, IngestionError> {
    let mut file = File::open(path).map_err(|source| IngestionError::Filesystem {
        operation: "open declared file",
        path: path.to_path_buf(),
        source,
    })?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| IngestionError::Filesystem {
                operation: "hash declared file",
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    SourceDigest::parse_hex(&hex::encode(hasher.finalize())).map_err(IngestionError::from)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::is_safe_relative_path;

    #[test]
    fn checksum_paths_cannot_escape_bundle() {
        assert!(is_safe_relative_path(Path::new("raw/rollout.jsonl")));
        assert!(!is_safe_relative_path(Path::new("../secret")));
        assert!(!is_safe_relative_path(Path::new("/absolute")));
    }
}
