//! Filesystem backend for the Eter protocol.
//!
//! Storage layout:
//! - `<root>/<entry_id>/<version>-<entry_id>.md`
//! - `<root>/.eter-gc-generation`
//! - `version` is a 64-bit value encoded as 16 lowercase hex digits.
//! - each file contains YAML frontmatter and a markdown body.
//!
//! Frontmatter stores protocol fields by key. A key's presence means the field
//! has content in that full-entry snapshot; omitting the key means the field is
//! absent. Normal backend writes copy unchanged values forward, so omission is
//! used for deletion rather than sparse inheritance.
//!
//! This backend stores one markdown file per `(EntryId, version)` snapshot. It
//! keeps no persistent retired-version state on disk. Retired/live version
//! bookkeeping is in memory and controlled by callers through [`crate::GcOption`].

use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tracing::trace;

use crate::{
    Eter, Eterator, Field, FieldRow, GcGeneration, GcOption, Lifecycle, LiveEntries, Resolution,
    SnapshotRef, VersionedRow, WriteTxn,
};

const GC_GENERATION_FILENAME: &str = ".eter-gc-generation";

type DecodedSnapshot = (Eterator, Map<String, Value>, String);

/// Filesystem-native entry identifier.
///
/// This type enforces path-safety invariants required by directory-backed
/// storage and avoids using raw `String` as protocol identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FilesystemEntryId(String);

impl FilesystemEntryId {
    /// Construct a validated filesystem entry id.
    pub fn new(raw: impl Into<String>) -> Result<Self, FilesystemError> {
        let raw = raw.into();
        Self::validate(&raw)?;
        Ok(Self(raw))
    }

    /// Borrow this identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(entry: &str) -> Result<(), FilesystemError> {
        if entry.is_empty() || entry == "." || entry == ".." {
            return Err(FilesystemError::InvalidEntryId(entry.to_owned()));
        }
        if entry.contains('/') || entry.contains('\0') {
            return Err(FilesystemError::InvalidEntryId(entry.to_owned()));
        }
        if entry.len() > 255 {
            return Err(FilesystemError::InvalidEntryId(entry.to_owned()));
        }
        Ok(())
    }
}

impl std::fmt::Display for FilesystemEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for FilesystemEntryId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for FilesystemEntryId {
    type Error = FilesystemError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Field registry for the filesystem backend.
///
/// Field membership is fixed when the backend is opened. Add user-defined
/// fields with [`FilesystemFieldRegistry::with_field`] using concrete field
/// types known at compile time.
///
/// The registry is static for a given backend instance: registering additional
/// fields after opening is unsupported by design.
#[derive(Clone, Debug, Default)]
pub struct FilesystemFieldRegistry {
    by_type: HashMap<TypeId, String>,
    by_key: HashMap<String, TypeId>,
}

impl FilesystemFieldRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a field type with a frontmatter key.
    ///
    /// The key is the exact YAML frontmatter key used for this field in every
    /// version file.
    ///
    /// # Panics
    ///
    /// Panics if `key` is empty or if the field/key has already been registered.
    pub fn with_field<F: Field>(mut self, key: impl Into<String>) -> Self {
        let key = key.into();
        assert!(!key.is_empty(), "filesystem field key must not be empty");

        let type_id = TypeId::of::<F>();
        if self.by_type.contains_key(&type_id) {
            panic!("field type registered more than once");
        }
        if self.by_key.contains_key(&key) {
            panic!("frontmatter key registered more than once");
        }
        self.by_type.insert(type_id, key.clone());
        self.by_key.insert(key, type_id);
        self
    }

    fn key_for<F: Field>(&self) -> Option<&str> {
        self.by_type.get(&TypeId::of::<F>()).map(String::as_str)
    }

    fn contains<F: Field>(&self) -> bool {
        self.by_type.contains_key(&TypeId::of::<F>())
    }
}

/// Filesystem backend implementation of [`Eter`].
///
/// `EntryId` is represented as [`FilesystemEntryId`] and validated for path safety.
/// `Lifecycle` state type is user-defined.
///
/// A write creates a full entry snapshot at the next allocated version:
/// updated fields are written from the transaction and unchanged fields are
/// copied from the latest earlier snapshot for that entry.
///
/// Note: this backend keeps the retired-version set only in memory. Reopening a
/// filesystem store starts with no retired snapshots.
#[derive(Debug)]
pub struct FilesystemBackend<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    root: PathBuf,
    fields: FilesystemFieldRegistry,
    retired: BTreeSet<Eterator>,
    current: Eterator,
    generation: GcGeneration,
    _lifecycle: std::marker::PhantomData<L>,
}

impl<L> FilesystemBackend<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// Open or initialize a filesystem store at `root`.
    ///
    /// If `root` does not exist, it is created. If it exists, it must be a
    /// directory and all existing entries must conform to this backend's
    /// on-disk naming and format rules.
    ///
    /// The returned backend starts with an empty in-memory retired set, even
    /// when opening an existing store.
    ///
    /// Note: opening an existing store may create `.eter-gc-generation` with
    /// the initial generation if the metadata file is missing.
    ///
    /// Note: retired snapshots are not persisted by this backend. Callers own
    /// live/retired bookkeeping across process restarts.
    ///
    /// # Panics
    ///
    /// Panics if the registry does not contain [`Lifecycle<L>`].
    pub fn open(
        root: impl Into<PathBuf>, fields: FilesystemFieldRegistry,
    ) -> Result<Self, FilesystemError> {
        trace!("filesystem open begin");
        assert!(
            fields.contains::<Lifecycle<L>>(),
            "filesystem backend requires Lifecycle field registration",
        );

        let root = root.into();
        if root.exists() {
            if !root.is_dir() {
                return Err(FilesystemError::InvalidStoreRoot(root));
            }
        } else {
            fs::create_dir_all(&root)?;
        }

        let current = Self::scan_current_version(&root)?;
        let stored_generation = Self::read_gc_generation(&root)?;
        let generation = stored_generation.unwrap_or(GcGeneration::INITIAL);
        if stored_generation != Some(generation) {
            Self::persist_gc_generation_to(&root, generation)?;
        }
        trace!(
            "filesystem open end: current_version={} gc_generation={}",
            current.version(),
            generation.number()
        );
        Ok(Self {
            root,
            fields,
            retired: BTreeSet::new(),
            current,
            generation,
            _lifecycle: std::marker::PhantomData,
        })
    }

    fn gc_generation_path(root: &Path) -> PathBuf {
        root.join(GC_GENERATION_FILENAME)
    }

    fn read_gc_generation(root: &Path) -> Result<Option<GcGeneration>, FilesystemError> {
        let path = Self::gc_generation_path(root);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(path)?;
        let trimmed = text.trim();
        if trimmed.len() != 16 {
            return Err(FilesystemError::InvalidGcGeneration(text));
        }
        let generation = u64::from_str_radix(trimmed, 16)
            .map_err(|_| FilesystemError::InvalidGcGeneration(text))?;
        Ok(Some(GcGeneration(generation)))
    }

    fn persist_gc_generation_to(
        root: &Path, generation: GcGeneration,
    ) -> Result<(), FilesystemError> {
        let path = Self::gc_generation_path(root);
        fs::write(path, format!("{:016x}\n", generation.number()))?;
        Ok(())
    }

    fn persist_gc_generation(&self, generation: GcGeneration) -> Result<(), FilesystemError> {
        Self::persist_gc_generation_to(&self.root, generation)
    }

    fn next_version(&self) -> Eterator {
        Eterator(
            self.current
                .version()
                .checked_add(1)
                .unwrap_or_else(|| panic!("filesystem version space exhausted")),
        )
    }

    fn entry_dir(&self, entry: &FilesystemEntryId) -> PathBuf {
        self.root.join(entry.as_str())
    }

    fn parse_versioned_filename(
        name: &str, entry: &FilesystemEntryId,
    ) -> Result<Eterator, FilesystemError> {
        let expected_suffix = format!("-{}.md", entry.as_str());
        if !name.ends_with(&expected_suffix) {
            return Err(FilesystemError::InvalidFilename(name.to_owned()));
        }
        let hex = name
            .strip_suffix(&expected_suffix)
            .ok_or_else(|| FilesystemError::InvalidFilename(name.to_owned()))?;
        if hex.len() != 16 {
            return Err(FilesystemError::InvalidFilename(name.to_owned()));
        }
        let version = u64::from_str_radix(hex, 16)
            .map_err(|_| FilesystemError::InvalidFilename(name.to_owned()))?;
        Ok(Eterator(version))
    }

    fn encode_snapshot(header: &Map<String, Value>, body: &str) -> Result<String, FilesystemError> {
        Self::validate_header(header)?;
        let yaml = serde_yaml::to_string(header)?;
        Ok(format!("---\n{yaml}---\n\n{body}"))
    }

    fn decode_snapshot(text: &str) -> Result<(Map<String, Value>, String), FilesystemError> {
        let rest = text.strip_prefix("---\n").ok_or(FilesystemError::InvalidFrontmatter)?;
        let sep = "\n---\n";
        let idx = rest.find(sep).ok_or(FilesystemError::InvalidFrontmatter)?;
        let yaml = &rest[..idx];
        let body = rest[idx + sep.len()..].strip_prefix('\n').unwrap_or(&rest[idx + sep.len()..]);
        let header: Map<String, Value> = serde_yaml::from_str(yaml)?;
        Self::validate_header(&header)?;
        Ok((header, body.to_owned()))
    }

    fn validate_header(header: &Map<String, Value>) -> Result<(), FilesystemError> {
        if let Some((key, _)) = header.iter().find(|(_, value)| value.is_null()) {
            return Err(FilesystemError::NullFieldValue(key.clone()));
        }
        Ok(())
    }

    fn list_entry_versions(
        &self, entry: &FilesystemEntryId,
    ) -> Result<Vec<(Eterator, PathBuf)>, FilesystemError> {
        let dir = self.entry_dir(entry);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for dir_entry in fs::read_dir(&dir)? {
            let dir_entry = dir_entry?;
            if !dir_entry.file_type()?.is_file() {
                continue;
            }
            let name = dir_entry.file_name().to_string_lossy().to_string();
            let version = Self::parse_versioned_filename(&name, entry)?;
            out.push((version, dir_entry.path()));
        }
        out.sort_by_key(|(version, _)| *version);
        Ok(out)
    }

    fn latest_snapshot_at(
        &self, entry: &FilesystemEntryId, at: Eterator,
    ) -> Result<Option<DecodedSnapshot>, FilesystemError> {
        let versions = self.list_entry_versions(entry)?;
        let candidate = versions.into_iter().rev().find(|(version, _)| *version <= at);
        if let Some((version, path)) = candidate {
            let text = fs::read_to_string(path)?;
            let (header, body) = Self::decode_snapshot(&text)?;
            Ok(Some((version, header, body)))
        } else {
            Ok(None)
        }
    }

    fn write_snapshot(
        &self, entry: &FilesystemEntryId, version: Eterator, header: &Map<String, Value>,
        body: &str,
    ) -> Result<(), FilesystemError> {
        let dir = self.entry_dir(entry);
        fs::create_dir_all(&dir)?;
        let filename = format!("{:016x}-{}.md", version.version(), entry.as_str());
        let path = dir.join(filename);
        let text = Self::encode_snapshot(header, body)?;
        fs::write(path, text)?;
        Ok(())
    }

    fn scan_entry_ids(&self) -> Result<Vec<FilesystemEntryId>, FilesystemError> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let entry =
                    FilesystemEntryId::new(entry.file_name().to_string_lossy().to_string())?;
                ids.push(entry);
            }
        }
        ids.sort();
        Ok(ids)
    }

    fn scan_current_version(root: &Path) -> Result<Eterator, FilesystemError> {
        let mut max = Eterator::EMPTY;
        for root_entry in fs::read_dir(root)? {
            let root_entry = root_entry?;
            if !root_entry.file_type()?.is_dir() {
                continue;
            }
            let entry =
                FilesystemEntryId::new(root_entry.file_name().to_string_lossy().to_string())?;
            for file_entry in fs::read_dir(root_entry.path())? {
                let file_entry = file_entry?;
                if !file_entry.file_type()?.is_file() {
                    continue;
                }
                let name = file_entry.file_name().to_string_lossy().to_string();
                let version = Self::parse_versioned_filename(&name, &entry)?;
                if version > max {
                    max = version;
                }
            }
        }
        Ok(max)
    }

    fn all_versions(&self) -> Result<BTreeSet<Eterator>, FilesystemError> {
        let mut versions = BTreeSet::new();
        for entry in self.scan_entry_ids()? {
            for (version, _) in self.list_entry_versions(&entry)? {
                versions.insert(version);
            }
        }
        Ok(versions)
    }

    fn current_snapshot_ref(&self) -> SnapshotRef {
        SnapshotRef::new(self.generation, self.current)
    }

    fn ensure_retained_snapshot(&self, at: SnapshotRef) -> Result<(), FilesystemError> {
        if at.generation != self.generation {
            return Err(FilesystemError::StaleSnapshot {
                requested: at.generation,
                current: self.generation,
            });
        }
        if at.eterator == Eterator::EMPTY {
            return Ok(());
        }
        if self.all_versions()?.contains(&at.eterator) {
            Ok(())
        } else {
            Err(FilesystemError::InvalidSnapshot(at))
        }
    }

    fn field_key_or_panic<F: Field>(&self) -> &str {
        self.fields
            .key_for::<F>()
            .unwrap_or_else(|| panic!("field type is not registered in filesystem backend"))
    }

    /// Resolve the markdown body for an entry at a given snapshot.
    ///
    /// Returns the body from the newest version file whose version is less
    /// than or equal to `at`. Unlike frontmatter fields, the body has no
    /// deletion marker; an existing snapshot always resolves to content.
    pub fn resolve_body(
        &self, at: SnapshotRef, entry: &FilesystemEntryId,
    ) -> Result<Resolution<String>, FilesystemError> {
        trace!("filesystem resolve_body begin: at={} entry={entry}", at.version());
        FilesystemEntryId::validate(entry.as_str())?;
        self.ensure_retained_snapshot(at)?;
        let result = match self.latest_snapshot_at(entry, at.eterator)? {
            | Some((_, _, body)) => Resolution::Content(body),
            | None => Resolution::Absent,
        };
        trace!("filesystem resolve_body end");
        Ok(result)
    }
}

/// Error type for filesystem backend operations.
///
/// These errors cover on-disk shape validation, frontmatter parsing, and I/O.
#[derive(Debug, Error)]
pub enum FilesystemError {
    /// The store root path is not a directory.
    #[error("invalid store root: {0}")]
    InvalidStoreRoot(PathBuf),
    /// Entry identifier cannot be represented as a safe directory name.
    #[error("invalid entry id: {0}")]
    InvalidEntryId(String),
    /// Version filename does not match `<version>-<entry_id>.md`.
    #[error("invalid version filename: {0}")]
    InvalidFilename(String),
    /// Persisted GC generation metadata is malformed.
    #[error("invalid GC generation metadata: {0:?}")]
    InvalidGcGeneration(String),
    /// The requested snapshot belongs to an older GC generation.
    #[error("stale snapshot generation: requested {requested:?}, current {current:?}")]
    StaleSnapshot {
        /// Generation on the supplied snapshot reference.
        requested: GcGeneration,
        /// Current store generation.
        current: GcGeneration,
    },
    /// The requested snapshot is no longer retained by the store.
    #[error("invalid or collected snapshot version: {0:?}")]
    InvalidSnapshot(SnapshotRef),
    /// Markdown frontmatter is malformed.
    #[error("invalid frontmatter format")]
    InvalidFrontmatter,
    /// A frontmatter key has a null value.
    #[error("null frontmatter value for field: {0}")]
    NullFieldValue(String),
    /// Filesystem I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON field serialization or deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// YAML frontmatter serialization or deserialization error.
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

/// Write transaction for [`FilesystemBackend`].
///
/// The transaction accumulates per-entry field updates. On commit, all updates
/// are materialized at one shared version and written as markdown snapshot
/// files.
pub struct FilesystemWriteTxn<'a, L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    store: &'a mut FilesystemBackend<L>,
    pending: BTreeMap<FilesystemEntryId, PendingSnapshot>,
}

#[derive(Debug, Default)]
struct PendingSnapshot {
    fields: BTreeMap<String, FieldRow<Value>>,
    body: Option<String>,
}

impl<'a, L> FilesystemWriteTxn<'a, L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// Replace the markdown body for an entry in this transaction.
    ///
    /// Body text is inherited from the previous snapshot when a transaction
    /// only changes frontmatter fields. Calling this method writes the supplied
    /// body at the transaction's committed version.
    ///
    /// # Panics
    ///
    /// Panics if `entry` does not satisfy filesystem entry id invariants.
    pub fn set_body(mut self, entry: &FilesystemEntryId, body: impl Into<String>) -> Self {
        FilesystemEntryId::validate(entry.as_str())
            .unwrap_or_else(|err| panic!("invalid entry id in write transaction: {err}"));
        self.pending.entry(entry.clone()).or_default().body = Some(body.into());
        self
    }
}

impl<'a, L> WriteTxn for FilesystemWriteTxn<'a, L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type EntryId = FilesystemEntryId;
    type Error = FilesystemError;

    fn apply<F: Field>(mut self, entry: &Self::EntryId, row: FieldRow<F::Content>) -> Self {
        FilesystemEntryId::validate(entry.as_str())
            .unwrap_or_else(|err| panic!("invalid entry id in write transaction: {err}"));

        let key = self.store.field_key_or_panic::<F>().to_owned();
        let encoded = match row {
            | FieldRow::Content(value) => {
                let json = serde_json::to_value(value)
                    .unwrap_or_else(|err| panic!("failed to serialize field content: {err}"));
                assert!(
                    !json.is_null(),
                    "filesystem field content serialized to null; delete the field by omitting it"
                );
                FieldRow::Content(json)
            }
            | FieldRow::Deleted => FieldRow::Deleted,
        };

        self.pending.entry(entry.clone()).or_default().fields.insert(key, encoded);
        self
    }

    fn commit(self) -> Result<SnapshotRef, Self::Error> {
        trace!("filesystem commit begin: entries={}", self.pending.len());
        if self.pending.is_empty() {
            trace!("filesystem commit end: no-op");
            return Ok(self.store.current_snapshot_ref());
        }

        let next = self.store.next_version();
        for (entry, updates) in self.pending {
            let previous = self.store.latest_snapshot_at(&entry, self.store.current)?;
            let (mut header, body) = match previous {
                | Some((_, h, b)) => (h, b),
                | None => (Map::new(), String::new()),
            };
            for (key, row) in updates.fields {
                match row {
                    | FieldRow::Content(value) => {
                        header.insert(key, value);
                    }
                    | FieldRow::Deleted => {
                        header.remove(&key);
                    }
                }
            }
            let body = updates.body.unwrap_or(body);
            self.store.write_snapshot(&entry, next, &header, &body)?;
        }
        self.store.current = next;
        trace!("filesystem commit end: version={}", next.version());
        Ok(self.store.current_snapshot_ref())
    }
}

impl<L> Eter for FilesystemBackend<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type EntryId = FilesystemEntryId;
    type Lifecycle = L;
    type Error = FilesystemError;
    type WriteTxn<'a>
        = FilesystemWriteTxn<'a, L>
    where
        Self: 'a;

    fn resolve<F: Field>(
        &self, at: SnapshotRef, entry: &Self::EntryId,
    ) -> Result<Resolution<F::Content>, Self::Error> {
        trace!("filesystem resolve begin: at={} entry={entry}", at.version());
        FilesystemEntryId::validate(entry.as_str())?;
        self.ensure_retained_snapshot(at)?;
        let key = self.field_key_or_panic::<F>();
        let result = match self.latest_snapshot_at(entry, at.eterator)? {
            | Some((_, header, _)) => match header.get(key) {
                | Some(value) => Resolution::Content(serde_json::from_value(value.clone())?),
                | None => Resolution::Absent,
            },
            | None => Resolution::Absent,
        };
        trace!("filesystem resolve end");
        Ok(result)
    }

    fn entry_exists(&self, at: SnapshotRef, entry: &Self::EntryId) -> Result<bool, Self::Error> {
        trace!("filesystem entry_exists begin: at={} entry={entry}", at.version());
        let exists = self.resolve::<Lifecycle<L>>(at, entry)?.is_content();
        trace!("filesystem entry_exists end: exists={exists}");
        Ok(exists)
    }

    fn gc_generation(&self) -> Result<GcGeneration, Self::Error> {
        trace!("filesystem gc_generation");
        Ok(self.generation)
    }

    fn current_snapshot(&self) -> Result<SnapshotRef, Self::Error> {
        trace!("filesystem current_snapshot");
        Ok(self.current_snapshot_ref())
    }

    fn current_version(&self) -> Result<Eterator, Self::Error> {
        trace!("filesystem current_version");
        Ok(self.current)
    }

    fn field_history<F: Field>(
        &self, entry: &Self::EntryId,
    ) -> Result<Vec<VersionedRow<F::Content>>, Self::Error> {
        trace!("filesystem field_history begin: entry={entry}");
        FilesystemEntryId::validate(entry.as_str())?;
        let key = self.field_key_or_panic::<F>();
        let mut out = Vec::new();
        let mut was_present = false;
        for (version, path) in self.list_entry_versions(entry)? {
            let text = fs::read_to_string(path)?;
            let (header, _) = Self::decode_snapshot(&text)?;
            if let Some(value) = header.get(key) {
                let row = FieldRow::Content(serde_json::from_value(value.clone())?);
                out.push((SnapshotRef::new(self.generation, version), row));
                was_present = true;
            } else if was_present {
                out.push((SnapshotRef::new(self.generation, version), FieldRow::Deleted));
                was_present = false;
            }
        }
        trace!("filesystem field_history end: rows={}", out.len());
        Ok(out)
    }

    fn entry_id_in_use(&self, id: &Self::EntryId) -> Result<bool, Self::Error> {
        trace!("filesystem entry_id_in_use begin: id={id}");
        FilesystemEntryId::validate(id.as_str())?;
        let dir = self.entry_dir(id);
        if !dir.exists() {
            trace!("filesystem entry_id_in_use end: in_use=false");
            return Ok(false);
        }
        let mut has_file = false;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                has_file = true;
                break;
            }
        }
        trace!("filesystem entry_id_in_use end: in_use={has_file}");
        Ok(has_file)
    }

    fn write(&mut self) -> Self::WriteTxn<'_> {
        trace!("filesystem write begin");
        FilesystemWriteTxn { store: self, pending: BTreeMap::new() }
    }

    fn retire(
        &mut self, snapshots: impl IntoIterator<Item = SnapshotRef>,
    ) -> Result<(), Self::Error> {
        trace!("filesystem retire begin");
        let snapshots = snapshots.into_iter().collect::<Vec<_>>();
        let retained = self.all_versions()?;
        let mut versions = Vec::new();
        for snapshot in snapshots {
            if snapshot.generation != self.generation {
                return Err(FilesystemError::StaleSnapshot {
                    requested: snapshot.generation,
                    current: self.generation,
                });
            }
            if snapshot.eterator == Eterator::EMPTY || !retained.contains(&snapshot.eterator) {
                return Err(FilesystemError::InvalidSnapshot(snapshot));
            }
            versions.push(snapshot.eterator);
        }
        self.retired.extend(versions);
        trace!("filesystem retire end: retired={}", self.retired.len());
        Ok(())
    }

    fn only_keep(
        &mut self, snapshots: impl IntoIterator<Item = SnapshotRef>,
    ) -> Result<(), Self::Error> {
        trace!("filesystem only_keep begin");
        let snapshots = snapshots.into_iter().collect::<Vec<_>>();
        let all = self.all_versions()?;
        let mut keep = BTreeSet::new();
        for snapshot in snapshots {
            if snapshot.generation != self.generation {
                return Err(FilesystemError::StaleSnapshot {
                    requested: snapshot.generation,
                    current: self.generation,
                });
            }
            if snapshot.eterator == Eterator::EMPTY || !all.contains(&snapshot.eterator) {
                return Err(FilesystemError::InvalidSnapshot(snapshot));
            }
            keep.insert(snapshot.eterator);
        }
        self.retired = all.into_iter().filter(|v| !keep.contains(v)).collect();
        trace!("filesystem only_keep end: retired={}", self.retired.len());
        Ok(())
    }

    fn gc(&mut self, option: GcOption) -> Result<(), Self::Error> {
        trace!("filesystem gc begin");
        let all_versions = self.all_versions()?;
        let live = match option {
            | GcOption::UseRetiredSet => all_versions
                .iter()
                .copied()
                .filter(|version| !self.retired.contains(version))
                .collect::<BTreeSet<_>>(),
            | GcOption::UseLiveSet(snapshots) => {
                let mut live = BTreeSet::new();
                for snapshot in snapshots {
                    if snapshot.generation != self.generation {
                        return Err(FilesystemError::StaleSnapshot {
                            requested: snapshot.generation,
                            current: self.generation,
                        });
                    }
                    if snapshot.eterator == Eterator::EMPTY
                        || !all_versions.contains(&snapshot.eterator)
                    {
                        return Err(FilesystemError::InvalidSnapshot(snapshot));
                    }
                    live.insert(snapshot.eterator);
                }
                live
            }
        };

        let mut delete_paths = Vec::new();
        for entry in self.scan_entry_ids()? {
            let versions = self.list_entry_versions(&entry)?;
            for (idx, (version, path)) in versions.iter().enumerate() {
                let next = versions.get(idx + 1).map(|(v, _)| *v).unwrap_or(Eterator(u64::MAX));
                let serves_live = live
                    .range(*version..)
                    .next()
                    .map(|candidate| *candidate < next)
                    .unwrap_or(false);
                if !serves_live {
                    delete_paths.push(path.clone());
                }
            }
        }
        if !delete_paths.is_empty() {
            let next_generation = self.generation.next();
            self.persist_gc_generation(next_generation)?;
            self.generation = next_generation;
            for path in delete_paths {
                fs::remove_file(path)?;
            }
            let retained_versions = self.all_versions()?;
            self.retired.retain(|version| retained_versions.contains(version));
        }
        self.current = Self::scan_current_version(&self.root)?;
        trace!(
            "filesystem gc end: current_version={} gc_generation={}",
            self.current.version(),
            self.generation.number()
        );
        Ok(())
    }

    fn retired_snapshots(&self) -> Result<BTreeSet<SnapshotRef>, Self::Error> {
        trace!("filesystem retired_snapshots");
        Ok(self
            .retired
            .iter()
            .copied()
            .map(|eterator| SnapshotRef::new(self.generation, eterator))
            .collect())
    }

    fn live_snapshots(&self) -> Result<BTreeSet<SnapshotRef>, Self::Error> {
        trace!("filesystem live_snapshots begin");
        let all = self.all_versions()?;
        let live = all
            .into_iter()
            .filter(|version| !self.retired.contains(version))
            .map(|eterator| SnapshotRef::new(self.generation, eterator))
            .collect();
        trace!("filesystem live_snapshots end");
        Ok(live)
    }
}

impl<L> LiveEntries for FilesystemBackend<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// Return all entries whose lifecycle field resolves to content at `at`.
    ///
    /// This implementation scans entry directories and resolves lifecycle for
    /// each candidate. It is intended for single-user filesystem stores where
    /// entry enumeration is needed for projection commits and checkouts.
    fn live_entries(&self, at: SnapshotRef) -> Result<BTreeSet<Self::EntryId>, Self::Error> {
        trace!("filesystem live_entries begin: at={}", at.version());
        self.ensure_retained_snapshot(at)?;
        let mut live = BTreeSet::new();
        for entry in self.scan_entry_ids()? {
            if self.entry_exists(at, &entry)? {
                live.insert(entry);
            }
        }
        trace!("filesystem live_entries end: count={}", live.len());
        Ok(live)
    }
}

/// Convenience constructor for a registry with built-in protocol fields.
///
/// Users can chain [`FilesystemFieldRegistry::with_field`] to add additional
/// compile-time field types before opening the backend.
///
/// Built-in key:
/// - `lifecycle` for [`Lifecycle<L>`]
pub fn builtins_registry<L>() -> FilesystemFieldRegistry
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    FilesystemFieldRegistry::new().with_field::<Lifecycle<L>>("lifecycle")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EntryFacet, EntryFacetStoreExt, Eter, Eterator, GcOption, Lifecycle, LiveEntries,
        Resolution, WriteTxn,
    };
    use serde::{Deserialize, Serialize};

    // -- Helpers --

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    enum State {
        Active,
    }

    struct TagField;
    impl Field for TagField {
        type Content = String;
    }

    struct CountField;
    impl Field for CountField {
        type Content = u32;
    }

    fn open(path: impl Into<PathBuf>) -> FilesystemBackend<State> {
        let registry = builtins_registry::<State>()
            .with_field::<TagField>("tag")
            .with_field::<CountField>("count");
        FilesystemBackend::<State>::open(path, registry).unwrap()
    }

    fn entry(s: &str) -> FilesystemEntryId {
        FilesystemEntryId::new(s).unwrap()
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct NoteFacet {
        tag: String,
        count: u32,
    }

    impl EntryFacet<FilesystemBackend<State>> for NoteFacet {
        fn load_from(
            store: &FilesystemBackend<State>, at: SnapshotRef, id: &FilesystemEntryId,
        ) -> Result<Option<Self>, FilesystemError> {
            if !store.entry_exists(at, id)? {
                return Ok(None);
            }
            let tag = match store.resolve::<TagField>(at, id)? {
                | Resolution::Content(value) => value,
                | Resolution::Deleted | Resolution::Absent => {
                    panic!("note entry is missing required tag field")
                }
            };
            let count = match store.resolve::<CountField>(at, id)? {
                | Resolution::Content(value) => value,
                | Resolution::Deleted | Resolution::Absent => {
                    panic!("note entry is missing required count field")
                }
            };
            Ok(Some(Self { tag, count }))
        }

        fn apply_to<'a>(
            &self, txn: FilesystemWriteTxn<'a, State>, id: &FilesystemEntryId,
        ) -> FilesystemWriteTxn<'a, State>
        where
            FilesystemBackend<State>: 'a,
        {
            txn.set::<Lifecycle<State>>(id, State::Active)
                .set::<TagField>(id, self.tag.clone())
                .set::<CountField>(id, self.count)
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TagFacet {
        tag: String,
    }

    impl EntryFacet<FilesystemBackend<State>> for TagFacet {
        fn load_from(
            store: &FilesystemBackend<State>, at: SnapshotRef, id: &FilesystemEntryId,
        ) -> Result<Option<Self>, FilesystemError> {
            match store.resolve::<TagField>(at, id)? {
                | Resolution::Content(tag) => Ok(Some(Self { tag })),
                | Resolution::Deleted | Resolution::Absent => Ok(None),
            }
        }

        fn apply_to<'a>(
            &self, txn: FilesystemWriteTxn<'a, State>, id: &FilesystemEntryId,
        ) -> FilesystemWriteTxn<'a, State>
        where
            FilesystemBackend<State>: 'a,
        {
            txn.set::<TagField>(id, self.tag.clone())
        }
    }

    // -- FilesystemEntryId --

    #[test]
    fn entry_id_valid() {
        assert!(FilesystemEntryId::new("hello").is_ok());
        assert!(FilesystemEntryId::new("a-b_c.d").is_ok());
        assert!(FilesystemEntryId::new("a".repeat(255)).is_ok());
    }

    #[test]
    fn entry_id_rejects_empty() {
        assert!(FilesystemEntryId::new("").is_err());
    }

    #[test]
    fn entry_id_rejects_dot() {
        assert!(FilesystemEntryId::new(".").is_err());
        assert!(FilesystemEntryId::new("..").is_err());
    }

    #[test]
    fn entry_id_rejects_slash() {
        assert!(FilesystemEntryId::new("a/b").is_err());
    }

    #[test]
    fn entry_id_rejects_null_byte() {
        assert!(FilesystemEntryId::new("a\0b").is_err());
    }

    #[test]
    fn entry_id_rejects_too_long() {
        assert!(FilesystemEntryId::new("a".repeat(256)).is_err());
    }

    #[test]
    fn entry_id_display_and_as_str_match() {
        let id = FilesystemEntryId::new("my-entry").unwrap();
        assert_eq!(id.as_str(), "my-entry");
        assert_eq!(id.to_string(), "my-entry");
    }

    #[test]
    fn entry_id_try_from_string() {
        assert!(FilesystemEntryId::try_from("valid".to_owned()).is_ok());
        assert!(FilesystemEntryId::try_from("".to_owned()).is_err());
    }

    // -- FilesystemFieldRegistry --

    #[test]
    fn registry_key_for_registered_field() {
        let reg = builtins_registry::<State>();
        assert_eq!(reg.key_for::<Lifecycle<State>>(), Some("lifecycle"));
    }

    #[test]
    fn registry_key_for_unregistered_field_is_none() {
        let reg = FilesystemFieldRegistry::new();
        assert_eq!(reg.key_for::<TagField>(), None);
    }

    #[test]
    fn registry_contains_after_registration() {
        let reg = FilesystemFieldRegistry::new().with_field::<TagField>("tag");
        assert!(reg.contains::<TagField>());
    }

    #[test]
    fn registry_does_not_contain_unregistered() {
        let reg = FilesystemFieldRegistry::new();
        assert!(!reg.contains::<TagField>());
    }

    #[test]
    #[should_panic(expected = "field type registered more than once")]
    fn registry_panics_on_duplicate_type() {
        FilesystemFieldRegistry::new().with_field::<TagField>("tag").with_field::<TagField>("tag2");
    }

    #[test]
    #[should_panic(expected = "frontmatter key registered more than once")]
    fn registry_panics_on_duplicate_key() {
        FilesystemFieldRegistry::new()
            .with_field::<TagField>("same")
            .with_field::<CountField>("same");
    }

    #[test]
    #[should_panic(expected = "filesystem field key must not be empty")]
    fn registry_panics_on_empty_key() {
        FilesystemFieldRegistry::new().with_field::<TagField>("");
    }

    // -- Snapshot encode / decode --

    #[test]
    fn encode_decode_roundtrip() {
        let mut header = serde_json::Map::new();
        header.insert("lifecycle".to_owned(), serde_json::json!("Active"));
        header.insert("count".to_owned(), serde_json::json!(7));
        let body = "some **markdown** text";

        let encoded = FilesystemBackend::<State>::encode_snapshot(&header, body).unwrap();
        let (decoded_header, decoded_body) =
            FilesystemBackend::<State>::decode_snapshot(&encoded).unwrap();

        assert_eq!(decoded_header, header);
        assert!(encoded.contains("\ncount: 7\n"));
        assert!(encoded.contains("\nlifecycle: Active\n"));
        assert!(!encoded.contains("\"count\""));

        assert_eq!(decoded_body, body);
    }

    #[test]
    fn encode_snapshot_rejects_null_frontmatter_value() {
        let mut header = serde_json::Map::new();
        header.insert("tag".to_owned(), serde_json::Value::Null);
        assert!(matches!(
            FilesystemBackend::<State>::encode_snapshot(&header, ""),
            Err(FilesystemError::NullFieldValue(key)) if key == "tag"
        ));
    }

    #[test]
    fn decode_snapshot_rejects_null_frontmatter_value() {
        assert!(matches!(
            FilesystemBackend::<State>::decode_snapshot("---\ntag: null\n---\n"),
            Err(FilesystemError::NullFieldValue(key)) if key == "tag"
        ));
    }

    #[test]
    fn decode_snapshot_rejects_missing_prefix() {
        assert!(FilesystemBackend::<State>::decode_snapshot("no frontmatter").is_err());
    }

    #[test]
    fn decode_snapshot_rejects_missing_closing_delimiter() {
        assert!(FilesystemBackend::<State>::decode_snapshot("---\n{}").is_err());
    }

    #[test]
    fn decode_snapshot_rejects_invalid_yaml() {
        assert!(FilesystemBackend::<State>::decode_snapshot("---\nnot: [closed\n---\n").is_err());
    }

    // -- Filename parsing --

    #[test]
    fn parse_versioned_filename_valid() {
        let id = entry("alpha");
        let v =
            FilesystemBackend::<State>::parse_versioned_filename("000000000000000f-alpha.md", &id)
                .unwrap();
        assert_eq!(v, Eterator(15));
    }

    #[test]
    fn parse_versioned_filename_wrong_entry_suffix() {
        let id = entry("alpha");
        assert!(
            FilesystemBackend::<State>::parse_versioned_filename("000000000000000f-beta.md", &id,)
                .is_err()
        );
    }

    #[test]
    fn parse_versioned_filename_wrong_hex_length() {
        let id = entry("alpha");
        assert!(
            FilesystemBackend::<State>::parse_versioned_filename("000f-alpha.md", &id,).is_err()
        );
    }

    #[test]
    fn parse_versioned_filename_non_hex_version() {
        let id = entry("alpha");
        assert!(
            FilesystemBackend::<State>::parse_versioned_filename("zzzzzzzzzzzzzzzz-alpha.md", &id,)
                .is_err()
        );
    }

    // -- open() --

    #[test]
    #[should_panic(expected = "filesystem backend requires Lifecycle field registration")]
    fn open_panics_without_lifecycle_field() {
        let temp = tempfile::tempdir().unwrap();
        let registry = FilesystemFieldRegistry::new();
        FilesystemBackend::<State>::open(temp.path(), registry).unwrap();
    }

    #[test]
    fn open_creates_root_directory_if_missing() {
        let temp = tempfile::tempdir().unwrap();
        let subdir = temp.path().join("new_store");
        assert!(!subdir.exists());
        let _ = open(&subdir);
        assert!(subdir.is_dir());
    }

    #[test]
    fn open_fails_when_root_is_a_file() {
        let temp = tempfile::tempdir().unwrap();
        let file_path = temp.path().join("not_a_dir");
        std::fs::write(&file_path, b"").unwrap();
        let registry = builtins_registry::<State>();
        assert!(FilesystemBackend::<State>::open(&file_path, registry).is_err());
    }

    #[test]
    fn gc_generation_persists_across_gc_and_allows_version_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let a = entry("a");
        let (v1, v2) = {
            let mut store = open(temp.path());
            let v1 = store.write().set::<Lifecycle<State>>(&a, State::Active).commit().unwrap();
            let v2 = store.write().set::<TagField>(&a, "old".to_owned()).commit().unwrap();
            store.only_keep([v1]).unwrap();
            store.gc(GcOption::UseRetiredSet).unwrap();
            assert_eq!(store.current_version().unwrap(), v1.eterator);
            assert!(store.gc_generation().unwrap() > v1.generation);
            assert!(matches!(
                store.resolve::<TagField>(v2, &a),
                Err(FilesystemError::StaleSnapshot { requested, current })
                    if requested == v2.generation && current == store.gc_generation().unwrap()
            ));
            (v1, v2)
        };

        let mut reopened = open(temp.path());
        assert_eq!(reopened.current_version().unwrap(), v1.eterator);
        assert!(reopened.gc_generation().unwrap() > v1.generation);
        let v3 = reopened.write().set::<TagField>(&a, "new".to_owned()).commit().unwrap();
        assert_eq!(v3.eterator, v2.eterator);
        assert_ne!(v3.generation, v2.generation);
    }

    // -- write / resolve / current_version --

    #[test]
    fn write_and_resolve_single_field() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<TagField>(&a, "hello".to_owned())
            .commit()
            .unwrap();
        assert_eq!(
            store.resolve::<TagField>(v1, &a).unwrap(),
            Resolution::Content("hello".to_owned())
        );
    }

    #[test]
    fn current_version_advances_on_each_write() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        assert_eq!(store.current_version().unwrap(), Eterator::EMPTY);
        let v1 = store.write().set::<Lifecycle<State>>(&a, State::Active).commit().unwrap();
        let v2 = store.write().set::<TagField>(&a, "x".to_owned()).commit().unwrap();
        assert!(Eterator::EMPTY < v1.eterator);
        assert!(v1 < v2);
        assert_eq!(store.current_version().unwrap(), v2.eterator);
    }

    #[test]
    fn resolve_deleted_field_returns_absent_and_history_records_deletion() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<TagField>(&a, "x".to_owned())
            .commit()
            .unwrap();
        let v2 = store.write().delete::<TagField>(&a).commit().unwrap();
        assert_eq!(store.resolve::<TagField>(v2, &a).unwrap(), Resolution::Absent);
        assert_eq!(
            store.field_history::<TagField>(&a).unwrap(),
            vec![(v1, FieldRow::Content("x".to_owned())), (v2, FieldRow::Deleted)]
        );

        let snapshots = store.list_entry_versions(&a).unwrap();
        let (_, latest_path) = snapshots.last().unwrap();
        let latest_text = fs::read_to_string(latest_path).unwrap();
        assert!(!latest_text.contains("\ntag:"));
        assert!(!latest_text.contains("null"));
    }

    #[test]
    fn entry_facet_roundtrips_complete_entry() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let note_id = entry("note");
        let note = NoteFacet { tag: "draft".to_owned(), count: 3 };

        let version = store.write_facet(&note_id, &note).unwrap();
        let loaded = store.load_facet::<NoteFacet>(version, &note_id).unwrap();

        assert_eq!(loaded, Some(note));
    }

    #[test]
    fn entry_facet_roundtrips_field_subset() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let id = entry("note");
        let v1 = store.write().set::<Lifecycle<State>>(&id, State::Active).commit().unwrap();
        let facet = TagFacet { tag: "draft".to_owned() };

        let v2 = store.write_facet(&id, &facet).unwrap();

        assert!(store.entry_exists(v2, &id).unwrap());
        assert_eq!(store.load_facet::<TagFacet>(v1, &id).unwrap(), None);
        assert_eq!(store.load_facet::<TagFacet>(v2, &id).unwrap(), Some(facet));
    }

    #[test]
    fn write_and_resolve_body() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let body = "some **markdown** text";
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set_body(&a, body)
            .commit()
            .unwrap();
        assert_eq!(store.resolve_body(v1, &a).unwrap(), Resolution::Content(body.to_owned()));
    }

    #[test]
    fn field_update_inherits_body_without_extra_blank_lines() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let body = "body line";
        store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set_body(&a, body)
            .commit()
            .unwrap();
        let v2 = store.write().set::<TagField>(&a, "tag".to_owned()).commit().unwrap();
        assert_eq!(store.resolve_body(v2, &a).unwrap(), Resolution::Content(body.to_owned()));
    }

    #[test]
    fn body_update_inherits_frontmatter() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<TagField>(&a, "tag".to_owned())
            .commit()
            .unwrap();
        let v2 = store.write().set_body(&a, "body").commit().unwrap();
        assert_eq!(
            store.resolve::<TagField>(v2, &a).unwrap(),
            Resolution::Content("tag".to_owned())
        );
    }

    // -- entry_id_in_use --

    #[test]
    fn entry_id_in_use_false_before_any_write() {
        let temp = tempfile::tempdir().unwrap();
        let store = open(temp.path());
        assert!(!store.entry_id_in_use(&entry("x")).unwrap());
    }

    #[test]
    fn entry_id_in_use_true_after_write() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        store.write().set::<Lifecycle<State>>(&a, State::Active).commit().unwrap();
        assert!(store.entry_id_in_use(&a).unwrap());
    }

    // -- retire / only_keep / live_snapshots / retired_snapshots --

    #[test]
    fn retire_adds_to_retired_set() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store.write().set::<Lifecycle<State>>(&a, State::Active).commit().unwrap();
        store.retire([v1]).unwrap();
        assert!(store.retired_snapshots().unwrap().contains(&v1));
    }

    #[test]
    fn only_keep_retires_all_others() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store.write().set::<Lifecycle<State>>(&a, State::Active).commit().unwrap();
        let v2 = store.write().set::<TagField>(&a, "t".to_owned()).commit().unwrap();
        store.only_keep([v2]).unwrap();
        let retired = store.retired_snapshots().unwrap();
        assert!(retired.contains(&v1));
        assert!(!retired.contains(&v2));
    }

    #[test]
    fn live_snapshots_is_complement_of_retired() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store.write().set::<Lifecycle<State>>(&a, State::Active).commit().unwrap();
        let v2 = store.write().set::<TagField>(&a, "t".to_owned()).commit().unwrap();
        store.retire([v1]).unwrap();
        let live = store.live_snapshots().unwrap();
        assert!(!live.contains(&v1));
        assert!(live.contains(&v2));
    }

    #[test]
    fn live_entries_reports_lifecycle_content_at_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let b = entry("b");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<Lifecycle<State>>(&b, State::Active)
            .commit()
            .unwrap();
        let v2 = store.write().delete::<Lifecycle<State>>(&b).commit().unwrap();

        assert_eq!(store.live_entries(v1).unwrap(), BTreeSet::from([a.clone(), b.clone()]));
        assert_eq!(store.live_entries(v2).unwrap(), BTreeSet::from([a]));
    }

    // -- gc --

    #[test]
    fn gc_use_retired_set_removes_redundant_rows() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<CountField>(&a, 1)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&a, 2).commit().unwrap();
        store.retire([v1]).unwrap();
        store.gc(GcOption::UseRetiredSet).unwrap();
        let v2 = SnapshotRef::new(store.gc_generation().unwrap(), v2.eterator);
        // v1 file is gone; reading at v2 in the new generation still works.
        assert_eq!(store.resolve::<CountField>(v2, &a).unwrap(), Resolution::Content(2));
        assert!(store.field_history::<CountField>(&a).unwrap().len() == 1);
    }

    #[test]
    fn gc_preserves_retired_snapshot_that_serves_live_reads() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let b = entry("b");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<CountField>(&a, 1)
            .commit()
            .unwrap();
        let v2 = store.write().set::<Lifecycle<State>>(&b, State::Active).commit().unwrap();
        let v3 = store.write().set::<TagField>(&b, "temporary".to_owned()).commit().unwrap();

        store.only_keep([v2]).unwrap();
        store.gc(GcOption::UseRetiredSet).unwrap();

        let generation = store.gc_generation().unwrap();
        let v1 = SnapshotRef::new(generation, v1.eterator);
        let v2 = SnapshotRef::new(generation, v2.eterator);
        let v3 = SnapshotRef::new(generation, v3.eterator);
        let retired = store.retired_snapshots().unwrap();
        assert!(retired.contains(&v1));
        assert!(!retired.contains(&v2));
        assert!(!retired.contains(&v3));
        assert_eq!(store.resolve::<CountField>(v2, &a).unwrap(), Resolution::Content(1));
    }

    #[test]
    fn gc_use_live_set_does_not_alter_live_reads() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = entry("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<CountField>(&a, 10)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&a, 20).commit().unwrap();
        store.gc(GcOption::UseLiveSet(std::collections::BTreeSet::from([v2]))).unwrap();
        let v2 = SnapshotRef::new(store.gc_generation().unwrap(), v2.eterator);
        assert_eq!(store.resolve::<CountField>(v2, &a).unwrap(), Resolution::Content(20));
        // v1 is now unreachable; its row was removed.
        let hist = store.field_history::<CountField>(&a).unwrap();
        assert!(!hist.iter().any(|(v, _)| *v == v1));
    }
}
