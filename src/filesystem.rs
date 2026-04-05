//! Filesystem backend for the Eter protocol.
//!
//! Storage layout:
//! - `<root>/<node_id>/<version>-<node_id>.md`
//! - `version` is a 64-bit value encoded as 16 lowercase hex digits.
//! - each file contains JSON frontmatter and a markdown body.
//!
//! Frontmatter stores protocol fields by key. A `null` value is a
//! [`FieldRow::Deleted`](crate::FieldRow::Deleted) marker, while an absent key
//! means the field is inherited from older snapshots during resolution.
//!
//! This backend stores one markdown file per `(NodeId, version)` snapshot. It
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
    Edges, Eter, Eterator, Field, FieldRow, GcOption, Lifecycle, Resolution, VersionedRow, Warning,
    WriteTxn,
};

type DecodedSnapshot = (Eterator, Map<String, Value>, String);

/// Filesystem-native node identifier.
///
/// This type enforces path-safety invariants required by directory-backed
/// storage and avoids using raw `String` as protocol identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FilesystemNodeId(String);

impl FilesystemNodeId {
    /// Construct a validated filesystem node id.
    pub fn new(raw: impl Into<String>) -> Result<Self, FilesystemError> {
        let raw = raw.into();
        Self::validate(&raw)?;
        Ok(Self(raw))
    }

    /// Borrow this identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(node: &str) -> Result<(), FilesystemError> {
        if node.is_empty() || node == "." || node == ".." {
            return Err(FilesystemError::InvalidNodeId(node.to_owned()));
        }
        if node.contains('/') || node.contains('\0') {
            return Err(FilesystemError::InvalidNodeId(node.to_owned()));
        }
        if node.len() > 255 {
            return Err(FilesystemError::InvalidNodeId(node.to_owned()));
        }
        Ok(())
    }
}

impl std::fmt::Display for FilesystemNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for FilesystemNodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl TryFrom<String> for FilesystemNodeId {
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
    /// The key is the exact JSON frontmatter key used for this field in every
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
/// `NodeId` is represented as [`FilesystemNodeId`] and validated for path safety.
/// `Lifecycle` state type is user-defined.
///
/// A write creates a full node snapshot at the next global version: updated
/// fields are written from the transaction and unchanged fields are copied from
/// the latest earlier snapshot for that node.
#[derive(Debug)]
pub struct FilesystemBackend<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    root: PathBuf,
    fields: FilesystemFieldRegistry,
    retired: BTreeSet<Eterator>,
    current: Eterator,
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
    /// Note: retired versions are not persisted by this backend. Callers own
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
        trace!("filesystem open end: current_version={}", current.version());
        Ok(Self {
            root,
            fields,
            retired: BTreeSet::new(),
            current,
            _lifecycle: std::marker::PhantomData,
        })
    }

    fn node_dir(&self, node: &FilesystemNodeId) -> PathBuf {
        self.root.join(node.as_str())
    }

    fn parse_versioned_filename(
        name: &str, node: &FilesystemNodeId,
    ) -> Result<Eterator, FilesystemError> {
        let expected_suffix = format!("-{}.md", node.as_str());
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
        let json = serde_json::to_string_pretty(header)?;
        Ok(format!("---\n{json}\n---\n\n{body}"))
    }

    fn decode_snapshot(text: &str) -> Result<(Map<String, Value>, String), FilesystemError> {
        let rest = text.strip_prefix("---\n").ok_or(FilesystemError::InvalidFrontmatter)?;
        let sep = "\n---\n";
        let idx = rest.find(sep).ok_or(FilesystemError::InvalidFrontmatter)?;
        let json = &rest[..idx];
        let body = &rest[idx + sep.len()..];
        let header: Map<String, Value> = serde_json::from_str(json)?;
        Ok((header, body.to_owned()))
    }

    fn list_node_versions(
        &self, node: &FilesystemNodeId,
    ) -> Result<Vec<(Eterator, PathBuf)>, FilesystemError> {
        let dir = self.node_dir(node);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let version = Self::parse_versioned_filename(&name, node)?;
            out.push((version, entry.path()));
        }
        out.sort_by_key(|(version, _)| *version);
        Ok(out)
    }

    fn latest_snapshot_at(
        &self, node: &FilesystemNodeId, at: Eterator,
    ) -> Result<Option<DecodedSnapshot>, FilesystemError> {
        let versions = self.list_node_versions(node)?;
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
        &self, node: &FilesystemNodeId, version: Eterator, header: &Map<String, Value>, body: &str,
    ) -> Result<(), FilesystemError> {
        let dir = self.node_dir(node);
        fs::create_dir_all(&dir)?;
        let filename = format!("{:016x}-{}.md", version.version(), node.as_str());
        let path = dir.join(filename);
        let text = Self::encode_snapshot(header, body)?;
        fs::write(path, text)?;
        Ok(())
    }

    fn scan_node_ids(&self) -> Result<Vec<FilesystemNodeId>, FilesystemError> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let node = FilesystemNodeId::new(entry.file_name().to_string_lossy().to_string())?;
                ids.push(node);
            }
        }
        ids.sort();
        Ok(ids)
    }

    fn scan_current_version(root: &Path) -> Result<Eterator, FilesystemError> {
        let mut max = Eterator::EMPTY;
        for node_entry in fs::read_dir(root)? {
            let node_entry = node_entry?;
            if !node_entry.file_type()?.is_dir() {
                continue;
            }
            let node = FilesystemNodeId::new(node_entry.file_name().to_string_lossy().to_string())?;
            for file_entry in fs::read_dir(node_entry.path())? {
                let file_entry = file_entry?;
                if !file_entry.file_type()?.is_file() {
                    continue;
                }
                let name = file_entry.file_name().to_string_lossy().to_string();
                let version = Self::parse_versioned_filename(&name, &node)?;
                if version > max {
                    max = version;
                }
            }
        }
        Ok(max)
    }

    fn all_versions(&self) -> Result<BTreeSet<Eterator>, FilesystemError> {
        let mut versions = BTreeSet::new();
        for node in self.scan_node_ids()? {
            for (version, _) in self.list_node_versions(&node)? {
                versions.insert(version);
            }
        }
        Ok(versions)
    }

    fn field_key_or_panic<F: Field>(&self) -> &str {
        self.fields
            .key_for::<F>()
            .unwrap_or_else(|| panic!("field type is not registered in filesystem backend"))
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
    /// Node identifier cannot be represented as a safe directory name.
    #[error("invalid node id: {0}")]
    InvalidNodeId(String),
    /// Version filename does not match `<version>-<node_id>.md`.
    #[error("invalid version filename: {0}")]
    InvalidFilename(String),
    /// Markdown frontmatter is malformed.
    #[error("invalid frontmatter format")]
    InvalidFrontmatter,
    /// Filesystem I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Write transaction for [`FilesystemBackend`].
///
/// The transaction accumulates per-node field updates. On commit, all updates
/// are materialized at one shared version and written as markdown snapshot
/// files.
pub struct FilesystemWriteTxn<'a, L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    store: &'a mut FilesystemBackend<L>,
    pending: BTreeMap<FilesystemNodeId, BTreeMap<String, FieldRow<Value>>>,
}

impl<'a, L> WriteTxn for FilesystemWriteTxn<'a, L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type NodeId = FilesystemNodeId;
    type Error = FilesystemError;

    fn apply<F: Field>(mut self, node: &Self::NodeId, row: FieldRow<F::Content>) -> Self {
        FilesystemNodeId::validate(node.as_str())
            .unwrap_or_else(|err| panic!("invalid node id in write transaction: {err}"));

        let key = self.store.field_key_or_panic::<F>().to_owned();
        let encoded = match row {
            | FieldRow::Content(value) => {
                let json = serde_json::to_value(value)
                    .unwrap_or_else(|err| panic!("failed to serialize field content: {err}"));
                FieldRow::Content(json)
            }
            | FieldRow::Deleted => FieldRow::Deleted,
        };

        self.pending.entry(node.clone()).or_default().insert(key, encoded);
        self
    }

    fn commit(self) -> Result<Eterator, Self::Error> {
        trace!("filesystem commit begin: nodes={}", self.pending.len());
        if self.pending.is_empty() {
            trace!("filesystem commit end: no-op");
            return Ok(self.store.current);
        }

        let next = Eterator(self.store.current.version() + 1);
        for (node, updates) in self.pending {
            let previous = self.store.latest_snapshot_at(&node, self.store.current)?;
            let (mut header, body) = match previous {
                | Some((_, h, b)) => (h, b),
                | None => (Map::new(), String::new()),
            };
            for (key, row) in updates {
                match row {
                    | FieldRow::Content(value) => {
                        header.insert(key, value);
                    }
                    | FieldRow::Deleted => {
                        header.insert(key, Value::Null);
                    }
                }
            }
            self.store.write_snapshot(&node, next, &header, &body)?;
        }
        self.store.current = next;
        trace!("filesystem commit end: version={}", next.version());
        Ok(next)
    }
}

impl<L> Eter for FilesystemBackend<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type NodeId = FilesystemNodeId;
    type Lifecycle = L;
    type Error = FilesystemError;
    type WriteTxn<'a>
        = FilesystemWriteTxn<'a, L>
    where
        Self: 'a;

    fn resolve<F: Field>(
        &self, at: Eterator, node: &Self::NodeId,
    ) -> Result<Resolution<F::Content>, Self::Error> {
        trace!("filesystem resolve begin: at={} node={node}", at.version());
        FilesystemNodeId::validate(node.as_str())?;
        let key = self.field_key_or_panic::<F>();
        let result = match self.latest_snapshot_at(node, at)? {
            | Some((_, header, _)) => match header.get(key) {
                | Some(value) if value.is_null() => Resolution::Deleted,
                | Some(value) => Resolution::Content(serde_json::from_value(value.clone())?),
                | None => Resolution::Absent,
            },
            | None => Resolution::Absent,
        };
        trace!("filesystem resolve end");
        Ok(result)
    }

    fn node_exists(&self, at: Eterator, node: &Self::NodeId) -> Result<bool, Self::Error> {
        trace!("filesystem node_exists begin: at={} node={node}", at.version());
        let exists = self.resolve::<Lifecycle<L>>(at, node)?.is_content();
        trace!("filesystem node_exists end: exists={exists}");
        Ok(exists)
    }

    fn current_version(&self) -> Result<Eterator, Self::Error> {
        trace!("filesystem current_version");
        Ok(self.current)
    }

    fn field_history<F: Field>(
        &self, node: &Self::NodeId,
    ) -> Result<Vec<VersionedRow<F::Content>>, Self::Error> {
        trace!("filesystem field_history begin: node={node}");
        FilesystemNodeId::validate(node.as_str())?;
        let key = self.field_key_or_panic::<F>();
        let mut out = Vec::new();
        for (version, path) in self.list_node_versions(node)? {
            let text = fs::read_to_string(path)?;
            let (header, _) = Self::decode_snapshot(&text)?;
            if let Some(value) = header.get(key) {
                let row = if value.is_null() {
                    FieldRow::Deleted
                } else {
                    FieldRow::Content(serde_json::from_value(value.clone())?)
                };
                out.push((version, row));
            }
        }
        trace!("filesystem field_history end: rows={}", out.len());
        Ok(out)
    }

    fn node_id_in_use(&self, id: &Self::NodeId) -> Result<bool, Self::Error> {
        trace!("filesystem node_id_in_use begin: id={id}");
        FilesystemNodeId::validate(id.as_str())?;
        let dir = self.node_dir(id);
        if !dir.exists() {
            trace!("filesystem node_id_in_use end: in_use=false");
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
        trace!("filesystem node_id_in_use end: in_use={has_file}");
        Ok(has_file)
    }

    fn check_edges(
        &self, at: Eterator, source: &Self::NodeId, targets: &BTreeSet<Self::NodeId>,
    ) -> Result<Vec<Warning<Self::NodeId>>, Self::Error> {
        trace!(
            "filesystem check_edges begin: at={} source={} targets={}",
            at.version(),
            source,
            targets.len()
        );
        let mut warnings = Vec::new();
        for target in targets {
            if !self.node_exists(at, target)? {
                warnings
                    .push(Warning::DanglingEdge { source: source.clone(), target: target.clone() });
            }
        }
        trace!("filesystem check_edges end: warnings={}", warnings.len());
        Ok(warnings)
    }

    fn write(&mut self) -> Self::WriteTxn<'_> {
        trace!("filesystem write begin");
        FilesystemWriteTxn { store: self, pending: BTreeMap::new() }
    }

    fn retire(&mut self, versions: impl IntoIterator<Item = Eterator>) -> Result<(), Self::Error> {
        trace!("filesystem retire begin");
        self.retired.extend(versions);
        trace!("filesystem retire end: retired={}", self.retired.len());
        Ok(())
    }

    fn only_keep(
        &mut self, versions: impl IntoIterator<Item = Eterator>,
    ) -> Result<(), Self::Error> {
        trace!("filesystem only_keep begin");
        let keep: BTreeSet<Eterator> = versions.into_iter().collect();
        let all = self.all_versions()?;
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
            | GcOption::UseLiveSet(live) => live,
        };

        for node in self.scan_node_ids()? {
            let versions = self.list_node_versions(&node)?;
            let mut delete_paths = Vec::new();
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
            for path in delete_paths {
                fs::remove_file(path)?;
            }
        }
        self.current = Self::scan_current_version(&self.root)?;
        trace!("filesystem gc end: current_version={}", self.current.version());
        Ok(())
    }

    fn retired_versions(&self) -> Result<BTreeSet<Eterator>, Self::Error> {
        trace!("filesystem retired_versions");
        Ok(self.retired.clone())
    }

    fn live_versions(&self) -> Result<BTreeSet<Eterator>, Self::Error> {
        trace!("filesystem live_versions begin");
        let all = self.all_versions()?;
        let live = all.into_iter().filter(|version| !self.retired.contains(version)).collect();
        trace!("filesystem live_versions end");
        Ok(live)
    }
}

/// Convenience constructor for a registry with built-in protocol fields.
///
/// Users can chain [`FilesystemFieldRegistry::with_field`] to add additional
/// compile-time field types before opening the backend.
///
/// Built-in keys are:
/// - `lifecycle` for [`Lifecycle<L>`]
/// - `edges` for [`Edges<FilesystemNodeId>`]
pub fn builtins_registry<L>() -> FilesystemFieldRegistry
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    FilesystemFieldRegistry::new()
        .with_field::<Lifecycle<L>>("lifecycle")
        .with_field::<Edges<FilesystemNodeId>>("edges")
}
