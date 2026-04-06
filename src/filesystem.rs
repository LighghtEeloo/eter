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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Eter, Eterator, GcOption, Lifecycle, Resolution, WriteTxn};
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

    fn node(s: &str) -> FilesystemNodeId {
        FilesystemNodeId::new(s).unwrap()
    }

    // -- FilesystemNodeId --

    #[test]
    fn node_id_valid() {
        assert!(FilesystemNodeId::new("hello").is_ok());
        assert!(FilesystemNodeId::new("a-b_c.d").is_ok());
        assert!(FilesystemNodeId::new("a".repeat(255)).is_ok());
    }

    #[test]
    fn node_id_rejects_empty() {
        assert!(FilesystemNodeId::new("").is_err());
    }

    #[test]
    fn node_id_rejects_dot() {
        assert!(FilesystemNodeId::new(".").is_err());
        assert!(FilesystemNodeId::new("..").is_err());
    }

    #[test]
    fn node_id_rejects_slash() {
        assert!(FilesystemNodeId::new("a/b").is_err());
    }

    #[test]
    fn node_id_rejects_null_byte() {
        assert!(FilesystemNodeId::new("a\0b").is_err());
    }

    #[test]
    fn node_id_rejects_too_long() {
        assert!(FilesystemNodeId::new("a".repeat(256)).is_err());
    }

    #[test]
    fn node_id_display_and_as_str_match() {
        let id = FilesystemNodeId::new("mynode").unwrap();
        assert_eq!(id.as_str(), "mynode");
        assert_eq!(id.to_string(), "mynode");
    }

    #[test]
    fn node_id_try_from_string() {
        assert!(FilesystemNodeId::try_from("valid".to_owned()).is_ok());
        assert!(FilesystemNodeId::try_from("".to_owned()).is_err());
    }

    // -- FilesystemFieldRegistry --

    #[test]
    fn registry_key_for_registered_field() {
        let reg = builtins_registry::<State>();
        assert_eq!(reg.key_for::<Lifecycle<State>>(), Some("lifecycle"));
        assert_eq!(reg.key_for::<Edges<FilesystemNodeId>>(), Some("edges"));
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
        FilesystemFieldRegistry::new()
            .with_field::<TagField>("tag")
            .with_field::<TagField>("tag2");
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
        // encode_snapshot emits "---\n{json}\n---\n\n{body}", so the decoded body
        // has a leading newline (the blank line separating frontmatter from content).
        assert_eq!(decoded_body, format!("\n{body}"));
    }

    #[test]
    fn encode_decode_null_deletion_marker() {
        let mut header = serde_json::Map::new();
        header.insert("tag".to_owned(), serde_json::Value::Null);
        let encoded = FilesystemBackend::<State>::encode_snapshot(&header, "").unwrap();
        let (decoded, _) = FilesystemBackend::<State>::decode_snapshot(&encoded).unwrap();
        assert!(decoded["tag"].is_null());
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
    fn decode_snapshot_rejects_invalid_json() {
        assert!(FilesystemBackend::<State>::decode_snapshot("---\nnot json\n---\n").is_err());
    }

    // -- Filename parsing --

    #[test]
    fn parse_versioned_filename_valid() {
        let id = node("alpha");
        let v = FilesystemBackend::<State>::parse_versioned_filename(
            "000000000000000f-alpha.md",
            &id,
        )
        .unwrap();
        assert_eq!(v, Eterator(15));
    }

    #[test]
    fn parse_versioned_filename_wrong_node_suffix() {
        let id = node("alpha");
        assert!(FilesystemBackend::<State>::parse_versioned_filename(
            "000000000000000f-beta.md",
            &id,
        )
        .is_err());
    }

    #[test]
    fn parse_versioned_filename_wrong_hex_length() {
        let id = node("alpha");
        assert!(FilesystemBackend::<State>::parse_versioned_filename(
            "000f-alpha.md",
            &id,
        )
        .is_err());
    }

    #[test]
    fn parse_versioned_filename_non_hex_version() {
        let id = node("alpha");
        assert!(FilesystemBackend::<State>::parse_versioned_filename(
            "zzzzzzzzzzzzzzzz-alpha.md",
            &id,
        )
        .is_err());
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

    // -- write / resolve / current_version --

    #[test]
    fn write_and_resolve_single_field() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
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
        let a = node("a");
        assert_eq!(store.current_version().unwrap(), Eterator::EMPTY);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .commit()
            .unwrap();
        let v2 = store
            .write()
            .set::<TagField>(&a, "x".to_owned())
            .commit()
            .unwrap();
        assert!(Eterator::EMPTY < v1);
        assert!(v1 < v2);
        assert_eq!(store.current_version().unwrap(), v2);
    }

    #[test]
    fn resolve_deleted_field_returns_deleted() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<TagField>(&a, "x".to_owned())
            .commit()
            .unwrap();
        let v2 = store.write().delete::<TagField>(&a).commit().unwrap();
        assert_eq!(store.resolve::<TagField>(v2, &a).unwrap(), Resolution::Deleted);
    }

    // -- node_id_in_use --

    #[test]
    fn node_id_in_use_false_before_any_write() {
        let temp = tempfile::tempdir().unwrap();
        let store = open(temp.path());
        assert!(!store.node_id_in_use(&node("x")).unwrap());
    }

    #[test]
    fn node_id_in_use_true_after_write() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .commit()
            .unwrap();
        assert!(store.node_id_in_use(&a).unwrap());
    }

    // -- retire / only_keep / live_versions / retired_versions --

    #[test]
    fn retire_adds_to_retired_set() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .commit()
            .unwrap();
        store.retire([v1]).unwrap();
        assert!(store.retired_versions().unwrap().contains(&v1));
    }

    #[test]
    fn only_keep_retires_all_others() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .commit()
            .unwrap();
        let v2 = store.write().set::<TagField>(&a, "t".to_owned()).commit().unwrap();
        store.only_keep([v2]).unwrap();
        let retired = store.retired_versions().unwrap();
        assert!(retired.contains(&v1));
        assert!(!retired.contains(&v2));
    }

    #[test]
    fn live_versions_is_complement_of_retired() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .commit()
            .unwrap();
        let v2 = store.write().set::<TagField>(&a, "t".to_owned()).commit().unwrap();
        store.retire([v1]).unwrap();
        let live = store.live_versions().unwrap();
        assert!(!live.contains(&v1));
        assert!(live.contains(&v2));
    }

    // -- gc --

    #[test]
    fn gc_use_retired_set_removes_redundant_rows() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<CountField>(&a, 1)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&a, 2).commit().unwrap();
        store.retire([v1]).unwrap();
        store.gc(GcOption::UseRetiredSet).unwrap();
        // v1 file is gone; reading at v2 still works.
        assert_eq!(
            store.resolve::<CountField>(v2, &a).unwrap(),
            Resolution::Content(2)
        );
        assert!(store.field_history::<CountField>(&a).unwrap().len() == 1);
    }

    #[test]
    fn gc_use_live_set_does_not_alter_live_reads() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = open(temp.path());
        let a = node("a");
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&a, State::Active)
            .set::<CountField>(&a, 10)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&a, 20).commit().unwrap();
        store.gc(GcOption::UseLiveSet(std::collections::BTreeSet::from([v2]))).unwrap();
        assert_eq!(
            store.resolve::<CountField>(v2, &a).unwrap(),
            Resolution::Content(20)
        );
        // v1 is now unreachable; its row was removed.
        let hist = store.field_history::<CountField>(&a).unwrap();
        assert!(!hist.iter().any(|(v, _)| *v == v1));
    }
}
