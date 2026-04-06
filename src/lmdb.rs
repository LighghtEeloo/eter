//! LMDB backend for the Eter protocol via the `heed` crate.
//!
//! Each registered [`Field`] gets its own named LMDB database. Two reserved
//! databases track version history:
//!
//! - `_versions`: every committed version number (8-byte big-endian key, empty
//!   value). The last entry is the current version; avoids a full field-table
//!   scan on every write.
//! - `_retired`: the persistent retired-version set. Survives process restarts,
//!   unlike the filesystem backend's in-memory retired set.
//!
//! Key encoding per field database: `NodeId.to_key_bytes() ++ version.to_be_bytes()`.
//! The node portion is exactly [`LmdbKey::KEY_LEN`] bytes; the version portion
//! is always 8 bytes. Fixed-length node IDs eliminate any composite-key ambiguity.
//!
//! Value encoding: `[0x00]` is a [`FieldRow::Deleted`] marker; `[0x01, ...json]`
//! is [`FieldRow::Content`] with JSON-serialized content.

use std::any::TypeId;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::path::Path;

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn, WithTls};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tracing::trace;

use crate::{
    Edges, Eter, Eterator, Field, FieldRow, GcOption, Lifecycle, Resolution, VersionedRow, Warning,
    WriteTxn,
};

// ---------------------------------------------------------------------------
// LmdbKey trait
// ---------------------------------------------------------------------------

/// Required for node identifier types stored in the LMDB backend.
///
/// The encoding must be:
/// - **Fixed-length**: every value produces exactly [`LmdbKey::KEY_LEN`] bytes.
/// - **Order-preserving**: `a < b` under [`Ord`] implies
///   `a.to_key_bytes() < b.to_key_bytes()` lexicographically.
///
/// UUIDs (16 bytes big-endian), integers (4 or 8 bytes big-endian), and
/// fixed-width zero-padded slugs all satisfy these constraints.
pub trait LmdbKey:
    Eq + Hash + Clone + Ord + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Exact byte length of the encoded node identifier.
    const KEY_LEN: usize;

    /// Encode to exactly `KEY_LEN` bytes.
    fn to_key_bytes(&self) -> Vec<u8>;

    /// Decode from exactly `KEY_LEN` bytes.
    fn from_key_bytes(bytes: &[u8]) -> Self;
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

fn node_prefix<Id: LmdbKey>(node: &Id) -> Vec<u8> {
    let prefix = node.to_key_bytes();
    debug_assert_eq!(prefix.len(), Id::KEY_LEN, "LmdbKey::to_key_bytes returned wrong length");
    prefix
}

fn split_composite_key<Id: LmdbKey>(key: &[u8]) -> (Id, Eterator) {
    let node = Id::from_key_bytes(&key[..Id::KEY_LEN]);
    let version =
        u64::from_be_bytes(key[Id::KEY_LEN..Id::KEY_LEN + 8].try_into().expect("key too short"));
    (node, Eterator(version))
}

fn encode_version_key(v: Eterator) -> [u8; 8] {
    v.version().to_be_bytes()
}

fn decode_version_key(key: &[u8]) -> Result<Eterator, LmdbError> {
    let arr: [u8; 8] = key.try_into().map_err(|_| LmdbError::InvalidVersionKey)?;
    Ok(Eterator(u64::from_be_bytes(arr)))
}

fn encode_field_row<T: Serialize>(row: &FieldRow<T>) -> Vec<u8> {
    match row {
        | FieldRow::Deleted => vec![0u8],
        | FieldRow::Content(v) => {
            let mut bytes = vec![1u8];
            let json = serde_json::to_vec(v)
                .unwrap_or_else(|e| panic!("failed to serialize field content: {e}"));
            bytes.extend_from_slice(&json);
            bytes
        }
    }
}

fn decode_field_row<T: DeserializeOwned>(bytes: &[u8]) -> Result<FieldRow<T>, LmdbError> {
    match bytes.first().copied() {
        | Some(0) => Ok(FieldRow::Deleted),
        | Some(1) => {
            let v = serde_json::from_slice(&bytes[1..])?;
            Ok(FieldRow::Content(v))
        }
        | _ => Err(LmdbError::InvalidRowEncoding),
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Error type for LMDB backend operations.
#[derive(Debug, Error)]
pub enum LmdbError {
    /// LMDB or heed error.
    #[error("heed error: {0}")]
    Heed(#[from] heed::Error),
    /// JSON serialization or deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// A stored row has an unrecognized encoding prefix.
    #[error("invalid row encoding in lmdb store")]
    InvalidRowEncoding,
    /// A key in `_versions` or `_retired` is not exactly 8 bytes.
    #[error("invalid version key in lmdb store")]
    InvalidVersionKey,
}

// ---------------------------------------------------------------------------
// Field configuration
// ---------------------------------------------------------------------------

/// Pre-open field configuration for the LMDB backend.
///
/// Build with [`LmdbFieldConfig::with_field`], then pass to
/// [`LmdbBackend::open`].  The configuration is consumed at open time to
/// create the named LMDB databases; fields cannot be added afterwards.
#[derive(Debug, Default)]
pub struct LmdbFieldConfig {
    entries: Vec<(TypeId, &'static str)>,
    by_type: HashMap<TypeId, &'static str>,
}

impl LmdbFieldConfig {
    /// Create an empty configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a field type with a named LMDB database.
    ///
    /// `name` becomes the LMDB database name for this field.
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty, reserved (`_versions`, `_retired`), or
    /// if the field type or name has already been registered.
    pub fn with_field<F: Field>(mut self, name: &'static str) -> Self {
        assert!(!name.is_empty(), "lmdb field name must not be empty");
        assert!(name != "_versions" && name != "_retired", "field name '{name}' is reserved");
        let tid = TypeId::of::<F>();
        assert!(!self.by_type.contains_key(&tid), "field type already registered");
        assert!(
            !self.by_type.values().any(|&n| n == name),
            "field name '{name}' already registered"
        );
        self.entries.push((tid, name));
        self.by_type.insert(tid, name);
        self
    }

    fn contains<F: Field>(&self) -> bool {
        self.by_type.contains_key(&TypeId::of::<F>())
    }
}

// ---------------------------------------------------------------------------
// Backend struct
// ---------------------------------------------------------------------------

/// LMDB-backed implementation of [`Eter`].
///
/// `Id` must implement [`LmdbKey`]. `L` is the user-defined lifecycle state.
///
/// The retired-version set is persisted in the `_retired` database, so it
/// survives process restarts without any caller-side bookkeeping.
pub struct LmdbBackend<Id, L>
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// The LMDB environment owning all databases.
    env: Env,
    /// One database per registered [`Field`] type, keyed by [`TypeId`].
    field_dbs: HashMap<TypeId, Database<Bytes, Bytes>>,
    /// `_versions`: 8-byte big-endian version → empty value.
    /// The last entry is the current version; updated atomically with every commit.
    versions_db: Database<Bytes, Bytes>,
    /// `_retired`: 8-byte big-endian version → empty value.
    /// Persisted across restarts; entries are removed when the corresponding
    /// version is fully collected by GC.
    retired_db: Database<Bytes, Bytes>,
    /// Cached current version; kept in sync with `versions_db` on every commit and GC.
    current: Eterator,
    _phantom: std::marker::PhantomData<(Id, L)>,
}

impl<Id, L> LmdbBackend<Id, L>
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// Open or create an LMDB-backed store at `path`.
    ///
    /// `map_size` is the maximum size of the LMDB memory map in bytes.
    /// A safe default for small stores is 1 GiB (`1 << 30`).
    ///
    /// # Panics
    ///
    /// Panics if `config` does not register [`Lifecycle<L>`].
    pub fn open(
        path: impl AsRef<Path>, map_size: usize, config: LmdbFieldConfig,
    ) -> Result<Self, LmdbError> {
        assert!(
            config.contains::<Lifecycle<L>>(),
            "lmdb backend requires Lifecycle<L> field registration"
        );

        let max_dbs = (config.entries.len() + 2) as u32;

        // Safety: the caller owns the environment path; no other process or
        // thread should open the same environment simultaneously.
        let env = unsafe {
            EnvOpenOptions::new().map_size(map_size).max_dbs(max_dbs).open(path.as_ref())?
        };

        let mut wtxn = env.write_txn()?;
        let mut field_dbs: HashMap<TypeId, Database<Bytes, Bytes>> = HashMap::new();
        for &(tid, name) in &config.entries {
            let db: Database<Bytes, Bytes> = env.create_database(&mut wtxn, Some(name))?;
            field_dbs.insert(tid, db);
        }
        let versions_db: Database<Bytes, Bytes> =
            env.create_database(&mut wtxn, Some("_versions"))?;
        let retired_db: Database<Bytes, Bytes> =
            env.create_database(&mut wtxn, Some("_retired"))?;
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        let current = scan_last_version(versions_db, &rtxn)?;
        drop(rtxn);

        trace!("lmdb open: current_version={}", current.version());
        Ok(Self {
            env,
            field_dbs,
            versions_db,
            retired_db,
            current,
            _phantom: std::marker::PhantomData,
        })
    }

    fn field_db_or_panic<F: Field>(&self) -> Database<Bytes, Bytes> {
        *self
            .field_dbs
            .get(&TypeId::of::<F>())
            .unwrap_or_else(|| panic!("field type not registered in lmdb backend"))
    }

    /// Open a read-only transaction on the underlying LMDB environment.
    ///
    /// Use this together with [`LmdbBackend::resolve_in`] when strict
    /// multi-field snapshot consistency is required: all `resolve_in` calls
    /// within one transaction observe the same committed state.
    ///
    /// The transaction must be closed promptly. Holding it open pins LMDB's
    /// freelist and occupies a reader-table slot; see the design notes in
    /// DESIGN.md under "Eterators and Read Transactions."
    /// Open a read-only transaction on the underlying LMDB environment.
    ///
    /// Use this together with [`LmdbBackend::resolve_in`] when strict
    /// multi-field snapshot consistency is required: all `resolve_in` calls
    /// within one transaction observe the same committed state.
    ///
    /// The transaction must be closed promptly. Holding it open pins LMDB's
    /// freelist and occupies a reader-table slot; see the design notes in
    /// DESIGN.md under "Eterators and Read Transactions."
    /// Open a read-only transaction on the underlying LMDB environment.
    ///
    /// Use this together with [`LmdbBackend::resolve_in`] when strict
    /// multi-field snapshot consistency is required: all `resolve_in` calls
    /// within one transaction observe the same committed state.
    ///
    /// The transaction must be closed promptly. Holding it open pins LMDB's
    /// freelist and occupies a reader-table slot; see the design notes in
    /// DESIGN.md under "Eterators and Read Transactions."
    ///
    /// Returns `RoTxn<'_, WithTls>`. To pass it to `resolve_in`, which takes
    /// `&RoTxn` (= `RoTxn<AnyTls>`), Rust applies deref-coercion automatically.
    pub fn read_txn(&self) -> Result<RoTxn<'_, WithTls>, LmdbError> {
        Ok(self.env.read_txn()?)
    }

    /// Resolve field `F` for `node` at `at` using a caller-supplied read transaction.
    ///
    /// The transaction is not closed; the caller controls its lifetime.
    /// All `resolve_in` calls sharing one transaction observe a consistent
    /// snapshot, unlike [`Eter::resolve`], which opens and closes a per-call
    /// transaction.
    ///
    /// The `txn` parameter has type `&RoTxn` (= `RoTxn<AnyTls>`), the canonical
    /// read-only reference in heed.  `RoTxn<WithTls>` (returned by
    /// [`LmdbBackend::read_txn`]) and `RwTxn` both deref to `RoTxn<AnyTls>`,
    /// so both are accepted via Rust's automatic deref-coercion.
    ///
    /// # Complexity
    ///
    /// Iterates the node's rows in descending version order via
    /// `rev_prefix_iter`, stopping at the first version ≤ `at`.  This is O(1)
    /// when `at` equals the current version (the common case) and O(k) in the
    /// worst case, where k is the number of versions newer than `at` for this
    /// `(node, field)` pair.
    ///
    /// Note: the ideal O(log n) algorithm described in DESIGN.md would use a
    /// lower-bound cursor seek to `(N, V)` and step back once.  The `heed`
    /// 0.22 `Bytes` codec does not expose range bounds in a form that permits
    /// this directly, so `rev_prefix_iter` is used instead.  The two
    /// approaches are equivalent for the common case; the O(log n) version is
    /// worth revisiting if a clean range API becomes available.
    pub fn resolve_in<F: Field>(
        &self, rtxn: &RoTxn<'_>, node: &Id, at: Eterator,
    ) -> Result<Resolution<F::Content>, LmdbError> {
        let db = self.field_db_or_panic::<F>();
        let prefix = node_prefix::<Id>(node);

        for result in db.rev_prefix_iter(rtxn, &prefix)? {
            let (key, value) = result?;
            let (_node, version) = split_composite_key::<Id>(key);
            if version <= at {
                return Ok(decode_field_row::<F::Content>(value)?.into());
            }
        }
        Ok(Resolution::Absent)
    }

    fn all_store_versions_in(
        &self, rtxn: &RoTxn<'_>,
    ) -> Result<BTreeSet<Eterator>, LmdbError> {
        let mut versions = BTreeSet::new();
        for result in self.versions_db.iter(rtxn)? {
            let (key, _) = result?;
            versions.insert(decode_version_key(key)?);
        }
        Ok(versions)
    }

    fn all_retired_versions_in(
        &self, rtxn: &RoTxn<'_>,
    ) -> Result<BTreeSet<Eterator>, LmdbError> {
        let mut retired = BTreeSet::new();
        for result in self.retired_db.iter(rtxn)? {
            let (key, _) = result?;
            retired.insert(decode_version_key(key)?);
        }
        Ok(retired)
    }

    /// Collect all composite keys in `db` whose rows are collectible given `live`.
    ///
    /// Keys are grouped by node prefix (they are already sorted, so same-node
    /// keys are contiguous). Within each group, a row at version `v` with next
    /// row at `next_v` is collectible iff no live version falls in `[v, next_v)`.
    fn collect_gc_keys(
        db: Database<Bytes, Bytes>, rtxn: &RoTxn<'_>, live: &BTreeSet<Eterator>,
    ) -> Result<Vec<Vec<u8>>, LmdbError> {
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut group: Vec<(Eterator, Vec<u8>)> = Vec::new();
        let mut current_prefix: Option<Vec<u8>> = None;

        for result in db.iter(rtxn)? {
            let (key, _) = result?;
            let prefix = key[..Id::KEY_LEN].to_vec();
            let (_, version) = split_composite_key::<Id>(key);

            if current_prefix.as_ref() != Some(&prefix) {
                process_gc_group(&group, live, &mut to_delete);
                group.clear();
                current_prefix = Some(prefix);
            }
            group.push((version, key.to_vec()));
        }
        process_gc_group(&group, live, &mut to_delete);
        Ok(to_delete)
    }
}

fn process_gc_group(
    group: &[(Eterator, Vec<u8>)], live: &BTreeSet<Eterator>, to_delete: &mut Vec<Vec<u8>>,
) {
    for (i, (version, key)) in group.iter().enumerate() {
        let next_version = group.get(i + 1).map(|(v, _)| *v);
        if is_collectible(*version, next_version, live) {
            to_delete.push(key.clone());
        }
    }
}

fn is_collectible(v: Eterator, next_v: Option<Eterator>, live: &BTreeSet<Eterator>) -> bool {
    match next_v {
        // Last row: collectible only if no live version >= v would resolve to it.
        | None => live.range(v..).next().is_none(),
        // Row has a successor: collectible if no live version falls in [v, next_v).
        | Some(next) => live.range(v..next).next().is_none(),
    }
}

fn scan_last_version(
    versions_db: Database<Bytes, Bytes>, rtxn: &RoTxn<'_>,
) -> Result<Eterator, LmdbError> {
    match versions_db.rev_iter(rtxn)?.next() {
        | Some(Ok((key, _))) => decode_version_key(key),
        | Some(Err(e)) => Err(e.into()),
        | None => Ok(Eterator::EMPTY),
    }
}

// ---------------------------------------------------------------------------
// Write transaction
// ---------------------------------------------------------------------------

/// Pending writes for a single transaction: field [`TypeId`] → list of
/// `(node_bytes, encoded_row)` pairs.  The version bytes are not yet appended;
/// they are added at commit time once the next version number is known.
type PendingWrites = HashMap<TypeId, Vec<(Vec<u8>, Vec<u8>)>>;

/// Write transaction for [`LmdbBackend`].
///
/// Buffers all field writes in memory. On [`WriteTxn::commit`], a single LMDB
/// write transaction assigns the next version to all buffered rows atomically.
///
/// Note: the LMDB write transaction is opened only at commit time, not when
/// `LmdbWriteTxn` is created.  Opening it earlier would hold the write lock
/// across the entire accumulation phase, blocking GC and consuming a reader
/// slot for the implicit MVCC snapshot.
pub struct LmdbWriteTxn<'a, Id, L>
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// The owning backend; provides access to the environment and field databases.
    store: &'a mut LmdbBackend<Id, L>,
    /// Pending writes; see [`PendingWrites`].
    pending: PendingWrites,
}

impl<'a, Id, L> WriteTxn for LmdbWriteTxn<'a, Id, L>
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type NodeId = Id;
    type Error = LmdbError;

    fn apply<F: Field>(mut self, node: &Self::NodeId, row: FieldRow<F::Content>) -> Self {
        // Panics here rather than at commit time, matching filesystem backend behaviour.
        let _db = self.store.field_db_or_panic::<F>();
        let node_bytes = node_prefix::<Id>(node);
        let encoded = encode_field_row::<F::Content>(&row);
        self.pending.entry(TypeId::of::<F>()).or_default().push((node_bytes, encoded));
        self
    }

    fn commit(self) -> Result<Eterator, Self::Error> {
        trace!("lmdb commit begin: pending_types={}", self.pending.len());
        if self.pending.is_empty() {
            trace!("lmdb commit end: no-op");
            return Ok(self.store.current);
        }

        let next = Eterator(self.store.current.version() + 1);
        let version_be = encode_version_key(next);

        let mut wtxn: RwTxn<'_> = self.store.env.write_txn()?;

        for (tid, writes) in self.pending {
            let db = *self
                .store
                .field_dbs
                .get(&tid)
                .expect("field type not registered (checked in apply)");
            for (mut node_bytes, encoded) in writes {
                node_bytes.extend_from_slice(&version_be);
                db.put(&mut wtxn, &node_bytes, &encoded)?;
            }
        }

        self.store.versions_db.put(&mut wtxn, &version_be, &[])?;
        wtxn.commit()?;

        self.store.current = next;
        trace!("lmdb commit end: version={}", next.version());
        Ok(next)
    }
}

// ---------------------------------------------------------------------------
// Eter trait implementation
// ---------------------------------------------------------------------------

impl<Id, L> Eter for LmdbBackend<Id, L>
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type NodeId = Id;
    type Lifecycle = L;
    type Error = LmdbError;
    type WriteTxn<'a>
        = LmdbWriteTxn<'a, Id, L>
    where
        Self: 'a;

    fn resolve<F: Field>(
        &self, at: Eterator, node: &Self::NodeId,
    ) -> Result<Resolution<F::Content>, Self::Error> {
        trace!("lmdb resolve begin: at={} node={node:?}", at.version());
        let rtxn = self.env.read_txn()?;
        let result = self.resolve_in::<F>(&rtxn, node, at)?;
        trace!("lmdb resolve end");
        Ok(result)
    }

    fn node_exists(&self, at: Eterator, node: &Self::NodeId) -> Result<bool, Self::Error> {
        Ok(self.resolve::<Lifecycle<L>>(at, node)?.is_content())
    }

    fn current_version(&self) -> Result<Eterator, Self::Error> {
        Ok(self.current)
    }

    fn field_history<F: Field>(
        &self, node: &Self::NodeId,
    ) -> Result<Vec<VersionedRow<F::Content>>, Self::Error> {
        trace!("lmdb field_history begin: node={node:?}");
        let db = self.field_db_or_panic::<F>();
        let prefix = node_prefix::<Id>(node);
        let rtxn = self.env.read_txn()?;
        let mut out = Vec::new();
        for result in db.prefix_iter(&rtxn, &prefix)? {
            let (key, value) = result?;
            let (_node, version) = split_composite_key::<Id>(key);
            let row = decode_field_row::<F::Content>(value)?;
            out.push((version, row));
        }
        trace!("lmdb field_history end: rows={}", out.len());
        Ok(out)
    }

    fn node_id_in_use(&self, id: &Self::NodeId) -> Result<bool, Self::Error> {
        trace!("lmdb node_id_in_use begin: id={id:?}");
        let prefix = node_prefix::<Id>(id);
        let rtxn = self.env.read_txn()?;
        // Check every field db: any row for this prefix means the id has been used.
        // The lifecycle db is first in the iteration by accident of insertion order,
        // but all dbs must be checked because nothing prevents a caller from writing
        // a non-lifecycle field without a corresponding lifecycle row.
        for &db in self.field_dbs.values() {
            if db.prefix_iter(&rtxn, &prefix)?.next().is_some() {
                trace!("lmdb node_id_in_use end: in_use=true");
                return Ok(true);
            }
        }
        trace!("lmdb node_id_in_use end: in_use=false");
        Ok(false)
    }

    fn check_edges(
        &self, at: Eterator, source: &Self::NodeId, targets: &BTreeSet<Self::NodeId>,
    ) -> Result<Vec<Warning<Self::NodeId>>, Self::Error> {
        trace!(
            "lmdb check_edges begin: at={} source={source:?} targets={}",
            at.version(),
            targets.len()
        );
        let mut warnings = Vec::new();
        for target in targets {
            if !self.node_exists(at, target)? {
                warnings
                    .push(Warning::DanglingEdge { source: source.clone(), target: target.clone() });
            }
        }
        trace!("lmdb check_edges end: warnings={}", warnings.len());
        Ok(warnings)
    }

    fn write(&mut self) -> Self::WriteTxn<'_> {
        trace!("lmdb write begin");
        LmdbWriteTxn { store: self, pending: HashMap::new() }
    }

    fn retire(&mut self, versions: impl IntoIterator<Item = Eterator>) -> Result<(), Self::Error> {
        trace!("lmdb retire begin");
        let mut wtxn = self.env.write_txn()?;
        for v in versions {
            self.retired_db.put(&mut wtxn, &encode_version_key(v), &[])?;
        }
        wtxn.commit()?;
        trace!("lmdb retire end");
        Ok(())
    }

    fn only_keep(
        &mut self, versions: impl IntoIterator<Item = Eterator>,
    ) -> Result<(), Self::Error> {
        trace!("lmdb only_keep begin");
        let keep: BTreeSet<Eterator> = versions.into_iter().collect();

        // Read all stored versions, then retire those outside the keep set.
        let all = {
            let rtxn = self.env.read_txn()?;
            self.all_store_versions_in(&rtxn)?
        };

        let mut wtxn = self.env.write_txn()?;
        for v in all {
            if !keep.contains(&v) {
                self.retired_db.put(&mut wtxn, &encode_version_key(v), &[])?;
            }
        }
        wtxn.commit()?;
        trace!("lmdb only_keep end");
        Ok(())
    }

    fn gc(&mut self, option: GcOption) -> Result<(), Self::Error> {
        trace!("lmdb gc begin");

        // ---- Phase 1: read phase (no write txn open) -----------------------
        let keys_to_delete_per_db: HashMap<TypeId, Vec<Vec<u8>>> = {
            let rtxn = self.env.read_txn()?;
            let all = self.all_store_versions_in(&rtxn)?;
            let retired = self.all_retired_versions_in(&rtxn)?;
            let live: BTreeSet<Eterator> = match option {
                | GcOption::UseRetiredSet => all.difference(&retired).copied().collect(),
                | GcOption::UseLiveSet(live_set) => live_set,
            };

            let mut map: HashMap<TypeId, Vec<Vec<u8>>> = HashMap::new();
            for (&tid, &db) in &self.field_dbs {
                let keys = Self::collect_gc_keys(db, &rtxn, &live)?;
                map.insert(tid, keys);
            }
            map
        };

        // ---- Phase 2: write phase ------------------------------------------
        let mut wtxn = self.env.write_txn()?;

        for (tid, keys) in &keys_to_delete_per_db {
            let db = self.field_dbs[tid];
            for key in keys {
                db.delete(&mut wtxn, key.as_slice())?;
            }
        }

        // Find which version numbers still have at least one row in any field
        // db after the deletions.  Then prune _versions and _retired for any
        // version that no longer appears in any field db.
        //
        // We collect remaining versions by scanning field dbs now (after
        // deletions have been written into wtxn but before commit).
        let mut remaining_versions: BTreeSet<Eterator> = BTreeSet::new();
        for &db in self.field_dbs.values() {
            for result in db.iter(&wtxn)? {
                let (key, _) = result?;
                if key.len() >= Id::KEY_LEN + 8 {
                    let (_, v) = split_composite_key::<Id>(key);
                    remaining_versions.insert(v);
                }
            }
        }

        // Collect _versions entries to remove (must not mutate while iterating).
        let mut versions_to_remove: Vec<Eterator> = Vec::new();
        for result in self.versions_db.iter(&wtxn)? {
            let (key, _) = result?;
            let v = decode_version_key(key)?;
            if !remaining_versions.contains(&v) {
                versions_to_remove.push(v);
            }
        }
        for v in versions_to_remove {
            let key = encode_version_key(v);
            self.versions_db.delete(&mut wtxn, &key)?;
            self.retired_db.delete(&mut wtxn, &key)?;
        }

        wtxn.commit()?;

        // Refresh cached current version.
        let rtxn = self.env.read_txn()?;
        self.current = scan_last_version(self.versions_db, &rtxn)?;
        drop(rtxn);

        trace!("lmdb gc end: current_version={}", self.current.version());
        Ok(())
    }

    fn retired_versions(&self) -> Result<BTreeSet<Eterator>, Self::Error> {
        let rtxn = self.env.read_txn()?;
        self.all_retired_versions_in(&rtxn)
    }

    fn live_versions(&self) -> Result<BTreeSet<Eterator>, Self::Error> {
        let rtxn = self.env.read_txn()?;
        let all = self.all_store_versions_in(&rtxn)?;
        let retired = self.all_retired_versions_in(&rtxn)?;
        Ok(all.difference(&retired).copied().collect())
    }
}

// ---------------------------------------------------------------------------
// LiveNodes optional trait
// ---------------------------------------------------------------------------

impl<Id, L> crate::LiveNodes for LmdbBackend<Id, L>
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    /// Returns all node IDs whose [`Lifecycle`] field resolves to content at `at`.
    ///
    /// Scans the entire lifecycle database in ascending key order — O(total
    /// lifecycle rows).  For each node, rows are visited in ascending version
    /// order; the set membership is updated on every row ≤ `at` so that the
    /// final state for each node reflects the most-recent row at or before `at`.
    /// Once a row with version > `at` is seen for a node, that node's group is
    /// marked done and its remaining rows are skipped.
    fn live_nodes(&self, at: Eterator) -> Result<BTreeSet<Id>, Self::Error> {
        trace!("lmdb live_nodes begin: at={}", at.version());
        let lifecycle_db = self.field_db_or_panic::<Lifecycle<L>>();
        let rtxn = self.env.read_txn()?;
        let mut live = BTreeSet::new();

        let mut current_prefix: Vec<u8> = Vec::new();
        let mut current_node_done = false;

        for result in lifecycle_db.iter(&rtxn)? {
            let (key, value) = result?;
            let prefix = key[..Id::KEY_LEN].to_vec();
            let (_, version) = split_composite_key::<Id>(key);

            if prefix != current_prefix {
                current_prefix = prefix;
                current_node_done = false;
            }
            if current_node_done {
                continue;
            }
            if version <= at {
                let row = decode_field_row::<L>(value)?;
                let node = Id::from_key_bytes(&key[..Id::KEY_LEN]);
                match row {
                    | FieldRow::Content(_) => {
                        live.insert(node);
                    }
                    | FieldRow::Deleted => {
                        live.remove(&node);
                    }
                }
            } else {
                current_node_done = true;
            }
        }

        trace!("lmdb live_nodes end: count={}", live.len());
        Ok(live)
    }
}

// ---------------------------------------------------------------------------
// Convenience constructor
// ---------------------------------------------------------------------------

/// Convenience constructor for a [`LmdbFieldConfig`] with the built-in
/// protocol fields pre-registered.
///
/// Built-in database names:
/// - `"lifecycle"` for [`Lifecycle<L>`]
/// - `"edges"` for [`Edges<Id>`]
///
/// Chain [`LmdbFieldConfig::with_field`] to add user-defined fields.
pub fn lmdb_builtins_config<Id, L>() -> LmdbFieldConfig
where
    Id: LmdbKey,
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    LmdbFieldConfig::new().with_field::<Lifecycle<L>>("lifecycle").with_field::<Edges<Id>>("edges")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Eter, Eterator, GcOption, Lifecycle, Resolution, WriteTxn};
    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    // --- Test NodeId: fixed 8-byte big-endian integer ---

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    struct TestId(u64);

    impl LmdbKey for TestId {
        const KEY_LEN: usize = 8;
        fn to_key_bytes(&self) -> Vec<u8> {
            self.0.to_be_bytes().to_vec()
        }
        fn from_key_bytes(bytes: &[u8]) -> Self {
            Self(u64::from_be_bytes(bytes.try_into().unwrap()))
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    enum State {
        Active,
        Removed,
    }

    struct TagField;
    impl Field for TagField {
        type Content = String;
    }

    struct CountField;
    impl Field for CountField {
        type Content = u32;
    }

    fn open(dir: &TempDir) -> LmdbBackend<TestId, State> {
        let config = lmdb_builtins_config::<TestId, State>()
            .with_field::<TagField>("tag")
            .with_field::<CountField>("count");
        LmdbBackend::<TestId, State>::open(dir.path(), 1 << 20, config).unwrap()
    }

    fn id(n: u64) -> TestId {
        TestId(n)
    }

    // --- open ---

    #[test]
    fn open_empty_store() {
        let dir = TempDir::new().unwrap();
        let store = open(&dir);
        assert_eq!(store.current_version().unwrap(), Eterator::EMPTY);
    }

    #[test]
    fn open_existing_store_recovers_current_version() {
        let dir = TempDir::new().unwrap();
        let v1 = {
            let mut store = open(&dir);
            store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap()
        };
        let store = open(&dir);
        assert_eq!(store.current_version().unwrap(), v1);
    }

    // --- write / resolve ---

    #[test]
    fn write_and_resolve_content() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<TagField>(&id(1), "hello".to_owned())
            .commit()
            .unwrap();
        assert_eq!(
            store.resolve::<TagField>(v1, &id(1)).unwrap(),
            Resolution::Content("hello".to_owned())
        );
    }

    #[test]
    fn resolve_absent_before_first_write() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
        assert_eq!(store.resolve::<TagField>(v1, &id(1)).unwrap(), Resolution::Absent);
    }

    #[test]
    fn resolve_deleted_field() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<TagField>(&id(1), "x".to_owned())
            .commit()
            .unwrap();
        let v2 = store.write().delete::<TagField>(&id(1)).commit().unwrap();
        assert_eq!(
            store.resolve::<TagField>(v1, &id(1)).unwrap(),
            Resolution::Content("x".to_owned())
        );
        assert_eq!(store.resolve::<TagField>(v2, &id(1)).unwrap(), Resolution::Deleted);
    }

    #[test]
    fn resolve_at_old_snapshot_returns_historical_value() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<CountField>(&id(1), 10)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&id(1), 20).commit().unwrap();
        assert_eq!(store.resolve::<CountField>(v1, &id(1)).unwrap(), Resolution::Content(10));
        assert_eq!(store.resolve::<CountField>(v2, &id(1)).unwrap(), Resolution::Content(20));
    }

    #[test]
    fn empty_commit_returns_current_version() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
        let v_noop = store.write().commit().unwrap();
        assert_eq!(v1, v_noop);
    }

    // --- node_exists / node_id_in_use ---

    #[test]
    fn node_exists_reflects_lifecycle() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
        let v2 = store.write().delete::<Lifecycle<State>>(&id(1)).commit().unwrap();
        assert!(store.node_exists(v1, &id(1)).unwrap());
        assert!(!store.node_exists(v2, &id(1)).unwrap());
    }

    #[test]
    fn node_id_in_use_after_write() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        assert!(!store.node_id_in_use(&id(42)).unwrap());
        store.write().set::<Lifecycle<State>>(&id(42), State::Active).commit().unwrap();
        assert!(store.node_id_in_use(&id(42)).unwrap());
    }

    // --- field_history ---

    #[test]
    fn field_history_returns_all_rows_in_order() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<CountField>(&id(1), 1)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&id(1), 2).commit().unwrap();
        let v3 = store.write().delete::<CountField>(&id(1)).commit().unwrap();

        let history = store.field_history::<CountField>(&id(1)).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0], (v1, FieldRow::Content(1)));
        assert_eq!(history[1], (v2, FieldRow::Content(2)));
        assert_eq!(history[2], (v3, FieldRow::Deleted));
    }

    // --- version management ---

    #[test]
    fn live_and_retired_versions() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
        let v2 = store.write().set::<CountField>(&id(1), 5).commit().unwrap();

        store.retire([v1]).unwrap();
        assert!(store.retired_versions().unwrap().contains(&v1));
        assert!(!store.retired_versions().unwrap().contains(&v2));
        assert!(!store.live_versions().unwrap().contains(&v1));
        assert!(store.live_versions().unwrap().contains(&v2));
    }

    #[test]
    fn only_keep_retires_others() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
        let v2 = store.write().set::<CountField>(&id(1), 1).commit().unwrap();
        let v3 = store.write().set::<CountField>(&id(1), 2).commit().unwrap();

        store.only_keep([v3]).unwrap();
        let retired = store.retired_versions().unwrap();
        assert!(retired.contains(&v1));
        assert!(retired.contains(&v2));
        assert!(!retired.contains(&v3));
    }

    #[test]
    fn retired_versions_persist_across_reopen() {
        let dir = TempDir::new().unwrap();
        let v1 = {
            let mut store = open(&dir);
            let v1 = store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
            store.retire([v1]).unwrap();
            v1
        };
        let store = open(&dir);
        assert!(store.retired_versions().unwrap().contains(&v1));
    }

    // --- garbage collection ---

    #[test]
    fn gc_removes_superseded_rows() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<CountField>(&id(1), 1)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&id(1), 2).commit().unwrap();
        let v3 = store.write().set::<CountField>(&id(1), 3).commit().unwrap();

        store.retire([v1, v2]).unwrap();
        store.gc(GcOption::UseRetiredSet).unwrap();

        // v1 and v2 rows are gone, but v3 still reads correctly.
        assert_eq!(store.resolve::<CountField>(v3, &id(1)).unwrap(), Resolution::Content(3));
        let history = store.field_history::<CountField>(&id(1)).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].0, v3);
    }

    #[test]
    fn gc_with_live_set() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        store.write().set::<Lifecycle<State>>(&id(1), State::Active).commit().unwrap();
        store.write().set::<CountField>(&id(1), 10).commit().unwrap();
        let v3 = store.write().set::<CountField>(&id(1), 20).commit().unwrap();

        let live = BTreeSet::from([v3]);
        store.gc(GcOption::UseLiveSet(live)).unwrap();

        assert_eq!(store.resolve::<CountField>(v3, &id(1)).unwrap(), Resolution::Content(20));
    }

    #[test]
    fn gc_does_not_alter_live_reads() {
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<CountField>(&id(1), 1)
            .commit()
            .unwrap();
        let v2 = store.write().set::<CountField>(&id(1), 2).commit().unwrap();

        // Both v1 and v2 are live; neither row should be collected.
        store.gc(GcOption::UseRetiredSet).unwrap();

        assert_eq!(store.resolve::<CountField>(v1, &id(1)).unwrap(), Resolution::Content(1));
        assert_eq!(store.resolve::<CountField>(v2, &id(1)).unwrap(), Resolution::Content(2));
    }

    // --- check_edges / dangling ---

    #[test]
    fn check_edges_reports_dangling() {
        use crate::Edges;
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<Edges<TestId>>(&id(1), BTreeSet::from([id(99)]))
            .commit()
            .unwrap();
        let warnings = store.check_edges(v1, &id(1), &BTreeSet::from([id(99)])).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(
            &warnings[0],
            crate::Warning::DanglingEdge { source, target }
            if *source == id(1) && *target == id(99)
        ));
    }

    #[test]
    fn check_edges_no_warnings_for_live_target() {
        use crate::Edges;
        let dir = TempDir::new().unwrap();
        let mut store = open(&dir);
        let v1 = store
            .write()
            .set::<Lifecycle<State>>(&id(1), State::Active)
            .set::<Lifecycle<State>>(&id(2), State::Active)
            .set::<Edges<TestId>>(&id(1), BTreeSet::from([id(2)]))
            .commit()
            .unwrap();
        let warnings = store.check_edges(v1, &id(1), &BTreeSet::from([id(2)])).unwrap();
        assert!(warnings.is_empty());
    }
}
