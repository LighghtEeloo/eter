# Eter: Immutable Persistent Entry Store Protocol

Eter is a protocol for versioned entry storage with immutable snapshots. The
interface is defined as Rust traits and implemented by backends such as
filesystem-backed stores and concurrent database engines. The name refers to
immutable historical states.

Users append history rather than mutating entries in place. Live
`SnapshotRef`s identify immutable snapshots of the entry store. Old states
remain available for as long as the user chooses to keep them. The user may
view old states or linearly revert to one of them and drop later updates.
Undo-tree-like branching is delegated to external systems such as git or
database snapshots.

An application may keep a separate working folder beside the Eter store. The
working folder is an unversioned projection of one entry set. The projection
layer maps files to typed entry updates; Eter stores the resulting snapshots.

The `Eterator` representation determines the storage model and the cost of
historical reads. The desired properties are:

- Efficient history management for changes within a single entry.
- Efficient retrieval of the entry store at a given snapshot.
- Stable entry identity across versions.
- Explicit selection of snapshots to keep or retire.

Three initial storage strategies define the trade space:

- Entries are immutable, and each update creates a new entry with a new `EntryId`.
  This is simple but can allocate many entries for small field changes.
- Entries are locally versioned, and the `EntryId`s are stable across versions.
  This preserves identity but requires vector-clock-like state to describe a
  global snapshot.
- Entries are globally versioned with stable `EntryId`s. `Eterator` is implemented
  simply as a global version number, and each entry field is versioned with the
  same version number. This is simple and efficient for both updates and
  retrieval; the only caveat is the version number may run out much faster than
  other strategies.

Global versioning is the chosen strategy. Every non-empty write operation
assigns the next version after the store's current retained version to every
field row it produces. The logical current version is the maximum retained
version in the store. A single operation may touch multiple entries and fields;
all rows written in the same operation share the same version, providing
atomic-snapshot semantics. An `Eterator` is therefore a single integer
coordinate inside one GC generation.

A 64-bit version space yields ~1.8 × 10^19 coordinates inside one generation.
Without collection that lowers the current retained version, one billion
writes per second would exhaust the space in roughly 584 years.

Cross-entry derived data such as the current retained maximum version, version
registries, the GC generation, and live-entry sets may be cached in memory or
auxiliary tables. The GC generation is auxiliary persistent state. It is not
entry content, but it is authoritative for snapshot-reference validity.

## Core Concepts

`Eterator` is a version coordinate: concretely, a global version number within
one GC generation. The user receives a `SnapshotRef` after each write and may
hold any number of live snapshots simultaneously. A `SnapshotRef` pairs a
`GcGeneration` with an `Eterator`. Backends reject reads through older
generations and through non-empty coordinates that are not retained in the
current generation.

Entries are the basic units of the entry store. Each entry has a fixed,
compile-time-defined set of fields. The protocol presents each field as a
logical history keyed by `(EntryId, version)`. A row holds either content or a
deletion marker. Resolution inherits unchanged fields from the nearest earlier
row in that field's history. Backends may store sparse field rows or equivalent
full-entry snapshots. Versioning is logical per-field even when the physical
layout co-locates fields.

Applications may define typed entry facets over those fields. An entry facet
is a Rust type isomorphic to a coherent subset of fields for one entry. A
facet loads and writes that subset by calling the field-level protocol. It
owns required-field checks, defaults, presence rules, and domain invariants.
It does not define file syntax.

`EntryId` is the unique identifier for an entry. The concrete type is chosen by
the user (e.g. UUIDv7, slug, integer). The only requirement is uniqueness
within the store for fresh entries, verifiable through the `Eter` interface
before insertion. Re-creating a deleted entry uses its existing `EntryId`.

`Eter` is the store itself. Version preservation is expressed through live and
retired snapshots. In retired-set mode, every retained snapshot not in the
retired set is considered live and must be preserved. The user adds snapshots
to the retired set explicitly, or uses an "only-keep" operation that retires
everything except a given set of snapshots.

When a backend persists the retired set, the failure mode is conservative: if
the retired set fails to persist, versions that could be collected survive.
Tracking pinned versions instead and treating everything else as retired risks
destroying live data on a failed write.

The store may also run garbage collection from an explicit live-snapshot set.
For that invocation, every retained snapshot outside the supplied set is
treated as retired. This mode places version bookkeeping in the caller's hands.
The two modes are compatible: the retired set is a convenience layer atop the
stateless GC primitive.

## Application Folder Projection

Folder-level operations belong to an application projection layer. A working
folder is an unversioned materialization of an Eter snapshot. The Eter store
is the durable snapshot store. A filesystem-backed store path must be distinct
from the working folder and must not be discovered as an entry inside it.

The projection layer owns the mapping from files to entries. It chooses which
files belong to the entry set, parses entry ids, maps frontmatter or other
document syntax to registered fields, supplies body text, renders files, and
defines conflict policy. Eter receives typed entry updates after that mapping.
Application syntax does not enter the core store protocol.

A folder commit adapter may represent the selected working folder as the
desired current entry set. Entries present in the working set are written with
lifecycle content. Entries present at the previous version but absent from the
working set receive a lifecycle deletion marker. Unchanged fields need not
produce new logical content, but all rows written by the adapter share one
global version.

If the projected working set is identical to the current snapshot, the adapter
returns the current `SnapshotRef` and writes no new rows. Otherwise it returns
the new `SnapshotRef`. The working folder is not mutated by commit.

A folder checkout adapter is the inverse projection. Given a `SnapshotRef`, it
resolves live entries at the selected version, renders them through the
application's document syntax, and writes the resulting files to a target
folder.

Checkout may replace the target folder contents for the entry set it owns. The
projection layer defines ownership boundaries, ignored files, and conflict
policy. Eter provides snapshot reads; it does not decide which application
files should survive checkout.

Checkout is not a change to Eter history. It writes a working materialization
of an existing snapshot. A later commit may import that folder and append a new
snapshot.

## Resolution

Reading field `F` of entry `N` at `SnapshotRef(G, V)`:

1. Verify that `G` is the current GC generation.
2. Verify that `V` is a retained snapshot version, unless `V` is
   `Eterator::EMPTY`.
3. Seek to the row in `F`'s table with key `(N, v)` where `v` is the largest
   version ≤ `V`.
4. If the row contains content, return it.
5. If the row is a deletion marker, return `Resolution::Deleted`.
6. If no row exists, return `Resolution::Absent`.

This is a single backward seek in a sorted key-value store, O(log k) where
k is the number of versions for the `(N, F)` pair. Backends may additionally
cache per-`SnapshotRef` resolution maps for hot-path queries.

## Entry Lifecycle

A built-in `lifecycle` field tracks entry existence and state. In storage it
behaves like any other field: keyed by `(EntryId, version)`, holding either
content or a deletion marker. The protocol checks this field to determine
whether an entry is present: if it resolves to content, the entry exists; if it
resolves to a deletion marker or has never been written, the entry is absent.
Other fields' state does not affect this determination.

The value stored in `lifecycle` when present is user-defined. A minimal
application uses a unit-like active marker. Richer applications can encode
states like archived, draft, or deprecated that carry meaning at the
application layer without affecting protocol-level resolution. The protocol
only distinguishes "has content" from "absent."

A deleted `EntryId` may be reactivated: writing a new content row to
`lifecycle` at a later version re-creates the entry under the same identity.

## Garbage Collection

Garbage collection is driven by the retired-snapshot set, or by the complement
of the live set supplied to a stateless GC call. A field row at version `v`
with successor row `v_next` serves live snapshot coordinates in
`[v, v_next)`. If there is no successor row, it serves live snapshot
coordinates in `[v, ∞)`. The row is collectible when this served range contains
no live coordinate.

Given two consecutive live coordinates `V_a < V_b` and rows at `v1 < v2 < v3`
all within `(V_a, V_b]`, rows `v1` and `v2` are redundant when `v3` is the row
that resolves at `V_b`. They can be freed.

Garbage collection preserves the resolution results for selected live
coordinates in the next generation. When a collection pass physically deletes
rows or snapshots, the backend advances the GC generation. `SnapshotRef`s from
older generations are stale and are rejected.

## Optional Caches

The retained field history is the single source of truth for entry content.
All other data structures are derived caches or indices.

- **Current version.** The maximum retained version. Avoids a full scan when
  answering `current_version`; updated on writes and rebuilt after GC or open
  when needed.
- **GC generation.** The current retained coordinate generation. It validates
  `SnapshotRef`s and records that numeric `Eterator` coordinates may have been
  reused after physical collection.
- **Live-entry set.** The set of `EntryId`s whose `lifecycle` field resolves to
  content at a given version. Without this cache, enumerating live entries
  requires scanning the full `EntryId` space.
- **Per-`SnapshotRef` resolution map.** Precomputed `(EntryId, field) → version`
  mappings for frequently accessed snapshots.

Backends decide which caches and derived indices to maintain. Derived indices
may be invalidated on startup when they can be rebuilt or validated from
retained field history. The GC generation is persisted by backends that
physically collect rows or snapshots. The protocol defines optional traits for
backends that support specific derived views but leaves persistence and
invalidation strategy to each backend.

## Concurrency

The version assigned to a write serializes retained writes into a total order.
Strategies:

- **Single-writer.** One writer holds exclusive access; readers use their
  `SnapshotRef`s without coordination. Sufficient when write throughput
  is not a bottleneck.
- **Compare-and-swap.** Writers prepare a batch optimistically, then CAS the
  current snapshot and GC generation. On conflict, retry.
- **Batched writes.** Multiple field mutations share a single version,
  conserving version space.

The protocol defines the logical model. Concurrency control is a backend
concern.

## Backend Considerations

Database backends can map the logical field histories to ordered
`(EntryId, field, version)` keys, prefix scans, and backward seeks. These
requirements point toward sorted key-value stores.

- **Filesystem**: Each entry is a directory in a history root, with versions
  encoded in filenames. No concurrency support; suitable for single-user,
  human-readable history stores.
- **LMDB** (via `heed`): B-tree, MVCC, memory-mapped. Single-writer by
  design, matching the simplest concurrency model. Lock-free read transactions.
  Requires `EntryId` to produce fixed-size, sort-preserving bytes;
  see the [LMDB Backend](#lmdb-backend) section.
- **redb**: Pure-Rust B-tree store. Simpler dependency tree. Similar access
  patterns.

The protocol is backend-agnostic: it defines traits that any conforming
backend implements.

## Filesystem Backend

The filesystem backend stores entry history as markdown files. It targets
single-user scenarios where the history root remains inspectable. A separate
working folder may provide the editable document tree. No concurrency support.

### Layout

The user provides a root directory for the history store.
It must be empty on first use or contain a valid prior history state.

```
<root>/
  Eter.lock.toml
  <entry_id>/
    <version>-<entry_id>.md
    ...
  ...
```

Each entry occupies a subdirectory named by its `EntryId`, which must be
filesystem-friendly: no path separators, no `.` or `..`, no null bytes, no
`Eter.lock.toml`, and reasonable length. Inside are markdown files, one per
version.

The filename is `<version>-<entry_id>.md` where `<version>` is the 64-bit
version number zero-padded to 16 hexadecimal digits. Zero-padding ensures
lexicographic filename order matches version order. The `<entry_id>` suffix
is redundant with the directory name but aids readability in editors and
tools that display only the filename.

The backend persists global metadata in `Eter.lock.toml`. The file stores the
lock-file format version and the current GC generation. It does not record a
retired-snapshot set. Retired snapshots are tracked in memory for the current
backend instance, and callers may also provide an explicit live set to garbage
collection. Derived caches are held in memory and rebuilt from the file tree on
startup.

```toml
lock_format_version = 1
gc_generation = 0
```

### File Format

Each version file uses YAML frontmatter delimited by `---`, followed by a
markdown body:

```md
---
lifecycle: active
title: Alpha
---

Body text in markdown.
```

The YAML mapping holds `lifecycle` and any user-defined fields registered with
the backend. Registration is static: user-defined `Field` types are fixed at
compile time and mapped to keys when the backend is constructed. A key's
presence means the field has content in that full-entry snapshot. An absent key
means the field is absent in that snapshot. Null values are invalid; deletion
is encoded by omitting the key.

Per-version metadata is complete across pathname and frontmatter:

- Path metadata: `EntryId` from `<root>/<entry_id>/`.
- Filename metadata: `version` from `<version>-<entry_id>.md`.
- Frontmatter metadata: `lifecycle` and registered user fields.

No additional per-entry hidden metadata exists for this backend.

The markdown text after the closing delimiter is the entry's body, a
privileged content field specific to this backend. It has no representation
in the YAML header.

### Protocol Mapping

All fields for a given `(EntryId, version)` are co-located in a single file.
This is full per-entry snapshot storage: every write creates a new file
containing updated fields and unchanged fields copied from the previous
version. The trade-off is more storage on partial updates in exchange for
simpler resolution, atomic per-entry snapshots, and human-readable files.

**resolve.** Verify the supplied `SnapshotRef`, scan filenames in
`<root>/<entry_id>/`, find the file with the largest hex version ≤ the queried
`Eterator`, parse the YAML header, and return the requested field. For the
body field, return the markdown text.

**write.** Assign the next version, one greater than the current retained
version. Create a new file in `<root>/<entry_id>/` with the updated fields and
all unchanged fields copied from the previous version. Return a `SnapshotRef`
in the current GC generation.

**current_version.** The maximum hex version across all filenames in the
root. Cached in memory after the initial scan, set to the committed version on
write, and rebuilt after GC. This value may move backward after GC removes the
newest retained snapshots.

**gc_generation.** The current GC generation. Stored in `Eter.lock.toml` as an
unsigned TOML integer. Created with the initial generation when missing on
open.

**field_history.** List all files in `<root>/<entry_id>/` in version order
and parse the requested field from each. Because files are full-entry
snapshots, copied unchanged values may appear as later physical rows. When a
field is present in one snapshot and absent in the next, the backend reports a
logical `FieldRow::Deleted` at the later version.

**live_entries.** Scan entry directories and resolve `lifecycle` at the
queried `SnapshotRef`. This is O(entries) and is intended for application
projection commit and checkout paths.

**gc.** Delete version files whose served version ranges contain no live
coordinate. The backend may use its in-memory retired set or an explicit live
set supplied to the collection call. When files are deleted, persist the next
GC generation before removing them.


## LMDB Backend

The LMDB backend targets durable, transactional storage under single-writer
access. It uses the `heed` crate for LMDB bindings. Read transactions are
lock-free; at most one write transaction may be open at a time.

### EntryId Constraint

The `EntryId` type used with this backend must implement `LmdbKey`, a
backend-local trait with two properties. `to_key_bytes` returns the byte
representation of the id. `KEY_LEN` is a compile-time constant declaring the
exact byte length; every value of the type must produce exactly that many
bytes. The byte representation must be order-preserving: `a < b` under the
type's `Ord` impl must imply `to_key_bytes(a) < to_key_bytes(b)`
lexicographically. UUIDs (16 bytes, big-endian), integer ids (4 or 8 bytes,
big-endian), and fixed-width padded slugs satisfy this constraint.
Variable-length encodings are not permitted.

The fixed-size requirement eliminates composite-key ambiguity: because the
`EntryId` portion of every key occupies exactly `KEY_LEN` bytes, the version
boundary is at a known offset and no separator or length prefix is needed.

### Layout

The backend opens a single LMDB environment. Opening the same environment
path from two processes simultaneously is unsupported; LMDB enforces
single-environment access via file locking.

The environment contains:

- One named database per registered `Field` type, identified by the field's
  static name string.
- `_versions`: the derived version registry, recording retained versions that
  still have at least one field row.
- `_gc_generation`: the current GC generation.
- `_retired`: the persistent retired-coordinate set.

The names `_versions`, `_gc_generation`, and `_retired` are reserved. A
registered `Field` whose static name matches one of them will collide with a
backend database at construction time; the backend rejects this at construction
with a panic.

The total database count is `|registered fields| + 3`. The backend derives
`max_dbs` from the registered field list at construction time; the caller does
not set it manually.

### Key Encoding

Within each per-field database, rows are keyed by a fixed-size composite key:
the `LmdbKey` bytes of the `EntryId` (exactly `KEY_LEN` bytes) followed by the
8-byte big-endian encoding of the version number. Big-endian encoding places
lower versions before higher versions lexicographically, which is required for
the backward-seek resolution algorithm. Because the `EntryId` prefix is
fixed-length, the split between the two parts is unambiguous.

### Value Encoding

Each row value uses a one-byte tag prefix followed by optional content.

- `0x00`: deletion marker (`FieldRow::Deleted`). No further bytes.
- `0x01, ...json`: content (`FieldRow::Content`), where the remaining bytes
  are the `serde_json`-serialized field value.

The tag distinguishes deletion markers from content without relying on absent
keys, which carry a separate meaning (the field has never been written for
this entry at or before the queried version).

### Write Transaction

The write transaction (`WriteTxn`) accumulates field rows in memory and opens
a single LMDB write transaction only at commit time. The alternative—holding
the LMDB write transaction open from the moment `WriteTxn` is created—would
block all GC passes and any operation that requires a write lock for the
duration of accumulation. Buffering in memory and committing atomically avoids
this hazard. On commit, all buffered field rows and a `_versions` entry are
written in one atomic step. The returned `SnapshotRef` uses the current GC
generation.

### Resolution

Reading field `F` of entry `N` at `SnapshotRef(G, V)`:

1. Open a short-lived read transaction.
2. Verify that `G` is the current GC generation.
3. Verify that `V` is present in `_versions`, unless `V` is `Eterator::EMPTY`.
4. Seek to the first key ≥ `N || V` using a lower-bound cursor seek.
5. If the key equals `N || V` exactly, the row at version `V` exists; return
   it directly.
6. Otherwise step the cursor back one position.
7. If the resulting key begins with the bytes of `N`, decode the version and
   value from the key and the stored data. The resolution is complete.
8. If the cursor is before the start of the database or the key prefix does
   not equal the bytes of `N`, the field is absent for this entry.
9. Close the read transaction.

This is O(log n) in the total number of rows for field `F`, since the
lower-bound seek is a single B-tree traversal. The current implementation
uses `rev_prefix_iter` instead of a lower-bound seek, because the `heed`
0.22 `Bytes` codec does not expose range bounds in a form that composes
cleanly with the composite `&[u8]` key type. `rev_prefix_iter` starts at
the newest row for `N` in field `F` and scans backward, stopping at the
first version ≤ `V`. This is O(1) for the common case (resolving at the
current version) and O(k) in the worst case, where k is the number of
versions of `(N, F)` newer than `V`. The two approaches produce identical
results; the lower-bound variant should be preferred if a compatible range
API becomes available.

### Snapshot References and Read Transactions

`SnapshotRef` holds no LMDB resource; it is a plain generation and version
coordinate. Each call to `resolve` opens a read transaction, executes the seek
described above, and closes the transaction before returning. No read
transaction persists beyond a single call.

This model avoids two LMDB hazards. First, LMDB's reader table has a finite
number of slots (configurable, defaulting to 126). Keeping one slot open per
live `SnapshotRef` would exhaust this table for workloads with many concurrent
snapshots. Second, long-lived read transactions pin LMDB's freelist: pages
freed by garbage collection cannot be recycled while a reader that predates
the deletion is open, causing the database file to grow without bound. The
per-call transaction model eliminates both problems.

The trade-off is that a sequence of `resolve` calls at the same `SnapshotRef`
is not protected by a single LMDB snapshot. Under single-writer access, a
write cannot interleave with an in-progress logical read operation, so this is
safe in practice. Applications that require strict multi-field snapshot
consistency may use the backend's `read_txn` method to open an explicit
`heed::RoTxn` and pass it to `resolve_in`, a backend-specific counterpart to
`Eter::resolve` that accepts a borrowed transaction. The caller is responsible
for closing that transaction promptly; holding it open reintroduces the
reader-table and freelist hazards described above. The per-`SnapshotRef`
resolution cache described in the Optional Caches section is an alternative
that avoids open transactions entirely.

### Configuration

The backend constructor accepts two caller-supplied parameters.

`map_size` sets the maximum size of the LMDB memory map in bytes. LMDB
requires this value at environment open time and cannot grow the map
automatically. If accumulated data exceeds `map_size`, subsequent writes
return an error. Resizing requires calling `env.resize` with no active
transactions of any kind (read or write); the caller is responsible for
choosing a `map_size` large enough for the expected working set and for
initiating resizes when needed. A safe default for small stores is 1 GiB;
production deployments
should size this according to data volume projections.

The registered field list is the second parameter. The backend enumerates it
at construction to open or create each named database and to derive `max_dbs`.
Every `Field` type that will be passed to `resolve` or `WriteTxn::set` must
appear in this list; an unregistered field type panics at call time.

No other persistent global configuration exists on disk.

### Version Registry and Generation

The `_versions` database maps each retained version number (8-byte big-endian
key) to an empty value. On each `WriteTxn::commit`, the new version is inserted
into `_versions` within the same write transaction. During GC, a version with
no remaining field rows is removed from `_versions`. The registry is therefore
a derived index over the field databases.

The `_gc_generation` database stores the current GC generation under a fixed
key. GC writes `_versions`, `_retired`, `_gc_generation`, and field-row
deletions in one LMDB transaction when physical data is removed.

`current_version` is a single backward cursor seek to the last key in
`_versions`, executing in O(log n). `live_snapshots` scans `_versions` and
subtracts `_retired`, both O(versions). `retired_snapshots` scans `_retired`,
O(retired). These scans complete with LMDB page-cache efficiency and do not
require a full field-table traversal.

### Garbage Collection

GC runs in two phases. The read phase opens a read transaction, computes the
live set, scans every per-field database for collectible keys, and collects
those keys in memory. The read transaction is then closed. The write phase
opens a write transaction, deletes all collected keys, and scans the field
databases (within the same write transaction, which sees the deletions) to
determine which version numbers still have at least one row. Any version
present in `_versions` but absent from the remaining rows is removed from
both `_versions` and `_retired`. This bounds the growth of both auxiliary
tables to retained row versions, not the total number of versions ever written.
When the pass removes any rows or registry entries, it writes the next GC
generation in the same transaction.

Splitting GC into a read phase and a write phase is necessary because LMDB
cursors used for scanning cannot coexist with mutations in the same table
within a single cursor lifetime. Collecting keys into memory first, then
deleting them in a separate pass, avoids this constraint.

Deleted rows return their pages to LMDB's freelist; the database file does not
shrink. To reclaim disk space after a GC pass, the caller must compact the
environment by copying it to a new path with the compaction flag enabled
(`MDB_CP_COMPACT` in LMDB terms; `heed` exposes this via its environment copy
API). The backend does not automate this step; compaction is a blocking
operation that requires no concurrent readers or writers.
