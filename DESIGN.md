# Eter: Immutable Persistent Graph Store Protocol

Eter is a protocol for versioned graph storage with immutable snapshots.
The interface is defined as Rust traits, backend-agnostic by design, with
implementations ranging from filesystem-backed stores to concurrent database
engines. `Eter` stands for eternity, reflecting the protocol's core principle of
immutable historical states.

The basic idea is that the user does not mutate a database in place. Instead,
the user works with stable "pointers" called `Eterator`s to immutable snapshots
of the whole graph, while new operations append new history. Old states remain
available for as long as the user chooses to keep them. User may view old states
or linearly revert to one of them and drop all the other updates that comes
after. For undo-tree like branching, the user can also choose external
approaches like git or database snapshots.

However, how the `Eterator` is implemented can cause significant design
fractions and runtime correctness / performance implications. The desired
features of `Eterator` and the corresponding data model include:

- Efficient history management for changes within a single node.
- Efficient retrieval of the whole graph at a given snapshot.
- Edge updates should be localized to the source node, without needing to update
  the target node or any global index.
- The user should be able to choose which snapshots to keep and which to retire,
  allowing for efficient storage management.

And below are several initial design options that have different trade-offs:

- Nodes are immutable, and each update creates a new node with a new `NodeId`.
  This is simple but can lead to a large number of node allocation, even if the
  changes are as small as adding a single edge.
- Nodes are locally versioned, and the `NodeId`s are stable across versions.
  This enables structural stability (e.g. edge updates only need to update the
  source node), but requires a more complex global state management to track the
  graph at a specific `Eterator`, i.e. something similar to a vector clock.
- Nodes are globally versioned with stable `NodeId`s. `Eterator` is implemented
  simply as a global version number, and each node field is versioned with the
  same version number. This is simple and efficient for both updates and
  retrieval; the only caveat is the version number may run out much faster than
  other strategies.

Global versioning is the chosen strategy. Every write operation assigns a new
version to every field row it produces. The current version is not stored as
a separate counter; it is derived as the maximum version across all field
rows in the store. A single operation may touch multiple nodes and fields;
all rows written in the same operation share the same version, providing
atomic-snapshot semantics. An `Eterator` is therefore a single integer
representing the version at the time the snapshot was created.

A 64-bit version space yields ~1.8 × 10^19 versions. At one billion writes
per second, exhaustion takes roughly 584 years.

Cross-node derived data such as the current maximum version, live-node sets,
and reverse-edge indices may be cached in memory or auxiliary tables. No cache
is authoritative; all are rebuildable from the field tables on startup and may
be freely invalidated between server launches.

## Core Concepts

`Eterator` is a snapshot handle: concretely, a global version number.
The user receives a fresh `Eterator` after each write and may hold any number
of them simultaneously. Each grants read access to the graph as it existed at
that version.

Nodes are the basic units of the graph. Each node has a fixed,
compile-time-defined set of fields, each backed by its own table. Every field
row is keyed by `(NodeId, version)`. A row holds either content or a deletion
marker. Only fields that change receive new rows on a write; unchanged fields
are inherited from the nearest earlier version. Versioning is per-field, not
per-node. A write touching one field produces no new rows for unaffected
fields. Resolution cost is one seek per field; storage cost is proportional to
the number of changed fields per write.

`NodeId` is the unique identifier for a node. The concrete type is chosen by
the user (e.g. UUIDv7, slug, integer). The only requirement is uniqueness
within the store, verifiable through the `Eter` interface before insertion.

Edges are a regular field in the source node, stored as a set of target
`NodeId`s. No separate edge entity or global edge index exists. Edges follow
the same versioning and resolution rules as any other field.

Nodes are self-contained. All data needed to render a node at a given version
lives within the node's own field rows. An edge referencing a `NodeId` that
does not exist or has been deleted is a dangling reference, not an error. The
store surfaces dangling references as warnings but still produces a complete,
viewable result for the source node. No foreign-key constraint is enforced.
This makes the system resilient to partial data, out-of-order ingestion, and
concurrent modifications: a node can always be read and displayed regardless
of the state of its neighbors.

`Eter` is the store itself. Its only persistent global state is a set of
retired versions. Every version not in the retired set is considered live and
must be preserved. The user adds versions to the retired set explicitly, or
uses an "only-keep" operation that retires everything except a given set of
versions.

This design favors safe failure: if the retired set fails to persist, the
consequence is wasted space (versions that could be collected survive), never
data loss. Tracking pinned versions instead and treating everything else as
retired risks destroying live data on a failed write.

Alternatively, the store may hold no global state at all. The user provides
an explicit set of live versions to each garbage-collection call, and the
store treats everything else as retired for that invocation. This maximizes
flexibility and eliminates persistent state, at the cost of placing version
bookkeeping entirely in the user's hands. The two modes are compatible: the
retired set is a convenience layer atop the stateless GC primitive.

## Resolution

Reading field `F` of node `N` at `Eterator(V)`:

1. Seek to the row in `F`'s table with key `(N, v)` where `v` is the largest
   version ≤ `V`.
2. If the row contains content, return it.
3. If the row is a deletion marker, the field is absent at this snapshot.
4. If no row exists, the field has never been written for this node.

This is a single backward seek in a sorted key-value store, O(log k) where
k is the number of versions for the `(N, F)` pair. Backends may additionally
cache per-`Eterator` resolution maps for hot-path queries.

## Node Lifecycle

A built-in `lifecycle` field tracks node existence and state. In storage it
behaves like any other field: keyed by `(NodeId, version)`, holding either
content or a deletion marker. The protocol checks this field to determine
whether a node is present: if it resolves to content, the node exists; if it
resolves to a deletion marker or has never been written, the node is absent.
Other fields' state does not affect this determination.

The value stored in `lifecycle` when present is user-defined. A minimal
application uses a two-state enum (active, removed). Richer applications can
encode additional states like archived, draft, or deprecated that carry meaning
at the application layer without affecting protocol-level resolution. The
protocol only distinguishes "has content" from "absent."

A deleted `NodeId` may be reused: writing a new content row to `lifecycle`
at a later version re-creates the node.


## Garbage Collection

Garbage collection is driven by the retired-version set (or, in stateless
mode, by the complement of the live set supplied to a GC call). A field row
at version `v` is collectible when every `Eterator` that would resolve to it
has been retired, meaning a later row at version `v'` exists such that no
live version falls in `(v, v')`. Concretely, given two consecutive live versions
`V_a < V_b` and rows at `v1 < v2 < v3` all within `(V_a, V_b]`, rows `v1`
and `v2` are redundant: any live `Eterator` in that interval resolves to
`v3`. They can be freed.

Garbage collection never alters the result of a read through any live version.

## Optional Caches

The field tables are the single source of truth. All other data structures
are derived caches that may be dropped and rebuilt at any time.

- **Current version.** The maximum version across all rows. Avoids a full
  scan on every write by caching and incrementing a single value.
- **Live-node set.** The set of `NodeId`s whose `lifecycle` field resolves to
  content at a given version. Without this cache, enumerating live nodes
  requires scanning the full `NodeId` space.
- **Reverse-edge index.** Maps target `NodeId` to the set of source `NodeId`s
  that reference it. Enables ingress-edge queries without a full scan.
- **Per-`Eterator` resolution map.** Precomputed `(NodeId, field) → version`
  mappings for frequently accessed snapshots.

Backends decide which caches to maintain. By default all caches are invalidated
on startup, but a backend may preserve them across restarts on a best-effort
basis, for example by storing a checksum of the underlying field tables and
skipping rebuild when the checksum matches. The protocol defines optional traits
for backends that support specific caches but leaves the persistence and
invalidation strategy to each backend.

## Concurrency

The monotonic version sequence serializes writes into a total order.
Strategies:

- **Single-writer.** One writer holds exclusive access; readers use their
  `Eterator` snapshots without coordination. Sufficient when write throughput
  is not a bottleneck.
- **Compare-and-swap.** Writers prepare a batch optimistically, then CAS the
  cached current version. On conflict, retry.
- **Batched writes.** Multiple field mutations share a single version,
  conserving version space.

The protocol defines the logical model. Concurrency control is a backend
concern.

## Backend Considerations

The storage model relies on ordered `(NodeId, field, version)` keys, prefix
scans, and backward seeks. These requirements point toward sorted key-value
stores.

- **Filesystem**: Each node is a directory, each field a file, versions
  encoded in filenames or appended entries. No concurrency support; suitable
  for single-user, human-readable scenarios.
- **LMDB** (via `heed`): B-tree, MVCC, memory-mapped. Single-writer by
  design, matching the simplest concurrency model. Lock-free read transactions.
  Requires `NodeId` to produce fixed-size, sort-preserving bytes;
  see the [LMDB Backend](#lmdb-backend) section.
- **redb**: Pure-Rust B-tree store. Simpler dependency graph. Similar access
  patterns.

The protocol is backend-agnostic: it defines traits that any conforming
backend implements.

## Filesystem Backend

The filesystem backend stores nodes as markdown files. It targets
single-user, human-readable scenarios where the store doubles as a
browsable document tree. No concurrency support.

### Layout

The user provides a root directory for the store.
It must be empty on first use or contain a valid prior store state.

```
<root>/
  <node_id>/
    <version>-<node_id>.md
    ...
  ...
```

Each node occupies a subdirectory named by its `NodeId`, which must be
filesystem-friendly: no path separators, no `.` or `..`, no null bytes,
and reasonable length. Inside are markdown files, one per version.

The filename is `<version>-<node_id>.md` where `<version>` is the 64-bit
version number zero-padded to 16 hexadecimal digits. Zero-padding ensures
lexicographic filename order matches version order. The `<node_id>` suffix
is redundant with the directory name but aids readability in editors and
tools that display only the filename.

The backend has no persistent global state on disk. It does not record a
retired-version set. Retired and live versions must be tracked by the user
and provided to garbage collection calls. Only derived caches are held in
memory and rebuilt from the file tree on startup.

### File Format

Each version file uses JSON frontmatter delimited by `---`, followed by a
markdown body:

```md
---
{
  "lifecycle": "active",
  "edges": ["target_a", "target_b"]
}
---

Body text in markdown.
```

The JSON object holds all structured fields: `lifecycle`, `edges`, and any
user-defined fields registered with the backend. Registration is static:
user-defined `Field` types are fixed at compile time and mapped to keys when
the backend is constructed. A key set to `null` represents a deletion
marker (`FieldRow::Deleted`) for that field. An absent key means the field
is unchanged from the previous version and should be inherited during
resolution.

Per-version metadata is complete across pathname and frontmatter:

- Path metadata: `NodeId` from `<root>/<node_id>/`.
- Filename metadata: `version` from `<version>-<node_id>.md`.
- Frontmatter metadata: all protocol fields (`lifecycle`, `edges`, and
  registered user fields), with `null` encoding field deletion markers.

No additional hidden metadata exists for this backend.

The markdown text after the closing delimiter is the node's body, a
privileged content field specific to this backend. It has no representation
in the JSON header.

### Protocol Mapping

All fields for a given `(NodeId, version)` are co-located in a single file.
This is per-node storage: every write creates a new file containing all
fields, copying unchanged values from the previous version. The trade-off
is more storage on partial updates in exchange for simpler resolution,
atomic per-node snapshots, and human-readable files.

**resolve.** Scan filenames in `<root>/<node_id>/`, find the file with the
largest hex version ≤ the queried `Eterator`, parse the JSON header, and
return the requested field. For the body field, return the markdown text.

**write.** Assign the next version (one greater than the current maximum).
Create a new file in `<root>/<node_id>/` with the updated fields and all
unchanged fields copied from the previous version.

**current_version.** The maximum hex version across all filenames in the
root. Cached in memory after the initial scan and incremented on each
write.

**field_history.** List all files in `<root>/<node_id>/` in version order
and parse the requested field from each.

**gc.** Delete version files whose versions are retired and superseded by
a later version for all live `Eterator`s. The backend does not persist the
retired set; callers must provide live or retired versions explicitly for
each collection run.


## LMDB Backend

The LMDB backend targets durable, transactional storage under single-writer
access. It uses the `heed` crate for LMDB bindings. Read transactions are
lock-free; at most one write transaction may be open at a time.

### NodeId Constraint

The `NodeId` type used with this backend must implement `LmdbKey`, a
backend-local trait with two properties. `to_key_bytes` returns the byte
representation of the id. `KEY_LEN` is a compile-time constant declaring the
exact byte length; every value of the type must produce exactly that many
bytes. The byte representation must be order-preserving: `a < b` under the
type's `Ord` impl must imply `to_key_bytes(a) < to_key_bytes(b)`
lexicographically. UUIDs (16 bytes, big-endian), integer ids (4 or 8 bytes,
big-endian), and fixed-width padded slugs satisfy this constraint.
Variable-length encodings are not permitted.

The fixed-size requirement eliminates composite-key ambiguity: because the
`NodeId` portion of every key occupies exactly `KEY_LEN` bytes, the version
boundary is at a known offset and no separator or length prefix is needed.

### Layout

The backend opens a single LMDB environment. Opening the same environment
path from two processes simultaneously is unsupported; LMDB enforces
single-environment access via file locking.

The environment contains:

- One named database per registered `Field` type, identified by the field's
  static name string.
- `_versions`: the version registry, recording every version ever committed.
- `_retired`: the persistent retired-version set.

The names `_versions` and `_retired` are reserved. A registered `Field` whose
static name matches either will collide with a backend database at construction
time; the backend rejects this at construction with a panic.

The total database count is `|registered fields| + 2`. The backend derives
`max_dbs` from the registered field list at construction time; the caller does
not set it manually.

### Key Encoding

Within each per-field database, rows are keyed by a fixed-size composite key:
the `LmdbKey` bytes of the `NodeId` (exactly `KEY_LEN` bytes) followed by the
8-byte big-endian encoding of the version number. Big-endian encoding places
lower versions before higher versions lexicographically, which is required for
the backward-seek resolution algorithm. Because the `NodeId` prefix is
fixed-length, the split between the two parts is unambiguous.

### Value Encoding

Each row value uses a one-byte tag prefix followed by optional content.

- `0x00`: deletion marker (`FieldRow::Deleted`). No further bytes.
- `0x01, ...json`: content (`FieldRow::Content`), where the remaining bytes
  are the `serde_json`-serialized field value.

The tag distinguishes deletion markers from content without relying on absent
keys, which carry a separate meaning (the field has never been written for
this node at or before the queried version).

### Write Transaction

The write transaction (`WriteTxn`) accumulates field rows in memory and opens
a single LMDB write transaction only at commit time. The alternative—holding
the LMDB write transaction open from the moment `WriteTxn` is created—would
block all GC passes and any operation that requires a write lock for the
duration of accumulation. Buffering in memory and committing atomically avoids
this hazard. On commit, all buffered field rows and a `_versions` entry are
written in one atomic step.

### Resolution

Reading field `F` of node `N` at `Eterator(V)`:

1. Open a short-lived read transaction.
2. Seek to the first key ≥ `N || V` using a lower-bound cursor seek.
3. If the key equals `N || V` exactly, the row at version `V` exists; return
   it directly.
4. Otherwise step the cursor back one position.
5. If the resulting key begins with the bytes of `N`, decode the version and
   value from the key and the stored data. The resolution is complete.
6. If the cursor is before the start of the database or the key prefix does
   not equal the bytes of `N`, the field is absent for this node.
7. Close the read transaction.

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

### Eterators and Read Transactions

`Eterator` holds no LMDB resource; it is a plain version number. Each call to
`resolve` opens a read transaction, executes the seek described above, and
closes the transaction before returning. No read transaction persists beyond a
single call.

This model avoids two LMDB hazards. First, LMDB's reader table has a finite
number of slots (configurable, defaulting to 126). Keeping one slot open per
live `Eterator` would exhaust this table for workloads with many concurrent
snapshots. Second, long-lived read transactions pin LMDB's freelist: pages
freed by garbage collection cannot be recycled while a reader that predates
the deletion is open, causing the database file to grow without bound. The
per-call transaction model eliminates both problems.

The trade-off is that a sequence of `resolve` calls at the same `Eterator`
is not protected by a single LMDB snapshot. Under single-writer access, a
write cannot interleave with an in-progress logical read operation, so this is
safe in practice. Applications that require strict multi-field snapshot
consistency may use the backend's `read_txn` method to open an explicit
`heed::RoTxn` and pass it to `resolve_in`, a backend-specific counterpart to
`Eter::resolve` that accepts a borrowed transaction. The caller is responsible
for closing that transaction promptly; holding it open reintroduces the
reader-table and freelist hazards described above. The per-`Eterator`
resolution cache described in the Optional Caches section is an alternative
that avoids open transactions entirely.

### Configuration

The backend constructor accepts two caller-supplied parameters.

`map_size` sets the maximum size of the LMDB memory map in bytes. LMDB
requires this value at environment open time and cannot grow the map
automatically. If accumulated data exceeds `map_size`, subsequent writes
return an error. Resizing requires calling `env.resize` with no active transactions of any
kind (read or write); the caller is responsible for choosing a `map_size`
large enough for the expected working set and for initiating resizes when
needed. A safe default for small stores is 1 GiB; production deployments
should size this according to data volume projections.

The registered field list is the second parameter. The backend enumerates it
at construction to open or create each named database and to derive `max_dbs`.
Every `Field` type that will be passed to `resolve` or `WriteTxn::set` must
appear in this list; an unregistered field type panics at call time.

No other persistent global configuration exists on disk.

### Version Registry

The `_versions` database maps each committed version number (8-byte
big-endian key) to an empty value. On each `WriteTxn::commit`, the new
version is inserted into `_versions` within the same write transaction.
`current_version` is a single backward cursor seek to the last key in
`_versions`, executing in O(log n). `live_versions` scans `_versions` and
subtracts `_retired`, both O(versions). `retired_versions` scans `_retired`,
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
tables to the number of live versions, not the total number of versions ever
written.

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

