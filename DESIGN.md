# Eter: Immutable Persistent Append-Only KV Graph Store

Eter is a protocol for building infrastructural graph-shaped text data storage
with explicit historical persistence. The protocol interface is defined in Rust,
with multiple backends for various scenarios from direct markdown file lookup on
host file system to concurrent database access.

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

- **LMDB** (via `heed`): B-tree, MVCC, memory-mapped. Single-writer by
  design, matching the simplest concurrency model. Lock-free read transactions.
- **redb**: Pure-Rust B-tree store. Simpler dependency graph. Similar access
  patterns.
- **Filesystem.** Each node is a directory, each field a file, versions
  encoded in filenames or appended entries. No concurrency support; suitable
  for single-user, human-readable scenarios.

The protocol is backend-agnostic: it defines traits that any conforming
backend implements.

