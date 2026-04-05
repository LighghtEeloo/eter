//! Eter: Immutable Persistent Append-Only KV Graph Store.
//!
//! This crate defines the protocol-level traits for Eter, a graph-shaped
//! data store with explicit historical persistence. Backends implement
//! [`Eter`] to provide concrete storage.
//!
//! See `DESIGN.md` for the full design rationale.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::Hash;

/// Global version number identifying an immutable snapshot of the graph.
///
/// Each write produces a new `Eterator` strictly larger than any existing
/// one. The version is derived as the maximum across all field rows in
/// the store, not stored as a separate value.
///
/// Only the store produces meaningful `Eterator` values. The inner field
/// is public for serialization convenience, but constructing arbitrary
/// values has no defined behavior unless the version exists in the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Eterator(pub u64);

impl Eterator {
    /// Sentinel for an empty store before any write.
    pub const EMPTY: Self = Self(0);

    /// The raw version number.
    pub fn version(self) -> u64 {
        self.0
    }
}

/// Result of resolving a field at a given [`Eterator`].
///
/// Three outcomes per the resolution algorithm:
/// - [`Content`](Resolution::Content): the row with the largest version
///   ≤ the queried `Eterator` holds a value.
/// - [`Deleted`](Resolution::Deleted): that row is a deletion marker.
/// - [`Absent`](Resolution::Absent): no row exists for this
///   `(NodeId, field)` pair at or before the queried version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution<T> {
    /// The field holds content at this snapshot.
    Content(T),
    /// The field was explicitly deleted at or before this snapshot.
    Deleted,
    /// No row has ever been written for this `(NodeId, field)` pair.
    Absent,
}

impl<T> Resolution<T> {
    /// Extracts the content, discarding the deleted/absent distinction.
    pub fn into_option(self) -> Option<T> {
        match self {
            | Self::Content(v) => Some(v),
            | Self::Deleted | Self::Absent => None,
        }
    }

    /// Returns `true` if the resolution holds content.
    pub fn is_content(&self) -> bool {
        matches!(self, Self::Content(_))
    }

    /// Returns `true` if the field has no content (deleted or never written).
    pub fn is_absent(&self) -> bool {
        matches!(self, Self::Deleted | Self::Absent)
    }

    /// Applies `f` to the contained content, preserving deleted/absent.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Resolution<U> {
        match self {
            | Self::Content(v) => Resolution::Content(f(v)),
            | Self::Deleted => Resolution::Deleted,
            | Self::Absent => Resolution::Absent,
        }
    }
}

/// Marker trait binding a field identity to its content type.
///
/// Each field in a node schema is a distinct zero-sized type implementing
/// `Field`. The store maintains a separate logical table per implementor,
/// keyed by `(NodeId, version)`.
///
/// # Panics
///
/// Calling [`Eter::resolve`] or [`WriteTxn::set`] with a `Field` type
/// that the backend does not support will panic.
pub trait Field: 'static {
    /// The content type stored in rows of this field's table.
    type Content;
}

/// Built-in field tracking node existence and lifecycle state.
///
/// The protocol checks this field to determine node presence:
/// [`Resolution::Content`] means the node exists; any other resolution
/// means it does not. The content type `L` is user-defined (e.g. a
/// two-state active/removed enum, or richer states like archived or
/// draft). The protocol only inspects presence, not the value.
pub struct Lifecycle<L>(std::marker::PhantomData<L>);

impl<L: 'static> Field for Lifecycle<L> {
    type Content = L;
}

/// Built-in egress-edge field.
///
/// Stored as a sorted set of target node identifiers. Follows the same
/// versioning and resolution rules as any other field.
pub struct Edges<Id>(std::marker::PhantomData<Id>);

impl<Id: Ord + 'static> Field for Edges<Id> {
    type Content = BTreeSet<Id>;
}

/// Write transaction accumulating field updates for a single version.
///
/// All rows produced by one transaction share the same version number.
/// Setters consume and return `self` for chaining; [`WriteTxn::commit`]
/// finalizes the transaction and produces the new [`Eterator`].
///
/// ```ignore
/// store.write()
///     .set::<Lifecycle<S>>(&id, State::Active)
///     .set::<Edges<Id>>(&id, edges)
///     .commit()?;
/// ```
pub trait WriteTxn: Sized {
    /// The node identifier type.
    type NodeId;
    /// Error type for the commit operation.
    type Error;

    /// Set a field's content for a node.
    fn set<F: Field>(self, node: &Self::NodeId, content: F::Content) -> Self;

    /// Write a deletion marker for a field on a node.
    fn delete<F: Field>(self, node: &Self::NodeId) -> Self;

    /// Commit all accumulated writes, producing a new snapshot.
    fn commit(self) -> Result<Eterator, Self::Error>;
}

/// The store.
///
/// Provides snapshot reads via [`Eterator`] handles, writes via
/// [`WriteTxn`] transactions, and version management (retirement,
/// garbage collection).
///
/// The only persistent global state is the set of retired versions.
/// All other cross-node data (current version, live-node sets,
/// reverse-edge indices) is derived and cacheable.
pub trait Eter {
    /// Node identifier type, chosen by the user.
    /// Must be unique within the store.
    type NodeId: Eq + Hash + Clone + Ord + Debug;

    /// User-defined lifecycle state stored in the [`Lifecycle`] field.
    type Lifecycle: Clone + Debug + 'static;

    /// Error type for fallible store operations.
    type Error;

    /// The write transaction type returned by [`Eter::write`].
    type WriteTxn<'a>: WriteTxn<NodeId = Self::NodeId, Error = Self::Error>
    where
        Self: 'a;

    // -- Reads --

    /// Resolve a field for a node at a given snapshot.
    ///
    /// Returns the row with the largest version ≤ `at` in the field's
    /// logical table for the given node.
    fn resolve<F: Field>(
        &self, at: Eterator, node: &Self::NodeId,
    ) -> Result<Resolution<F::Content>, Self::Error>;

    /// Check whether a node exists at a given snapshot.
    ///
    /// Equivalent to checking whether the [`Lifecycle`] field resolves
    /// to [`Resolution::Content`] at `at`.
    fn node_exists(&self, at: Eterator, node: &Self::NodeId) -> Result<bool, Self::Error>;

    /// The current version, derived as the maximum version across all
    /// field rows. Returns [`Eterator::EMPTY`] for an empty store.
    /// May be served from cache.
    fn current_version(&self) -> Result<Eterator, Self::Error>;

    /// Check whether a `NodeId` has ever been used in the store.
    ///
    /// Returns `true` if any field row exists for this id at any
    /// version, including nodes that have since been deleted. Use this
    /// to verify uniqueness before inserting a new node.
    fn node_id_in_use(&self, id: &Self::NodeId) -> Result<bool, Self::Error>;

    // -- Writes --

    /// Begin a write transaction.
    ///
    /// The returned [`WriteTxn`] accumulates field updates. Calling
    /// [`WriteTxn::commit`] assigns a new version to all accumulated
    /// rows and returns the corresponding [`Eterator`].
    #[must_use = "a write transaction does nothing until committed"]
    fn write(&mut self) -> Self::WriteTxn<'_>;

    // -- Version management --

    /// Add versions to the retired set, making their exclusive rows
    /// candidates for garbage collection.
    ///
    /// Safe failure: if this write does not persist, the only consequence
    /// is that rows remain uncollected.
    fn retire(&mut self, versions: impl IntoIterator<Item = Eterator>) -> Result<(), Self::Error>;

    /// Retire all versions except the given set.
    fn only_keep(
        &mut self, versions: impl IntoIterator<Item = Eterator>,
    ) -> Result<(), Self::Error>;

    /// Run garbage collection using the persistent retired set.
    ///
    /// Frees field rows unreachable from any live (non-retired) version.
    /// Never alters the result of a read through a live version.
    fn gc(&mut self) -> Result<(), Self::Error>;

    /// Run garbage collection with an explicit set of live versions.
    ///
    /// Stateless mode: treats everything not in `live` as retired for
    /// this invocation only. Does not modify the persistent retired set.
    fn gc_with_live(&mut self, live: impl IntoIterator<Item = Eterator>)
    -> Result<(), Self::Error>;
}
