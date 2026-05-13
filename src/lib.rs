//! Eter: immutable persistent entry store protocol.
//!
//! This crate defines the protocol-level traits for Eter, a versioned
//! entry store with immutable snapshots. Backends implement [`Eter`] to
//! provide concrete storage.
//!
//! Applications may project snapshots into working folders, documents, or
//! other editable forms. Those projections parse and render application syntax;
//! the core protocol stores typed field updates.
//!
//! See `DESIGN.md` for the full design rationale.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub mod filesystem;
#[cfg(feature = "lmdb")]
pub mod lmdb;

/// Global version number identifying an immutable snapshot of the entry store.
///
/// Each non-empty write produces a version greater than any version previously
/// allocated by that backend. Backends keep an allocation high-water mark so
/// collected version numbers are not reused.
///
/// Only the store produces meaningful `Eterator` values. The inner field
/// is public for serialization convenience, but constructing arbitrary
/// values has no defined behavior unless the version is live in the store.
///
/// Note: a version that has been retired and collected is no longer a valid
/// snapshot handle. Backends should reject reads through collected snapshots
/// rather than resolving them against a different retained version.
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
///   `(EntryId, field)` pair at or before the queried version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution<T> {
    /// The field holds content at this snapshot.
    Content(T),
    /// The field was explicitly deleted at or before this snapshot.
    Deleted,
    /// No row has ever been written for this `(EntryId, field)` pair.
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

/// A stored field row: either content or a deletion marker.
///
/// This is the write-side and storage-side representation. Unlike
/// [`Resolution`], there is no `Absent` variant; absence is a query-time
/// concept indicating no row was found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldRow<T> {
    /// The row holds content.
    Content(T),
    /// The row is a deletion marker.
    Deleted,
}

impl<T> FieldRow<T> {
    /// Returns `true` if the row holds content.
    pub fn is_content(&self) -> bool {
        matches!(self, Self::Content(_))
    }

    /// Applies `f` to the contained content, preserving deletion markers.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> FieldRow<U> {
        match self {
            | Self::Content(v) => FieldRow::Content(f(v)),
            | Self::Deleted => FieldRow::Deleted,
        }
    }
}

impl<T> From<FieldRow<T>> for Resolution<T> {
    fn from(row: FieldRow<T>) -> Self {
        match row {
            | FieldRow::Content(v) => Resolution::Content(v),
            | FieldRow::Deleted => Resolution::Deleted,
        }
    }
}

/// A versioned field row: the [`Eterator`] at which the row was written
/// paired with its [`FieldRow`] content.
pub type VersionedRow<T> = (Eterator, FieldRow<T>);

/// Garbage-collection mode selection.
///
/// This unifies retired-set and stateless GC into one entrypoint.
/// Backends choose the live-version set according to this option, then
/// collect rows that are unreachable from that live set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GcOption {
    /// Use the backend's retired-version set.
    ///
    /// Live versions are computed as "all store versions not retired."
    UseRetiredSet,
    /// Use an explicit live-version set for this call only.
    ///
    /// Does not modify the backend's retired-version set.
    UseLiveSet(BTreeSet<Eterator>),
}

/// Marker trait binding a field identity to its content type.
///
/// Each field in an entry facet is a distinct zero-sized type implementing
/// `Field`. The store maintains a separate logical table per implementor,
/// keyed by `(EntryId, version)`.
///
/// # Panics
///
/// Calling [`Eter::resolve`] or [`WriteTxn::set`] with a `Field` type
/// that the backend does not support will panic.
pub trait Field: 'static {
    /// The content type stored in rows of this field's table.
    type Content: Clone + Debug + Serialize + DeserializeOwned + 'static;
}

/// Built-in field tracking entry existence and lifecycle state.
///
/// The protocol checks this field to determine entry presence:
/// [`Resolution::Content`] means the entry exists; any other resolution
/// means it does not. The content type `L` is user-defined (e.g. a
/// unit-like active marker, or richer states like archived or draft). The
/// protocol only inspects presence, not the value.
pub struct Lifecycle<L>(std::marker::PhantomData<L>);

impl<L> Field for Lifecycle<L>
where
    L: Clone + Debug + Serialize + DeserializeOwned + 'static,
{
    type Content = L;
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
///     .commit()?;
/// ```
pub trait WriteTxn: Sized {
    /// The entry identifier type.
    type EntryId;
    /// Error type for the commit operation.
    type Error;

    /// Write a [`FieldRow`] for a field on an entry.
    ///
    /// This is the primitive write operation. [`WriteTxn::set`] and
    /// [`WriteTxn::delete`] are convenience wrappers.
    fn apply<F: Field>(self, entry: &Self::EntryId, row: FieldRow<F::Content>) -> Self;

    /// Set a field's content for an entry.
    fn set<F: Field>(self, entry: &Self::EntryId, content: F::Content) -> Self {
        self.apply::<F>(entry, FieldRow::Content(content))
    }

    /// Write a deletion marker for a field on an entry.
    fn delete<F: Field>(self, entry: &Self::EntryId) -> Self {
        self.apply::<F>(entry, FieldRow::Deleted)
    }

    /// Commit all accumulated writes, producing a new snapshot.
    fn commit(self) -> Result<Eterator, Self::Error>;
}

/// The store.
///
/// Provides snapshot reads via [`Eterator`] handles, writes via
/// [`WriteTxn`] transactions, and version management (retirement,
/// garbage collection).
///
/// Entry content is defined by retained field history. Cross-entry data such
/// as current version indices, version registries, allocation high-water marks,
/// and live-entry sets is derived or auxiliary. Backends decide which derived
/// indices are persisted.
pub trait Eter {
    /// Entry identifier type, chosen by the user.
    /// Must be unique within the store.
    type EntryId: Eq + Hash + Clone + Ord + Debug;

    /// User-defined lifecycle state stored in the [`Lifecycle`] field.
    type Lifecycle: Clone + Debug + Serialize + DeserializeOwned + 'static;

    /// Error type for fallible store operations.
    type Error;

    /// The write transaction type returned by [`Eter::write`].
    type WriteTxn<'a>: WriteTxn<EntryId = Self::EntryId, Error = Self::Error>
    where
        Self: 'a;

    // -- Reads --

    /// Resolve a field for an entry at a given snapshot.
    ///
    /// Returns the row with the largest version ≤ `at` in the field's
    /// logical table for the given entry.
    ///
    /// Backends reject non-empty snapshot handles that are no longer retained.
    fn resolve<F: Field>(
        &self, at: Eterator, entry: &Self::EntryId,
    ) -> Result<Resolution<F::Content>, Self::Error>;

    /// Check whether an entry exists at a given snapshot.
    ///
    /// Equivalent to checking whether the [`Lifecycle`] field resolves
    /// to [`Resolution::Content`] at `at`.
    fn entry_exists(&self, at: Eterator, entry: &Self::EntryId) -> Result<bool, Self::Error>;

    /// The current retained version.
    ///
    /// Returns [`Eterator::EMPTY`] for an empty store. May be served from
    /// cache or from a backend-specific derived index.
    ///
    /// Note: this value may move backward after garbage collection removes the
    /// newest retained versions. Future writes still use a backend high-water
    /// mark and do not reuse collected version numbers.
    fn current_version(&self) -> Result<Eterator, Self::Error>;

    /// All rows ever written for a field on an entry, in version order.
    ///
    /// Returns `(Eterator, FieldRow)` pairs spanning the full history
    /// of this `(EntryId, field)`. Useful for auditing, diffing, and
    /// building undo interfaces.
    fn field_history<F: Field>(
        &self, entry: &Self::EntryId,
    ) -> Result<Vec<VersionedRow<F::Content>>, Self::Error>;

    /// Check whether an `EntryId` has ever been used in the store.
    ///
    /// Returns `true` if any field row exists for this id at any
    /// version, including entries that have since been deleted. Use this
    /// to verify freshness before inserting a new entry. Reactivating a
    /// deleted entry intentionally uses an id that is already in use.
    fn entry_id_in_use(&self, id: &Self::EntryId) -> Result<bool, Self::Error>;

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
    /// Every supplied version must be retained by the store. The empty
    /// sentinel is not a retained version.
    ///
    /// Safe failure: if this write does not persist, the only consequence
    /// is that rows remain uncollected.
    fn retire(&mut self, versions: impl IntoIterator<Item = Eterator>) -> Result<(), Self::Error>;

    /// Retire all versions except the given retained set.
    ///
    /// Every supplied version must be retained by the store. The empty
    /// sentinel is not a retained version.
    fn only_keep(
        &mut self, versions: impl IntoIterator<Item = Eterator>,
    ) -> Result<(), Self::Error>;

    /// Run garbage collection with an explicit mode.
    ///
    /// - [`GcOption::UseRetiredSet`] uses the backend's retired-version state.
    /// - [`GcOption::UseLiveSet`] uses a caller-provided live set for
    ///   this invocation only.
    ///
    /// In both modes, garbage collection frees rows unreachable from the
    /// selected live-version set and never alters reads through those live
    /// versions. In [`GcOption::UseLiveSet`] mode, every supplied version must
    /// be retained by the store.
    fn gc(&mut self, option: GcOption) -> Result<(), Self::Error>;

    /// The current retired-version set.
    ///
    /// Every version in this set is a candidate for garbage collection.
    /// Versions not in this set are considered live and must be preserved.
    fn retired_versions(&self) -> Result<BTreeSet<Eterator>, Self::Error>;

    /// All live (non-retired) versions in the store.
    ///
    /// These are the versions for which reads are guaranteed to be
    /// stable. Useful for deciding which versions to pass to
    /// [`Eter::only_keep`] or [`Eter::retire`].
    fn live_versions(&self) -> Result<BTreeSet<Eterator>, Self::Error>;
}

/// Optional trait for backends that can enumerate live entries.
///
/// Backends may implement this with a cache, an index, or a direct scan over
/// the lifecycle field. Without such support, callers must enumerate candidate
/// [`Eter::EntryId`] values and check each entry's [`Lifecycle`] field.
pub trait LiveEntries: Eter {
    /// All entry identifiers whose [`Lifecycle`] field resolves to content at `at`.
    fn live_entries(&self, at: Eterator) -> Result<BTreeSet<Self::EntryId>, Self::Error>;
}

/// Typed application-level facet of one entry for one store type.
///
/// A facet is a Rust type isomorphic to a coherent subset of an entry's fields.
/// It may represent a complete lifecycle-bearing entry or a smaller view such
/// as title metadata, scheduling fields, or application-specific state.
///
/// The facet owns required-field checks, defaulting rules, presence rules, and
/// domain invariants for its field subset. Folder parsing and rendering still
/// belong outside the core protocol.
pub trait EntryFacet<S: Eter>: Sized {
    /// Load this facet from `store` at `at` for entry `id`.
    ///
    /// Returns `Ok(None)` when the facet's presence rule is not satisfied. A
    /// full-entry facet normally derives presence from [`Lifecycle`]; a partial
    /// facet may use one required field, any required field set, or defaults.
    /// Implementations should panic for facet-internal invariant violations.
    fn load_from(store: &S, at: Eterator, id: &S::EntryId) -> Result<Option<Self>, S::Error>;

    /// Apply this facet's field subset to `txn` for entry `id`.
    ///
    /// Implementations should write every field owned by the facet and leave
    /// unrelated fields untouched.
    fn apply_to<'a>(&self, txn: S::WriteTxn<'a>, id: &S::EntryId) -> S::WriteTxn<'a>
    where
        S: 'a;
}

/// Convenience methods for stores that use [`EntryFacet`] records.
pub trait EntryFacetStoreExt: Eter {
    /// Load a typed entry facet at `at`.
    fn load_facet<F: EntryFacet<Self>>(
        &self, at: Eterator, id: &Self::EntryId,
    ) -> Result<Option<F>, Self::Error>
    where
        Self: Sized,
    {
        F::load_from(self, at, id)
    }

    /// Commit a typed entry facet as one write transaction.
    fn write_facet<F: EntryFacet<Self>>(
        &mut self, id: &Self::EntryId, facet: &F,
    ) -> Result<Eterator, Self::Error>
    where
        Self: Sized,
    {
        facet.apply_to(self.write(), id).commit()
    }
}

impl<T: Eter> EntryFacetStoreExt for T {}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Eterator --

    #[test]
    fn eterator_empty_has_version_zero() {
        assert_eq!(Eterator::EMPTY.version(), 0);
    }

    #[test]
    fn eterator_ordering_follows_version_number() {
        let a = Eterator(1);
        let b = Eterator(2);
        assert!(a < b);
        assert_eq!(a, a);
    }

    // -- Resolution --

    #[test]
    fn resolution_into_option_content() {
        assert_eq!(Resolution::Content(42_u32).into_option(), Some(42));
    }

    #[test]
    fn resolution_into_option_deleted_is_none() {
        assert_eq!(Resolution::<u32>::Deleted.into_option(), None);
    }

    #[test]
    fn resolution_into_option_absent_is_none() {
        assert_eq!(Resolution::<u32>::Absent.into_option(), None);
    }

    #[test]
    fn resolution_is_content_only_for_content_variant() {
        assert!(Resolution::Content(1_u8).is_content());
        assert!(!Resolution::<u8>::Deleted.is_content());
        assert!(!Resolution::<u8>::Absent.is_content());
    }

    #[test]
    fn resolution_is_absent_for_deleted_and_absent() {
        assert!(!Resolution::Content(1_u8).is_absent());
        assert!(Resolution::<u8>::Deleted.is_absent());
        assert!(Resolution::<u8>::Absent.is_absent());
    }

    #[test]
    fn resolution_map_transforms_content() {
        let r: Resolution<u32> = Resolution::Content(3);
        assert_eq!(r.map(|v| v * 2), Resolution::Content(6));
    }

    #[test]
    fn resolution_map_preserves_deleted() {
        let r: Resolution<u32> = Resolution::Deleted;
        assert_eq!(r.map(|v| v * 2), Resolution::Deleted);
    }

    #[test]
    fn resolution_map_preserves_absent() {
        let r: Resolution<u32> = Resolution::Absent;
        assert_eq!(r.map(|v| v * 2), Resolution::Absent);
    }

    // -- FieldRow --

    #[test]
    fn field_row_is_content_flag() {
        assert!(FieldRow::Content(0_u8).is_content());
        assert!(!FieldRow::<u8>::Deleted.is_content());
    }

    #[test]
    fn field_row_map_transforms_content() {
        assert_eq!(FieldRow::Content(10_u32).map(|v| v + 1), FieldRow::Content(11));
    }

    #[test]
    fn field_row_map_preserves_deleted() {
        assert_eq!(FieldRow::<u32>::Deleted.map(|v| v + 1), FieldRow::Deleted);
    }

    #[test]
    fn field_row_into_resolution_content() {
        let r: Resolution<u32> = FieldRow::Content(7).into();
        assert_eq!(r, Resolution::Content(7));
    }

    #[test]
    fn field_row_into_resolution_deleted() {
        let r: Resolution<u32> = FieldRow::<u32>::Deleted.into();
        assert_eq!(r, Resolution::Deleted);
    }
}
