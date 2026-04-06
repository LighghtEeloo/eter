use std::collections::BTreeSet;

use eter::filesystem::{FilesystemBackend, FilesystemNodeId, builtins_registry};
use eter::{Edges, Eter, Eterator, Field, FieldRow, GcOption, Lifecycle, Resolution, Warning, WriteTxn};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LifeState {
    Active,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TitleField;

impl Field for TitleField {
    type Content = String;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PriorityField;

impl Field for PriorityField {
    type Content = u8;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AliasesField;

impl Field for AliasesField {
    type Content = BTreeSet<String>;
}

#[test]
fn filesystem_backend_supports_static_user_defined_fields() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let registry = builtins_registry::<LifeState>()
        .with_field::<TitleField>("title")
        .with_field::<PriorityField>("priority")
        .with_field::<AliasesField>("aliases");

    let mut store = FilesystemBackend::<LifeState>::open(temp.path(), registry)?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let beta = FilesystemNodeId::new("beta")?;

    let alpha_edges = BTreeSet::from([beta.clone()]);
    let aliases_v1 = BTreeSet::from(["a".to_owned(), "alpha".to_owned()]);

    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<Edges<FilesystemNodeId>>(&alpha, alpha_edges)
        .set::<TitleField>(&alpha, "Alpha".to_owned())
        .set::<PriorityField>(&alpha, 1)
        .set::<AliasesField>(&alpha, aliases_v1.clone())
        .set::<Lifecycle<LifeState>>(&beta, LifeState::Active)
        .set::<TitleField>(&beta, "Beta".to_owned())
        .set::<PriorityField>(&beta, 5)
        .commit()?;

    assert_eq!(store.resolve::<TitleField>(v1, &alpha)?, Resolution::Content("Alpha".to_owned()));
    assert_eq!(store.resolve::<PriorityField>(v1, &alpha)?, Resolution::Content(1));
    assert_eq!(store.resolve::<AliasesField>(v1, &alpha)?, Resolution::Content(aliases_v1));

    let v2 = store.write().set::<PriorityField>(&alpha, 2).commit()?;
    assert_eq!(store.resolve::<PriorityField>(v2, &alpha)?, Resolution::Content(2));
    assert_eq!(
        store.field_history::<PriorityField>(&alpha)?,
        vec![(v1, FieldRow::Content(1)), (v2, FieldRow::Content(2))]
    );

    let v3 = store.write().delete::<AliasesField>(&alpha).commit()?;
    assert_eq!(store.resolve::<AliasesField>(v3, &alpha)?, Resolution::Deleted);
    assert_eq!(
        store.field_history::<AliasesField>(&alpha)?,
        vec![
            (v1, FieldRow::Content(BTreeSet::from(["a".to_owned(), "alpha".to_owned()])),),
            (v2, FieldRow::Content(BTreeSet::from(["a".to_owned(), "alpha".to_owned()])),),
            (v3, FieldRow::Deleted),
        ]
    );

    store.gc(GcOption::UseLiveSet(BTreeSet::from([v3])))?;
    assert_eq!(store.field_history::<PriorityField>(&alpha)?, vec![(v3, FieldRow::Content(2))]);
    assert!(store.node_exists(v3, &beta)?);
    assert_eq!(store.resolve::<TitleField>(v3, &beta)?, Resolution::Content("Beta".to_owned()));

    Ok(())
}

// -- Helpers --

fn open_store(
    path: impl Into<std::path::PathBuf>,
) -> Result<FilesystemBackend<LifeState>, Box<dyn std::error::Error>> {
    let registry = builtins_registry::<LifeState>()
        .with_field::<TitleField>("title")
        .with_field::<PriorityField>("priority")
        .with_field::<AliasesField>("aliases");
    Ok(FilesystemBackend::<LifeState>::open(path, registry)?)
}

// -- Tests --

#[test]
fn empty_store_current_version_is_empty_sentinel() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let store = open_store(temp.path())?;
    assert_eq!(store.current_version()?, Eterator::EMPTY);
    Ok(())
}

#[test]
fn empty_transaction_is_noop_and_returns_current_version() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;
    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .commit()?;

    // Empty transaction: no pending mutations.
    let v_noop = store.write().commit()?;
    assert_eq!(v_noop, v1, "empty commit must return the unchanged current version");
    Ok(())
}

#[test]
fn multi_node_single_transaction_shares_version() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let beta = FilesystemNodeId::new("beta")?;

    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<TitleField>(&alpha, "Alpha".to_owned())
        .set::<Lifecycle<LifeState>>(&beta, LifeState::Active)
        .set::<TitleField>(&beta, "Beta".to_owned())
        .commit()?;

    // Both nodes exist at exactly v1.
    assert!(store.node_exists(v1, &alpha)?);
    assert!(store.node_exists(v1, &beta)?);

    // Both nodes appear at the same version in their history.
    let alpha_hist = store.field_history::<TitleField>(&alpha)?;
    let beta_hist = store.field_history::<TitleField>(&beta)?;
    assert_eq!(alpha_hist.len(), 1);
    assert_eq!(beta_hist.len(), 1);
    assert_eq!(alpha_hist[0].0, v1);
    assert_eq!(beta_hist[0].0, v1);
    Ok(())
}

#[test]
fn resolution_absent_for_never_written_field() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .commit()?;

    // PriorityField was never written for alpha.
    assert_eq!(store.resolve::<PriorityField>(v1, &alpha)?, Resolution::Absent);
    Ok(())
}

#[test]
fn resolution_absent_for_unknown_node() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .commit()?;

    let ghost = FilesystemNodeId::new("ghost")?;
    assert_eq!(store.resolve::<TitleField>(v1, &ghost)?, Resolution::Absent);
    assert!(!store.node_exists(v1, &ghost)?);
    Ok(())
}

#[test]
fn historical_snapshot_read_returns_old_value() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<TitleField>(&alpha, "First".to_owned())
        .commit()?;

    let v2 = store.write().set::<TitleField>(&alpha, "Second".to_owned()).commit()?;
    let v3 = store.write().set::<TitleField>(&alpha, "Third".to_owned()).commit()?;

    assert_eq!(
        store.resolve::<TitleField>(v1, &alpha)?,
        Resolution::Content("First".to_owned())
    );
    assert_eq!(
        store.resolve::<TitleField>(v2, &alpha)?,
        Resolution::Content("Second".to_owned())
    );
    assert_eq!(
        store.resolve::<TitleField>(v3, &alpha)?,
        Resolution::Content("Third".to_owned())
    );
    Ok(())
}

#[test]
fn node_lifecycle_delete_and_recreate() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<TitleField>(&alpha, "Original".to_owned())
        .commit()?;

    assert!(store.node_exists(v1, &alpha)?);

    // Delete the node by writing a deletion marker to lifecycle.
    let v2 = store.write().delete::<Lifecycle<LifeState>>(&alpha).commit()?;
    assert!(!store.node_exists(v2, &alpha)?);

    // NodeId remains "in use" even after deletion.
    assert!(store.node_id_in_use(&alpha)?);

    // Re-create the node at a later version.
    let v3 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<TitleField>(&alpha, "Reborn".to_owned())
        .commit()?;

    assert!(store.node_exists(v3, &alpha)?);
    assert_eq!(
        store.resolve::<TitleField>(v3, &alpha)?,
        Resolution::Content("Reborn".to_owned())
    );
    // Historical read: node was absent at v2.
    assert!(!store.node_exists(v2, &alpha)?);
    Ok(())
}

#[test]
fn node_id_in_use_returns_false_for_fresh_id() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let store = open_store(temp.path())?;

    let ghost = FilesystemNodeId::new("ghost")?;
    assert!(!store.node_id_in_use(&ghost)?);
    Ok(())
}

#[test]
fn check_edges_reports_dangling_targets() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let ghost = FilesystemNodeId::new("ghost")?;

    let edges = BTreeSet::from([ghost.clone()]);
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<Edges<FilesystemNodeId>>(&alpha, edges.clone())
        .commit()?;

    let warnings = store.check_edges(v1, &alpha, &edges)?;
    assert_eq!(warnings.len(), 1);
    assert_eq!(
        warnings[0],
        Warning::DanglingEdge { source: alpha.clone(), target: ghost.clone() }
    );
    Ok(())
}

#[test]
fn check_edges_no_warnings_for_existing_targets() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let beta = FilesystemNodeId::new("beta")?;

    let edges = BTreeSet::from([beta.clone()]);
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<Lifecycle<LifeState>>(&beta, LifeState::Active)
        .set::<Edges<FilesystemNodeId>>(&alpha, edges.clone())
        .commit()?;

    let warnings = store.check_edges(v1, &alpha, &edges)?;
    assert!(warnings.is_empty());
    Ok(())
}

#[test]
fn check_edges_warns_when_target_deleted_at_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let beta = FilesystemNodeId::new("beta")?;
    let edges = BTreeSet::from([beta.clone()]);

    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<Lifecycle<LifeState>>(&beta, LifeState::Active)
        .set::<Edges<FilesystemNodeId>>(&alpha, edges.clone())
        .commit()?;

    // Delete beta.
    let v2 = store.write().delete::<Lifecycle<LifeState>>(&beta).commit()?;

    // At v1 no warnings; at v2 beta is gone → dangling.
    assert!(store.check_edges(v1, &alpha, &edges)?.is_empty());
    let warnings = store.check_edges(v2, &alpha, &edges)?;
    assert_eq!(warnings.len(), 1);
    assert!(matches!(&warnings[0], Warning::DanglingEdge { target, .. } if target == &beta));
    Ok(())
}

#[test]
fn retire_and_gc_with_retired_set() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<PriorityField>(&alpha, 10)
        .commit()?;
    let v2 = store.write().set::<PriorityField>(&alpha, 20).commit()?;
    let v3 = store.write().set::<PriorityField>(&alpha, 30).commit()?;

    store.retire([v1, v2])?;
    assert_eq!(store.retired_versions()?, BTreeSet::from([v1, v2]));

    store.gc(GcOption::UseRetiredSet)?;

    // Only v3 is live; v1 and v2 history is gone.
    assert_eq!(
        store.field_history::<PriorityField>(&alpha)?,
        vec![(v3, FieldRow::Content(30))]
    );
    assert_eq!(store.current_version()?, v3);
    Ok(())
}

#[test]
fn only_keep_retires_all_except_specified() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<PriorityField>(&alpha, 1)
        .commit()?;
    let v2 = store.write().set::<PriorityField>(&alpha, 2).commit()?;
    let v3 = store.write().set::<PriorityField>(&alpha, 3).commit()?;

    store.only_keep([v3])?;
    // v1 and v2 should be retired; v3 should not be.
    let retired = store.retired_versions()?;
    assert!(retired.contains(&v1));
    assert!(retired.contains(&v2));
    assert!(!retired.contains(&v3));

    let live = store.live_versions()?;
    assert_eq!(live, BTreeSet::from([v3]));

    store.gc(GcOption::UseRetiredSet)?;
    assert_eq!(
        store.field_history::<PriorityField>(&alpha)?,
        vec![(v3, FieldRow::Content(3))]
    );
    Ok(())
}

#[test]
fn live_versions_excludes_retired() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .commit()?;
    let v2 = store.write().set::<TitleField>(&alpha, "hello".to_owned()).commit()?;

    assert_eq!(store.live_versions()?, BTreeSet::from([v1, v2]));

    store.retire([v1])?;
    assert_eq!(store.live_versions()?, BTreeSet::from([v2]));
    Ok(())
}

#[test]
fn reopen_store_recovers_current_version() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;

    let v_final = {
        let mut store = open_store(temp.path())?;
        let alpha = FilesystemNodeId::new("alpha")?;
        store
            .write()
            .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
            .set::<TitleField>(&alpha, "Alpha".to_owned())
            .commit()?;
        let v = store.write().set::<TitleField>(&alpha, "Updated".to_owned()).commit()?;
        v
    };

    // Re-open the same directory.
    let store2 = open_store(temp.path())?;
    assert_eq!(store2.current_version()?, v_final);

    let alpha = FilesystemNodeId::new("alpha")?;
    assert_eq!(
        store2.resolve::<TitleField>(v_final, &alpha)?,
        Resolution::Content("Updated".to_owned())
    );
    Ok(())
}

#[test]
fn gc_preserves_reads_through_live_versions() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<PriorityField>(&alpha, 1)
        .commit()?;
    let _v2 = store.write().set::<PriorityField>(&alpha, 2).commit()?;
    let v3 = store.write().set::<PriorityField>(&alpha, 3).commit()?;
    let _v4 = store.write().set::<PriorityField>(&alpha, 4).commit()?;

    // Keep v1 and v3; GC should drop what it can without breaking reads.
    store.gc(GcOption::UseLiveSet(BTreeSet::from([v1, v3])))?;

    assert_eq!(store.resolve::<PriorityField>(v1, &alpha)?, Resolution::Content(1));
    assert_eq!(store.resolve::<PriorityField>(v3, &alpha)?, Resolution::Content(3));
    // v2 was between two live versions and not live itself; it may have been dropped.
    // But reads at v1 and v3 must still be correct.
    let hist = store.field_history::<PriorityField>(&alpha)?;
    assert!(hist.iter().any(|(v, r)| *v == v1 && *r == FieldRow::Content(1)));
    assert!(hist.iter().any(|(v, r)| *v == v3 && *r == FieldRow::Content(3)));
    Ok(())
}

#[test]
fn filesystem_node_id_rejects_invalid_values() {
    assert!(FilesystemNodeId::new("").is_err());
    assert!(FilesystemNodeId::new(".").is_err());
    assert!(FilesystemNodeId::new("..").is_err());
    assert!(FilesystemNodeId::new("a/b").is_err());
    assert!(FilesystemNodeId::new("a\0b").is_err());
    assert!(FilesystemNodeId::new("a".repeat(256)).is_err());
    // Valid ids should succeed.
    assert!(FilesystemNodeId::new("valid-id").is_ok());
    assert!(FilesystemNodeId::new("a".repeat(255)).is_ok());
}

#[test]
fn field_history_empty_for_node_with_no_writes() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let store = open_store(temp.path())?;
    let ghost = FilesystemNodeId::new("ghost")?;
    assert!(store.field_history::<TitleField>(&ghost)?.is_empty());
    Ok(())
}

#[test]
fn unchanged_fields_are_inherited_across_versions() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = open_store(temp.path())?;

    let alpha = FilesystemNodeId::new("alpha")?;
    let v1 = store
        .write()
        .set::<Lifecycle<LifeState>>(&alpha, LifeState::Active)
        .set::<TitleField>(&alpha, "Stable".to_owned())
        .set::<PriorityField>(&alpha, 7)
        .commit()?;

    // Only update priority; title should be inherited.
    let v2 = store.write().set::<PriorityField>(&alpha, 99).commit()?;

    assert_eq!(
        store.resolve::<TitleField>(v2, &alpha)?,
        Resolution::Content("Stable".to_owned()),
        "title should be inherited at v2 without an explicit write"
    );
    assert_eq!(store.resolve::<PriorityField>(v2, &alpha)?, Resolution::Content(99));

    // The filesystem backend uses per-node storage: every write copies all fields
    // into the new snapshot file. So field_history for title contains an entry at
    // every version, even though the value did not change at v2.
    assert_eq!(
        store.field_history::<TitleField>(&alpha)?,
        vec![
            (v1, FieldRow::Content("Stable".to_owned())),
            (v2, FieldRow::Content("Stable".to_owned())),
        ]
    );
    Ok(())
}
