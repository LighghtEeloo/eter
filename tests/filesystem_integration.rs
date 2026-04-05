use std::collections::BTreeSet;

use eter::filesystem::{FilesystemBackend, FilesystemNodeId, builtins_registry};
use eter::{Edges, Eter, Field, FieldRow, GcOption, Lifecycle, Resolution, WriteTxn};
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
