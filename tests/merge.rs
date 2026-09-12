use cavlc::{Error, Patch, Store, Value};
use serde_json::json;

fn patch(value: serde_json::Value) -> Patch {
    serde_json::from_value(value).unwrap()
}

#[test]
fn merges_disjoint_operations_and_nested_object_updates() {
    let base = Value::from(json!({"parent":{"a":0,"b":0},"x":true,"y":true}));
    let a =
        patch(json!({"Create":{"new":{}},"Update":{"parent":{"Update":{"a":1}}},"Delete":["x"]}));
    let b = patch(
        json!({"Create":{"other":false},"Update":{"parent":{"Update":{"b":2}}},"Delete":["y"]}),
    );
    let merged = a.merge(&b).unwrap();
    assert_eq!(merged, b.merge(&a).unwrap());
    assert_eq!(
        merged.apply(&base).unwrap(),
        b.apply(&a.apply(&base).unwrap()).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&merged).unwrap(),
        json!({"Create":{"new":{},"other":false},"Update":{"parent":{"Update":{"a":1,"b":2}}},"Delete":["x","y"]})
    );
    let mut store = Store::from_value(base).unwrap();
    let result = store.apply(&merged).unwrap();
    assert_eq!(result.snapshot.version(), 1);
    assert_eq!(result.record.unwrap().patches, vec![merged]);
}

#[test]
fn overlapping_edits_report_the_exact_path_without_changing_inputs() {
    let a = patch(json!({"Update":{"parent":{"Update":{"name":"first"}}}}));
    let b = patch(json!({"Update":{"parent":{"Update":{"name":"second"}}}}));
    let original = a.clone();
    assert_eq!(
        a.merge(&b).unwrap_err(),
        Error::MergeConflict {
            path: vec!["parent".into(), "name".into()]
        }
    );
    assert_eq!(a, original);
    let create = patch(json!({"Create":{"parent":{}}}));
    let delete = patch(json!({"Delete":["parent"]}));
    for (left, right) in [
        (&create, &a),
        (&a, &delete),
        (&delete, &create),
        (&create, &create),
        (&delete, &delete),
    ] {
        assert_eq!(
            left.merge(right).unwrap_err(),
            Error::MergeConflict {
                path: vec!["parent".into()]
            }
        );
    }
}

#[test]
fn empty_patch_is_identity_and_disjoint_merge_is_associative() {
    let empty = Patch::default();
    let a = patch(json!({"Update":{"p":{"Create":{"a":1}}}}));
    let b = patch(json!({"Update":{"p":{"Create":{"b":2}}}}));
    let c = patch(json!({"Update":{"p":{"Create":{"c":3}}}}));
    assert_eq!(a.merge(&empty).unwrap(), a);
    assert_eq!(empty.merge(&a).unwrap(), a);
    assert_eq!(
        a.merge(&b).unwrap().merge(&c).unwrap(),
        a.merge(&b.merge(&c).unwrap()).unwrap()
    );
}

#[test]
fn merge_validates_programmatic_patches() {
    let invalid = Patch {
        create: [("p".into(), Value::from(json!({"child":1})))].into(),
        ..Default::default()
    };
    assert!(matches!(
        invalid.merge(&Patch::default()),
        Err(Error::InvalidPatch(_))
    ));
    assert!(matches!(
        Patch::default().merge(&invalid),
        Err(Error::InvalidPatch(_))
    ));
}
