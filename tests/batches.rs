use cavlc::{Commit, Error, Patch, Store, Update, Value};
use serde_json::{Value as Json, json};

fn patch(value: Json) -> Patch {
    serde_json::from_value(value).unwrap()
}
fn create(key: &str, value: Json) -> Patch {
    patch(json!({"Create":{key:value}}))
}
fn update(key: &str, value: Json) -> Patch {
    patch(json!({"Update":{key:value}}))
}
fn apply(store: &mut Store, value: Json) {
    store.apply(&patch(value)).unwrap();
}

#[test]
fn every_property_requires_create_and_parents_are_never_implicit() {
    let mut store = Store::new();
    assert!(store.apply(&update("count", json!(1))).is_err());
    assert!(
        store
            .apply(&patch(
                json!({"Update":{"profile":{"Create":{"name":"Gluck"}}}})
            ))
            .is_err()
    );
    assert_eq!(store.snapshot().version(), 0);
    apply(&mut store, json!({"Create":{"profile":{}}}));
    let rename = patch(json!({"Update":{"profile":{"Update":{"name":"Figaro"}}}}));
    assert!(store.apply(&rename).is_err());
    apply(
        &mut store,
        json!({"Update":{"profile":{"Create":{"name":"Gluck"}}}}),
    );
    store.apply(&rename).unwrap();
    assert_eq!(
        store.snapshot().root().to_json(),
        json!({"profile":{"name":"Figaro"}})
    );
    assert!(store.apply(&create("profile", json!({}))).is_err());
    apply(&mut store, json!({"Create":{"leaf":0}}));
    assert!(
        store
            .apply(&patch(json!({"Update":{"leaf":{"Create":{"child":1}}}})))
            .is_err()
    );
}

#[test]
fn reject_bulk_objects_old_operations_and_malformed_patches() {
    for invalid in [
        json!({"Create":{"profile":{"test":"test"}}}),
        json!({"Create":{"profile":{"child":{}}}}),
        json!({"Update":{"profile":{"Create":{"child":{"name":"x"}}}}}),
        json!({"Update":{"profile":{"name":"x"}}}),
        json!({"Set":{"a":1}}),
        json!({"object":{"Create":{"a":1}}}),
        json!({"scalar":{"Before":1,"After":2}}),
        json!({"New":{"a":true}}),
        json!({"Delete":{"a":99}}),
        json!({"create":{"a":1}}),
        json!({"Create":{"a":1},"Update":{"a":2}}),
        json!({"Create":{"a":{}},"Update":{"a":{"Create":{"b":1}}}}),
        json!({"Update":{"a":1},"Delete":["a"]}),
        json!({"Delete":["a","a"]}),
        json!([{"op":"add","path":"/a","value":1}]),
        json!([]),
    ] {
        assert!(
            serde_json::from_value::<Patch>(invalid.clone()).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn rust_api_cannot_bypass_explicit_creation() {
    let bulk = Patch {
        create: [("p".into(), Value::from(json!({"child":1})))].into(),
        ..Default::default()
    };
    assert!(bulk.apply(&Value::default()).is_err());
    let replacement = Patch {
        update: [("p".into(), Update::Value(Value::from(json!({"child":1}))))].into(),
        ..Default::default()
    };
    assert!(replacement.apply(&Value::from(json!({"p":{}}))).is_err());
}

#[test]
fn objects_cannot_be_replaced_or_created_by_leaf_updates() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"object":{},"leaf":0}}));
    let original = store.snapshot();
    for invalid in [
        json!({"Update":{"object":null}}),
        json!({"Update":{"object":[1,2]}}),
        json!({"Update":{"leaf":{}}}),
        json!({"Update":{"leaf":{"Create":{"child":1}}}}),
    ] {
        assert!(store.apply(&patch(invalid)).is_err());
    }
    assert!(store.snapshot().root().ptr_eq(original.root()));
    let mut tx = store.begin_batch();
    tx.apply(&patch(json!({"Delete":["leaf"]}))).unwrap();
    tx.apply(&create("leaf", json!({}))).unwrap();
    tx.apply(&patch(json!({"Update":{"leaf":{"Create":{"child":1}}}})))
        .unwrap();
    store.apply_batch(tx).unwrap();
    assert_eq!(
        store.snapshot().root().to_json(),
        json!({"object":{},"leaf":{"child":1}})
    );
}

#[test]
fn inner_deletion_uses_updates_and_parent_deletion_removes_subtree() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"profile":{},"keep":true}}));
    apply(
        &mut store,
        json!({"Update":{"profile":{"Create":{"nested":{}}}}}),
    );
    apply(
        &mut store,
        json!({"Update":{"profile":{"Update":{"nested":{"Create":{"a":1,"b":null}}}}}}),
    );
    let old = store.snapshot();
    apply(
        &mut store,
        json!({"Update":{"profile":{"Update":{"nested":{"Delete":["a","b"]}}}}}),
    );
    assert_eq!(
        store.snapshot().root().to_json(),
        json!({"profile":{"nested":{}},"keep":true})
    );
    apply(
        &mut store,
        json!({"Update":{"profile":{"Update":{"nested":{"Create":{"c":2}}}}}}),
    );
    apply(&mut store, json!({"Delete":["profile"]}));
    assert_eq!(store.snapshot().root().to_json(), json!({"keep":true}));
    assert_eq!(
        old.root().at(["profile", "nested", "a"]).unwrap().to_json(),
        json!(1)
    );
    assert!(
        store
            .apply(&patch(
                json!({"Update":{"profile":{"Create":{"new":true}}}})
            ))
            .is_err()
    );
    assert!(store.apply(&patch(json!({"Delete":["profile"]}))).is_err());
}

#[test]
fn patch_failure_is_atomic_and_preserves_earlier_staged_edits() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"settings":{}}}));
    let before = store.snapshot();
    let bad =
        patch(json!({"Create":{"first":true},"Update":{"settings":{"Update":{"missing":99}}}}));
    assert!(store.apply(&bad).is_err());
    assert!(store.snapshot().root().ptr_eq(before.root()));
    assert_eq!(store.log().len(), 1);
    let mut tx = store.begin_batch();
    tx.apply(&create("ok", json!(true))).unwrap();
    let staged = tx.root().clone();
    assert!(tx.apply(&bad).is_err());
    assert!(tx.root().ptr_eq(&staged));
    let result = store.apply_batch(tx).unwrap();
    assert_eq!(
        result.record.unwrap().patches,
        vec![create("ok", json!(true))]
    );
    let before = store.snapshot();
    assert!(
        store
            .apply(&patch(json!({"Delete":["ok","missing"]})))
            .is_err()
    );
    assert!(store.snapshot().root().ptr_eq(before.root()));
}

#[test]
fn stale_and_foreign_batches_cannot_publish() {
    let mut store = Store::new();
    let old = store.snapshot();
    let mut a = store.begin_batch();
    let mut b = store.begin_batch();
    a.apply(&create("count", json!(1))).unwrap();
    b.apply(&create("other", json!(2))).unwrap();
    assert_eq!(store.snapshot().root().to_json(), json!({}));
    store.apply_batch(a).unwrap();
    assert_eq!(
        store.apply_batch(b).unwrap_err(),
        Error::Conflict {
            expected: 0,
            actual: 1
        }
    );
    assert!(store.apply(&Patch::default()).unwrap().record.is_none());
    assert_eq!(
        store.apply_batch(Store::new().begin_batch()).unwrap_err(),
        Error::ForeignBatch
    );
    assert_eq!(old.root().to_json(), json!({}));
    let held = store.snapshot();
    drop(store);
    std::thread::spawn(move || assert_eq!(held.root().to_json(), json!({"count":1})))
        .join()
        .unwrap();
}

#[test]
fn deletion_blocks_stale_additions_even_after_identical_recreation() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"parent":{}}}));
    let before = store.snapshot();
    let mut stale = store.begin_batch();
    stale
        .apply(&patch(
            json!({"Update":{"parent":{"Create":{"stale":true}}}}),
        ))
        .unwrap();
    let mut replacement = store.begin_batch();
    replacement
        .apply(&patch(json!({"Delete":["parent"]})))
        .unwrap();
    replacement.apply(&create("parent", json!({}))).unwrap();
    store.apply_batch(replacement).unwrap();
    assert_eq!(store.snapshot().root(), before.root());
    assert_eq!(store.snapshot().version(), 2);
    assert_eq!(store.log()[1].patches.len(), 2);
    assert_eq!(
        store.apply_batch(stale).unwrap_err(),
        Error::Conflict {
            expected: 1,
            actual: 2
        }
    );
    apply(
        &mut store,
        json!({"Update":{"parent":{"Create":{"fresh":true}}}}),
    );
    assert_eq!(
        store.snapshot().root().to_json(),
        json!({"parent":{"fresh":true}})
    );
    let replay = Store::replay(Value::default(), store.log().to_vec()).unwrap();
    assert_eq!(replay.snapshot_at(2).unwrap().root(), before.root());
    assert_eq!(replay.snapshot().root(), store.snapshot().root());
}

#[test]
fn deleting_a_parent_invalidates_an_open_batch() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"parent":{}}}));
    let mut stale = store.begin_batch();
    stale
        .apply(&patch(json!({"Update":{"parent":{"Create":{"child":1}}}})))
        .unwrap();
    apply(&mut store, json!({"Delete":["parent"]}));
    assert!(matches!(
        store.apply_batch(stale),
        Err(Error::Conflict { .. })
    ));
    assert_eq!(store.snapshot().root().to_json(), json!({}));
    assert!(
        store
            .apply(&patch(json!({"Update":{"parent":{"Create":{"child":1}}}})))
            .is_err()
    );
}

#[test]
fn log_keeps_explicit_creation_order_and_replays_one_version_per_batch() {
    let mut store = Store::new();
    let patches = vec![
        create("parent", json!({})),
        patch(json!({"Update":{"parent":{"Create":{"child":{}}}}})),
        patch(json!({"Update":{"parent":{"Update":{"child":{"Create":{"value":"hello"}}}}}})),
        patch(json!({"Update":{"parent":{"Update":{"child":{"Update":{"value":"goodbye"}}}}}})),
    ];
    let mut tx = store.begin_batch();
    for patch in &patches {
        tx.apply(patch).unwrap();
    }
    let result = store.apply_batch(tx).unwrap();
    assert_eq!(result.snapshot.version(), 1);
    assert_eq!(result.record.unwrap().patches, patches);
    let wire = serde_json::to_string(store.log()).unwrap();
    let records: Vec<Commit> = serde_json::from_str(&wire).unwrap();
    assert_eq!(records, store.log());
    let replay = Store::replay(Value::default(), records).unwrap();
    assert_eq!(replay.snapshot().root(), store.snapshot().root());
    assert_eq!(replay.snapshot_at(0).unwrap().root().to_json(), json!({}));
}

#[test]
fn no_op_updates_do_not_advance_but_ordered_changes_do() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"a":1,"parent":{}}}));
    let before = store.snapshot();
    assert!(store.apply(&Patch::default()).unwrap().record.is_none());
    assert!(
        store
            .apply(&update("a", json!(1)))
            .unwrap()
            .record
            .is_none()
    );
    assert!(
        store
            .apply(&patch(json!({"Update":{"parent":{}}})))
            .unwrap()
            .record
            .is_none()
    );
    assert!(store.snapshot().root().ptr_eq(before.root()));
    let mut tx = store.begin_batch();
    tx.apply(&update("a", json!(1))).unwrap();
    tx.apply(&update("a", json!(2))).unwrap();
    tx.apply(&update("a", json!(1))).unwrap();
    store.apply_batch(tx).unwrap();
    assert_eq!(store.snapshot().version(), 2);
    assert_eq!(store.log()[1].patches.len(), 2);
    assert_eq!(store.snapshot().root(), before.root());
}

#[test]
fn before_text_is_in_the_base_not_in_submitted_patches() {
    let mut store = Store::new();
    store.apply(&create("text", json!("old text"))).unwrap();
    let mut tx = store.begin_batch();
    tx.apply(&update("text", json!("new text"))).unwrap();
    assert_eq!(
        tx.base().root().get("text").unwrap().to_json(),
        json!("old text")
    );
    assert_eq!(tx.root().get("text").unwrap().to_json(), json!("new text"));
    store.apply_batch(tx).unwrap();
    assert_eq!(
        serde_json::to_value(&store.log()[1].patches).unwrap(),
        json!([{"Update":{"text":"new text"}}])
    );
    assert_eq!(
        store
            .snapshot_at(1)
            .unwrap()
            .root()
            .get("text")
            .unwrap()
            .to_json(),
        json!("old text")
    );
}

#[test]
fn untouched_values_are_shared_and_keys_are_literal() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"config":{},"other":{}}}));
    apply(
        &mut store,
        json!({"Update":{"config":{"Create":{"a.b":1,"a/b":2}},"other":{"Create":{"data":[1,2,3]}}}}),
    );
    let before = store.snapshot();
    apply(
        &mut store,
        json!({"Update":{"config":{"Update":{"a.b":3}}}}),
    );
    let after = store.snapshot();
    assert!(
        before
            .root()
            .get("other")
            .unwrap()
            .ptr_eq(after.root().get("other").unwrap())
    );
    assert_eq!(
        after.root().at(["config", "a.b"]).unwrap().to_json(),
        json!(3)
    );
    assert_eq!(
        after.root().at(["config", "a/b"]).unwrap().to_json(),
        json!(2)
    );
}

#[test]
fn leaf_updates_replace_atomic_values_and_arrays_cannot_be_descended_into() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"a":null}}));
    for value in [
        json!(7),
        json!("text"),
        json!(false),
        json!([1, 2]),
        json!(null),
    ] {
        store.apply(&update("a", value.clone())).unwrap();
        assert_eq!(store.snapshot().root().get("a").unwrap().to_json(), value);
    }
    apply(&mut store, json!({"Update":{"a":[{"opaque":1}]}}));
    assert!(
        store
            .apply(&patch(
                json!({"Update":{"a":{"Update":{"0":{"Create":{"b":2}}}}}})
            ))
            .is_err()
    );
    apply(&mut store, json!({"Delete":["a"]}));
    assert_eq!(store.snapshot().root().to_json(), json!({}));
}

#[test]
fn replay_rejects_invalid_batches_and_ranges_are_checked() {
    let mut store = Store::new();
    apply(&mut store, json!({"Create":{"a":1}}));
    apply(&mut store, json!({"Update":{"a":2}}));
    apply(&mut store, json!({"Delete":["a"]}));
    assert_eq!(store.patches_between(0, 3).unwrap().len(), 3);
    assert_eq!(store.patches_between(1, 2).unwrap()[0].version, 2);
    assert!(store.patches_between(2, 1).is_none());
    assert!(store.patches_between(0, 4).is_none());
    assert_eq!(store.patches_between(2, 2).unwrap(), &[]);
    assert!(store.snapshot_at(4).is_none());
    for record in [
        Commit {
            version: 2,
            patches: vec![create("a", json!(1))],
        },
        Commit {
            version: 1,
            patches: vec![],
        },
        Commit {
            version: 1,
            patches: vec![Patch::default()],
        },
        Commit {
            version: 1,
            patches: vec![create("a", json!(1)), update("missing", json!(2))],
        },
        Commit {
            version: 1,
            patches: vec![create("a", json!(1)), update("a", json!(1))],
        },
    ] {
        assert!(Store::replay(Value::default(), [record]).is_err());
    }
    assert!(Store::from_value(Value::from(json!([]))).is_err());
}

#[test]
fn writer_domains_can_be_locked_and_partitioned_independently() {
    use std::sync::{Arc, Mutex};
    let shared = Arc::new(Mutex::new(Store::new()));
    let held = shared.lock().unwrap().snapshot();
    let mut workers = Vec::new();
    for id in 0..4 {
        let shared = shared.clone();
        workers.push(std::thread::spawn(move || {
            for i in 0..10 {
                shared
                    .lock()
                    .unwrap()
                    .apply(&create(&format!("{id}:{i}"), json!(i)))
                    .unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(shared.lock().unwrap().snapshot().version(), 40);
    assert_eq!(held.root().to_json(), json!({}));
    let mut other = Store::new();
    let mut tx = other.begin_batch();
    tx.apply(&create("independent", json!(true))).unwrap();
    shared
        .lock()
        .unwrap()
        .apply(&create("unrelated", json!(true)))
        .unwrap();
    other.apply_batch(tx).unwrap();
}
