use cavlc::{ApplyResult, Batch, Commit, Patch, Snapshot, Store, Value, avl::AvlMap};
use serde_json::json;
use std::sync::{Arc, Barrier, Mutex};

fn patch(value: serde_json::Value) -> Patch {
    serde_json::from_value(value).unwrap()
}

#[test]
fn public_handles_are_send_sync_and_static() {
    fn check<T: Send + Sync + 'static>() {}
    check::<Value>();
    check::<Snapshot>();
    check::<Store>();
    check::<Batch>();
    check::<Commit>();
    check::<ApplyResult>();
    check::<AvlMap<String, Value>>();
}

#[test]
fn retained_nodes_are_borrowable_after_parent_deletion_and_store_drop() {
    let mut store = Store::new();
    store
        .apply(&patch(json!({"Create":{"parent":{}}})))
        .unwrap();
    store
        .apply(&patch(
            json!({"Update":{"parent":{"Create":{"array":[1,2,3],"text":"old"}}}}),
        ))
        .unwrap();
    let snapshot = store.snapshot();
    let parent = snapshot.root().get("parent").unwrap().clone();
    let leaf = parent.get("array").unwrap().clone();
    assert!(parent.as_atom().is_none());
    assert!(leaf.as_object().is_none());
    assert!(std::ptr::eq(
        leaf.as_atom().unwrap(),
        leaf.clone().as_atom().unwrap()
    ));

    let barrier = Arc::new(Barrier::new(5));
    let mut readers = Vec::new();
    for _ in 0..4 {
        let parent = parent.clone();
        let leaf = leaf.clone();
        let barrier = barrier.clone();
        readers.push(std::thread::spawn(move || {
            let array = leaf.as_atom().unwrap().as_array().unwrap();
            let text = parent
                .get("text")
                .unwrap()
                .as_atom()
                .unwrap()
                .as_str()
                .unwrap();
            barrier.wait();
            barrier.wait();
            assert_eq!(array, &vec![json!(1), json!(2), json!(3)]);
            assert_eq!(text, "old");
            assert!(leaf.ptr_eq(parent.get("array").unwrap()));
        }));
    }
    barrier.wait();
    store
        .apply(&patch(
            json!({"Update":{"parent":{"Update":{"array":[9],"text":"new"}}}}),
        ))
        .unwrap();
    store.apply(&patch(json!({"Delete":["parent"]}))).unwrap();
    drop(snapshot);
    drop(parent);
    drop(leaf);
    drop(store);
    barrier.wait();
    for reader in readers {
        reader.join().unwrap();
    }
}

#[test]
fn batch_can_leave_the_writer_lock_and_return_from_another_thread() {
    let store = Mutex::new(Store::new());
    let batch = store.lock().unwrap().begin_batch();
    let batch = std::thread::spawn(move || {
        let mut batch = batch;
        batch.apply(&patch(json!({"Create":{"count":1}}))).unwrap();
        assert_eq!(batch.base().root().to_json(), json!({}));
        assert_eq!(
            batch.root().get("count").unwrap().as_atom(),
            Some(&json!(1))
        );
        batch
    })
    .join()
    .unwrap();
    let result = store.lock().unwrap().apply_batch(batch).unwrap();
    assert_eq!(result.snapshot.version(), 1);
    assert!(result.record.is_some());
}
