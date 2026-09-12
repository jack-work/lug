//! Log invariants, exercised against storage that can be made to fail.

use cavlc::{Patch, Store};
use lug_core::{Durability, Log, Record, Recovered, Storage, Version, Versioned};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};

#[derive(Clone, Default)]
struct Counter(Arc<AtomicUsize>);
impl Counter {
    fn bump(&self) {
        self.0.fetch_add(1, Relaxed);
    }
    fn get(&self) -> usize {
        self.0.load(Relaxed)
    }
}

#[derive(Clone, Default)]
struct Switch(Arc<AtomicBool>);
impl Switch {
    fn set(&self, on: bool) {
        self.0.store(on, Relaxed);
    }
    fn on(&self) -> bool {
        self.0.load(Relaxed)
    }
}

/// In-memory storage with a switch for making the next write fail, and a
/// count of syncs so group commit can be observed rather than assumed.
#[derive(Default)]
struct Mem {
    records: Vec<Record>,
    header: Option<cavlc::Snapshot>,
    oldest: Version,
    fail: Switch,
    syncs: Counter,
    writes: Counter,
}

#[derive(Debug, thiserror::Error)]
#[error("storage is refusing writes")]
struct Refused;

impl Storage for Mem {
    type View = cavlc::Snapshot;
    type Error = Refused;

    fn append(&mut self, records: &[Record]) -> Result<(), Refused> {
        if self.fail.on() {
            return Err(Refused);
        }
        self.writes.bump();
        self.records.extend_from_slice(records);
        Ok(())
    }

    fn write_header(&mut self, view: &cavlc::Snapshot) -> Result<(), Refused> {
        if self.fail.on() {
            return Err(Refused);
        }
        self.oldest = view.version();
        self.records.retain(|r| r.version > view.version());
        self.header = Some(view.clone());
        Ok(())
    }

    fn sync(&mut self) -> Result<(), Refused> {
        if self.fail.on() {
            return Err(Refused);
        }
        self.syncs.bump();
        Ok(())
    }

    fn load(&mut self) -> Result<Recovered<cavlc::Snapshot>, Refused> {
        Ok(Recovered { header: self.header.clone(), tail: self.records.clone() })
    }

    fn read_after(&self, after: Version, limit: usize) -> Result<Vec<Record>, Refused> {
        Ok(self
            .records
            .iter()
            .filter(|r| r.version > after)
            .take(limit)
            .cloned()
            .collect())
    }

    fn oldest(&self) -> Version {
        self.oldest
    }
}

fn patch(json: &str) -> Patch {
    serde_json::from_str(json).expect("valid patch")
}

fn create(key: &str, value: i64) -> Patch {
    patch(&format!(r#"{{"Create":{{"{key}":{value}}}}}"#))
}

#[test]
fn each_append_mints_one_contiguous_version() {
    let mut log = Log::open(Store::new(), Mem::default()).unwrap();
    let views = log
        .append(&[create("a", 1), create("b", 2), create("c", 3)], Durability::Durable)
        .unwrap();

    let versions: Vec<_> = views.iter().map(|v| v.version()).collect();
    assert_eq!(versions, vec![1, 2, 3]);
    assert_eq!(log.watermark(), 3);
}

#[test]
fn a_batch_costs_one_write_and_one_sync() {
    let storage = Mem::default();
    let (writes, syncs) = (storage.writes.clone(), storage.syncs.clone());
    let mut log = Log::open(Store::new(), storage).unwrap();

    let batch: Vec<_> = (0..64).map(|i| create(&format!("k{i}"), i)).collect();
    log.append(&batch, Durability::Durable).unwrap();

    assert_eq!(log.watermark(), 64);
    assert_eq!(writes.get(), 1, "64 patches must not cost 64 writes");
    assert_eq!(syncs.get(), 1, "64 patches must not cost 64 syncs");
}

#[test]
fn written_durability_does_not_sync() {
    let storage = Mem::default();
    let syncs = storage.syncs.clone();
    let mut log = Log::open(Store::new(), storage).unwrap();

    log.append(&[create("a", 1)], Durability::Written).unwrap();

    assert_eq!(syncs.get(), 0);
    assert_eq!(log.watermark(), 1, "observable without being durable");
    assert_eq!(log.synced(), 0, "but not claimed as durable");
}

#[test]
fn a_failed_write_leaves_no_observable_version() {
    let storage = Mem::default();
    let fail = storage.fail.clone();
    let mut log = Log::open(Store::new(), storage).unwrap();
    log.append(&[create("a", 1)], Durability::Durable).unwrap();

    fail.set(true);
    let err = log.append(&[create("b", 2)], Durability::Durable).unwrap_err();

    assert!(matches!(err, lug_core::Error::Storage(_)));
    assert_eq!(log.watermark(), 1, "memory must not lead the log");
    assert_eq!(log.view().version(), 1, "the fold was unwound");
    assert!(log.view().view.root().get("b").is_none());
}

#[test]
fn a_rejected_patch_aborts_the_whole_batch() {
    let mut log = Log::open(Store::new(), Mem::default()).unwrap();
    log.append(&[create("a", 1)], Durability::Durable).unwrap();

    // The second Create collides, so nothing in this batch may land.
    let err = log
        .append(&[create("b", 2), create("a", 9), create("c", 3)], Durability::Durable)
        .unwrap_err();

    assert!(matches!(err, lug_core::Error::Data(_)));
    assert_eq!(log.watermark(), 1);
    assert!(log.view().view.root().get("b").is_none());
}

#[test]
fn an_identity_patch_mints_no_version_and_writes_no_record() {
    let storage = Mem::default();
    let writes = storage.writes.clone();
    let mut log = Log::open(Store::new(), storage).unwrap();
    log.append(&[create("a", 1)], Durability::Durable).unwrap();

    let before = log.watermark();
    log.append(&[patch("{}")], Durability::Durable).unwrap();

    assert_eq!(log.watermark(), before);
    assert_eq!(writes.get(), 1, "an empty patch must not reach storage");
}

#[test]
fn recovery_replays_records() {
    let storage = Mem::default();
    let mut log = Log::open(Store::new(), storage).unwrap();
    log.append(&[create("a", 1), create("b", 2)], Durability::Durable).unwrap();
    let (_data, storage) = log.into_parts();

    let recovered = Log::open(Store::new(), storage).unwrap();

    assert_eq!(recovered.watermark(), 2);
    assert_eq!(recovered.view().view.root().get("b").unwrap().to_json(), serde_json::json!(2));
}

#[test]
fn a_checkpoint_replaces_the_records_it_covers() {
    let mut log = Log::open(Store::new(), Mem::default()).unwrap();
    log.append(&[create("a", 1), create("b", 2)], Durability::Durable).unwrap();
    log.checkpoint().unwrap();
    log.append(&[create("c", 3)], Durability::Durable).unwrap();
    let (_data, storage) = log.into_parts();

    // Version 0 here is deliberately the wrong zero value. If recovery
    // replayed from it instead of adopting the header, `a` would be missing.
    let recovered = Log::open(Store::new(), storage).unwrap();

    assert_eq!(recovered.watermark(), 3);
    let root = recovered.view();
    let root = root.view.root();
    assert_eq!(root.get("a").unwrap().to_json(), serde_json::json!(1));
    assert_eq!(root.get("c").unwrap().to_json(), serde_json::json!(3));
}

#[test]
fn historical_views_stay_readable() {
    let mut log = Log::open(Store::new(), Mem::default()).unwrap();
    log.append(&[create("a", 1), create("b", 2)], Durability::Durable).unwrap();

    let old = log.view_at(1).expect("version 1 retained");
    assert_eq!(old.version(), 1);
    assert!(old.view.root().get("b").is_none(), "version 1 predates b");
    assert!(log.view_at(99).is_none());
}

#[test]
fn an_empty_append_is_a_no_op() {
    let storage = Mem::default();
    let writes = storage.writes.clone();
    let mut log = Log::open(Store::new(), storage).unwrap();

    assert!(log.append(&[], Durability::Durable).unwrap().is_empty());
    assert_eq!(writes.get(), 0);
}
