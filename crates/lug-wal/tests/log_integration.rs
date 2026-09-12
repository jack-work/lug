//! The store under the thing that will actually use it.

use lug_core::{Durability, Log, Noop, Tick};
use lug_wal::{Options, SegmentStore};
use serde_json::json;
use std::path::Path;

type Store = SegmentStore<Tick>;

fn open(dir: &Path) -> Log<Noop, Store> {
    let store = SegmentStore::open_with(dir, Options { rotate_bytes: 4096 }).expect("open store");
    Log::open(Noop::default(), store).expect("open log")
}

fn patches(range: std::ops::RangeInclusive<u64>) -> Vec<serde_json::Value> {
    range.map(|i| json!({ "n": i, "body": "x".repeat(64) })).collect()
}

#[test]
fn a_log_comes_back_at_the_version_it_left() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut log = open(dir.path());
    log.append(&patches(1..=100), Durability::Durable).expect("append");
    assert_eq!(log.watermark(), 100);
    assert_eq!(log.synced(), 100);
    drop(log);

    let mut log = open(dir.path());
    assert_eq!(log.watermark(), 100);
    log.append(&patches(101..=110), Durability::Written).expect("append");
    log.sync().expect("sync");
    assert_eq!(log.watermark(), 110);
    drop(log);

    let log = open(dir.path());
    assert_eq!(log.watermark(), 110);
    assert_eq!(log.read_after(105, 3).expect("read").len(), 3);
}

#[test]
fn a_checkpoint_lets_recovery_skip_the_records_it_covers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut log = open(dir.path());
    // Many small batches, because a batch is never split and so a single
    // giant append would leave one segment with nothing to reclaim.
    for batch in 0..20 {
        let base = batch * 10 + 1;
        log.append(&patches(base..=base + 9), Durability::Written).expect("append");
    }
    log.checkpoint().expect("checkpoint");
    let oldest = log.oldest();
    assert!(oldest > 1, "a checkpoint over every record reclaimed nothing");
    drop(log);

    let log = open(dir.path());
    assert_eq!(log.watermark(), 200);
    assert_eq!(log.oldest(), oldest);

    // Below the oldest retained version the subscriber is told, not lied to.
    assert!(log.read_after(0, 4).is_err());
    assert_eq!(log.read_after(oldest - 1, 4).expect("read").len(), 4);
}

/// A sync that fails after the records are written is not a failed write. The
/// log truncating memory back for it would put the structure *behind* storage,
/// which is the mirror of the invariant it is protecting: the next append
/// would mint versions the segment already holds, and every append after that
/// would be refused.
#[test]
fn a_failed_sync_does_not_unwind_records_storage_already_holds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut log = open(dir.path());
    log.append(&patches(1..=3), Durability::Durable).expect("append");

    lug_wal::fault::fail_syncs(0, 1);
    let err = log.append(&patches(4..=4), Durability::Durable).expect_err("the sync must fail");
    lug_wal::fault::clear();
    assert!(matches!(err, lug_core::Error::Storage(_)), "{err}");

    assert_eq!(log.watermark(), 4, "the record was written, only the flush failed");
    assert_eq!(log.synced(), 3, "and it is not claimed as durable");

    log.append(&patches(5..=5), Durability::Durable).expect("the log must carry on");
    assert_eq!(log.synced(), 5);
    drop(log);

    let log = open(dir.path());
    assert_eq!(log.watermark(), 5);
}
