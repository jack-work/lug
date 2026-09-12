//! Black box tests over the segment format. Nothing here mocks storage: every
//! case writes real files, corrupts real bytes, and reopens.

use bytes::Bytes;
use lug_core::{Record, Storage, Version, Versioned};
use lug_wal::{Error, Options, SegmentStore};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

/// Stand in for a real view. `lug_core::Noop` has one already, but its `Tick`
/// is not re-exported, so it cannot be named here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Mark(Version);

impl Versioned for Mark {
    fn version(&self) -> Version {
        self.0
    }

    fn state(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

type Store = SegmentStore<Mark>;

const PROLOGUE: u64 = 16;
const PREFIX: u64 = 16;

fn store(dir: &Path, rotate_bytes: u64) -> Store {
    SegmentStore::open_with(dir, Options { rotate_bytes }).expect("open")
}

fn payload(version: Version) -> Bytes {
    Bytes::from(format!("patch number {version:>8}"))
}

fn rec(version: Version) -> Record {
    Record { version, patch: payload(version) }
}

fn records(range: std::ops::RangeInclusive<Version>) -> Vec<Record> {
    range.map(rec).collect()
}

fn versions(records: &[Record]) -> Vec<Version> {
    records.iter().map(|r| r.version).collect()
}

/// Offset just past the last record, given what was written into one segment.
fn data_end(first: Version, last: Version) -> u64 {
    (first..=last).map(|v| PREFIX + payload(v).len() as u64).sum::<u64>() + PROLOGUE
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<_> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == "seg"))
        .collect();
    found.sort();
    found
}

/// Overwrite `len` bytes at `offset` in a file that is already there.
fn poke(path: &Path, offset: u64, bytes: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = OpenOptions::new().write(true).open(path).expect("open for poke");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    file.write_all(bytes).expect("write");
}

/// Fixed width payload, so every record in a test occupies exactly 64 bytes
/// and offsets can be reasoned about in records.
fn wide(version: Version, mark: char) -> Bytes {
    let mut body = format!("{mark} {version:>8} ").into_bytes();
    body.resize(48, b'.');
    Bytes::from(body)
}

fn wide_records(range: std::ops::RangeInclusive<Version>, mark: char) -> Vec<Record> {
    range.map(|version| Record { version, patch: wide(version, mark) }).collect()
}

/// A power cut in the middle of an unflushed batch can drop one page and keep
/// the ones behind it. Recovery stops at the hole, correctly, because none of
/// that batch was ever acknowledged. What it must not do is leave the records
/// above the hole sitting there: the log goes on appending, and when it lands
/// on the offset one of those leftovers starts at, that leftover is a whole,
/// correctly checksummed record at exactly the version the scan is expecting.
#[test]
fn a_crash_leaves_nothing_above_the_end_that_can_come_back() {
    const RECORD: u64 = 64;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&wide_records(1..=3, 'a')).expect("append");
    wal.sync().expect("sync");

    // Never flushed, so a crash is free to take any of it.
    wal.append(&wide_records(4..=200, 'a')).expect("append");
    let seg = segment_files(dir.path()).remove(0);
    let end = PROLOGUE + 3 * RECORD;
    lug_wal::fault::lose(&seg, end, 4096).expect("the first page of the batch was still dirty");
    lug_wal::fault::clear();
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    assert_eq!(versions(&wal.load().expect("load").tail), vec![1, 2, 3]);

    // 4096 bytes of hole is 64 records, so the batch that fills it stops
    // exactly where the first surviving leftover begins.
    wal.append(&wide_records(4..=67, 'b')).expect("append after the crash");
    wal.sync().expect("sync");
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert_eq!(
        versions(&recovered.tail),
        (1..=67).collect::<Vec<_>>(),
        "records that were never acknowledged came back from above the end"
    );
    for record in &recovered.tail[3..] {
        assert_eq!(record.patch, wide(record.version, 'b'), "version {}", record.version);
    }
}

/// The one that matters. A batch that fails partway has already put complete,
/// correctly checksummed records on disk. If they are left above the logical
/// end, the shorter batch that the caller retries with does not cover them,
/// and recovery reads the leftovers as records at the next contiguous version:
/// no checksum complaint, no gap, just a patch nobody was ever told about.
#[test]
fn a_failed_append_leaves_no_ghost_above_what_followed_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=3)).expect("append");

    // Two chunks of the batch land, 512 records each, then the third fails.
    lug_wal::fault::fail_writes(2, 1);
    let err = wal.append(&records(4..=3000)).expect_err("the injected failure must surface");
    lug_wal::fault::clear();
    assert!(matches!(err, Error::Io { .. }), "{err}");

    // The caller retries from the version it was left at, with less than it
    // tried to write the first time. Same patches, so the retry ends exactly
    // on one of the failed batch's record boundaries and the next leftover is
    // a whole, valid record at version 6.
    wal.append(&records(4..=5)).expect("retry");
    wal.sync().expect("sync");
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert_eq!(
        versions(&recovered.tail),
        vec![1, 2, 3, 4, 5],
        "a record that was never acknowledged came back from the dead"
    );
    assert_eq!(recovered.tail[4].patch, payload(5));

    // And the log carries on from there rather than from the ghost.
    wal.append(&records(6..=7)).expect("append after the ghost");
    assert_eq!(versions(&wal.read_after(5, 9).expect("read")), vec![6, 7]);
}

#[test]
fn a_store_that_cannot_clean_up_refuses_to_append_again() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=3)).expect("append");

    // The batch write fails and so does the zeroing that would erase it.
    lug_wal::fault::fail_writes(2, 2);
    let err = wal.append(&records(4..=3000)).expect_err("the injected failure must surface");
    assert!(matches!(err, Error::CleanupFailed { .. }), "{err}");

    let err = wal.append(&records(4..=5)).expect_err("a poisoned store must not append");
    assert!(matches!(err, Error::Poisoned { .. }), "{err}");

    // Reloading rescans, so the records that did land are adopted and the
    // store knows where it stands again.
    let tail = wal.load().expect("load").tail;
    let last = tail.last().expect("something landed").version;
    assert_eq!(versions(&tail), (1..=last).collect::<Vec<_>>());
    wal.append(&records(last + 1..=last + 2)).expect("append after reload");
}

#[test]
fn reopen_replays_every_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=200)).expect("append");
    wal.sync().expect("sync");
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert!(recovered.header.is_none());
    assert_eq!(versions(&recovered.tail), (1..=200).collect::<Vec<_>>());
    assert_eq!(recovered.tail[7].patch, payload(8));
    assert_eq!(wal.oldest(), 1);

    // The reopened store keeps counting where the file left off.
    wal.append(&records(201..=202)).expect("append after reopen");
    assert_eq!(versions(&wal.read_after(200, 10).expect("read")), vec![201, 202]);
}

#[test]
fn append_rejects_a_version_that_is_not_next() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=3)).expect("append");

    let err = wal.append(&records(5..=6)).expect_err("gap must be refused");
    assert!(matches!(err, Error::OutOfOrder { expected: 4, found: 5 }), "{err}");
}

#[test]
fn a_torn_record_is_cut_off_and_everything_before_it_survives() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=40)).expect("append");
    wal.sync().expect("sync");
    drop(wal);

    // Cut three bytes out of record 40's payload, as a power cut mid write
    // would have.
    let seg = segment_files(dir.path()).remove(0);
    let end = data_end(1, 40);
    OpenOptions::new().write(true).open(&seg).expect("open").set_len(end - 3).expect("truncate");

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert_eq!(versions(&recovered.tail), (1..=39).collect::<Vec<_>>());

    // Recovery cut the partial record away, so version 40 is free again and
    // the next append lands where it used to be.
    wal.append(&records(40..=41)).expect("append over the torn tail");
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    assert_eq!(versions(&wal.load().expect("load").tail), (1..=41).collect::<Vec<_>>());
}

#[test]
fn a_bad_checksum_discards_the_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=10)).expect("append");
    wal.sync().expect("sync");
    drop(wal);

    // One bit flip inside record 9's payload; record 10 is intact but
    // unreachable behind it.
    let seg = segment_files(dir.path()).remove(0);
    poke(&seg, data_end(1, 8) + PREFIX + 2, b"X");

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert_eq!(versions(&recovered.tail), (1..=8).collect::<Vec<_>>());

    // Record 10 was intact but sat behind the damage. It is gone, and its
    // version is reused rather than skipped.
    wal.append(&records(9..=10)).expect("append");
    drop(wal);
    let mut wal = store(dir.path(), 1 << 16);
    assert_eq!(versions(&wal.load().expect("load").tail), (1..=10).collect::<Vec<_>>());
}

#[test]
fn a_version_gap_is_corruption_not_a_torn_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let seg = dir.path().join("000000000000000001.seg");
    let mut bytes = b"LUGSEG\x00\x01".to_vec();
    bytes.extend_from_slice(&[0u8; 8]);
    for version in [1u64, 2, 4] {
        let body = payload(version);
        let mut crc = crc32fast::Hasher::new();
        crc.update(&version.to_le_bytes());
        crc.update(&body);
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc.finalize().to_le_bytes());
        bytes.extend_from_slice(&version.to_le_bytes());
        bytes.extend_from_slice(&body);
    }
    std::fs::write(&seg, &bytes).expect("write segment");

    let mut wal = store(dir.path(), 1 << 16);
    // `Recovered` is not `Debug`, so `expect_err` is out of reach here.
    let Err(err) = wal.load() else {
        panic!("a gap must fail loudly");
    };
    assert!(matches!(err, Error::Gap { expected: 3, found: 4, .. }), "{err}");
}

#[test]
fn a_header_moves_where_replay_starts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=10)).expect("append");
    wal.write_header(&Mark(6)).expect("checkpoint");
    wal.sync().expect("sync");
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert_eq!(recovered.header, Some(Mark(6)));
    assert_eq!(versions(&recovered.tail), vec![7, 8, 9, 10]);

    // Nothing was reclaimed, so the covered records are still readable.
    assert_eq!(wal.oldest(), 1);
    assert_eq!(versions(&wal.read_after(0, 3).expect("read")), vec![1, 2, 3]);
    assert!(!dir.path().join("header.tmp").exists(), "the temporary header outlived the rename");
}

/// A checkpoint says "everything up to this version is in the view, do not
/// bother replaying it". Publishing that by rename before the records it
/// covers are on stable storage means a crash can leave the claim standing
/// over records that are not there.
#[test]
fn a_checkpoint_is_not_published_before_the_records_it_covers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=10)).expect("append");
    wal.sync().expect("sync");

    // Written, not flushed, which is the ordinary state of a log between
    // syncs.
    wal.append(&records(11..=20)).expect("append");
    wal.write_header(&Mark(20)).expect("checkpoint");

    let seg = segment_files(dir.path()).remove(0);
    lug_wal::fault::lose_unsynced(&seg).expect("lose whatever had not reached the platter");
    lug_wal::fault::clear();
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    let Ok(recovered) = wal.load() else {
        panic!("a header that outran its records left the log unopenable");
    };
    assert_eq!(recovered.header, Some(Mark(20)));
    assert!(recovered.tail.is_empty(), "nothing is above the checkpoint");
    assert_eq!(versions(&wal.read_after(10, 3).expect("read")), vec![11, 12, 13]);
    wal.append(&records(21..=22)).expect("the log carries on above the checkpoint");
}

#[test]
fn read_after_spans_segment_boundaries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 256);
    for version in 1..=60 {
        wal.append(&records(version..=version)).expect("append");
    }
    wal.sync().expect("sync");

    let rolled = segment_files(dir.path());
    assert!(rolled.len() >= 5, "expected several segments, got {}", rolled.len());

    assert_eq!(versions(&wal.read_after(0, 60).expect("read")), (1..=60).collect::<Vec<_>>());
    assert_eq!(versions(&wal.read_after(28, 5).expect("read")), vec![29, 30, 31, 32, 33]);
    assert_eq!(versions(&wal.read_after(58, 10).expect("read")), vec![59, 60]);
    assert!(wal.read_after(60, 10).expect("past the end").is_empty());

    drop(wal);
    let mut wal = store(dir.path(), 256);
    assert_eq!(versions(&wal.load().expect("load").tail), (1..=60).collect::<Vec<_>>());
}

#[test]
fn a_checkpoint_reclaims_the_segments_below_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 256);
    for version in 1..=60 {
        wal.append(&records(version..=version)).expect("append");
    }
    let before = segment_files(dir.path());

    wal.write_header(&Mark(40)).expect("checkpoint");
    let after = segment_files(dir.path());
    assert!(after.len() < before.len(), "nothing was reclaimed");

    let oldest = wal.oldest();
    assert!(oldest > 1 && oldest <= 41, "oldest {oldest} is not inside the retained segment");
    assert_eq!(versions(&wal.read_after(oldest - 1, 2).expect("read")), vec![oldest, oldest + 1]);

    let err = wal.read_after(0, 5).expect_err("reclaimed records must not read as empty");
    assert!(matches!(err, Error::Reclaimed { requested: 1, .. }), "{err}");

    drop(wal);
    let mut wal = store(dir.path(), 256);
    let recovered = wal.load().expect("load");
    assert_eq!(recovered.header, Some(Mark(40)));
    assert_eq!(versions(&recovered.tail), (41..=60).collect::<Vec<_>>());
    assert_eq!(wal.oldest(), oldest);
}

#[test]
fn an_empty_payload_is_refused_because_it_reads_as_the_end() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    let empty = Record { version: 1, patch: Bytes::new() };

    let err = wal.append(&[empty]).expect_err("a zero length record must be refused");
    assert!(matches!(err, Error::Empty { version: 1 }), "{err}");
}

#[test]
fn an_empty_log_recovers_to_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert!(recovered.header.is_none() && recovered.tail.is_empty());
    assert_eq!(wal.oldest(), 1);
    assert!(wal.read_after(0, 8).expect("read").is_empty());
    wal.sync().expect("sync with no segment open");
}

#[test]
fn one_append_carries_more_records_than_a_single_writev_can() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);

    // 3000 records is 6000 buffers, well past the 1024 the kernel takes per
    // call, and more bytes than the segment was preallocated for.
    wal.append(&records(1..=3000)).expect("append");
    wal.sync().expect("sync");
    assert_eq!(segment_files(dir.path()).len(), 1, "a batch must not be split");
    drop(wal);

    let mut wal = store(dir.path(), 1 << 16);
    let recovered = wal.load().expect("load");
    assert_eq!(versions(&recovered.tail), (1..=3000).collect::<Vec<_>>());
    assert_eq!(recovered.tail[2999].patch, payload(3000));
}

#[test]
fn a_corrupt_header_is_refused_rather_than_ignored() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=4)).expect("append");
    wal.write_header(&Mark(2)).expect("checkpoint");
    drop(wal);

    // One byte of the json. A header that cannot be trusted must not be
    // silently replaced by a full replay: the view it names may already be
    // serving readers.
    poke(&dir.path().join("header"), 17, b"X");

    let err = SegmentStore::<Mark>::open_with(dir.path(), Options { rotate_bytes: 1 << 16 })
        .err()
        .expect("a bad header checksum must fail loudly");
    assert!(matches!(err, Error::HeaderChecksum { .. }), "{err}");
}

#[test]
fn a_segment_is_sized_before_it_is_written() {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 1 << 16);
    wal.append(&records(1..=3)).expect("append");
    wal.sync().expect("sync");

    // Records land inside a region that already has its size and its blocks,
    // which is what lets sync be fdatasync and touch no metadata.
    let meta = std::fs::metadata(segment_files(dir.path()).remove(0)).expect("stat");
    assert_eq!(meta.len(), 1 << 16, "the segment was not sized up front");
    assert!(meta.blocks() * 512 >= data_end(1, 3), "no blocks were reserved");
}

/// Segment names are the first version inside them, zero padded to eighteen
/// digits, and the parser insists on exactly eighteen. A log that rotates at
/// or above 10^18 therefore writes a segment whose name it cannot read back:
/// on the next open the file is invisible, its records are gone, and the very
/// next append opens it again and writes a fresh prologue over them.
#[test]
fn a_segment_named_past_eighteen_digits_is_still_found() {
    const HIGH: Version = 999_999_999_999_999_998;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wal = store(dir.path(), 32);
    wal.write_header(&Mark(HIGH - 1)).expect("checkpoint the log up near the end of the counter");

    // Rotation is per append here, so every version lands in a segment of its
    // own and the third one crosses into nineteen digits.
    for version in HIGH..=HIGH + 2 {
        wal.append(&records(version..=version)).expect("append");
    }
    wal.sync().expect("sync");
    assert_eq!(segment_files(dir.path()).len(), 3);
    drop(wal);

    let mut wal = store(dir.path(), 32);
    let recovered = wal.load().expect("load");
    assert_eq!(
        versions(&recovered.tail),
        (HIGH..=HIGH + 2).collect::<Vec<_>>(),
        "an acknowledged record went missing with the segment holding it"
    );
    wal.append(&records(HIGH + 3..=HIGH + 3)).expect("append");
    drop(wal);

    let mut wal = store(dir.path(), 32);
    assert_eq!(versions(&wal.load().expect("load").tail), (HIGH..=HIGH + 3).collect::<Vec<_>>());
}
