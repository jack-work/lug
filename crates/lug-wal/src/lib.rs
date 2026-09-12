//! A [`Storage`] made of segment files.
//!
//! A log directory holds segments named for the first version inside them and
//! one header file checkpointing a view. Appends are one vectored write per
//! batch into a preallocated segment, so a flush is [`fdatasync`] over data
//! that already has its blocks and its size. Recovery scans forward from the
//! header, stops at the torn tail a crash leaves behind, and truncates there.
//!
//! The invariant everything else hangs off: **nothing above the logical end of
//! a segment may be recoverable as a record.** Preallocation gives it for free,
//! since a zero record length reads as the end. A failed append restores it by
//! zeroing what it wrote, because the next batch may be shorter and would
//! otherwise leave the old tail exposed as a valid record at the next version.
//! If that zeroing itself fails the store is poisoned: further appends are
//! refused with [`Error::Poisoned`] until it is reopened or reloaded, which
//! rescans and settles where the records really end.
//!
//! Two behaviours that tend to surprise:
//!
//! - A batch is never split across segments, so rotation happens *before* a
//!   batch and a segment can overshoot [`DEFAULT_ROTATE_BYTES`] by one batch.
//!   One enormous append therefore produces one enormous segment, and since
//!   the segment being written is never reclaimed, a checkpoint over it frees
//!   nothing.
//! - [`oldest`](Storage::oldest) on a log with no segments reports
//!   `header version + 1`, the version that will be written next. Nothing is
//!   readable, and nothing below that will ever be.
//!
//! [`fdatasync`]: sys::fdatasync

mod format;
mod segment;
mod sys;

#[cfg(feature = "fault-injection")]
pub use sys::fault;

use bytes::Bytes;
use format::{
    HDR_MAGIC, REC_PREFIX, SEG_PROLOGUE, header_bytes, parse_segment_name, record_prefix,
    segment_name, segment_prologue,
};
use lug_core::{Record, Recovered, Storage, Version, Versioned};
use segment::Segment;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{IoSlice, Write};
use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

/// Rotate once a segment reaches this size. A batch is never split, so a
/// segment may end slightly above it.
pub const DEFAULT_ROTATE_BYTES: u64 = 64 << 20;

const HEADER: &str = "header";
const HEADER_TMP: &str = "header.tmp";

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub rotate_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self { rotate_bytes: DEFAULT_ROTATE_BYTES }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{0}")]
    Bare(#[from] std::io::Error),
    #[error("{path} does not start with the segment magic")]
    NotASegment { path: PathBuf },
    #[error("{path} does not start with the header magic")]
    NotAHeader { path: PathBuf },
    #[error("header checksum is {found:#010x}, computed {computed:#010x} over {len} bytes")]
    HeaderChecksum { found: u32, computed: u32, len: usize },
    #[error("header is {len} bytes, too short to hold its own frame")]
    ShortHeader { len: usize },
    #[error("header says version {declared} but carries a view at {view}")]
    HeaderDisagrees { declared: Version, view: Version },
    #[error("header is at version {header}, above the last record {last}")]
    HeaderAhead { header: Version, last: Version },
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("version gap in {path}: expected {expected}, found {found}")]
    Gap { path: PathBuf, expected: Version, found: Version },
    #[error("{path} is named for version {expected} but starts at {found}")]
    Misnamed { path: PathBuf, expected: Version, found: Version },
    #[error("torn record in {path}, which is not the last segment")]
    TornMidLog { path: PathBuf },
    #[error("append expected version {expected}, got {found}")]
    OutOfOrder { expected: Version, found: Version },
    #[error("payload of {len} bytes at version {version} exceeds the limit of {max}")]
    Oversized { version: Version, len: usize, max: u32 },
    #[error("version {requested} was reclaimed; oldest readable is {oldest}")]
    Reclaimed { requested: Version, oldest: Version },
    #[error(
        "version {version} carries an empty payload, which reads back as the end of the segment"
    )]
    Empty { version: Version },
    #[error(
        "{path}: an append failed at offset {offset} and the bytes above it could not be zeroed: {source}"
    )]
    CleanupFailed {
        path: PathBuf,
        offset: u64,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "{path} is poisoned: bytes above offset {offset} may replay as records nobody was told about, so appending is refused until the store is reopened"
    )]
    Poisoned { path: PathBuf, offset: u64 },
    #[error("{dir} is already open: a log directory has exactly one writer")]
    Locked { dir: PathBuf },
    #[error("version {version} is the last the counter holds, and the log needs a version above it")]
    Exhausted { version: Version },
}

impl Error {
    fn at(path: impl AsRef<Path>) -> impl FnOnce(std::io::Error) -> Error {
        let path = path.as_ref().to_path_buf();
        move |source| Error::Io { path, source }
    }
}

/// Where an append failed to clean up after itself.
#[derive(Clone, Debug)]
struct Poison {
    path: PathBuf,
    offset: u64,
}

/// The checkpoint, as it appears inside the header file.
#[derive(Serialize, Deserialize)]
// Serialized from a borrowed view and deserialized into an owned one, so the
// two halves do not carry the same bound.
#[serde(bound(serialize = "V: Serialize", deserialize = "V: Versioned"))]
struct Header<V> {
    version: Version,
    view: V,
}

/// [`Storage`] over a directory of segments.
pub struct SegmentStore<V: Versioned> {
    dir: PathBuf,
    /// Held open so renames and unlinks can be made durable with one fsync.
    dir_handle: File,
    segments: Vec<Segment>,
    /// Write handle on the last segment, the only one ever written.
    active: Option<File>,
    /// Bytes preallocated in the active segment.
    capacity: u64,
    header_version: Version,
    rotate_bytes: u64,
    /// Whether the torn tail has been found. Appending before that would
    /// write over a record, or into the middle of one.
    recovered: bool,
    /// Set when a failed append could not erase what it had written. Every
    /// later append is refused, because a shorter one would leave those bytes
    /// readable above it.
    poison: Option<Poison>,
    view: PhantomData<fn() -> V>,
}

impl<V: Versioned> SegmentStore<V> {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, Error> {
        Self::open_with(dir, Options::default())
    }

    pub fn open_with(dir: impl Into<PathBuf>, options: Options) -> Result<Self, Error> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(Error::at(&dir))?;
        let dir_handle = File::open(&dir).map_err(Error::at(&dir))?;
        // Each store keeps its own idea of where the records end, so a second
        // one writes over the tail the first already acknowledged, and the
        // records in between are lost with nothing to show for it.
        if !sys::try_lock_dir(&dir_handle).map_err(Error::at(&dir))? {
            return Err(Error::Locked { dir });
        }
        let mut store = Self {
            dir,
            dir_handle,
            segments: Vec::new(),
            active: None,
            capacity: 0,
            header_version: 0,
            rotate_bytes: options.rotate_bytes.max((SEG_PROLOGUE + REC_PREFIX) as u64),
            recovered: false,
            poison: None,
            view: PhantomData,
        };
        store.list_segments()?;
        store.header_version = store.read_header()?.map_or(0, |h| h.version);
        Ok(store)
    }

    /// Next version this log will accept.
    fn next_version(&self) -> Version {
        self.segments.last().map_or(self.header_version, |s| s.last) + 1
    }

    fn list_segments(&mut self) -> Result<(), Error> {
        let mut segments = Vec::new();
        for entry in std::fs::read_dir(&self.dir).map_err(Error::at(&self.dir))? {
            let entry = entry.map_err(Error::at(&self.dir))?;
            let name = entry.file_name();
            let Some(first) = name.to_str().and_then(parse_segment_name) else {
                continue;
            };
            let path = entry.path();
            segments.push(Segment { first, path, end: 0, last: first.saturating_sub(1) });
        }
        segments.sort_by_key(|s| s.first);
        self.segments = segments;
        Ok(())
    }

    fn read_header(&self) -> Result<Option<Header<V>>, Error> {
        let path = self.dir.join(HEADER);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::at(&path)(e)),
        };
        if bytes.len() < 16 {
            return Err(Error::ShortHeader { len: bytes.len() });
        }
        if bytes[..8] != HDR_MAGIC {
            return Err(Error::NotAHeader { path });
        }
        let found = u32::from_le_bytes(bytes[8..12].try_into().expect("4 bytes"));
        let len = u32::from_le_bytes(bytes[12..16].try_into().expect("4 bytes")) as usize;
        let json = bytes.get(16..16 + len).ok_or(Error::ShortHeader { len: bytes.len() })?;
        let computed = crc32fast::hash(json);
        if computed != found {
            return Err(Error::HeaderChecksum { found, computed, len });
        }
        let header: Header<V> = serde_json::from_slice(json)?;
        if header.version != header.view.version() {
            return Err(Error::HeaderDisagrees {
                declared: header.version,
                view: header.view.version(),
            });
        }
        Ok(Some(header))
    }

    /// Scan forward over every segment, truncating a torn tail, and collect
    /// the records above the header when asked.
    fn recover(&mut self, collect: bool) -> Result<Vec<Record>, Error> {
        let header_version = self.header_version;
        let last_index = self.segments.len().saturating_sub(1);
        let mut tail: Vec<Record> = Vec::new();
        let mut expect: Option<Version> = None;

        for index in 0..self.segments.len() {
            let path = self.segments[index].path.clone();
            let first = self.segments[index].first;
            let scan = segment::scan(&path, |version, payload| {
                match expect {
                    Some(e) if version != e => {
                        return Err(Error::Gap { path: path.clone(), expected: e, found: version });
                    }
                    None if version != first => {
                        return Err(Error::Misnamed {
                            path: path.clone(),
                            expected: first,
                            found: version,
                        });
                    }
                    _ => {}
                }
                expect = Some(version + 1);
                if collect && version > header_version {
                    tail.push(Record { version, patch: Bytes::copy_from_slice(payload) });
                }
                Ok(ControlFlow::Continue(()))
            })?;

            if scan.torn && index != last_index {
                return Err(Error::TornMidLog { path });
            }
            // Not only for the torn tail. A crash can drop one page of an
            // unflushed batch and keep the pages behind it, which leaves whole
            // records above the hole the scan stopped at. Left in place they
            // are unreachable only until the log appends back up to the offset
            // one of them starts at, and then it reads as the very record the
            // scan is expecting next. Cutting here, with preallocation putting
            // zeroes back afterwards, is what makes the end of a segment mean
            // the end.
            if scan.file_len > scan.end {
                truncate(&path, scan.end)?;
            }
            let segment = &mut self.segments[index];
            segment.end = scan.end;
            segment.last = expect.map_or(first.saturating_sub(1), |e| e - 1);
        }

        if let Some(segment) = self.segments.last() {
            if segment.last < header_version {
                return Err(Error::HeaderAhead { header: header_version, last: segment.last });
            }
            let file = open_rw(&segment.path)?;
            self.capacity = segment.end.max(self.rotate_bytes);
            sys::preallocate(&file, self.capacity).map_err(Error::at(&segment.path))?;
            self.active = Some(file);
        }
        if let Some(first) = tail.first()
            && first.version != header_version + 1
        {
            return Err(Error::Gap {
                path: self.dir.join(HEADER),
                expected: header_version + 1,
                found: first.version,
            });
        }
        self.recovered = true;
        // The scan just established where the records really end, so whatever
        // an earlier failure left behind is either adopted or unreachable.
        self.poison = None;
        Ok(tail)
    }

    fn ensure_recovered(&mut self) -> Result<(), Error> {
        if !self.recovered {
            self.recover(false)?;
        }
        Ok(())
    }

    /// Close the active segment at its true length and start one named for
    /// `first`. The directory entry is made durable before anything is
    /// written into it.
    fn rotate(&mut self, first: Version) -> Result<(), Error> {
        if let (Some(file), Some(segment)) = (self.active.take(), self.segments.last()) {
            file.set_len(segment.end).map_err(Error::at(&segment.path))?;
            sys::fdatasync(&file).map_err(Error::at(&segment.path))?;
        }
        let path = self.dir.join(segment_name(first));
        let file = open_rw(&path)?;
        sys::preallocate(&file, self.rotate_bytes).map_err(Error::at(&path))?;
        let prologue = segment_prologue();
        sys::pwritev_all(&file, &mut [IoSlice::new(&prologue)], 0).map_err(Error::at(&path))?;
        sys::fsync(&file).map_err(Error::at(&path))?;
        self.sync_dir()?;

        self.segments.push(Segment { first, path, end: prologue.len() as u64, last: first - 1 });
        self.capacity = self.rotate_bytes;
        self.active = Some(file);
        Ok(())
    }

    fn sync_dir(&self) -> Result<(), Error> {
        sys::fsync(&self.dir_handle).map_err(Error::at(&self.dir))
    }

    /// Drop every segment whose records are all covered by the header. The
    /// segment being appended to stays regardless of its versions.
    fn reclaim(&mut self) -> Result<(), Error> {
        let mut removed = false;
        while self.segments.len() > 1 && self.segments[1].first <= self.header_version + 1 {
            let segment = self.segments.remove(0);
            std::fs::remove_file(&segment.path).map_err(Error::at(&segment.path))?;
            removed = true;
        }
        if removed {
            self.sync_dir()?;
        }
        Ok(())
    }

    /// Index of the segment that would hold `version`, if any does.
    fn segment_for(&self, version: Version) -> Option<usize> {
        self.segments.iter().rposition(|s| s.first <= version)
    }
}

fn open_rw(path: &Path) -> Result<File, Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(Error::at(path))
}

/// Cut a segment back to its last intact record and make the cut durable, so
/// a second crash recovers to the same place.
fn truncate(path: &Path, len: u64) -> Result<(), Error> {
    let file = open_rw(path)?;
    file.set_len(len).map_err(Error::at(path))?;
    sys::fsync(&file).map_err(Error::at(path))
}

impl<V: Versioned> Storage for SegmentStore<V> {
    type View = V;
    type Error = Error;

    fn append(&mut self, records: &[Record]) -> Result<(), Error> {
        if records.is_empty() {
            return Ok(());
        }
        if let Some(poison) = &self.poison {
            return Err(Error::Poisoned { path: poison.path.clone(), offset: poison.offset });
        }
        self.ensure_recovered()?;

        let mut expected = self.next_version();
        for record in records {
            if record.version != expected {
                return Err(Error::OutOfOrder { expected, found: record.version });
            }
            if record.patch.is_empty() {
                return Err(Error::Empty { version: record.version });
            }
            if record.patch.len() as u64 > u64::from(format::MAX_PAYLOAD) {
                return Err(Error::Oversized {
                    version: record.version,
                    len: record.patch.len(),
                    max: format::MAX_PAYLOAD,
                });
            }
            // Taking the top of the counter would leave the store with no
            // next version to name, and every offset arithmetic below assumes
            // one exists.
            let Some(next) = expected.checked_add(1) else {
                return Err(Error::Exhausted { version: record.version });
            };
            expected = next;
        }
        let rotating = match self.segments.last() {
            Some(segment) => segment.end >= self.rotate_bytes,
            None => true,
        };
        if rotating || self.active.is_none() {
            self.rotate(records[0].version)?;
        }

        let mut prefixes = Vec::with_capacity(records.len() * REC_PREFIX);
        for record in records {
            prefixes.extend_from_slice(&record_prefix(record.version, &record.patch));
        }
        let mut bufs = Vec::with_capacity(records.len() * 2);
        let mut total = 0u64;
        for (i, record) in records.iter().enumerate() {
            bufs.push(IoSlice::new(&prefixes[i * REC_PREFIX..(i + 1) * REC_PREFIX]));
            bufs.push(IoSlice::new(&record.patch));
            total += (REC_PREFIX + record.patch.len()) as u64;
        }

        let segment = self.segments.last_mut().expect("rotate left a segment");
        let file = self.active.as_ref().expect("rotate left a handle");
        if segment.end + total > self.capacity {
            let want = (segment.end + total).next_multiple_of(self.rotate_bytes);
            sys::preallocate(file, want).map_err(Error::at(&segment.path))?;
            self.capacity = want;
        }
        if let Err(source) = sys::pwritev_all(file, &mut bufs, segment.end) {
            // Whatever landed sits above the logical end, where the next,
            // possibly shorter, batch will not reach it. Left there it is a
            // well formed record at the next contiguous version: no checksum
            // and no gap check would catch it, and recovery would replay a
            // patch the caller was told had failed. Zeroing it puts the file
            // back to what preallocation promised.
            let (offset, len) = (segment.end, total);
            let path = segment.path.clone();
            let cleaned = sys::zero_range(file, offset, len).and_then(|()| sys::fdatasync(file));
            return match cleaned {
                Ok(()) => Err(Error::Io { path, source }),
                Err(source) => {
                    self.poison = Some(Poison { path: path.clone(), offset });
                    Err(Error::CleanupFailed { path, offset, source })
                }
            };
        }
        segment.end += total;
        segment.last = records.last().expect("non-empty").version;
        Ok(())
    }

    fn write_header(&mut self, view: &V) -> Result<(), Error> {
        // A header is a claim that recovery may skip every record below it, so
        // those records have to be on stable storage before the name that
        // covers them exists. Published first, it survives a crash that the
        // records it stands over do not.
        self.sync()?;

        let version = view.version();
        let json = serde_json::to_vec(&Header { version, view })?;
        let bytes = header_bytes(&json);
        let tmp = self.dir.join(HEADER_TMP);

        // The rename is what publishes it, so the bytes have to be on disk
        // before the name points at them.
        let mut file = File::create(&tmp).map_err(Error::at(&tmp))?;
        file.write_all(&bytes).map_err(Error::at(&tmp))?;
        sys::fsync(&file).map_err(Error::at(&tmp))?;
        drop(file);
        sys::rename_at(&self.dir_handle, HEADER_TMP, HEADER).map_err(Error::at(&self.dir))?;
        self.sync_dir()?;

        self.header_version = version;
        self.reclaim()
    }

    fn sync(&mut self) -> Result<(), Error> {
        match (&self.active, self.segments.last()) {
            (Some(file), Some(segment)) => sys::fdatasync(file).map_err(Error::at(&segment.path)),
            _ => Ok(()),
        }
    }

    fn load(&mut self) -> Result<Recovered<V>, Error> {
        self.active = None;
        self.list_segments()?;
        let header = self.read_header()?;
        self.header_version = header.as_ref().map_or(0, |h| h.version);
        let tail = self.recover(true)?;
        Ok(Recovered { header: header.map(|h| h.view), tail })
    }

    fn read_after(&self, after: Version, limit: usize) -> Result<Vec<Record>, Error> {
        let start = after.saturating_add(1);
        let oldest = self.oldest();
        if start < oldest {
            return Err(Error::Reclaimed { requested: start, oldest });
        }
        let mut out = Vec::new();
        if limit == 0 {
            return Ok(out);
        }
        let Some(from) = self.segment_for(start) else {
            return Ok(out);
        };
        for segment in &self.segments[from..] {
            segment::scan(&segment.path, |version, payload| {
                if version >= start {
                    out.push(Record { version, patch: Bytes::copy_from_slice(payload) });
                }
                Ok(if out.len() >= limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            })?;
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    fn oldest(&self) -> Version {
        self.segments.first().map_or(self.header_version.saturating_add(1), |s| s.first)
    }
}
