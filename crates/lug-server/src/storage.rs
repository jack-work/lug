//! Storage the daemon can open, and the in-memory one it ships with.
//!
//! The daemon is generic over this factory so it can be driven by segment
//! files or by memory without knowing which. Two log shapes exist, so the
//! factory has two methods rather than a generic one: a plain byte log over
//! [`Noop`](lug_core::Noop), and a reducible log over [`cavlc::Store`].

use cavlc::Snapshot;
use lug_core::{Record, Recovered, Storage, Tick, Version, Versioned};
use std::collections::VecDeque;

pub trait StorageFactory: Send + Sync + 'static {
    type Plain: Storage<View = Tick>;
    type Reducible: Storage<View = Snapshot>;
    type Error: std::error::Error + Send + Sync + 'static;

    fn plain(&self, log: &str) -> Result<Self::Plain, Self::Error>;
    fn reducible(&self, log: &str) -> Result<Self::Reducible, Self::Error>;

    /// Record the shape of a new log, so a restart reopens it the same way.
    fn remember(&self, log: &str, reducible: bool) -> Result<(), Self::Error> {
        let _ = (log, reducible);
        Ok(())
    }

    /// Logs already on disk and the shape each was created with. A daemon
    /// that forgot these would answer NoSuchLog for data it is holding.
    fn discover(&self) -> Result<Vec<(String, bool)>, Self::Error> {
        Ok(Vec::new())
    }
}

/// Segment files under `<data>/<log>`, one directory per log.
#[derive(Debug, Clone)]
pub struct Segments {
    root: std::path::PathBuf,
    options: lug_wal::Options,
}

impl Segments {
    pub fn new(root: impl Into<std::path::PathBuf>, rotate_bytes: u64) -> Self {
        Self { root: root.into(), options: lug_wal::Options { rotate_bytes } }
    }
}

impl StorageFactory for Segments {
    type Plain = lug_wal::SegmentStore<Tick>;
    type Reducible = lug_wal::SegmentStore<Snapshot>;
    type Error = lug_wal::Error;

    fn plain(&self, log: &str) -> Result<Self::Plain, Self::Error> {
        lug_wal::SegmentStore::open_with(self.root.join(log), self.options)
    }

    fn reducible(&self, log: &str) -> Result<Self::Reducible, Self::Error> {
        lug_wal::SegmentStore::open_with(self.root.join(log), self.options)
    }

    fn remember(&self, log: &str, reducible: bool) -> Result<(), Self::Error> {
        let dir = self.root.join(log);
        std::fs::create_dir_all(&dir)?;
        let kind = if reducible { "reducible\n" } else { "plain\n" };
        let tmp = dir.join("kind.tmp");
        std::fs::write(&tmp, kind)?;
        std::fs::rename(&tmp, dir.join(KIND))?;
        Ok(())
    }

    fn discover(&self) -> Result<Vec<(String, bool)>, Self::Error> {
        let mut found = Vec::new();
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(found),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let kind = std::fs::read_to_string(entry.path().join(KIND)).unwrap_or_default();
            found.push((name, kind.trim() == "reducible"));
        }
        found.sort();
        Ok(found)
    }
}

/// Names the data structure a log folds with, next to its segments.
const KIND: &str = "kind";

/// Storage that keeps the last `retain` records in memory and forgets the
/// rest, so retention and `Gap` are exercised without touching a disk.
#[derive(Debug)]
pub struct Memory<V> {
    records: VecDeque<Record>,
    header: Option<V>,
    retain: usize,
    oldest: Version,
}

impl<V> Memory<V> {
    pub fn new(retain: usize) -> Self {
        Self { records: VecDeque::new(), header: None, retain: retain.max(1), oldest: 1 }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("memory storage: {0}")]
pub struct MemoryError(String);

impl<V: Versioned> Storage for Memory<V> {
    type View = V;
    type Error = MemoryError;

    fn append(&mut self, records: &[Record]) -> Result<(), MemoryError> {
        for record in records {
            if self.records.len() == self.retain {
                if let Some(dropped) = self.records.pop_front() {
                    self.oldest = dropped.version + 1;
                }
            }
            self.records.push_back(record.clone());
        }
        Ok(())
    }

    fn write_header(&mut self, view: &V) -> Result<(), MemoryError> {
        self.header = Some(view.clone());
        Ok(())
    }

    fn sync(&mut self) -> Result<(), MemoryError> {
        Ok(())
    }

    fn load(&mut self) -> Result<Recovered<V>, MemoryError> {
        let after = self.header.as_ref().map(|v| v.version()).unwrap_or_default();
        let tail =
            self.records.iter().filter(|r| r.version > after).cloned().collect::<Vec<_>>();
        Ok(Recovered { header: self.header.clone(), tail })
    }

    fn read_after(&self, after: Version, limit: usize) -> Result<Vec<Record>, MemoryError> {
        Ok(self
            .records
            .iter()
            .filter(|r| r.version > after)
            .take(limit)
            .cloned()
            .collect())
    }

    fn oldest(&self) -> Version {
        self.records.front().map(|r| r.version).unwrap_or(self.oldest)
    }
}

/// Opens a fresh [`Memory`] per log. The daemon's default until segment
/// storage is wired in.
#[derive(Debug, Clone, Copy)]
pub struct MemoryFactory {
    pub retain: usize,
}

impl MemoryFactory {
    pub fn new(retain: usize) -> Self {
        Self { retain }
    }
}

impl Default for MemoryFactory {
    fn default() -> Self {
        Self { retain: 1 << 16 }
    }
}

impl StorageFactory for MemoryFactory {
    type Plain = Memory<Tick>;
    type Reducible = Memory<Snapshot>;
    type Error = MemoryError;

    fn plain(&self, _: &str) -> Result<Memory<Tick>, MemoryError> {
        Ok(Memory::new(self.retain))
    }

    fn reducible(&self, _: &str) -> Result<Memory<Snapshot>, MemoryError> {
        Ok(Memory::new(self.retain))
    }
}
