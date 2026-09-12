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
}

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
