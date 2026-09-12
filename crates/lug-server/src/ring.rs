//! Recent records, shared by every subscriber of one log.

use lug_core::Version;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

/// A record as subscribers see it. `bytes` is the encoded size, kept so a
/// push can be capped by frame size without serializing the patch twice.
#[derive(Debug)]
pub struct Entry {
    pub version: Version,
    pub patch: Value,
    pub bytes: usize,
}

/// The fan-out buffer: one wakeup lets a subscriber take a whole range.
///
/// The log actor is the only writer and takes the lock once per committed
/// batch, so the serialization point stays the actor's inbox. Readers hold
/// the lock only long enough to clone a run of `Arc`s, never across an await.
pub struct Ring {
    capacity: usize,
    entries: RwLock<VecDeque<Arc<Entry>>>,
}

pub enum Read {
    Records(Vec<Arc<Entry>>),
    /// The cursor is below the oldest entry still held; storage must serve it.
    TooOld,
}

impl Ring {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self { capacity, entries: RwLock::new(VecDeque::with_capacity(capacity)) }
    }

    pub fn push(&self, batch: &[Arc<Entry>]) {
        let mut entries = self.write();
        for entry in batch {
            if entries.len() == self.capacity {
                entries.pop_front();
            }
            entries.push_back(entry.clone());
        }
    }

    pub fn oldest(&self) -> Option<Version> {
        self.read().front().map(|e| e.version)
    }

    /// Records in `(after, after + limit]`, stopping early at `max_bytes`.
    pub fn read_after(&self, after: Version, limit: usize, max_bytes: usize) -> Read {
        let entries = self.read();
        let Some(front) = entries.front() else {
            return Read::Records(Vec::new());
        };
        if after + 1 < front.version {
            return Read::TooOld;
        }
        let skip = (after + 1 - front.version) as usize;
        let mut out = Vec::new();
        let mut bytes = 0;
        for entry in entries.iter().skip(skip).take(limit) {
            bytes += entry.bytes;
            out.push(entry.clone());
            if bytes >= max_bytes {
                break;
            }
        }
        Read::Records(out)
    }

    // A panicking holder must not wedge every subscriber of the log; the ring
    // is a cache and its worst case is a stale read.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, VecDeque<Arc<Entry>>> {
        self.entries.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, VecDeque<Arc<Entry>>> {
        self.entries.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// Encoded size of a JSON value, without encoding it.
pub fn estimate(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(b) => {
            if *b {
                4
            } else {
                5
            }
        }
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(items) => 2 + items.iter().map(|v| estimate(v) + 1).sum::<usize>(),
        Value::Object(fields) => {
            2 + fields.iter().map(|(k, v)| k.len() + 4 + estimate(v)).sum::<usize>()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(version: Version) -> Arc<Entry> {
        Arc::new(Entry { version, patch: Value::Null, bytes: 4 })
    }

    #[test]
    fn reads_a_range_and_evicts_the_front() {
        let ring = Ring::new(4);
        let batch: Vec<_> = (1..=6).map(entry).collect();
        ring.push(&batch);
        assert_eq!(ring.oldest(), Some(3));
        assert!(matches!(ring.read_after(1, 10, usize::MAX), Read::TooOld));
        let Read::Records(got) = ring.read_after(3, 10, usize::MAX) else {
            panic!("expected records");
        };
        assert_eq!(got.iter().map(|e| e.version).collect::<Vec<_>>(), vec![4, 5, 6]);
    }

    #[test]
    fn byte_cap_cuts_the_run_short() {
        let ring = Ring::new(8);
        let batch: Vec<_> = (1..=8).map(entry).collect();
        ring.push(&batch);
        let Read::Records(got) = ring.read_after(0, 8, 9) else { panic!("expected records") };
        assert_eq!(got.len(), 3);
    }
}
