use crate::{Error, Patch, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type Version = u64;

/// An immutable root and the version it represents, copied together.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    version: Version,
    root: Value,
}
impl Snapshot {
    /// Import a detached snapshot, as recovered from a checkpoint header.
    /// The root must be an object; the version is taken on trust.
    pub fn new(version: Version, root: Value) -> Result<Self, Error> {
        if root.as_object().is_none() {
            return Err(Error::RootMustBeObject);
        }
        Ok(Self { version, root })
    }
    pub fn version(&self) -> Version {
        self.version
    }
    pub fn root(&self) -> &Value {
        &self.root
    }
}

/// One batch, in application order. Versions start at 1.
/// Its prior state is snapshot `version - 1`, not duplicated in the patch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    pub version: Version,
    pub patches: Vec<Patch>,
}

#[derive(Clone, Debug)]
pub struct ApplyResult {
    pub snapshot: Snapshot,
    /// None when no submitted patch changed the working state.
    pub record: Option<Commit>,
}

/// Private working state. Failed patches leave earlier successful edits intact.
/// Dropping this value aborts the batch. A commit consumes it.
#[derive(Debug)]
pub struct Batch {
    owner: Arc<()>,
    base: Snapshot,
    working: Value,
    patches: Vec<Patch>,
}
impl Batch {
    pub fn base(&self) -> &Snapshot {
        &self.base
    }
    pub fn root(&self) -> &Value {
        &self.working
    }
    pub fn apply(&mut self, patch: &Patch) -> Result<(), Error> {
        let next = patch.apply(&self.working)?;
        if !next.ptr_eq(&self.working) {
            self.patches.push(patch.clone());
            self.working = next;
        }
        Ok(())
    }
}

/// One single-writer concurrency domain. All history stays in memory until dropped.
/// No locks or runtime are required. Use external synchronization if sharing
/// the writer; owned snapshots are immutable and may be read independently.
#[derive(Debug)]
pub struct Store {
    owner: Arc<()>,
    /// Version of `snapshots[0]`. Nonzero only after resuming a checkpoint.
    base: Version,
    snapshots: Vec<Snapshot>,
    log: Vec<Commit>,
}
impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
impl Store {
    pub fn new() -> Self {
        Self::from_value(Value::default()).expect("empty object")
    }
    /// Initial state is version 0 and is not recorded in the patch log.
    pub fn from_value(root: Value) -> Result<Self, Error> {
        if root.as_object().is_none() {
            return Err(Error::RootMustBeObject);
        }
        Ok(Self {
            owner: Arc::new(()),
            base: 0,
            snapshots: vec![Snapshot { version: 0, root }],
            log: Vec::new(),
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        self.snapshots.last().unwrap().clone()
    }
    /// The oldest version still retained. Nonzero after a resume or truncate
    /// below the base.
    pub fn base(&self) -> Version {
        self.base
    }
    pub fn snapshot_at(&self, version: Version) -> Option<Snapshot> {
        let index = usize::try_from(version.checked_sub(self.base)?).ok()?;
        self.snapshots.get(index).cloned()
    }
    pub fn log(&self) -> &[Commit] {
        &self.log
    }
    /// Read the half-open version range (after, through]. Invalid bounds fail.
    pub fn patches_between(&self, after: Version, through: Version) -> Option<&[Commit]> {
        if after > through || after < self.base {
            return None;
        }
        let start = usize::try_from(after - self.base).ok()?;
        let end = usize::try_from(through.checked_sub(self.base)?).ok()?;
        self.log.get(start..end)
    }
    /// Discard every version above `version`, making it current again.
    /// O(discarded); retained snapshots keep sharing their structure. A
    /// version that was never reached is a no-op. Outstanding batches based
    /// on a discarded version are rejected on publication, as always.
    pub fn truncate(&mut self, version: Version) {
        let Some(keep) = version.checked_sub(self.base).and_then(|n| usize::try_from(n).ok())
        else {
            return;
        };
        if keep + 1 >= self.snapshots.len() {
            return;
        }
        self.snapshots.truncate(keep + 1);
        self.log.truncate(keep);
    }

    /// Resume from a checkpointed snapshot. Its version becomes the base, and
    /// the returned store has no history below it.
    pub fn resume(snapshot: Snapshot) -> Self {
        Self {
            owner: Arc::new(()),
            base: snapshot.version,
            snapshots: vec![snapshot],
            log: Vec::new(),
        }
    }

    pub fn begin_batch(&self) -> Batch {
        let base = self.snapshot();
        Batch {
            owner: self.owner.clone(),
            working: base.root.clone(),
            base,
            patches: Vec::new(),
        }
    }
    /// Strict version check, even for empty batches and version 0.
    /// A stale or foreign batch cannot publish any part of its state.
    pub fn apply_batch(&mut self, batch: Batch) -> Result<ApplyResult, Error> {
        if !Arc::ptr_eq(&self.owner, &batch.owner) {
            return Err(Error::ForeignBatch);
        }
        let current = self.snapshot();
        if current.version != batch.base.version {
            return Err(Error::Conflict {
                expected: batch.base.version,
                actual: current.version,
            });
        }
        if batch.patches.is_empty() {
            return Ok(ApplyResult {
                snapshot: current,
                record: None,
            });
        }
        let version = current
            .version
            .checked_add(1)
            .ok_or(Error::VersionOverflow)?;
        let record = Commit {
            version,
            patches: batch.patches,
        };
        let snapshot = Snapshot {
            version,
            root: batch.working,
        };
        self.log.push(record.clone());
        self.snapshots.push(snapshot.clone());
        Ok(ApplyResult {
            snapshot,
            record: Some(record),
        })
    }
    pub fn apply(&mut self, patch: &Patch) -> Result<ApplyResult, Error> {
        let mut batch = self.begin_batch();
        batch.apply(patch)?;
        self.apply_batch(batch)
    }
    /// Reconstructs from version 0 plus contiguous batch records.
    /// Empty batches, identity patches, and invalid operations are rejected.
    /// No partial store is returned on failure. Replay preserves record order.
    pub fn replay(
        initial: Value,
        records: impl IntoIterator<Item = Commit>,
    ) -> Result<Self, Error> {
        let mut store = Self::from_value(initial)?;
        for record in records {
            let expected = store
                .snapshot()
                .version
                .checked_add(1)
                .ok_or(Error::VersionOverflow)?;
            if record.version != expected {
                return Err(Error::InvalidVersion {
                    expected,
                    actual: record.version,
                });
            }
            let mut batch = store.begin_batch();
            if record.patches.is_empty() {
                return Err(Error::InvalidPatch("empty batch in log".into()));
            }
            for patch in &record.patches {
                let before = batch.patches.len();
                batch.apply(patch)?;
                if batch.patches.len() == before {
                    return Err(Error::InvalidPatch("identity patch in batch log".into()));
                }
            }
            store.apply_batch(batch)?;
        }
        Ok(store)
    }
}
