//! [`cavlc::Store`] as a [`Reducible`].
//!
//! The mapping is direct because cavlc already separates working state from
//! published state: `Batch` is staged work, `apply_batch` is the commit, and
//! `Snapshot` is the MVCC pointer.

use crate::{Reducible, Version, Versioned};
use cavlc::{Batch, Error, Patch, Snapshot, Store};

impl Versioned for Snapshot {
    fn version(&self) -> Version {
        self.version()
    }
}

impl Reducible for Store {
    type Patch = Patch;
    type View = Snapshot;
    type Staged = Batch;
    type Error = Error;

    fn stage(&self) -> Batch {
        self.begin_batch()
    }

    fn stage_patch(&self, staged: &mut Batch, patch: &Patch) -> Result<(), Error> {
        staged.apply(patch)
    }

    fn commit(&mut self, staged: Batch) -> Result<Option<Snapshot>, Error> {
        let applied = self.apply_batch(staged)?;
        Ok(applied.record.is_some().then_some(applied.snapshot))
    }

    fn view(&self) -> Snapshot {
        self.snapshot()
    }

    fn view_at(&self, version: Version) -> Option<Snapshot> {
        self.snapshot_at(version)
    }

    fn truncate(&mut self, version: Version) -> Result<(), Error> {
        Store::truncate(self, version);
        Ok(())
    }

    fn resume(view: Snapshot) -> Result<Self, Error> {
        Ok(Store::resume(view))
    }
}
