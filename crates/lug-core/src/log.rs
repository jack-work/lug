use crate::{Error, Record, Reducible, Storage, Version, Versioned};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// How far an append must get before it is acknowledged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// Acknowledge once the version exists in memory. Survives nothing.
    Memory,
    /// Acknowledge once the record reached the kernel. Survives a process
    /// crash, not a power cut.
    #[default]
    Written,
    /// Acknowledge once the record is on stable storage.
    Durable,
}

/// A [`Reducible`] made durable by a [`Storage`].
///
/// The log is itself a `Reducible` over the same patch and view types, so a
/// log wraps a structure the same way anything else does, and logs compose.
///
/// Two version counters run here. The structure's own counter moves as soon
/// as a patch folds into memory. The *watermark* moves only once the record
/// backing that version is on disk to the requested degree. Everything
/// outward-facing (acknowledgements, subscriber fan-out, reads) rides the
/// watermark, so nothing observable ever exists that the write-ahead log has
/// not already recorded. If a write fails after the fold, the structure is
/// truncated back to the watermark and the failure is reported.
pub struct Log<D: Reducible, S: Storage<View = D::View>> {
    data: D,
    storage: S,
    watermark: Version,
    synced: Version,
}

/// A view plus the durability frontier it was observed under.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(bound = "V: Versioned")]
pub struct LogView<V> {
    pub view: V,
    /// Highest version on stable storage at observation time.
    pub synced: Version,
}

impl<V: Versioned> Versioned for LogView<V> {
    fn version(&self) -> Version {
        self.view.version()
    }
}

type LogError<D, S> = Error<<D as Reducible>::Error, <S as Storage>::Error>;

impl<D, S> Log<D, S>
where
    D: Reducible,
    S: Storage<View = D::View>,
{
    /// Recover from storage: adopt the checkpointed header if there is one,
    /// then replay the records after it.
    pub fn open(mut data: D, mut storage: S) -> Result<Self, LogError<D, S>> {
        let recovered = storage.load().map_err(Error::Storage)?;
        for record in &recovered.tail {
            let patch: D::Patch = serde_json::from_slice(&record.patch)?;
            let mut staged = data.stage();
            data.stage_patch(&mut staged, &patch).map_err(Error::Data)?;
            let view = data.commit(staged).map_err(Error::Data)?;
            let minted = view.map(|v| v.version()).unwrap_or_default();
            if minted != record.version {
                return Err(Error::Version { expected: record.version, actual: minted });
            }
        }
        let watermark = data.version();
        Ok(Self { data, storage, watermark, synced: watermark })
    }

    /// Highest version that is observable: folded into memory and recorded.
    pub fn watermark(&self) -> Version {
        self.watermark
    }

    /// Highest version on stable storage.
    pub fn synced(&self) -> Version {
        self.synced
    }

    pub fn view(&self) -> LogView<D::View> {
        LogView { view: self.data.view(), synced: self.synced }
    }

    pub fn view_at(&self, version: Version) -> Option<LogView<D::View>> {
        (version <= self.watermark)
            .then(|| self.data.view_at(version))
            .flatten()
            .map(|view| LogView { view, synced: self.synced })
    }

    /// Records after `version`, for a subscriber that fell behind the ring.
    pub fn read_after(&self, after: Version, limit: usize) -> Result<Vec<Record>, LogError<D, S>> {
        self.storage.read_after(after, limit).map_err(Error::Storage)
    }

    pub fn oldest(&self) -> Version {
        self.storage.oldest()
    }

    /// Fold many patches, one version each, behind a single write and at most
    /// a single sync. This is the group-commit path: cost per patch falls as
    /// the batch grows, with no timer and no artificial delay.
    ///
    /// On a storage failure the structure is truncated back to the watermark
    /// so that memory never leads the log, and the error is returned. Patches
    /// rejected by the structure abort the whole batch before anything is
    /// written.
    pub fn append(
        &mut self,
        patches: &[D::Patch],
        durability: Durability,
    ) -> Result<Vec<D::View>, LogError<D, S>> {
        if patches.is_empty() {
            return Ok(Vec::new());
        }
        let base = self.watermark;
        let mut records = Vec::with_capacity(patches.len());
        let mut views = Vec::with_capacity(patches.len());

        for patch in patches {
            let encoded = Bytes::from(serde_json::to_vec(patch)?);
            let mut staged = self.data.stage();
            match self.data.stage_patch(&mut staged, patch) {
                Ok(()) => {}
                Err(e) => return self.unwind(base, Error::Data(e)),
            }
            match self.data.commit(staged) {
                Ok(Some(view)) => {
                    records.push(Record { version: view.version(), patch: encoded });
                    views.push(view);
                }
                // An identity patch mints no version and leaves no record.
                Ok(None) => views.push(self.data.view()),
                Err(e) => return self.unwind(base, Error::Data(e)),
            }
        }

        if records.is_empty() {
            return Ok(views);
        }
        if let Err(e) = self.storage.append(&records) {
            return self.unwind(base, Error::Storage(e));
        }
        let top = records.last().expect("non-empty").version;
        if durability == Durability::Durable {
            if let Err(e) = self.storage.sync() {
                return self.unwind(base, Error::Storage(e));
            }
            self.synced = top;
        }
        self.watermark = top;
        Ok(views)
    }

    /// Checkpoint the current MVCC pointer into the storage header, letting
    /// recovery skip every record it covers.
    pub fn checkpoint(&mut self) -> Result<(), LogError<D, S>> {
        let view = self.data.view();
        self.storage.write_header(&view).map_err(Error::Storage)?;
        self.storage.sync().map_err(Error::Storage)?;
        self.synced = self.watermark;
        Ok(())
    }

    /// Flush without checkpointing, advancing the sync frontier.
    pub fn sync(&mut self) -> Result<(), LogError<D, S>> {
        self.storage.sync().map_err(Error::Storage)?;
        self.synced = self.watermark;
        Ok(())
    }

    fn unwind<T>(&mut self, base: Version, err: LogError<D, S>) -> Result<T, LogError<D, S>> {
        let _ = self.data.truncate(base);
        Err(err)
    }
}
