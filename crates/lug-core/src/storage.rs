use crate::{Version, Versioned};
use bytes::Bytes;

/// One encoded patch at the version it minted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub version: Version,
    pub patch: Bytes,
}

/// What a cold start found on disk.
#[derive(Debug)]
pub struct Recovered<V> {
    /// The checkpointed pointer, if a header was written. `None` means replay
    /// from the structure's own zero value.
    pub header: Option<V>,
    /// Records after the checkpoint, in version order, contiguous.
    pub tail: Vec<Record>,
}

impl<V> Default for Recovered<V> {
    fn default() -> Self {
        Self { header: None, tail: Vec::new() }
    }
}

/// Durable backing for a log.
///
/// Parameterized on the view type its data structure produces, because
/// [`write_header`](Storage::write_header) checkpoints exactly that pointer.
/// A header lets recovery skip the records it covers; without one, every
/// record must be replayed.
///
/// Nothing here promises durability except [`sync`](Storage::sync). `append`
/// may return before the bytes leave the page cache, which is what makes
/// group commit possible.
pub trait Storage: Send + 'static {
    type View: Versioned;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Append records in version order.
    ///
    /// Atomicity is over a *prefix*, not the whole call. No file-backed store
    /// can promise all-or-nothing once a batch outgrows `IOV_MAX` or a write
    /// returns short, so what is guaranteed is weaker and precise:
    ///
    /// - On `Ok`, every record is recoverable.
    /// - On `Err`, the store is left exactly as it was before the call, as
    ///   observed through [`load`](Storage::load) and
    ///   [`read_after`](Storage::read_after). Bytes may have reached the file,
    ///   but they must not be recoverable as records. The caller may retry the
    ///   same batch or a different one.
    /// - After a crash mid-call, a contiguous prefix of the batch may survive.
    ///   That is sound because the caller's watermark never advanced, so none
    ///   of those versions was ever acknowledged or observable.
    ///
    /// The `Err` case is the sharp one. It is not enough to leave the logical
    /// end where it was: a later, shorter batch written at that offset can
    /// leave the tail of the failed batch beyond it, and those bytes are a
    /// well-formed record at the next contiguous version. Neither a checksum
    /// nor a gap check would notice. A store must therefore keep everything
    /// above its logical end unrecoverable, which for a preallocated file
    /// means keeping it zeroed.
    fn append(&mut self, records: &[Record]) -> Result<(), Self::Error>;

    /// Checkpoint the MVCC pointer. Records at or below `view.version()`
    /// become eligible for reclamation.
    fn write_header(&mut self, view: &Self::View) -> Result<(), Self::Error>;

    /// Flush to stable storage. Returns once the bytes survive power loss.
    fn sync(&mut self) -> Result<(), Self::Error>;

    /// Read what a cold start should replay.
    fn load(&mut self) -> Result<Recovered<Self::View>, Self::Error>;

    /// Records in `(after, after + limit]`, for subscribers that fell behind
    /// the in-memory ring. Returns fewer than `limit` at the end of the log.
    fn read_after(&self, after: Version, limit: usize) -> Result<Vec<Record>, Self::Error>;

    /// Oldest version still readable. Anything below has been reclaimed.
    fn oldest(&self) -> Version;
}

/// Storage that forgets. Every append is accepted and discarded; recovery
/// always reports an empty log.
#[derive(Debug)]
pub struct Discard<V>(std::marker::PhantomData<fn() -> V>);

impl<V> Default for Discard<V> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unreachable")]
pub struct Never;

impl<V: Versioned> Storage for Discard<V> {
    type View = V;
    type Error = Never;

    fn append(&mut self, _: &[Record]) -> Result<(), Never> {
        Ok(())
    }
    fn write_header(&mut self, _: &V) -> Result<(), Never> {
        Ok(())
    }
    fn sync(&mut self) -> Result<(), Never> {
        Ok(())
    }
    fn load(&mut self) -> Result<Recovered<V>, Never> {
        Ok(Recovered::default())
    }
    fn read_after(&self, _: Version, _: usize) -> Result<Vec<Record>, Never> {
        Ok(Vec::new())
    }
    fn oldest(&self) -> Version {
        0
    }
}
