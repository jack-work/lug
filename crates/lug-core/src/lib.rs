//! The contracts every other crate hangs off.
//!
//! A [`Reducible`] folds serializable patches into versioned views. Its
//! [`Reducible::View`] is an MVCC pointer: immutable, cheap to clone, free to
//! outlive the structure that produced it. A [`Storage`] persists the patches
//! and checkpoints one of those pointers as a header. A [`Log`] is the two
//! bolted together, and is itself a `Reducible`, so logs nest.

mod log;
mod reducible;
mod storage;

#[cfg(feature = "cavlc")]
mod cavlc_impl;

pub use log::{Durability, Log, LogView};
pub use reducible::{Noop, Reducible, Versioned};
pub use storage::{Discard, Record, Recovered, Storage};

/// Monotonic version counter. Every committed patch mints exactly one.
/// Version 0 is the initial state and is never carried by a record.
pub type Version = u64;

#[derive(Debug, thiserror::Error)]
pub enum Error<D, S> {
    #[error("data structure rejected the patch: {0}")]
    Data(D),
    #[error("storage failed: {0}")]
    Storage(S),
    #[error("codec failed: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("version {actual} is not the expected {expected}")]
    Version { expected: Version, actual: Version },
}
