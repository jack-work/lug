//! Persistent AVL maps, immutable JSON, and a single-writer MVCC store.
//!
//! No I/O, actors, or background tasks. Snapshots can outlive the store and
//! cross threads. Writers require exclusive access to `Store`; batches
//! work on private roots and fail if their base is no longer current.
pub mod avl;
mod error;
mod mvcc;
mod patch;
mod value;

pub use error::Error;
pub use mvcc::{ApplyResult, Batch, Commit, Snapshot, Store, Version};
pub use patch::{Patch, Update};
pub use value::Value;
