//! What the viewer needs from whatever is feeding it.
//!
//! Deliberately smaller than a client follower: a materialized view, a stream
//! of arriving records, and a tick when the version moves. Keeping it local
//! means the viewer builds and is tested without a daemon, and the adapter over
//! `lug_client::Follower` is a handful of lines.

use lug_proto::{Record, Version};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Debug, PartialEq)]
pub struct View {
    pub version: Version,
    pub value: Value,
}

pub trait Follow: Send + 'static {
    /// The current view, or `None` when the log is not reducible.
    ///
    /// Must be cheap: it is called from the render loop. A reducible source is
    /// required to have its first view in hand before it is handed over, so
    /// `None` here is the signal that reducible mode is not on offer.
    fn view(&self) -> Option<View>;

    /// Ticks on every version advance, reducible or not. `*rx.borrow()` is the
    /// version the source is at.
    fn changed(&self) -> watch::Receiver<Version>;

    /// The record stream, in version order. Handed out once; later calls give
    /// `None` because the receiver has already been taken.
    fn records(&mut self) -> Option<mpsc::Receiver<Record>>;
}
