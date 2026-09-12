//! What the viewer needs from whatever is feeding it.
//!
//! Deliberately smaller than a client follower: a materialized view, a stream
//! of arriving records, and a tick when the version moves. Keeping it local
//! means the viewer builds and is tested without a daemon, and the adapter over
//! `lug_client::Follower` is a handful of lines.

use lug_proto::{Event, Version};
use serde_json::Value;
use tokio::sync::{mpsc, watch};

#[derive(Clone, Debug, PartialEq)]
pub struct View {
    pub version: Version,
    pub value: Value,
}

pub trait Follow: Send + 'static {
    /// Whether this log has a view to render at all.
    fn reducible(&self) -> bool;

    /// The current view, `None` on a log that is not reducible.
    ///
    /// Must be cheap: it is called from the render loop. A reducible source is
    /// expected to have its first view in hand before it is handed over.
    fn view(&self) -> Option<View>;

    /// Ticks on every version advance, reducible or not. `*rx.borrow()` is the
    /// version the source is at.
    fn changed(&self) -> watch::Receiver<Version>;

    /// The event stream, in version order. Handed out once; later calls give
    /// `None` because the receiver has already been taken.
    ///
    /// A [`Event::Gap`] must be queued here *before* the watch ticks with the
    /// view refetched after it. The viewer reads the gap first and repaints
    /// wholesale; the other order would diff a fresh view against a state the
    /// missing patches never reached, and light up the wrong subtree.
    fn events(&mut self) -> Option<mpsc::Receiver<Event>>;
}
