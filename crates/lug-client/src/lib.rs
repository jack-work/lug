//! The lug client: one hub, pooled and multiplexed, over either transport.
//!
//! ```no_run
//! use lug_client::{Hub, Transport};
//!
//! # async fn example() -> lug_client::Result<()> {
//! let hub = Hub::connect(Transport::unix("/run/lug/lug.sock")).await?;
//! hub.create("orders", true).await?;
//! hub.append("orders", vec![serde_json::json!({"Create": {"id": 1}})]).await?;
//!
//! // A live mirror of the log, rebuilt here and kept current.
//! let follower = hub.follow("orders").await?;
//! if let Some(view) = follower.view() {
//!     println!("version {}", view.version);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The shape of the thing:
//!
//! - A [`Hub`] owns a pool of connections and picks the least loaded one per
//!   call. Many exchanges share a connection at once, matched by
//!   [`lug_proto::Id`].
//! - Inside each connection exactly one task owns the sink, because a socket
//!   is one byte stream and two concurrent writes interleave into garbage.
//!   Frames that pile up while it is parked ride out on one flush.
//! - Every call has a deadline, and a connection that dies fails everything
//!   riding it at once rather than leaving callers to time out one by one.
//! - Retries are opt-in ([`Retry`]), because the hub cannot know whether a
//!   lost `Append` was applied.
//! - [`Subscription`] is what a subscriber observes: records in version order
//!   and the gaps between them. It grants credit as the consumer drains it
//!   and withholds it when the consumer stalls, which is the client half of
//!   the server's flow control.
//! - [`Follower`] is the reason the rest exists: a log, rebuilt in memory and
//!   kept live across reconnects and gaps.

mod config;
mod conn;
mod error;
mod follower;
mod http;
mod hub;
mod pool;
mod sub;
mod transport;
mod unix;

pub use config::{Backoff, Config, Retry};
pub use error::{Error, Result};
pub use follower::{Follower, State, Status};
pub use hub::{Ack, Builder, Hub, Subscribe, View};
pub use sub::{Frames, Subscription};
pub use transport::Transport;

pub use lug_proto::{Code, Durability, Event, Id, LogInfo, Mode, Record, Response, Version};
