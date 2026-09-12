use lug_proto::{Code, Id};
use std::time::Duration;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything a caller can be told went wrong.
///
/// Failures carry the value that failed where there is one: the server's own
/// code and message, the id of the exchange, the timeout that elapsed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server answered [`lug_proto::Response::Error`].
    #[error("server refused request {id}: {code:?}: {message}")]
    Server { id: Id, code: Code, message: String },

    /// The connection carrying this exchange broke. Nothing is known about
    /// whether the server acted on the request, which is why a resend is the
    /// caller's decision and not the hub's.
    #[error("connection lost with request {id} in flight")]
    Disconnected { id: Id },

    /// No connection could be established, or none became usable in time.
    #[error("no connection to {target}: {source}")]
    Connect {
        target: String,
        source: std::io::Error,
    },

    #[error("request {id} timed out after {elapsed:?}")]
    Timeout { id: Id, elapsed: Duration },

    /// The server sent a frame that does not belong here: a reply of the wrong
    /// shape, a view the follower cannot read, records out of order.
    #[error("protocol violation on {id}: {detail}")]
    Protocol { id: Id, detail: String },

    /// A subscription outran the credit it was granted, so frames would have
    /// had to be buffered without bound. That stream is dropped; the
    /// connection is not.
    #[error("subscription {id} overran its credit window")]
    Overrun { id: Id },

    /// The view or a patch did not fold into a [`cavlc::Store`].
    #[error("view does not fold: {0}")]
    View(cavlc::Error),

    #[error("frame codec failed: {0}")]
    Codec(#[from] lug_proto::CodecError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("http transport: {0}")]
    Http(String),

    /// The hub was closed, or dropped, while this call was outstanding.
    #[error("hub is closed")]
    Closed,
}

impl Error {
    /// A server error frame, unpacked.
    pub(crate) fn from_frame(id: Id, code: Code, message: String) -> Self {
        Self::Server { id, code, message }
    }

    /// Whether a resend could plausibly succeed. Says nothing about whether a
    /// resend is *safe*: that depends on the request, and is the caller's
    /// call.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Disconnected { .. } | Self::Connect { .. } | Self::Timeout { .. } => true,
            Self::Server { code, .. } => {
                matches!(code, Code::Storage | Code::Internal | Code::Backpressure)
            }
            _ => false,
        }
    }
}
