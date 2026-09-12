//! The wire contract.
//!
//! One frame is a big-endian `u32` byte length followed by that many bytes of
//! JSON. Every frame carries an [`Id`] chosen by the client. A request and its
//! reply share one; a subscription's id is also its stream id, so pushed
//! records need no second namespace.
//!
//! The same frames travel over a unix socket and over HTTP. On HTTP a request
//! is a POST body and the pushed frames are SSE `data:` lines, so both
//! transports decode with this one set of types.
//!
//! Every request is answered by exactly one frame, except `Subscribe`, which
//! is answered by one frame and then pushes until cancelled:
//!
//! | request | answer |
//! | --- | --- |
//! | [`Request::Hello`] | [`Response::Welcome`] |
//! | [`Request::Append`] | [`Response::Ack`] |
//! | [`Request::Create`] | [`Response::Logs`] with one entry, the log as it now stands |
//! | [`Request::List`] | [`Response::Logs`] |
//! | [`Request::Read`] | [`Response::View`] |
//! | [`Request::Credit`] | [`Response::Ok`] |
//! | [`Request::Cancel`] | [`Response::End`] |
//! | [`Request::Ping`] | [`Response::Pong`] |
//! | [`Request::Subscribe`] | [`Response::Ok`], then the stream |
//!
//! Any of them may instead be answered by [`Response::Error`], which is
//! always final for that id.
//!
//! `Subscribe` is acknowledged *before* anything is pushed, which is what
//! makes a subscription opened with zero credit distinguishable from one that
//! failed: exactly one `Ok`, then silence until credit arrives. Over SSE that
//! acknowledgement is [`Response::Welcome`] instead, because it also has to
//! carry the session.

mod codec;
pub mod http;

pub use codec::{Codec, Error as CodecError, MAX_FRAME, encode};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Protocol version. Bumped when a frame changes shape incompatibly.
pub const VERSION: u16 = 1;

/// Correlates a reply with its request, and names a subscription stream.
/// Client-chosen, unique per connection, never reused while in flight.
pub type Id = u64;

pub type Version = u64;

/// What a subscriber wants pushed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Records only. A tail.
    #[default]
    Records,
    /// A materialized [`Response::View`] first, then the records after it, so
    /// a follower can rebuild state without replaying from version 0. Only
    /// valid on a log whose data structure is reducible.
    Reducible,
}

/// How far an append must get before it is acknowledged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    Memory,
    #[default]
    Written,
    Durable,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Request {
    /// First frame on a connection. The server answers [`Response::Welcome`].
    Hello { id: Id, version: u16 },
    /// Append patches, one version minted per patch.
    Append {
        id: Id,
        log: String,
        patches: Vec<Value>,
        #[serde(default)]
        durability: Durability,
    },
    /// Open a stream. `from` is exclusive: `0` replays everything retained.
    Subscribe {
        id: Id,
        log: String,
        #[serde(default)]
        from: Version,
        #[serde(default)]
        mode: Mode,
        /// Records the server may push before waiting for more credit.
        credit: u32,
    },
    /// Extend a stream's credit. Without this the server stops pushing.
    Credit { id: Id, grant: u32 },
    /// Close a stream. The server answers [`Response::End`]. A stream that
    /// was never opened is [`Code::BadId`], not silence.
    Cancel { id: Id },
    /// Read a materialized view, current or historical.
    Read {
        id: Id,
        log: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<Version>,
    },
    List { id: Id },
    /// Create a log. Idempotent: an existing log of the same shape succeeds
    /// and returns its current state, a different shape is [`Code::LogExists`].
    Create { id: Id, log: String, reducible: bool },
    Ping { id: Id },
}

impl Request {
    pub fn id(&self) -> Id {
        match self {
            Self::Hello { id, .. }
            | Self::Append { id, .. }
            | Self::Subscribe { id, .. }
            | Self::Credit { id, .. }
            | Self::Cancel { id, .. }
            | Self::Read { id, .. }
            | Self::List { id, .. }
            | Self::Create { id, .. }
            | Self::Ping { id } => *id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum Response {
    /// Answers `Hello` on a socket, and is the first SSE event on a stream.
    /// `session` is set only over HTTP, where it is the value to send back in
    /// `X-Lug-Session` so that calls reach this stream.
    Welcome {
        id: Id,
        version: u16,
        max_frame: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<String>,
    },
    /// One version per patch that changed state, in order.
    Ack { id: Id, versions: Vec<Version>, synced: Version },
    /// Pushed on a subscription, contiguous and in version order.
    Records { id: Id, records: Vec<Record> },
    /// A materialized view: the reply to [`Request::Read`], or the preamble of
    /// a [`Mode::Reducible`] subscription.
    View { id: Id, version: Version, value: Value },
    /// The subscriber fell behind retention. Versions in `(from, to]` are gone
    /// and the stream resumes at `to`.
    Gap { id: Id, from: Version, to: Version },
    Logs { id: Id, logs: Vec<LogInfo> },
    /// Accepted, with nothing to report. Answers `Credit`, and opens a
    /// subscription before any records are pushed.
    Ok { id: Id },
    /// The stream is closed; the id may be reused.
    End { id: Id },
    Pong { id: Id },
    Error { id: Id, code: Code, message: String },
}

impl Response {
    pub fn id(&self) -> Id {
        match self {
            Self::Welcome { id, .. }
            | Self::Ack { id, .. }
            | Self::Records { id, .. }
            | Self::View { id, .. }
            | Self::Gap { id, .. }
            | Self::Logs { id, .. }
            | Self::Ok { id }
            | Self::End { id }
            | Self::Pong { id }
            | Self::Error { id, .. } => *id,
        }
    }

    /// Whether this frame ends the exchange its id names.
    pub fn is_final(&self) -> bool {
        matches!(self, Self::End { .. } | Self::Error { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub version: Version,
    pub patch: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogInfo {
    pub name: String,
    pub reducible: bool,
    /// Highest observable version.
    pub version: Version,
    /// Oldest version still readable; below this, records were reclaimed.
    pub oldest: Version,
    pub subscribers: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    /// The patch was rejected by the data structure. The caller's fault, and
    /// retrying the same patch will fail the same way.
    Rejected,
    NoSuchLog,
    LogExists,
    /// Not reducible, or otherwise the wrong shape for the request.
    NotReducible,
    /// Version requested is below retention, or above the watermark.
    OutOfRange,
    /// Id already in flight, or unknown on Cancel/Credit.
    BadId,
    Unauthorized,
    /// Frame exceeded `max_frame`, or was not valid for this protocol version.
    Malformed,
    /// Storage failed. The append did not happen; retrying is safe.
    Storage,
    /// Client is not reading and its buffer is full.
    Backpressure,
    Internal,
}
