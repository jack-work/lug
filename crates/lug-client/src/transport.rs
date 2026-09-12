use crate::error::{Error, Result};
use crate::http;
use lug_proto::{Request, Response};
use std::path::PathBuf;
use tokio::sync::mpsc;

/// Where a [`Hub`](crate::Hub) connects, chosen by the caller.
///
/// The pool and the multiplexer above it are transport-agnostic: the same
/// [`Hub`](crate::Hub) API, the same frames, the same ids. Only the bytes
/// underneath differ.
#[derive(Clone, Debug)]
pub enum Transport {
    /// The daemon's unix socket, `<run>/lug.sock`.
    Unix(PathBuf),
    /// Loopback HTTP. `base` is an origin such as `http://127.0.0.1:7717`;
    /// `token` is the bearer token, which is sent and never logged.
    Http { base: String, token: String },
}

impl Transport {
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::Unix(path.into())
    }

    pub fn http(base: impl Into<String>, token: impl Into<String>) -> Self {
        Self::Http {
            base: base.into(),
            token: token.into(),
        }
    }

    /// For error messages, never for routing.
    pub(crate) fn target(&self) -> String {
        match self {
            Self::Unix(path) => path.display().to_string(),
            Self::Http { base, .. } => base.clone(),
        }
    }

    /// Only a unix connection begins with a `Hello`. Over HTTP each call is
    /// its own request and there is no connection to greet.
    pub(crate) fn wants_handshake(&self) -> bool {
        matches!(self, Self::Unix(_))
    }

    pub(crate) async fn dial(&self, buffer: usize) -> Result<Wire> {
        match self {
            Self::Unix(path) => crate::unix::dial(path, buffer).await,
            Self::Http { base, token } => http::dial(base, token, buffer).await,
        }
    }
}

/// One live connection, reduced to two channels.
///
/// Above this line nothing knows whether the bytes are a unix stream or a pile
/// of HTTP requests. `out` is owned by exactly one writer task inside the
/// transport, which is why the multiplexer can never interleave two frames on
/// one sink. When the peer dies `inbound` closes, and that closure is the only
/// death signal the connection layer needs.
pub(crate) struct Wire {
    pub out: mpsc::Sender<Request>,
    pub inbound: mpsc::Receiver<Response>,
}

impl Wire {
    pub(crate) fn connect_error(target: &str, source: std::io::Error) -> Error {
        Error::Connect {
            target: target.to_string(),
            source,
        }
    }
}
