use std::time::Duration;

/// Reconnection delays. Deterministic doubling; there is one daemon on the
/// other end, not a thundering herd of shards, so jitter buys nothing.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            min: Duration::from_millis(50),
            max: Duration::from_secs(5),
        }
    }
}

/// How a [`Hub`](crate::Hub) is built. Every field has a sane default; reach
/// for [`Hub::builder`](crate::Hub::builder) rather than this struct.
#[derive(Clone, Debug)]
pub struct Config {
    /// Connections held open. One core's worth of parallelism each, since the
    /// server assigns log actors to cores.
    pub connections: usize,
    /// Ceiling on every call. A call that has not been answered by now fails
    /// with [`Error::Timeout`](crate::Error::Timeout); it never hangs.
    pub timeout: Duration,
    /// Initial credit granted to a new subscription, and the size of the
    /// buffer that holds pushed frames for the consumer.
    pub credit: u32,
    /// Depth of each connection's command queue. Also caps how many written
    /// frames may be waiting on a slow sink.
    pub queue: usize,
    pub backoff: Backoff,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connections: std::thread::available_parallelism().map_or(4, |n| n.get()),
            timeout: Duration::from_secs(10),
            credit: 256,
            queue: 256,
            backoff: Backoff::default(),
        }
    }
}

/// Whether a failed call may be sent again.
///
/// The default is never, because the hub cannot know what a lost connection
/// did with the request it was carrying: an `Append` may well have been
/// applied before the socket died. Only the caller knows whether resending is
/// harmless, so only the caller can ask for it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Retry {
    #[default]
    Never,
    /// Resend up to `attempts` further times when the failure was transient:
    /// a broken connection, a timeout, a storage error the server says did
    /// not happen.
    Idempotent { attempts: u8 },
}

impl Retry {
    pub fn idempotent(attempts: u8) -> Self {
        Self::Idempotent { attempts }
    }

    pub(crate) fn attempts(self) -> u8 {
        match self {
            Self::Never => 0,
            Self::Idempotent { attempts } => attempts,
        }
    }
}
