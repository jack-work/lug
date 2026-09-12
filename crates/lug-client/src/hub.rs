use crate::config::{Backoff, Config, Retry};
use crate::conn::Command;
use crate::error::{Error, Result};
use crate::follower::Follower;
use crate::pool::Pool;
use crate::sub::{Frames, Subscription};
use crate::transport::Transport;
use lug_proto::{Durability, Id, LogInfo, Mode, Request, Response, Version};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// A pooled, multiplexing client.
///
/// A hub owns a small pool of connections and spreads work over the least
/// loaded of them. Many calls share one connection at once, matched by id, so
/// concurrency costs channel sends rather than sockets. Cloning a hub is
/// cheap and shares the pool; hand clones out freely.
///
/// Every call has a deadline. If a connection dies, the calls riding it fail
/// at once rather than waiting out their timeouts, and a supervisor dials
/// again behind the scenes.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
    retry: Retry,
}

struct Inner {
    pool: Pool,
    cfg: Arc<Config>,
    ids: Arc<AtomicU64>,
}

/// The reply to an append: one version per patch that changed state, plus the
/// durability frontier at the time it was acknowledged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    pub versions: Vec<Version>,
    pub synced: Version,
}

/// A materialized view as the server sent it.
#[derive(Clone, Debug, PartialEq)]
pub struct View {
    pub version: Version,
    pub value: Value,
}

impl View {
    /// Read this view as a [`cavlc::Snapshot`], the form a
    /// [`Follower`] folds patches into.
    pub fn snapshot(&self) -> Result<cavlc::Snapshot> {
        crate::follower::snapshot_from(self.version, &self.value)
    }
}

/// What a subscription asks for.
#[derive(Clone, Debug, Default)]
pub struct Subscribe {
    /// Exclusive cursor. `0` replays everything still retained.
    pub from: Version,
    pub mode: Mode,
    /// Records the server may push before it must be granted more. Defaults
    /// to the hub's configured credit.
    pub credit: Option<u32>,
}

impl Subscribe {
    /// Tail the record stream.
    pub fn records() -> Self {
        Self::default()
    }

    /// Take a view preamble first, then the records after it.
    pub fn reducible() -> Self {
        Self {
            mode: Mode::Reducible,
            ..Self::default()
        }
    }

    pub fn from(mut self, version: Version) -> Self {
        self.from = version;
        self
    }

    pub fn credit(mut self, credit: u32) -> Self {
        self.credit = Some(credit);
        self
    }
}

/// Builds a [`Hub`]. `Hub::builder(transport).connect().await` is the whole
/// story unless a default needs moving.
pub struct Builder {
    transport: Transport,
    cfg: Config,
}

impl Builder {
    pub fn connections(mut self, n: usize) -> Self {
        self.cfg.connections = n.max(1);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.cfg.timeout = timeout;
        self
    }

    pub fn credit(mut self, credit: u32) -> Self {
        self.cfg.credit = credit;
        self
    }

    pub fn queue(mut self, depth: usize) -> Self {
        self.cfg.queue = depth.max(1);
        self
    }

    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.cfg.backoff = backoff;
        self
    }

    /// Dial, and wait for the first connection to be usable, so that a bad
    /// path or a dead daemon is reported here rather than at first call.
    pub async fn connect(self) -> Result<Hub> {
        let cfg = Arc::new(self.cfg);
        let ids = Arc::new(AtomicU64::new(1));
        let pool = Pool::spawn(self.transport, cfg.clone(), ids.clone());
        pool.wait_ready(cfg.timeout).await?;
        Ok(Hub {
            inner: Arc::new(Inner { pool, cfg, ids }),
            retry: Retry::Never,
        })
    }

    /// Return immediately, leaving the pool to connect in the background.
    /// Calls made before a connection exists wait for one, up to the timeout.
    pub fn connect_lazy(self) -> Hub {
        let cfg = Arc::new(self.cfg);
        let ids = Arc::new(AtomicU64::new(1));
        let pool = Pool::spawn(self.transport, cfg.clone(), ids.clone());
        Hub {
            inner: Arc::new(Inner { pool, cfg, ids }),
            retry: Retry::Never,
        }
    }
}

impl Hub {
    pub fn builder(transport: Transport) -> Builder {
        Builder {
            transport,
            cfg: Config::default(),
        }
    }

    pub async fn connect(transport: Transport) -> Result<Self> {
        Self::builder(transport).connect().await
    }

    /// A view of this hub whose calls are resent on a transient failure.
    ///
    /// Only use it for requests that are safe to repeat. `ls`, `read` and
    /// `create` are; `append` is not, unless the patches themselves are.
    pub fn with_retry(&self, retry: Retry) -> Self {
        Self {
            inner: self.inner.clone(),
            retry,
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.cfg
    }

    pub async fn ping(&self) -> Result<()> {
        match self.call(|id| Request::Ping { id }).await? {
            Response::Pong { .. } => Ok(()),
            other => Err(mismatch("pong", other)),
        }
    }

    pub async fn list(&self) -> Result<Vec<LogInfo>> {
        match self.call(|id| Request::List { id }).await? {
            Response::Logs { logs, .. } => Ok(logs),
            other => Err(mismatch("logs", other)),
        }
    }

    /// Create a log, and describe it as it now stands.
    ///
    /// Idempotent: an existing log of the same shape succeeds, which is what
    /// makes this safe to call on every start. A different shape is
    /// [`Code::LogExists`](lug_proto::Code::LogExists).
    pub async fn create(&self, log: &str, reducible: bool) -> Result<LogInfo> {
        let reply = self
            .call(|id| Request::Create {
                id,
                log: log.to_string(),
                reducible,
            })
            .await?;
        match reply {
            Response::Logs { logs, .. } => logs.into_iter().next().ok_or(Error::Protocol {
                id: 0,
                detail: "create answered with no log".into(),
            }),
            other => Err(mismatch("logs", other)),
        }
    }

    pub async fn append(&self, log: &str, patches: Vec<Value>) -> Result<Ack> {
        self.append_with(log, patches, Durability::default()).await
    }

    pub async fn append_with(
        &self,
        log: &str,
        patches: Vec<Value>,
        durability: Durability,
    ) -> Result<Ack> {
        let reply = self
            .call(move |id| Request::Append {
                id,
                log: log.to_string(),
                patches: patches.clone(),
                durability,
            })
            .await?;
        match reply {
            Response::Ack {
                versions, synced, ..
            } => Ok(Ack { versions, synced }),
            other => Err(mismatch("ack", other)),
        }
    }

    /// Read a materialized view, current or at a past version.
    pub async fn read(&self, log: &str, at: Option<Version>) -> Result<View> {
        let reply = self
            .call(|id| Request::Read {
                id,
                log: log.to_string(),
                at,
            })
            .await?;
        match reply {
            Response::View { version, value, .. } => Ok(View { version, value }),
            other => Err(mismatch("view", other)),
        }
    }

    /// Open a stream of what a subscriber observes: records in version order,
    /// and the gaps between them.
    ///
    /// Credit is granted automatically as the returned [`Subscription`] is
    /// drained, and withheld while it is not.
    pub async fn subscribe(&self, log: &str, options: Subscribe) -> Result<Subscription> {
        Ok(self.subscribe_frames(log, options).await?.into())
    }

    /// The same stream as raw frames, including the view preamble of a
    /// [`Mode::Reducible`] subscription. [`Follower`] is built on this;
    /// most callers want [`subscribe`](Self::subscribe).
    pub async fn subscribe_frames(&self, log: &str, options: Subscribe) -> Result<Frames> {
        let credit = options.credit.unwrap_or(self.inner.cfg.credit).max(1);
        let timeout = self.inner.cfg.timeout;
        let handle = self.inner.pool.acquire(timeout).await?;
        let id = self.next_id();
        let req = Request::Subscribe {
            id,
            log: log.to_string(),
            from: options.from,
            mode: options.mode,
            credit,
        };
        Frames::open(handle, id, req, credit, timeout).await
    }

    /// Follow a reducible log: rebuild its view here and keep it live.
    pub async fn follow(&self, log: &str) -> Result<Follower> {
        Follower::start(self.clone(), log, self.inner.cfg.credit).await
    }

    /// Stop every connection. Outstanding calls fail; the hub stays usable
    /// only in the sense that it will report being closed.
    pub fn close(&self) {
        self.inner.pool.close();
    }

    fn next_id(&self) -> Id {
        self.inner.ids.fetch_add(1, Ordering::Relaxed)
    }

    /// One request, one reply, with the hub's deadline and retry policy.
    async fn call(&self, build: impl Fn(Id) -> Request) -> Result<Response> {
        let mut left = self.retry.attempts();
        loop {
            match self.attempt(&build).await {
                Err(e) if left > 0 && e.is_transient() => {
                    left -= 1;
                    tracing::debug!(error = %e, "retrying idempotent call");
                }
                result => return result,
            }
        }
    }

    async fn attempt(&self, build: &impl Fn(Id) -> Request) -> Result<Response> {
        let budget = self.inner.cfg.timeout;
        let started = Instant::now();
        let handle = self.inner.pool.acquire(budget).await?;
        let _charged = handle.charge();
        let id = self.next_id();

        let (reply, answer) = oneshot::channel();
        let queued = tokio::time::timeout(
            budget.saturating_sub(started.elapsed()),
            handle.cmd.send(Command::Call {
                req: build(id),
                reply,
            }),
        )
        .await;
        match queued {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(Error::Disconnected { id }),
            Err(_) => {
                return Err(Error::Timeout {
                    id,
                    elapsed: started.elapsed(),
                });
            }
        }

        match tokio::time::timeout(budget.saturating_sub(started.elapsed()), answer).await {
            Ok(Ok(result)) => result,
            // The dispatcher went away without answering, which it only does
            // after failing every waiter, so this is a lost race with death.
            Ok(Err(_)) => Err(Error::Disconnected { id }),
            Err(_) => {
                // Stop the connection from holding a waiter nobody will read.
                let _ = handle.cmd.try_send(Command::Forget(id));
                Err(Error::Timeout {
                    id,
                    elapsed: started.elapsed(),
                })
            }
        }
    }
}

/// Says what it is connected to and how wide the pool is, never what is in
/// flight, which changes under you as you print it.
impl std::fmt::Debug for Hub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("connections", &self.inner.cfg.connections)
            .field("timeout", &self.inner.cfg.timeout)
            .field("retry", &self.retry)
            .finish()
    }
}

fn mismatch(expected: &str, got: Response) -> Error {
    Error::Protocol {
        id: got.id(),
        detail: format!("expected {expected}, got {got:?}"),
    }
}
