//! A pool of connections, each kept alive by its own supervisor.

use crate::config::Config;
use crate::conn::{self, Command, Handle};
use crate::error::{Error, Result};
use crate::transport::Transport;
use lug_proto::Id;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

struct Slot {
    load: Arc<AtomicUsize>,
    current: watch::Receiver<Option<Handle>>,
}

pub(crate) struct Pool {
    slots: Vec<Slot>,
    /// Bumped whenever any slot gains a connection, so a caller waiting for
    /// capacity wakes once rather than polling.
    ready: watch::Sender<u64>,
    shutdown: watch::Sender<bool>,
    target: String,
}

impl Pool {
    pub fn spawn(transport: Transport, cfg: Arc<Config>, ids: Arc<AtomicU64>) -> Self {
        let (ready, _) = watch::channel(0u64);
        let (shutdown, _) = watch::channel(false);
        let mut slots = Vec::with_capacity(cfg.connections);
        for index in 0..cfg.connections {
            let load = Arc::new(AtomicUsize::new(0));
            let (publish, current) = watch::channel(None);
            slots.push(Slot {
                load: load.clone(),
                current,
            });
            tokio::spawn(supervise(Supervisor {
                index,
                transport: transport.clone(),
                cfg: cfg.clone(),
                ids: ids.clone(),
                load,
                publish,
                ready: ready.clone(),
                shutdown: shutdown.subscribe(),
            }));
        }
        Self {
            ready,
            shutdown,
            target: transport.target(),
            slots,
        }
    }

    /// The least loaded live connection, or `None` if every slot is down.
    ///
    /// Least-loaded rather than round-robin because one slow exchange must not
    /// keep drawing its share of new work.
    fn pick(&self) -> Option<Handle> {
        let mut best: Option<(usize, Handle)> = None;
        for slot in &self.slots {
            let Some(handle) = slot.current.borrow().clone() else {
                continue;
            };
            let load = slot.load.load(Ordering::Relaxed);
            if best.as_ref().is_none_or(|(seen, _)| load < *seen) {
                best = Some((load, handle));
            }
        }
        best.map(|(_, handle)| handle)
    }

    /// Wait for a usable connection, up to `deadline`.
    pub async fn acquire(&self, deadline: Duration) -> Result<Handle> {
        let mut ready = self.ready.subscribe();
        let wait = async {
            loop {
                if let Some(handle) = self.pick() {
                    return Ok(handle);
                }
                if *self.shutdown.borrow() {
                    return Err(Error::Closed);
                }
                if ready.changed().await.is_err() {
                    return Err(Error::Closed);
                }
            }
        };
        match tokio::time::timeout(deadline, wait).await {
            Ok(result) => result,
            Err(_) => Err(Error::Connect {
                target: self.target.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "no connection became usable",
                ),
            }),
        }
    }

    /// Resolve once any slot holds a live connection. Used at startup so a
    /// misconfigured transport fails immediately instead of at first call.
    pub async fn wait_ready(&self, deadline: Duration) -> Result<()> {
        self.acquire(deadline).await.map(|_| ())
    }

    pub fn close(&self) {
        let _ = self.shutdown.send(true);
        self.ready.send_modify(|n| *n += 1);
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.close();
    }
}

struct Supervisor {
    index: usize,
    transport: Transport,
    cfg: Arc<Config>,
    ids: Arc<AtomicU64>,
    load: Arc<AtomicUsize>,
    publish: watch::Sender<Option<Handle>>,
    ready: watch::Sender<u64>,
    shutdown: watch::Receiver<bool>,
}

/// Keep one slot connected: dial, greet, serve, and on death back off and
/// dial again, until the hub is closed.
async fn supervise(mut s: Supervisor) {
    let mut backoff = s.cfg.backoff.min;
    loop {
        if *s.shutdown.borrow() {
            break;
        }
        match dial(&s).await {
            Ok(wire) => {
                backoff = s.cfg.backoff.min;
                let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(s.cfg.queue);
                if s.publish
                    .send(Some(Handle::new(cmd_tx, s.load.clone())))
                    .is_err()
                {
                    break;
                }
                s.ready.send_modify(|n| *n += 1);
                tokio::select! {
                    _ = conn::run(wire, cmd_rx, s.cfg.queue) => {}
                    _ = s.shutdown.changed() => {}
                }
                let _ = s.publish.send(None);
            }
            Err(e) => {
                tracing::debug!(slot = s.index, error = %e, "connect failed");
                let _ = s.publish.send(None);
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = s.shutdown.changed() => {}
                }
                backoff = (backoff * 2).min(s.cfg.backoff.max);
            }
        }
    }
    let _ = s.publish.send(None);
}

async fn dial(s: &Supervisor) -> Result<crate::transport::Wire> {
    let connect = async {
        let mut wire = s.transport.dial(s.cfg.queue).await?;
        if s.transport.wants_handshake() {
            let id: Id = s.ids.fetch_add(1, Ordering::Relaxed);
            conn::handshake(&mut wire, id).await?;
        }
        Ok(wire)
    };
    match tokio::time::timeout(s.cfg.timeout, connect).await {
        Ok(result) => result,
        Err(_) => Err(Error::Connect {
            target: s.transport.target(),
            source: std::io::Error::new(std::io::ErrorKind::TimedOut, "handshake timed out"),
        }),
    }
}
