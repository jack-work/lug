//! The log registry: one actor per log, addressed only by channels.
//!
//! Handles are uniform. The actor behind one is monomorphized once, at
//! creation, over the data structure the log was created with; after that its
//! type is gone and every connection speaks to it in JSON.

use crate::actor::{Actor, Command, Failure, Metrics, Shared, refuse};
use crate::config::Limits;
use crate::cores::Cores;
use crate::ring::Ring;
use crate::storage::StorageFactory;
use lug_core::{Log, Noop, Reducible, Storage, Version};
use lug_proto::Code;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

pub struct LogHandle {
    pub name: String,
    pub reducible: bool,
    pub commands: mpsc::Sender<Command>,
    pub watermark: watch::Receiver<Version>,
    pub shared: Arc<Shared>,
    pub subscribers: AtomicU32,
}

impl LogHandle {
    pub fn version(&self) -> Version {
        *self.watermark.borrow()
    }

    pub fn oldest(&self) -> Version {
        self.shared.oldest.load(Ordering::Relaxed)
    }

    pub fn info(&self) -> lug_proto::LogInfo {
        lug_proto::LogInfo {
            name: self.name.clone(),
            reducible: self.reducible,
            version: self.version(),
            oldest: self.oldest(),
            subscribers: self.subscribers.load(Ordering::Relaxed),
        }
    }
}

/// What connection tasks see of the registry. Looked up once per subscription
/// or first append, never per patch.
pub trait Logs: Send + Sync + 'static {
    fn get(&self, name: &str) -> Option<Arc<LogHandle>>;
    fn create(&self, name: &str, reducible: bool) -> Result<Arc<LogHandle>, Failure>;
    fn list(&self) -> Vec<Arc<LogHandle>>;
}

pub struct Registry<F: StorageFactory> {
    factory: Arc<F>,
    cores: Cores,
    limits: Limits,
    checkpoint_every: u64,
    logs: Mutex<HashMap<String, Arc<LogHandle>>>,
}

impl<F: StorageFactory> Registry<F> {
    pub fn new(factory: F, cores: Cores, limits: Limits, checkpoint_every: u64) -> Self {
        Self {
            factory: Arc::new(factory),
            cores,
            limits,
            checkpoint_every,
            logs: Mutex::new(HashMap::new()),
        }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<LogHandle>>> {
        self.logs.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl<F: StorageFactory> Logs for Registry<F> {
    fn get(&self, name: &str) -> Option<Arc<LogHandle>> {
        self.map().get(name).cloned()
    }

    fn list(&self) -> Vec<Arc<LogHandle>> {
        let mut logs: Vec<_> = self.map().values().cloned().collect();
        logs.sort_by(|a, b| a.name.cmp(&b.name));
        logs
    }

    fn create(&self, name: &str, reducible: bool) -> Result<Arc<LogHandle>, Failure> {
        check_name(name)?;
        let mut logs = self.map();
        if let Some(existing) = logs.get(name) {
            if existing.reducible != reducible {
                return Err(Failure::new(
                    Code::LogExists,
                    format!("log {name} already exists with reducible={}", existing.reducible),
                ));
            }
            return Ok(existing.clone());
        }

        let (commands, inbox) = mpsc::channel(self.limits.inbox);
        let (watermark, watch_rx) = watch::channel(0);
        let shared = Arc::new(Shared {
            ring: Arc::new(Ring::new(self.limits.ring)),
            watermark,
            oldest: AtomicU64::new(0),
            metrics: Metrics::default(),
        });
        let handle = Arc::new(LogHandle {
            name: name.to_string(),
            reducible,
            commands,
            watermark: watch_rx,
            shared: shared.clone(),
            subscribers: AtomicU32::new(0),
        });

        let factory = self.factory.clone();
        let owned = name.to_string();
        let batch = self.limits.batch;
        let every = self.checkpoint_every;
        let spawned = if reducible {
            self.cores.spawn(name, async move {
                match factory.reducible(&owned) {
                    Ok(storage) => serve(cavlc::Store::new(), storage, shared, inbox, batch, every).await,
                    Err(e) => refuse(inbox, e.to_string()).await,
                }
            })
        } else {
            self.cores.spawn(name, async move {
                match factory.plain(&owned) {
                    Ok(storage) => serve(Noop::default(), storage, shared, inbox, batch, every).await,
                    Err(e) => refuse(inbox, e.to_string()).await,
                }
            })
        };
        spawned.map_err(|e| Failure::new(Code::Internal, e))?;

        logs.insert(name.to_string(), handle.clone());
        Ok(handle)
    }
}

/// Open the log on the core that will own it, then run the actor. Opening can
/// block on recovery, which is exactly why it happens here and not on the
/// connection task that asked for the log.
async fn serve<D, S>(
    data: D,
    storage: S,
    shared: Arc<Shared>,
    inbox: mpsc::Receiver<Command>,
    batch: usize,
    checkpoint_every: u64,
) where
    D: Reducible,
    S: Storage<View = D::View>,
{
    match Log::open(data, storage) {
        Ok(log) => Actor::new(log, shared, batch, checkpoint_every).run(inbox).await,
        Err(e) => refuse(inbox, e.to_string()).await,
    }
}

/// Log names become directory names, so keep them boring.
fn check_name(name: &str) -> Result<(), Failure> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && name != "."
        && name != "..";
    if ok {
        Ok(())
    } else {
        Err(Failure::new(Code::Malformed, format!("bad log name {name:?}")))
    }
}
