//! The lug daemon.
//!
//! One actor per log owns the `Log`, a ring of recent records and a watch of
//! the watermark. Connections never touch a log directly: they put commands
//! on the actor's bounded mailbox and read fan-out from the ring, so the only
//! serialization point on the write path is that mailbox.

pub mod acl;
mod actor;
pub mod config;
mod cores;
mod http;
mod registry;
mod ring;
mod session;
pub mod storage;
mod stream;
mod unix;

pub use actor::{Failure, Metrics};
pub use config::{Config, Limits};
pub use registry::{LogHandle, Logs};
pub use storage::{Memory, MemoryFactory, StorageFactory};

use acl::Acl;
use anyhow::Context;
use cores::Cores;
use registry::Registry;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

pub struct Server {
    socket: PathBuf,
    http: Option<SocketAddr>,
    logs: Arc<dyn Logs>,
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Server {
    pub async fn start<F: StorageFactory>(config: Config, factory: F) -> anyhow::Result<Self> {
        config.validate()?;
        let limits = config.limits();
        let cores = config.cores.unwrap_or_else(|| {
            std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
        });
        let owner = config.owner.unwrap_or_else(acl::own_uid);

        let mut acl = Acl::new(owner, config.allow_uid.iter().copied());
        acl.logs = config
            .acl
            .iter()
            .map(|(log, uids)| (log.clone(), uids.iter().copied().collect::<HashSet<_>>()))
            .collect();
        let acl = Arc::new(acl);

        std::fs::create_dir_all(&config.data)
            .with_context(|| format!("creating data dir {:?}", config.data))?;

        let registry =
            Registry::new(factory, Cores::new(cores)?, limits, config.checkpoint_every);
        let logs: Arc<dyn Logs> = Arc::new(registry);

        let (listener, socket) = unix::bind(&config.run, &config.socket)
            .with_context(|| format!("binding {:?}", config.socket_path()))?;
        let (shutdown, stopping) = watch::channel(false);
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(unix::accept(unix::Accept {
            listener,
            logs: logs.clone(),
            acl: acl.clone(),
            limits,
            shutdown: stopping.clone(),
        })));

        let mut http = None;
        if let Some(addr) = config.http {
            let token = config::read_token(
                config.token.as_deref().unwrap_or_else(|| Path::new("/etc/lug/token")),
            )?;
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("binding http {addr}"))?;
            http = Some(listener.local_addr()?);
            let api = http::Api::new(logs.clone(), acl.clone(), limits, owner, token);
            let mut stopping = stopping.clone();
            tasks.push(tokio::spawn(async move {
                let serve = axum::serve(listener, http::router(api))
                    .with_graceful_shutdown(async move {
                        let _ = stopping.changed().await;
                    });
                if let Err(e) = serve.await {
                    tracing::error!(error = %e, "http listener stopped");
                }
            }));
        }

        Ok(Self { socket, http, logs, shutdown, tasks })
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn http(&self) -> Option<SocketAddr> {
        self.http
    }

    pub fn logs(&self) -> Arc<dyn Logs> {
        self.logs.clone()
    }

    /// Stop accepting, checkpoint every log, unlink the socket.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.shutdown.send(true);
        for handle in self.logs.list() {
            let (reply, rx) = oneshot::channel();
            if handle.commands.send(actor::Command::Checkpoint { reply }).await.is_err() {
                continue;
            }
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::error!(log = handle.name, error = %e, "checkpoint failed"),
                Err(_) => tracing::warn!(log = handle.name, "actor stopped before checkpoint"),
            }
        }
        match std::fs::remove_file(&self.socket) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::error!(error = %e, socket = ?self.socket, "unlink failed"),
        }
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
        // Dropping the registry stops the actors and joins their core threads.
        drop(self.logs);
        Ok(())
    }
}
