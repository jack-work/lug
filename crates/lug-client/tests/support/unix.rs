//! The mock daemon on a real unix socket.

use super::sim::{Pusher, Sim};
use futures::{SinkExt, StreamExt};
use lug_proto::{Codec, Request, Response};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::UnixListener;
use tokio::sync::{mpsc, watch};
use tokio_util::codec::Framed;

pub struct Mock {
    pub sim: Arc<Sim>,
    dir: PathBuf,
    path: PathBuf,
    /// Bumped to drop every live connection, the way a daemon crash would.
    kill: watch::Sender<u64>,
    stop: watch::Sender<bool>,
}

impl Mock {
    pub async fn start(tag: &str) -> Self {
        Self::start_with(tag, Arc::new(Sim::new())).await
    }

    pub async fn start_with(tag: &str, sim: Arc<Sim>) -> Self {
        let dir = super::scratch(tag);
        let path = dir.join("lug.sock");
        let listener = UnixListener::bind(&path).expect("bind mock socket");
        let (kill, _) = watch::channel(0);
        let (stop, mut stopped) = watch::channel(false);

        let accept_sim = sim.clone();
        let kill_tx = kill.clone();
        tokio::spawn(async move {
            static CONN: AtomicU64 = AtomicU64::new(0);
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = stopped.changed() => return,
                };
                let Ok((stream, _)) = accepted else { return };
                let id = CONN.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(serve(stream, id, accept_sim.clone(), kill_tx.subscribe()));
            }
        });

        Self {
            sim,
            dir,
            path,
            kill,
            stop,
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn transport(&self) -> lug_client::Transport {
        lug_client::Transport::unix(&self.path)
    }

    /// Drop every live connection. The listener keeps accepting, so a client
    /// that reconnects will get through.
    pub fn kill_connections(&self) {
        self.kill.send_modify(|n| *n += 1);
    }

    /// Stop accepting entirely.
    pub fn stop_listening(&self) {
        let _ = self.stop.send(true);
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn serve(
    stream: tokio::net::UnixStream,
    conn: u64,
    sim: Arc<Sim>,
    mut kill: watch::Receiver<u64>,
) {
    let mut framed = Framed::new(stream, Codec::<Request, Response>::new());
    let (out, mut out_rx) = mpsc::channel::<Response>(1024);
    loop {
        tokio::select! {
            frame = framed.next() => match frame {
                Some(Ok(req)) => sim.handle(conn, req, Pusher(out.clone())),
                _ => return,
            },
            reply = out_rx.recv() => match reply {
                Some(reply) => {
                    if framed.send(reply).await.is_err() {
                        return;
                    }
                }
                None => return,
            },
            _ = kill.changed() => return,
        }
    }
}
