//! One subscription: a cursor, a credit balance, and a wait on the watermark.
//!
//! The task wakes once per watermark move and takes as many records as its
//! credit allows, so a single wakeup can carry hundreds of them. When credit
//! runs out it parks, which is what keeps a slow subscriber from touching
//! anything else on its connection.

use crate::actor::{CatchUp, Command};
use crate::config::Limits;
use crate::registry::LogHandle;
use crate::ring;
use crate::session::Session;
use lug_proto::{Code, Id, Mode, Record, Response, Version};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, oneshot, watch};

/// Credit granted so far, counted from the start of the stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Flow {
    pub granted: u64,
}

pub struct Stream {
    pub id: Id,
    pub from: Version,
    pub mode: Mode,
    pub handle: Arc<LogHandle>,
    pub watermark: watch::Receiver<Version>,
    pub out: mpsc::Sender<Response>,
    pub flow: watch::Receiver<Flow>,
    pub limits: Limits,
    pub session: std::sync::Weak<Session>,
}

pub async fn run(stream: Stream) {
    let id = stream.id;
    let session = stream.session.clone();
    let out = stream.out.clone();
    let handle = stream.handle.clone();
    handle.subscribers.fetch_add(1, Ordering::Relaxed);

    serve(stream).await;

    handle.subscribers.fetch_sub(1, Ordering::Relaxed);
    if let Some(session) = session.upgrade() {
        session.forget(id);
    }
    let _ = out.send(Response::End { id }).await;
}

async fn serve(mut s: Stream) {
    let mut cursor = s.from;
    let mut used = 0u64;

    if s.mode == Mode::Reducible {
        match view(&s).await {
            Ok((version, value)) => {
                if s.out.send(Response::View { id: s.id, version, value }).await.is_err() {
                    return;
                }
                // The view already carries everything up to its version.
                cursor = cursor.max(version);
            }
            Err(response) => {
                let _ = s.out.send(response).await;
                return;
            }
        }
    }

    loop {
        let granted = s.flow.borrow_and_update().granted;
        if used >= granted {
            if s.flow.changed().await.is_err() {
                return;
            }
            continue;
        }
        let watermark = *s.watermark.borrow_and_update();
        if cursor >= watermark {
            let waited = tokio::select! {
                changed = s.watermark.changed() => changed.is_ok(),
                changed = s.flow.changed() => changed.is_ok(),
            };
            if !waited {
                return;
            }
            continue;
        }

        let allowance = (granted - used).min(s.limits.push as u64) as usize;
        let entries = match s.handle.shared.ring.read_after(cursor, allowance, s.limits.push_bytes)
        {
            ring::Read::Records(entries) if !entries.is_empty() => entries,
            // Below the ring: storage serves the catch-up, or reports the
            // hole if the records were reclaimed.
            _ => match catch_up(&s, cursor, allowance).await {
                // Nothing to serve yet; wait rather than spin.
                Ok(CatchUp::Records(entries)) if entries.is_empty() => {
                    if s.watermark.changed().await.is_err() {
                        return;
                    }
                    continue;
                }
                Ok(CatchUp::Records(entries)) => entries,
                Ok(CatchUp::Gap { to }) => {
                    let gap = Response::Gap { id: s.id, from: cursor, to };
                    if s.out.send(gap).await.is_err() {
                        return;
                    }
                    cursor = to;
                    continue;
                }
                Err(response) => {
                    let _ = s.out.send(response).await;
                    return;
                }
            },
        };

        let records: Vec<Record> = entries
            .iter()
            .map(|e| Record { version: e.version, patch: e.patch.clone() })
            .collect();
        used += records.len() as u64;
        cursor = entries.last().map(|e| e.version).unwrap_or(cursor);
        if s.out.send(Response::Records { id: s.id, records }).await.is_err() {
            return;
        }
    }
}

async fn view(s: &Stream) -> Result<(Version, serde_json::Value), Response> {
    let (reply, rx) = oneshot::channel();
    ask(s, Command::View { at: None, reply }, rx).await
}

async fn catch_up(s: &Stream, after: Version, limit: usize) -> Result<CatchUp, Response> {
    let (reply, rx) = oneshot::channel();
    let limit = limit.max(1);
    ask(s, Command::CatchUp { after, limit, reply }, rx).await
}

async fn ask<T>(
    s: &Stream,
    command: Command,
    rx: oneshot::Receiver<Result<T, crate::actor::Failure>>,
) -> Result<T, Response> {
    if s.handle.commands.send(command).await.is_err() {
        return Err(stopped(s.id));
    }
    match rx.await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(Response::Error { id: s.id, code: e.code, message: e.message }),
        Err(_) => Err(stopped(s.id)),
    }
}

fn stopped(id: Id) -> Response {
    Response::Error { id, code: Code::Internal, message: "log actor stopped".into() }
}
