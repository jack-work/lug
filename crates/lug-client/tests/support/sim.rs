//! A small in-process lug server, good enough to hold the client to the
//! contract: real sockets, real frames, real credit accounting.
//!
//! It keeps one reducible log, folds appends through `cavlc::Store`, and
//! pushes to subscribers only as far as their credit allows.

#![allow(dead_code)]

use lug_proto::{Code, Durability, Id, LogInfo, Mode, Record, Request, Response, Version};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};

/// A frame sink for one connection. Immediate replies go out with [`post`];
/// a pushing task uses [`push`], which waits for room.
#[derive(Clone)]
pub struct Pusher(pub mpsc::Sender<Response>);

impl Pusher {
    pub fn post(&self, frame: Response) {
        let _ = self.0.try_send(frame);
    }

    pub async fn push(&self, frame: Response) -> bool {
        self.0.send(frame).await.is_ok()
    }
}

struct SubState {
    credit: AtomicU32,
    cancelled: AtomicBool,
}

struct Inner {
    store: cavlc::Store,
    records: Vec<Record>,
    /// Versions at or below this were reclaimed, which is what produces a gap.
    oldest: Version,
    logs: HashMap<String, bool>,
    subs: HashMap<Id, Arc<SubState>>,
}

/// The server's state, shared by every connection.
pub struct Sim {
    inner: Mutex<Inner>,
    /// Rung whenever a subscriber might have work: new records, new credit,
    /// a cancel, a truncation.
    bell: Notify,
    pub seen: Mutex<Vec<(u64, Request)>>,
    pub pushed: AtomicU64,
    pub grants: AtomicU64,
    /// When set, `Read` answers with the bare root instead of a serialized
    /// snapshot, to prove the client accepts either shape.
    pub bare_views: AtomicBool,
    /// Never answer these; used to prove a dead connection fails its waiters.
    pub swallow_pings: AtomicBool,
}

impl Default for Sim {
    fn default() -> Self {
        Self::new()
    }
}

impl Sim {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                store: cavlc::Store::new(),
                records: Vec::new(),
                oldest: 0,
                logs: HashMap::from([("log".to_string(), true)]),
                subs: HashMap::new(),
            }),
            bell: Notify::new(),
            seen: Mutex::new(Vec::new()),
            pushed: AtomicU64::new(0),
            grants: AtomicU64::new(0),
            bare_views: AtomicBool::new(false),
            swallow_pings: AtomicBool::new(false),
        }
    }

    pub fn version(&self) -> Version {
        self.inner.lock().unwrap().store.snapshot().version()
    }

    /// The server's own view, for comparing against a follower's.
    pub fn root(&self) -> Value {
        self.inner.lock().unwrap().store.snapshot().root().to_json()
    }

    pub fn requests(&self) -> Vec<Request> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, r)| r.clone())
            .collect()
    }

    pub fn connections_used(&self) -> usize {
        let seen = self.seen.lock().unwrap();
        let mut ids: Vec<u64> = seen.iter().map(|(c, _)| *c).collect();
        ids.sort_unstable();
        ids.dedup();
        ids.len()
    }

    /// Apply patches exactly as the daemon would, minting one version each.
    pub fn append(&self, patches: &[Value]) -> Result<Vec<Version>, String> {
        let mut inner = self.inner.lock().unwrap();
        let mut versions = Vec::new();
        for patch in patches {
            let parsed: cavlc::Patch =
                serde_json::from_value(patch.clone()).map_err(|e| e.to_string())?;
            let applied = inner.store.apply(&parsed).map_err(|e| e.to_string())?;
            if applied.record.is_some() {
                let version = applied.snapshot.version();
                inner.records.push(Record {
                    version,
                    patch: patch.clone(),
                });
                versions.push(version);
            }
        }
        drop(inner);
        self.bell.notify_waiters();
        Ok(versions)
    }

    /// End a stream from the server's side, as a daemon would when the log
    /// goes away.
    pub fn end_stream(&self, id: Id) {
        if let Some(state) = self.inner.lock().unwrap().subs.get(&id) {
            state.cancelled.store(true, Ordering::Relaxed);
        }
        self.bell.notify_waiters();
    }

    /// Reclaim everything at or below `version`, so a subscriber below it
    /// must be told about a gap.
    pub fn reclaim(&self, version: Version) {
        self.inner.lock().unwrap().oldest = version;
        self.bell.notify_waiters();
    }

    fn snapshot_frame(&self, id: Id) -> Response {
        let inner = self.inner.lock().unwrap();
        let snapshot = inner.store.snapshot();
        let value = if self.bare_views.load(Ordering::Relaxed) {
            snapshot.root().to_json()
        } else {
            serde_json::to_value(&snapshot).expect("snapshot serializes")
        };
        Response::View {
            id,
            version: snapshot.version(),
            value,
        }
    }

    /// One request from one connection.
    pub fn handle(self: &Arc<Self>, conn: u64, req: Request, out: Pusher) {
        self.seen.lock().unwrap().push((conn, req.clone()));
        match req {
            Request::Hello { id, .. } => out.post(Response::Welcome {
                id,
                version: lug_proto::VERSION,
                max_frame: lug_proto::MAX_FRAME,
                session: None,
            }),
            Request::Ping { id } => {
                if !self.swallow_pings.load(Ordering::Relaxed) {
                    out.post(Response::Pong { id });
                }
            }
            Request::List { id } => {
                let names: Vec<String> = self.inner.lock().unwrap().logs.keys().cloned().collect();
                let logs = names.iter().map(|name| self.info(name)).collect();
                out.post(Response::Logs { id, logs });
            }
            Request::Create { id, log, reducible } => {
                self.inner
                    .lock()
                    .unwrap()
                    .logs
                    .insert(log.clone(), reducible);
                out.post(Response::Logs {
                    id,
                    logs: vec![self.info(&log)],
                });
            }
            Request::Append {
                id,
                log,
                patches,
                durability,
            } => {
                if !self.known(&log) {
                    return out.post(no_such_log(id, &log));
                }
                match self.append(&patches) {
                    Ok(versions) => {
                        let synced = match durability {
                            Durability::Memory => 0,
                            _ => versions.last().copied().unwrap_or_default(),
                        };
                        out.post(Response::Ack {
                            id,
                            versions,
                            synced,
                        });
                    }
                    Err(message) => out.post(Response::Error {
                        id,
                        code: Code::Rejected,
                        message,
                    }),
                }
            }
            Request::Read { id, log, at } => {
                if !self.known(&log) {
                    return out.post(no_such_log(id, &log));
                }
                if !self.reducible(&log) {
                    return out.post(Response::Error {
                        id,
                        code: Code::NotReducible,
                        message: format!("{log} keeps no view"),
                    });
                }
                let sim = self.clone();
                tokio::spawn(async move {
                    match at {
                        // A reply whose content identifies its caller, delayed
                        // unevenly so that replies interleave on the wire.
                        Some(at) => {
                            tokio::time::sleep(Duration::from_millis(at % 5)).await;
                            out.push(Response::View {
                                id,
                                version: at,
                                value: json!({ "at": at }),
                            })
                            .await;
                        }
                        None => {
                            out.push(sim.snapshot_frame(id)).await;
                        }
                    }
                });
            }
            Request::Subscribe {
                id,
                log,
                from,
                mode,
                credit,
            } => {
                if !self.known(&log) {
                    return out.post(no_such_log(id, &log));
                }
                if mode == Mode::Reducible && !self.reducible(&log) {
                    return out.post(Response::Error {
                        id,
                        code: Code::NotReducible,
                        message: format!("{log} keeps no view"),
                    });
                }
                let state = Arc::new(SubState {
                    credit: AtomicU32::new(credit),
                    cancelled: AtomicBool::new(false),
                });
                self.inner.lock().unwrap().subs.insert(id, state.clone());
                // Acknowledged before a single record moves, so an opened
                // stream is distinguishable from a refused one.
                out.post(Response::Ok { id });
                let sim = self.clone();
                tokio::spawn(async move { sim.serve(id, from, mode, state, out).await });
            }
            Request::Credit { id, grant } => {
                self.grants.fetch_add(1, Ordering::Relaxed);
                if let Some(state) = self.inner.lock().unwrap().subs.get(&id) {
                    state.credit.fetch_add(grant, Ordering::Relaxed);
                }
                self.bell.notify_waiters();
                out.post(Response::Ok { id });
            }
            Request::Cancel { id } => {
                if let Some(state) = self.inner.lock().unwrap().subs.get(&id) {
                    state.cancelled.store(true, Ordering::Relaxed);
                }
                self.bell.notify_waiters();
            }
        }
    }

    fn info(&self, log: &str) -> LogInfo {
        let inner = self.inner.lock().unwrap();
        LogInfo {
            name: log.to_string(),
            reducible: inner.logs.get(log).copied().unwrap_or(true),
            version: inner.store.snapshot().version(),
            oldest: inner.oldest,
            subscribers: inner.subs.len() as u32,
        }
    }

    fn reducible(&self, log: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .logs
            .get(log)
            .copied()
            .unwrap_or(false)
    }

    fn known(&self, log: &str) -> bool {
        self.inner.lock().unwrap().logs.contains_key(log)
    }

    /// One subscription: a view preamble when asked for, then records, never
    /// more than the credit granted.
    async fn serve(
        self: Arc<Self>,
        id: Id,
        from: Version,
        mode: Mode,
        state: Arc<SubState>,
        out: Pusher,
    ) {
        let mut cursor = from;
        if mode == Mode::Reducible {
            let frame = self.snapshot_frame(id);
            if let Response::View { version, .. } = &frame {
                cursor = *version;
            }
            if !out.push(frame).await {
                return;
            }
        }
        loop {
            // Registered before the state is read, so a change that lands in
            // between still wakes this loop.
            let rung = self.bell.notified();
            if state.cancelled.load(Ordering::Relaxed) {
                out.push(Response::End { id }).await;
                self.inner.lock().unwrap().subs.remove(&id);
                return;
            }
            let oldest = self.inner.lock().unwrap().oldest;
            if cursor < oldest {
                let gap = Response::Gap {
                    id,
                    from: cursor,
                    to: oldest,
                };
                cursor = oldest;
                if !out.push(gap).await {
                    return;
                }
                continue;
            }
            let credit = state.credit.load(Ordering::Relaxed);
            if credit > 0 {
                let batch: Vec<Record> = {
                    let inner = self.inner.lock().unwrap();
                    inner
                        .records
                        .iter()
                        .filter(|r| r.version > cursor)
                        .take(credit as usize)
                        .cloned()
                        .collect()
                };
                if let Some(last) = batch.last() {
                    cursor = last.version;
                    state
                        .credit
                        .fetch_sub(batch.len() as u32, Ordering::Relaxed);
                    self.pushed.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    if !out.push(Response::Records { id, records: batch }).await {
                        return;
                    }
                    continue;
                }
            }
            rung.await;
        }
    }
}

fn no_such_log(id: Id, log: &str) -> Response {
    Response::Error {
        id,
        code: Code::NoSuchLog,
        message: format!("no log {log}"),
    }
}
