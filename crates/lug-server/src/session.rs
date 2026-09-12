//! One connection's state: the stream table, the outbound queue, and the
//! pipeline that turns actor replies into response frames.
//!
//! Nothing here blocks on another stream. A request goes to its log actor and
//! its reply is awaited by a separate pump, so a subscriber that has stopped
//! granting credit cannot delay an append arriving on the same socket.

use crate::acl::Acl;
use crate::actor::{Ack, Command, Failure};
use crate::config::Limits;
use crate::registry::{LogHandle, Logs};
use crate::stream::{self, Flow};
use lug_proto::{Code, Id, MAX_FRAME, Mode, Request, Response, VERSION, Version};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};

/// A reply the connection is waiting on, and what to call the answer.
enum Waiting {
    Ack { id: Id, rx: oneshot::Receiver<Result<Ack, Failure>> },
    View { id: Id, rx: oneshot::Receiver<Result<(Version, Value), Failure>> },
}

pub struct Session {
    pub logs: Arc<dyn Logs>,
    pub out: mpsc::Sender<Response>,
    pub uid: u32,
    pub acl: Arc<Acl>,
    pub limits: Limits,
    /// Set on HTTP, where a stream and its credit calls arrive on different
    /// connections. `None` on a unix socket, which correlates itself.
    pub token: Option<String>,
    pending: mpsc::Sender<Waiting>,
    streams: Mutex<HashMap<Id, watch::Sender<Flow>>>,
}

impl Session {
    pub fn new(
        logs: Arc<dyn Logs>,
        out: mpsc::Sender<Response>,
        uid: u32,
        acl: Arc<Acl>,
        limits: Limits,
        token: Option<String>,
    ) -> Arc<Self> {
        let (pending, rx) = mpsc::channel(limits.pending);
        let session = Arc::new(Self {
            logs,
            out: out.clone(),
            uid,
            acl,
            limits,
            token,
            pending,
            streams: Mutex::new(HashMap::new()),
        });
        tokio::spawn(pump(rx, out));
        session
    }

    pub async fn reply(&self, response: Response) {
        let _ = self.out.send(response).await;
    }

    async fn fail(&self, id: Id, failure: Failure) {
        self.reply(Response::Error { id, code: failure.code, message: failure.message }).await;
    }

    pub async fn dispatch(self: &Arc<Self>, request: Request) {
        match request {
            Request::Hello { id, .. } => {
                self.reply(Response::Welcome {
                    id,
                    version: VERSION,
                    max_frame: MAX_FRAME,
                    session: self.token.clone(),
                })
                .await;
            }
            Request::Ping { id } => self.reply(Response::Pong { id }).await,
            Request::List { id } => {
                let logs = self.logs.list().iter().map(|h| h.info()).collect();
                self.reply(Response::Logs { id, logs }).await;
            }
            Request::Create { id, log, reducible } => {
                if !self.acl.allows(self.uid, &log) {
                    return self.fail(id, unauthorized(&log)).await;
                }
                match self.logs.create(&log, reducible) {
                    // A created log is reported as itself: the client learns
                    // its version and retention in the same breath.
                    Ok(handle) => {
                        self.reply(Response::Logs { id, logs: vec![handle.info()] }).await
                    }
                    Err(e) => self.fail(id, e).await,
                }
            }
            Request::Append { id, log, patches, durability } => {
                let Some(handle) = self.handle(id, &log).await else { return };
                let (reply, rx) = oneshot::channel();
                let durability = durability_of(durability);
                let command = Command::Append { patches, durability, reply };
                if handle.commands.send(command).await.is_err() {
                    return self.fail(id, gone(&log)).await;
                }
                if self.pending.send(Waiting::Ack { id, rx }).await.is_err() {
                    return self.fail(id, Failure::new(Code::Internal, "connection closing")).await;
                }
            }
            Request::Read { id, log, at } => {
                let Some(handle) = self.handle(id, &log).await else { return };
                if !handle.reducible {
                    return self.fail(id, not_reducible(&log)).await;
                }
                let (reply, rx) = oneshot::channel();
                if handle.commands.send(Command::View { at, reply }).await.is_err() {
                    return self.fail(id, gone(&log)).await;
                }
                if self.pending.send(Waiting::View { id, rx }).await.is_err() {
                    return self.fail(id, Failure::new(Code::Internal, "connection closing")).await;
                }
            }
            Request::Subscribe { id, log, from, mode, credit } => {
                let Some(handle) = self.handle(id, &log).await else { return };
                if mode == Mode::Reducible && !handle.reducible {
                    return self.fail(id, not_reducible(&log)).await;
                }
                let (flow, watcher) = watch::channel(Flow { granted: credit as u64 });
                let taken = {
                    let mut streams = self.streams();
                    match streams.contains_key(&id) {
                        true => true,
                        false => {
                            streams.insert(id, flow);
                            false
                        }
                    }
                };
                if taken {
                    return self
                        .fail(id, Failure::new(Code::BadId, format!("stream {id} is open")))
                        .await;
                }
                // Acknowledge before pushing anything, so a subscription
                // opened with no credit is one Ok and then silence. Over SSE
                // the Welcome already said this, and saying it twice would
                // make that silence ambiguous.
                if self.token.is_none() {
                    self.reply(Response::Ok { id }).await;
                }
                tokio::spawn(stream::run(stream::Stream {
                    id,
                    from,
                    mode,
                    watermark: handle.watermark.clone(),
                    handle,
                    out: self.out.clone(),
                    flow: watcher,
                    limits: self.limits,
                    session: Arc::downgrade(self),
                }));
            }
            Request::Credit { id, grant } => match self.grant(id, grant) {
                Ok(()) => self.reply(Response::Ok { id }).await,
                Err(e) => self.fail(id, e).await,
            },
            // The stream task sends the End as it unwinds, so a cancel that
            // found a stream says nothing more here.
            Request::Cancel { id } => {
                if let Err(e) = self.cancel(id) {
                    self.fail(id, e).await;
                }
            }
        }
    }

    /// Extend a stream's credit. Separate from `dispatch` because HTTP
    /// steers a stream from a different connection and answers inline.
    pub fn grant(&self, id: Id, grant: u32) -> Result<(), Failure> {
        match self.streams().get(&id) {
            Some(flow) => {
                flow.send_modify(|f| f.granted = f.granted.saturating_add(grant as u64));
                Ok(())
            }
            None => Err(no_stream(id)),
        }
    }

    /// Dropping the flow sender is the cancel signal.
    pub fn cancel(&self, id: Id) -> Result<(), Failure> {
        match self.streams().remove(&id) {
            Some(_) => Ok(()),
            None => Err(no_stream(id)),
        }
    }

    /// Called by a stream task once it is finished with its id.
    pub fn forget(&self, id: Id) {
        self.streams().remove(&id);
    }

    /// Cancel every stream, used when the connection goes away.
    pub fn close(&self) {
        self.streams().clear();
    }

    async fn handle(&self, id: Id, log: &str) -> Option<Arc<LogHandle>> {
        if !self.acl.allows(self.uid, log) {
            self.fail(id, unauthorized(log)).await;
            return None;
        }
        match self.logs.get(log) {
            Some(handle) => Some(handle),
            None => {
                self.fail(id, Failure::new(Code::NoSuchLog, format!("no log {log}"))).await;
                None
            }
        }
    }

    fn streams(&self) -> std::sync::MutexGuard<'_, HashMap<Id, watch::Sender<Flow>>> {
        self.streams.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Awaits actor replies in the order they were issued and turns them into
/// frames. Bounded, so a client that pipelines without reading is slowed at
/// the source rather than buffered forever.
async fn pump(mut pending: mpsc::Receiver<Waiting>, out: mpsc::Sender<Response>) {
    while let Some(waiting) = pending.recv().await {
        let response = match waiting {
            Waiting::Ack { id, rx } => match rx.await {
                Ok(Ok(ack)) => Response::Ack { id, versions: ack.versions, synced: ack.synced },
                Ok(Err(e)) => Response::Error { id, code: e.code, message: e.message },
                Err(_) => dropped(id),
            },
            Waiting::View { id, rx } => match rx.await {
                Ok(Ok((version, value))) => Response::View { id, version, value },
                Ok(Err(e)) => Response::Error { id, code: e.code, message: e.message },
                Err(_) => dropped(id),
            },
        };
        if out.send(response).await.is_err() {
            break;
        }
    }
}

fn dropped(id: Id) -> Response {
    Response::Error { id, code: Code::Internal, message: "log actor stopped".into() }
}

fn no_stream(id: Id) -> Failure {
    Failure::new(Code::BadId, format!("no stream {id}"))
}

fn unauthorized(log: &str) -> Failure {
    Failure::new(Code::Unauthorized, format!("not permitted on log {log}"))
}

fn not_reducible(log: &str) -> Failure {
    Failure::new(Code::NotReducible, format!("log {log} keeps no view"))
}

fn gone(log: &str) -> Failure {
    Failure::new(Code::Internal, format!("log {log} is not running"))
}

fn durability_of(durability: lug_proto::Durability) -> lug_core::Durability {
    match durability {
        lug_proto::Durability::Memory => lug_core::Durability::Memory,
        lug_proto::Durability::Written => lug_core::Durability::Written,
        lug_proto::Durability::Durable => lug_core::Durability::Durable,
    }
}
