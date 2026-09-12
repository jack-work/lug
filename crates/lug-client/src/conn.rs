//! One connection's multiplexer: many exchanges, one socket, one writer.

use crate::error::{Error, Result};
use crate::transport::Wire;
use lug_proto::{Id, Request, Response};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

/// What a caller asks a connection to do.
pub(crate) enum Command {
    /// One request, one reply, matched by id.
    Call {
        req: Request,
        reply: oneshot::Sender<Result<Response>>,
    },
    /// One request, many replies, until `End` or `Error`.
    Stream {
        req: Request,
        sink: mpsc::Sender<Result<Response>>,
        overrun: Arc<AtomicBool>,
    },
    /// A frame whose reply is not awaited: `Credit`, `Cancel`.
    Fire(Request),
    /// The caller gave up. Drop the waiter so the map cannot grow without
    /// bound on a server that never answers.
    Forget(Id),
}

/// A cheap, clonable reference to a live connection.
#[derive(Clone)]
pub(crate) struct Handle {
    pub cmd: mpsc::Sender<Command>,
    load: Arc<AtomicUsize>,
}

impl Handle {
    pub fn new(cmd: mpsc::Sender<Command>, load: Arc<AtomicUsize>) -> Self {
        Self { cmd, load }
    }

    /// Count one exchange against this connection until the guard drops. This
    /// is the number the pool minimizes over, so it must cover the whole life
    /// of a call or a subscription, not just the write.
    pub fn charge(&self) -> Load {
        self.load.fetch_add(1, Ordering::Relaxed);
        Load(self.load.clone())
    }
}

pub(crate) struct Load(Arc<AtomicUsize>);

impl Drop for Load {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

enum Waiter {
    Call(oneshot::Sender<Result<Response>>),
    Stream {
        sink: mpsc::Sender<Result<Response>>,
        overrun: Arc<AtomicBool>,
    },
}

impl Waiter {
    fn fail(self, err: Error) {
        match self {
            Self::Call(reply) => {
                let _ = reply.send(Err(err));
            }
            Self::Stream { sink, .. } => {
                let _ = sink.try_send(Err(err));
            }
        }
    }
}

/// Greet the server and wait for its `Welcome`.
///
/// Anything else arriving first is a protocol violation, since no other
/// exchange exists yet on this connection.
pub(crate) async fn handshake(wire: &mut Wire, id: Id) -> Result<()> {
    let hello = Request::Hello {
        id,
        version: lug_proto::VERSION,
    };
    wire.out.send(hello).await.map_err(|_| Error::Closed)?;
    match wire.inbound.recv().await {
        Some(Response::Welcome { version, .. }) => {
            if version != lug_proto::VERSION {
                tracing::warn!(
                    server = version,
                    client = lug_proto::VERSION,
                    "protocol skew"
                );
            }
            Ok(())
        }
        Some(Response::Error { id, code, message }) => Err(Error::from_frame(id, code, message)),
        Some(other) => Err(Error::Protocol {
            id,
            detail: format!("expected welcome, got {other:?}"),
        }),
        None => Err(Error::Disconnected { id }),
    }
}

/// Drive one connection until its socket dies or its last handle is dropped.
///
/// Returns when the connection is finished; every waiter has been failed by
/// then, so no caller is left hanging on its own timeout after the socket is
/// known to be gone.
pub(crate) async fn run(wire: Wire, mut cmds: mpsc::Receiver<Command>, pending_cap: usize) {
    let Wire { out, mut inbound } = wire;
    let mut waiters: HashMap<Id, Waiter> = HashMap::new();
    let mut pending: VecDeque<Request> = VecDeque::new();

    loop {
        tokio::select! {
            // Requests go out through a reserved permit rather than a blocking
            // send, so a slow sink can never stop this task from reading
            // replies and unblocking the callers holding them.
            permit = out.reserve(), if !pending.is_empty() => {
                let Ok(permit) = permit else { break };
                if let Some(req) = pending.pop_front() {
                    permit.send(req);
                }
            }
            cmd = cmds.recv(), if pending.len() < pending_cap => {
                match cmd {
                    Some(cmd) => accept(cmd, &mut waiters, &mut pending),
                    // Every handle is gone, so nothing can be waiting either.
                    None => break,
                }
            }
            frame = inbound.recv() => {
                match frame {
                    Some(frame) => deliver(frame, &mut waiters),
                    None => break,
                }
            }
        }
    }

    // The connection is gone. Fail everything at once: a caller who already
    // knows the answer can never arrive must not sit out its own timeout.
    for (id, waiter) in waiters.drain() {
        waiter.fail(Error::Disconnected { id });
    }
    // Anything still queued was never written.
    for req in pending.drain(..) {
        tracing::debug!(id = req.id(), "request dropped with the connection");
    }
}

fn accept(cmd: Command, waiters: &mut HashMap<Id, Waiter>, pending: &mut VecDeque<Request>) {
    match cmd {
        Command::Call { req, reply } => {
            waiters.insert(req.id(), Waiter::Call(reply));
            pending.push_back(req);
        }
        Command::Stream { req, sink, overrun } => {
            waiters.insert(req.id(), Waiter::Stream { sink, overrun });
            pending.push_back(req);
        }
        Command::Fire(req) => pending.push_back(req),
        Command::Forget(id) => {
            waiters.remove(&id);
        }
    }
}

fn deliver(frame: Response, waiters: &mut HashMap<Id, Waiter>) {
    let id = frame.id();
    let Some(waiter) = waiters.get_mut(&id) else {
        // A reply to something already abandoned, or a stream the caller
        // cancelled. Both are ordinary races, not errors.
        tracing::debug!(id, "frame for an unknown exchange");
        return;
    };
    match waiter {
        Waiter::Call(_) => {
            let Some(Waiter::Call(reply)) = waiters.remove(&id) else {
                return;
            };
            let _ = reply.send(unpack(frame));
        }
        Waiter::Stream { sink, overrun } => {
            let done = frame.is_final();
            if sink.try_send(unpack(frame)).is_err() {
                // Full means the server pushed past the credit it was granted,
                // since the channel is sized to that window. Drop this stream
                // alone; the connection keeps serving everyone else.
                overrun.store(true, Ordering::Release);
                waiters.remove(&id);
                return;
            }
            if done {
                waiters.remove(&id);
            }
        }
    }
}

fn unpack(frame: Response) -> Result<Response> {
    match frame {
        Response::Error { id, code, message } => Err(Error::from_frame(id, code, message)),
        other => Ok(other),
    }
}
