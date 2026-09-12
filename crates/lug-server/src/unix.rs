//! The unix listener: one socket, peer credentials checked on accept.

use crate::acl::Acl;
use crate::config::Limits;
use crate::registry::Logs;
use crate::session::Session;
use futures::{SinkExt, StreamExt};
use lug_proto::{Code, Codec, CodecError, Request, Response};
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::codec::Framed;

/// Create the run directory 0700 and bind inside it, so the parent enforces
/// access whatever the umask does to the socket itself.
///
/// The socket is bound under a private name and renamed into place, so the
/// published path never points at a dead socket: a client restarting the
/// daemon sees the old one until the moment the new one is listening.
pub fn bind(run: &Path, socket: &Path) -> io::Result<(UnixListener, PathBuf)> {
    match std::fs::DirBuilder::new().recursive(true).mode(0o700).create(run) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    std::fs::set_permissions(run, std::fs::Permissions::from_mode(0o700))?;

    let path = run.join(socket);
    let mut staging = path.clone().into_os_string();
    staging.push(format!(".{}", std::process::id()));
    let staging = PathBuf::from(staging);
    match std::fs::remove_file(&staging) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let listener = UnixListener::bind(&staging)?;
    std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&staging, &path)?;
    Ok((listener, path))
}

pub struct Accept {
    pub listener: UnixListener,
    pub logs: Arc<dyn Logs>,
    pub acl: Arc<Acl>,
    pub limits: Limits,
    pub shutdown: watch::Receiver<bool>,
}

pub async fn accept(mut accept: Accept) {
    let permits = Arc::new(Semaphore::new(accept.limits.connections));
    loop {
        let stream = tokio::select! {
            _ = accept.shutdown.changed() => break,
            accepted = accept.listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            },
        };

        let uid = match stream.peer_cred() {
            Ok(cred) => cred.uid(),
            Err(e) => {
                tracing::warn!(error = %e, "no peer credentials");
                continue;
            }
        };
        if !accept.acl.accepts(uid) {
            tracing::warn!(uid, "rejected connection from foreign uid");
            tokio::spawn(refuse(stream, Code::Unauthorized, "peer uid is not permitted"));
            continue;
        }
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            // Logged once per refusal on purpose: a client that cannot tell
            // why it was dropped will blame the wrong thing, and so will
            // whoever reads the journal.
            tracing::warn!(limit = accept.limits.connections, "connection limit reached");
            tokio::spawn(refuse(
                stream,
                Code::Backpressure,
                "connection limit reached; raise limits.connections",
            ));
            continue;
        };

        let logs = accept.logs.clone();
        let acl = accept.acl.clone();
        let limits = accept.limits;
        tokio::spawn(async move {
            serve(stream, uid, logs, acl, limits).await;
            drop(permit);
        });
    }
}

/// Say why before closing: a client that is merely on the wrong uid should
/// not have to guess.
/// Say why before hanging up. A closed socket with no frame leaves the peer
/// with a broken pipe and nothing to act on.
async fn refuse(stream: UnixStream, code: Code, message: &str) {
    let mut framed = Framed::new(stream, Codec::<Request, Response>::new());
    let error = Response::Error { id: 0, code, message: message.into() };
    let _ = framed.send(error).await;
    let _ = framed.into_inner().shutdown().await;
}

async fn serve(
    stream: UnixStream,
    uid: u32,
    logs: Arc<dyn Logs>,
    acl: Arc<Acl>,
    limits: Limits,
) {
    let (sink, mut requests) = Framed::new(stream, Codec::<Request, Response>::new()).split();
    let (out, outbox) = mpsc::channel(limits.outbox);
    let writer = tokio::spawn(write(sink, outbox));
    let session = Session::new(logs, out, uid, acl, limits, None);

    while let Some(frame) = requests.next().await {
        match frame {
            Ok(request) => session.dispatch(request).await,
            Err(e) => {
                let code = match e {
                    CodecError::Io(_) => break,
                    CodecError::TooLarge(_) | CodecError::Json(_) => Code::Malformed,
                };
                session
                    .reply(Response::Error { id: 0, code, message: e.to_string() })
                    .await;
                break;
            }
        }
    }

    // Dropping the session cancels its streams and closes the outbox, which
    // lets the writer flush what is already queued and then finish.
    session.close();
    drop(session);
    let _ = writer.await;
}

/// The only task that touches the sink. Two concurrent writes to one socket
/// interleave into garbage, so everything outbound funnels through here, and
/// a wakeup carrying many frames costs one flush.
async fn write<S>(mut sink: S, mut outbox: mpsc::Receiver<Response>)
where
    S: futures::Sink<Response, Error = CodecError> + Unpin,
{
    while let Some(first) = outbox.recv().await {
        if sink.feed(first).await.is_err() {
            break;
        }
        while let Ok(next) = outbox.try_recv() {
            if sink.feed(next).await.is_err() {
                return;
            }
        }
        if sink.flush().await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}
