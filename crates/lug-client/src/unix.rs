//! The unix socket transport: one framed socket, one writer, one reader.

use crate::error::Result;
use crate::transport::Wire;
use futures::{SinkExt, StreamExt};
use lug_proto::{Codec, Request, Response};
use std::path::Path;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

pub(crate) async fn dial(path: &Path, buffer: usize) -> Result<Wire> {
    let stream = UnixStream::connect(path)
        .await
        .map_err(|e| Wire::connect_error(&path.display().to_string(), e))?;
    let framed = Framed::new(stream, Codec::<Response, Request>::new());
    let (mut sink, mut source) = framed.split();

    let (out, mut out_rx) = mpsc::channel::<Request>(buffer);
    let (inbound_tx, inbound) = mpsc::channel::<Response>(buffer);

    tokio::spawn(async move {
        // One task owns the sink for its whole life. Two tasks writing one
        // socket would interleave halves of two frames into garbage.
        while let Some(first) = out_rx.recv().await {
            if sink.feed(first).await.is_err() {
                break;
            }
            // Whatever else queued up while we were parked rides the same
            // flush: one syscall for a burst of appends and credit grants.
            let mut failed = false;
            while let Ok(next) = out_rx.try_recv() {
                if sink.feed(next).await.is_err() {
                    failed = true;
                    break;
                }
            }
            if failed || sink.flush().await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    tokio::spawn(async move {
        while let Some(frame) = source.next().await {
            match frame {
                Ok(frame) => {
                    if inbound_tx.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, "unix connection decode failed");
                    break;
                }
            }
        }
        // Dropping inbound_tx is the death notice.
    });

    Ok(Wire { out, inbound })
}
