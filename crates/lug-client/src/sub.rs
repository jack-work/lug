//! Subscriptions: the ordered stream a subscriber observes, with the client
//! half of the credit contract wired to the consumer's own draining.

use crate::conn::{Command, Handle, Load};
use crate::error::{Error, Result};
use futures::Stream;
use lug_proto::{Event, Id, Request, Response};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// What a subscriber observes: records in version order, and the gaps between
/// them, ending after the server closes the stream.
///
/// A gap is part of the stream rather than an error beside it, so a consumer
/// cannot accidentally present a reclaimed range as an invisible jump in
/// versions.
///
/// Credit is the point. The server may push at most the granted number of
/// records and then stops. This type grants more only as events are actually
/// taken out of it, so a consumer that stops polling stops the flow rather
/// than growing a buffer somewhere. Dropping it cancels the subscription.
pub struct Subscription {
    frames: Frames,
    pending: VecDeque<Event>,
}

impl Subscription {
    pub fn id(&self) -> Id {
        self.frames.id()
    }

    /// Records the server may have in flight toward us.
    pub fn window(&self) -> u32 {
        self.frames.window()
    }

    /// The underlying frames, including the view preamble of a
    /// [`Mode::Reducible`](lug_proto::Mode) subscription, which an event
    /// stream has nowhere to put.
    pub fn frames(self) -> Frames {
        self.frames
    }
}

impl From<Frames> for Subscription {
    fn from(frames: Frames) -> Self {
        Self {
            frames,
            pending: VecDeque::new(),
        }
    }
}

impl Stream for Subscription {
    type Item = Result<Event>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(event)));
            }
            match Pin::new(&mut this.frames).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(frame))) => match frame {
                    Response::Records { records, .. } => {
                        this.pending.extend(records.into_iter().map(Event::Record));
                    }
                    Response::Gap { from, to, .. } => {
                        this.pending.push_back(Event::Gap { from, to });
                    }
                    // A view preamble belongs to a follower, not to a stream
                    // of what changed.
                    _ => continue,
                },
            }
        }
    }
}

/// The raw frames of one subscription, for a caller that needs more than the
/// events: a [`Response::View`] preamble, or the server's acknowledgement.
pub struct Frames {
    id: Id,
    handle: Handle,
    rx: mpsc::Receiver<Result<Response>>,
    overrun: Arc<AtomicBool>,
    window: u32,
    /// Records handed to the consumer since the last grant went out.
    drained: u32,
    done: bool,
    _charged: Load,
}

impl Frames {
    pub(crate) async fn open(
        handle: Handle,
        id: Id,
        req: Request,
        window: u32,
        timeout: std::time::Duration,
    ) -> Result<Self> {
        // Sized to the credit window: the server may not push past it, so a
        // full channel means the contract was broken, not that we are slow.
        let (sink, rx) = mpsc::channel(window.clamp(1, 4096) as usize);
        let overrun = Arc::new(AtomicBool::new(false));
        let charged = handle.charge();
        let cmd = Command::Stream {
            req,
            sink,
            overrun: overrun.clone(),
        };
        match tokio::time::timeout(timeout, handle.cmd.send(cmd)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(Error::Disconnected { id }),
            Err(_) => {
                return Err(Error::Timeout {
                    id,
                    elapsed: timeout,
                });
            }
        }
        Ok(Self {
            id,
            handle,
            rx,
            overrun,
            window,
            drained: 0,
            done: false,
            _charged: charged,
        })
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn window(&self) -> u32 {
        self.window
    }

    /// Ask for more room before the consumer has earned it. Rarely needed:
    /// draining grants credit on its own.
    pub fn grant(&mut self, records: u32) {
        self.send_credit(records);
    }

    fn send_credit(&mut self, grant: u32) {
        if grant == 0 {
            return;
        }
        let req = Request::Credit { id: self.id, grant };
        // A full command queue means the connection is congested; keep the
        // count and try again on the next drain rather than blocking a poll.
        if self.handle.cmd.try_send(Command::Fire(req)).is_ok() {
            self.drained = self.drained.saturating_sub(grant);
        }
    }

    /// Replenish once the consumer has taken half a window, which keeps the
    /// server pushing without a grant per record.
    fn maybe_grant(&mut self) {
        let threshold = (self.window / 2).max(1);
        if self.drained >= threshold {
            let grant = self.drained;
            self.send_credit(grant);
        }
    }
}

impl Stream for Frames {
    type Item = Result<Response>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            match this.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.done = true;
                    let err = if this.overrun.load(Ordering::Acquire) {
                        Error::Overrun { id: this.id }
                    } else {
                        Error::Disconnected { id: this.id }
                    };
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Err(e))) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(Some(Ok(frame))) => match frame {
                    // The acknowledgement that the stream is open, and, over
                    // SSE, the session the transport already consumed. Both
                    // are plumbing, not data.
                    Response::Ok { .. } | Response::Welcome { .. } => continue,
                    Response::End { .. } => {
                        this.done = true;
                        return Poll::Ready(None);
                    }
                    Response::Records { id, records } => {
                        this.drained = this.drained.saturating_add(records.len() as u32);
                        this.maybe_grant();
                        return Poll::Ready(Some(Ok(Response::Records { id, records })));
                    }
                    other => return Poll::Ready(Some(Ok(other))),
                },
            }
        }
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        if !self.done {
            let _ = self
                .handle
                .cmd
                .try_send(Command::Fire(Request::Cancel { id: self.id }));
        }
        let _ = self.handle.cmd.try_send(Command::Forget(self.id));
    }
}
