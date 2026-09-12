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
use tokio_util::sync::PollSender;

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
    pending_credit: u32,
    credit: PollSender<Command>,
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
            credit: PollSender::new(handle.cmd.clone()),
            handle,
            rx,
            overrun,
            window,
            drained: 0,
            pending_credit: 0,
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
        self.pending_credit = self.pending_credit.saturating_add(records);
        let req = Request::Credit { id: self.id, grant: self.pending_credit };
        if self.pending_credit > 0 && self.handle.cmd.try_send(Command::Fire(req)).is_ok() {
            self.pending_credit = 0;
        }
    }

    fn maybe_grant(&mut self, cx: &mut Context<'_>) {
        let threshold = (self.window / 2).max(1);
        if self.drained >= threshold {
            self.pending_credit = self.pending_credit.saturating_add(std::mem::take(&mut self.drained));
        }
        // A full queue must register a wakeup: the server may already have
        // exhausted its credit, leaving no next record to trigger a retry.
        if self.pending_credit > 0 && matches!(self.credit.poll_reserve(cx), Poll::Ready(Ok(()))) {
            let grant = std::mem::take(&mut self.pending_credit);
            let _ = self.credit.send_item(Command::Fire(Request::Credit { id: self.id, grant }));
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
            this.maybe_grant(cx);
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
                        this.maybe_grant(cx);
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use lug_proto::{Mode, Record};
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn the_last_credit_grant_survives_a_full_command_queue() {
        let (cmd, mut commands) = mpsc::channel(1);
        let handle = Handle::new(cmd.clone(), Arc::new(AtomicUsize::new(0)));
        let mut frames = Frames::open(
            handle,
            7,
            Request::Subscribe { id: 7, log: "log".into(), from: 0, mode: Mode::Records, credit: 1 },
            1,
            Duration::from_secs(1),
        ).await.unwrap();
        let Some(Command::Stream { sink, .. }) = commands.recv().await else { panic!("subscribe") };
        cmd.send(Command::Fire(Request::Ping { id: 8 })).await.unwrap_or_else(|_| panic!("fill queue"));
        sink.send(Ok(Response::Records { id: 7, records: vec![Record { version: 1, patch: serde_json::Value::Null }] })).await.unwrap();
        assert!(matches!(frames.next().await, Some(Ok(Response::Records { .. }))));

        let next = frames.next();
        futures::pin_mut!(next);
        assert!(futures::poll!(&mut next).is_pending());
        assert!(matches!(commands.recv().await, Some(Command::Fire(Request::Ping { id: 8 }))));
        // No more records can arrive until this grant goes out. Retrying only
        // on the next record leaves both sides waiting for each other.
        let grant = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                frame = &mut next => panic!("stream ended while granting credit: {frame:?}"),
                command = commands.recv() => command,
            }
        }).await.expect("credit was lost when the command queue filled");
        assert!(matches!(grant, Some(Command::Fire(Request::Credit { id: 7, grant: 1 }))));
    }
}
