//! The follower: a server-side log, rebuilt and kept live in this process.

use crate::error::{Error, Result};
use crate::hub::{Hub, Subscribe};
use cavlc::{Snapshot, Store};
use futures::StreamExt;
use lug_proto::{Code, Mode, Record, Response, Version};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

/// How a follower is faring, alongside the view it last managed to build.
#[derive(Clone, Debug)]
pub enum Status {
    /// Attached to a stream and folding patches as they arrive.
    Live,
    /// The stream broke. The view is the last good one and a resubscribe is
    /// running; nothing is lost, the view is simply stale.
    Reconnecting,
    /// The server closed the stream. Nothing further will arrive.
    Ended,
    /// Stopped for good, with the reason. The view stays readable.
    Failed(Arc<Error>),
}

/// Failures compare by what they say, since the errors themselves are not
/// comparable and a UI only ever cares whether the reason changed.
impl PartialEq for Status {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Live, Self::Live)
            | (Self::Reconnecting, Self::Reconnecting)
            | (Self::Ended, Self::Ended) => true,
            (Self::Failed(a), Self::Failed(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
}

impl Status {
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }

    /// Whether anything more can arrive.
    pub fn is_final(&self) -> bool {
        matches!(self, Self::Ended | Self::Failed(_))
    }
}

/// Everything a renderer needs in one immutable value.
#[derive(Clone, Debug)]
pub struct State {
    /// The materialized view, or `None` on a log that is not reducible, where
    /// only the record stream means anything.
    pub view: Option<Snapshot>,
    /// Highest version folded in so far.
    pub version: Version,
    pub status: Status,
}

/// Something that happened on the stream, for a caller that wants the records
/// themselves rather than just the folded view.
#[derive(Clone, Debug)]
pub enum Event {
    /// One record, in version order.
    Record(Record),
    /// Versions in `(from, to]` are gone from the server's retention. The
    /// view is refetched, so it stays correct; the records are not.
    Gap { from: Version, to: Version },
    /// The view was rebuilt from scratch at this version, after a gap or a
    /// reconnect. Anything a consumer derived from earlier records is stale.
    Reset { version: Version },
    /// Events were dropped because the consumer was not reading them. The
    /// view is unaffected; it is maintained independently of this channel.
    Dropped(u64),
}

/// A live mirror of a log.
///
/// Subscribes in [`Mode::Reducible`], rebuilds a [`cavlc::Store`] from the
/// view preamble, and folds every arriving patch into it. [`view`](Self::view)
/// hands out an immutable MVCC snapshot that is O(1) to clone and safe to
/// hold across a render; the follower keeps advancing behind it.
///
/// It survives the connection dying: it resubscribes from its cursor, takes a
/// fresh view preamble, and carries on, reporting [`Status::Reconnecting`] in
/// between. A [`Response::Gap`] is recovered the same way, by refetching the
/// view rather than pretending the missing versions never existed.
///
/// Cloning is cheap and every clone sees the same stream, so a UI can hand a
/// clone to each widget. The background task stops when the last clone drops.
///
/// ```no_run
/// # async fn example(hub: lug_client::Hub) -> lug_client::Result<()> {
/// let mut follower = hub.follow("orders").await?;
/// loop {
///     if let Some(view) = follower.view() {
///         render(&view);
///     }
///     follower.changed().await?;
/// }
/// # }
/// # fn render(_: &cavlc::Snapshot) {}
/// ```
#[derive(Clone)]
pub struct Follower {
    log: Arc<str>,
    state: watch::Receiver<State>,
    /// Separate from `state` so a consumer that only cares about "something
    /// advanced" can await a `u64` without cloning a whole state.
    versions: watch::Receiver<Version>,
    events: Arc<Mutex<Option<mpsc::Receiver<Event>>>>,
}

impl Follower {
    pub(crate) async fn start(hub: Hub, log: &str, credit: u32) -> Result<Self> {
        let log: Arc<str> = Arc::from(log);
        let initial = State {
            view: None,
            version: 0,
            status: Status::Reconnecting,
        };
        let (state_tx, state) = watch::channel(initial);
        let (version_tx, versions) = watch::channel(0);
        let (events_tx, events_rx) = mpsc::channel(credit.clamp(1, 4096) as usize);
        let task = Task {
            hub,
            log: log.clone(),
            credit,
            state: state_tx,
            versions: version_tx,
            events: events_tx,
            dropped: 0,
            cursor: 0,
            store: None,
            mode: Mode::Reducible,
        };
        tokio::spawn(task.run());
        Ok(Self {
            log,
            state,
            versions,
            events: Arc::new(Mutex::new(Some(events_rx))),
        })
    }

    pub fn log(&self) -> &str {
        &self.log
    }

    /// The current view. Cheap: an MVCC pointer clone, no copying and no
    /// locking, safe to hold while the follower keeps advancing.
    pub fn view(&self) -> Option<Snapshot> {
        self.state.borrow().view.clone()
    }

    /// The current view as plain JSON. Costs a walk of the tree, so call it
    /// when the version changes, not once per frame of a render loop.
    pub fn value(&self) -> Option<Value> {
        self.state
            .borrow()
            .view
            .as_ref()
            .map(|v| v.root().to_json())
    }

    pub fn version(&self) -> Version {
        *self.versions.borrow()
    }

    pub fn status(&self) -> Status {
        self.state.borrow().status.clone()
    }

    /// Everything at once, consistent with itself.
    pub fn state(&self) -> State {
        self.state.borrow().clone()
    }

    /// Wait for the next change: a new version, or a change of status.
    ///
    /// Returns the state that caused the wake. Fails with [`Error::Closed`]
    /// once the follower has stopped for good.
    pub async fn changed(&mut self) -> Result<State> {
        self.state.changed().await.map_err(|_| Error::Closed)?;
        Ok(self.state.borrow_and_update().clone())
    }

    /// Wait until the view has folded in `version`.
    ///
    /// The natural way to read your own write: append, then wait for the
    /// version the ack named.
    pub async fn wait_for(&mut self, version: Version) -> Result<State> {
        loop {
            {
                let state = self.state.borrow_and_update();
                if state.version >= version {
                    return Ok(state.clone());
                }
                if let Status::Failed(e) = &state.status {
                    return Err(Error::Protocol {
                        id: 0,
                        detail: format!("follower stopped before version {version}: {e}"),
                    });
                }
            }
            self.state.changed().await.map_err(|_| Error::Closed)?;
        }
    }

    /// A watch of the whole state, for binding a UI directly to it.
    pub fn watch(&self) -> watch::Receiver<State> {
        self.state.clone()
    }

    /// A watch that ticks once per version advance, on reducible logs and
    /// plain ones alike.
    pub fn versions(&self) -> watch::Receiver<Version> {
        self.versions.clone()
    }

    /// The arriving records, in version order, with gaps marked.
    ///
    /// Handed out once; later calls return `None`. A consumer that stops
    /// reading loses events, reported as [`Event::Dropped`], and never slows
    /// the view down.
    pub fn events(&self) -> Option<mpsc::Receiver<Event>> {
        self.events.lock().ok()?.take()
    }
}

/// What ended a subscription attempt.
enum Step {
    /// The server closed the stream.
    Ended,
    /// Resubscribe now, without backing off: this was our decision, not a
    /// failure.
    Restart,
    /// Every handle is gone.
    Closed,
}

struct Task {
    hub: Hub,
    log: Arc<str>,
    credit: u32,
    state: watch::Sender<State>,
    versions: watch::Sender<Version>,
    events: mpsc::Sender<Event>,
    dropped: u64,
    cursor: Version,
    store: Option<Store>,
    mode: Mode,
}

impl Task {
    async fn run(mut self) {
        let backoff = self.hub.config().backoff;
        let mut delay = backoff.min;
        loop {
            match self.session().await {
                Ok(Step::Closed) => return,
                Ok(Step::Restart) => {
                    delay = backoff.min;
                }
                Ok(Step::Ended) => {
                    self.publish(Status::Ended);
                    return;
                }
                Err(e) if fatal(&e) => {
                    self.publish(Status::Failed(Arc::new(e)));
                    return;
                }
                Err(e) => {
                    tracing::debug!(log = %self.log, error = %e, "follower resubscribing");
                    self.publish(Status::Reconnecting);
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.state.closed() => return,
                    }
                    delay = (delay * 2).min(backoff.max);
                }
            }
            if self.state.is_closed() {
                return;
            }
        }
    }

    /// One subscription, from open until it ends or breaks.
    async fn session(&mut self) -> Result<Step> {
        let options = Subscribe {
            from: self.cursor,
            mode: self.mode,
            credit: Some(self.credit),
        };
        let sub = self.hub.subscribe(&self.log, options).await?;
        futures::pin_mut!(sub);
        loop {
            let frame = tokio::select! {
                _ = self.state.closed() => return Ok(Step::Closed),
                frame = sub.next() => frame,
            };
            let Some(frame) = frame else {
                // The stream ended cleanly; a broken connection arrives as an
                // error item instead.
                return Ok(Step::Ended);
            };
            match frame {
                Err(Error::Server {
                    code: Code::NotReducible,
                    ..
                }) if self.mode == Mode::Reducible => {
                    // The log keeps no view. Follow the records instead, which
                    // is all there is to follow.
                    self.mode = Mode::Records;
                    return Ok(Step::Restart);
                }
                Err(Error::Server {
                    code: Code::OutOfRange,
                    ..
                }) => {
                    // Our cursor is outside what the server can serve, so ask
                    // for everything it still has.
                    self.cursor = 0;
                    self.store = None;
                    return Ok(Step::Restart);
                }
                Err(e) => return Err(e),
                Ok(Response::View { version, value, .. }) => {
                    self.adopt(version, &value)?;
                }
                Ok(Response::Records { records, .. }) => {
                    if !self.fold(records).await? {
                        return Ok(Step::Restart);
                    }
                }
                Ok(Response::Gap { from, to, .. }) => {
                    self.emit(Event::Gap { from, to }).await;
                    if !self.recover(to).await? {
                        return Ok(Step::Restart);
                    }
                }
                Ok(_) => {}
            }
        }
    }

    /// Rebuild from a view preamble. The server's view is authoritative: it
    /// replaces whatever we held, and the cursor moves to its version.
    fn adopt(&mut self, version: Version, value: &Value) -> Result<()> {
        let snapshot = snapshot_from(version, value)?;
        self.cursor = snapshot.version();
        self.store = Some(Store::resume(snapshot));
        self.publish(Status::Live);
        Ok(())
    }

    /// Fold a pushed batch. `false` asks for a restart, because the stream no
    /// longer lines up with what we hold.
    async fn fold(&mut self, records: Vec<Record>) -> Result<bool> {
        for record in records {
            if record.version <= self.cursor {
                // A replay of what we already folded, which a resubscribe from
                // a cursor the server rounded down can produce.
                continue;
            }
            if record.version != self.cursor + 1 {
                // Versions are contiguous by contract, so a jump means we
                // missed something and must not fold on top of it.
                return self.recover(record.version - 1).await;
            }
            if let Some(store) = self.store.as_mut() {
                let patch: cavlc::Patch =
                    serde_json::from_value(record.patch.clone()).map_err(|e| Error::Protocol {
                        id: 0,
                        detail: format!("version {} is not a patch: {e}", record.version),
                    })?;
                let applied = store.apply(&patch).map_err(Error::View)?;
                if applied.snapshot.version() != record.version {
                    return Err(Error::Protocol {
                        id: 0,
                        detail: format!(
                            "folded to version {} but the record says {}",
                            applied.snapshot.version(),
                            record.version
                        ),
                    });
                }
            }
            self.cursor = record.version;
            self.emit(Event::Record(record)).await;
        }
        self.publish(Status::Live);
        Ok(true)
    }

    /// Refetch the view after a gap, so the structure is correct even though
    /// the records that produced it are gone. `false` asks for a restart when
    /// the refetch cannot be trusted.
    async fn recover(&mut self, floor: Version) -> Result<bool> {
        if self.mode == Mode::Records {
            self.cursor = floor;
            self.publish(Status::Live);
            return Ok(true);
        }
        match self.hub.read(&self.log, None).await {
            Ok(view) => {
                if view.version < floor {
                    // The view is older than the gap's floor, which can only
                    // mean we raced a truncation; start the whole stream over.
                    return Ok(false);
                }
                self.adopt(view.version, &view.value)?;
                self.emit(Event::Reset {
                    version: self.cursor,
                })
                .await;
                Ok(true)
            }
            Err(e) if e.is_transient() => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn publish(&mut self, status: Status) {
        let view = self.store.as_ref().map(|s| s.snapshot());
        let version = self.cursor;
        let _ = self.versions.send(version);
        let _ = self.state.send(State {
            view,
            version,
            status,
        });
    }

    /// Never blocks the fold: the view is maintained independently of whether
    /// anyone is reading the record stream.
    async fn emit(&mut self, event: Event) {
        if self.dropped > 0 && self.events.try_send(Event::Dropped(self.dropped)).is_ok() {
            self.dropped = 0;
        }
        if self.events.try_send(event).is_err() {
            self.dropped += 1;
        }
    }
}

/// Whether an error means stop rather than try again.
fn fatal(err: &Error) -> bool {
    match err {
        Error::Server { code, .. } => matches!(
            code,
            Code::NoSuchLog | Code::Unauthorized | Code::Malformed | Code::Rejected
        ),
        Error::View(_) => true,
        Error::Closed => true,
        _ => false,
    }
}

/// Read a [`Response::View`] payload as a snapshot.
///
/// The server may send the snapshot itself, a log view wrapping it, or the
/// bare root, so all three are accepted; the frame's version decides when the
/// payload does not carry one.
pub(crate) fn snapshot_from(version: Version, value: &Value) -> Result<Snapshot> {
    if let Ok(snapshot) = serde_json::from_value::<Snapshot>(value.clone()) {
        return Ok(snapshot);
    }
    if let Some(inner) = value.get("view") {
        return snapshot_from(version, inner);
    }
    Snapshot::new(version, cavlc::Value::from(value.clone())).map_err(Error::View)
}
