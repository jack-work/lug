//! The follower: a server-side log, rebuilt and kept live in this process.

use crate::error::{Error, Result};
use crate::hub::{Hub, Subscribe, View};
use cavlc::{Snapshot, Store};
use futures::StreamExt;
use lug_proto::{Code, Event, Mode, Record, Response, Version};
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
    /// The materialized view as an MVCC pointer: `None` on a log that keeps
    /// no view, where only the record stream means anything.
    pub snapshot: Option<Snapshot>,
    /// Highest version folded in so far.
    pub version: Version,
    /// Whether this log folds patches into a view at all.
    pub reducible: bool,
    pub status: Status,
}

/// A live mirror of a log.
///
/// Subscribes in [`Mode::Reducible`], rebuilds a [`cavlc::Store`] from the
/// view preamble, and folds every arriving patch into it. The view is an MVCC
/// pointer: O(1) to clone and safe to hold across a render while the follower
/// keeps advancing behind it.
///
/// It survives the connection dying: it resubscribes from its cursor, takes a
/// fresh view preamble, and carries on, reporting [`Status::Reconnecting`] in
/// between. A gap is recovered the same way, by refetching the view rather
/// than folding on top of versions that are gone, and the gap still reaches
/// [`records`](Self::records) so a viewer can say what it lost.
///
/// Cloning is cheap and every clone sees the same stream, so a UI can hand a
/// clone to each widget. The background task stops when the last clone drops.
///
/// ```no_run
/// # async fn example(hub: lug_client::Hub) -> lug_client::Result<()> {
/// let follower = hub.follow("orders").await?;
/// let mut changed = follower.changed();
/// loop {
///     if let Some(view) = follower.view() {
///         render(view.version, &view.value);
///     }
///     changed.changed().await.ok();
/// }
/// # }
/// # fn render(_: u64, _: &serde_json::Value) {}
/// ```
#[derive(Clone)]
pub struct Follower {
    log: Arc<str>,
    state: watch::Receiver<State>,
    /// Separate from `state` so a render loop can await a `u64` without
    /// cloning a whole state to find out nothing it cares about moved.
    versions: watch::Receiver<Version>,
    records: Arc<Mutex<Option<mpsc::Receiver<Event>>>>,
    /// The current view as JSON, built once per version rather than once per
    /// frame of whoever is drawing it.
    json: Arc<Mutex<Option<View>>>,
}

impl Follower {
    pub(crate) async fn start(hub: Hub, log: &str, credit: u32) -> Result<Self> {
        let log: Arc<str> = Arc::from(log);
        let initial = State {
            snapshot: None,
            version: 0,
            reducible: true,
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
            dropped: None,
            cursor: 0,
            store: None,
            mode: Mode::Reducible,
        };
        tokio::spawn(task.run());
        Ok(Self {
            log,
            state,
            versions,
            records: Arc::new(Mutex::new(Some(events_rx))),
            json: Arc::new(Mutex::new(None)),
        })
    }

    pub fn log(&self) -> &str {
        &self.log
    }

    /// The current view, or `None` on a log that keeps none.
    ///
    /// Cheap enough for a render loop: the JSON is built once per version and
    /// cached, so repeated calls at the same version copy a value instead of
    /// walking the tree.
    pub fn view(&self) -> Option<View> {
        let snapshot = self.state.borrow().snapshot.clone()?;
        let mut cached = self.json.lock().ok()?;
        if cached
            .as_ref()
            .is_none_or(|v| v.version != snapshot.version())
        {
            *cached = Some(View {
                version: snapshot.version(),
                value: snapshot.root().to_json(),
            });
        }
        cached.clone()
    }

    /// The view as an MVCC pointer: O(1), no JSON at all. The cheapest way to
    /// read the structure, and the one to prefer when the caller speaks
    /// `cavlc`.
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.state.borrow().snapshot.clone()
    }

    /// Whether this log folds patches into a view.
    ///
    /// Separate from [`view`](Self::view) being `None`, which is also true of
    /// a reducible log that has not received its preamble yet.
    pub fn reducible(&self) -> bool {
        self.state.borrow().reducible
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

    /// A watch that ticks once per version advance, on a reducible log and a
    /// plain one alike. `*rx.borrow()` is the current version.
    pub fn changed(&self) -> watch::Receiver<Version> {
        self.versions.clone()
    }

    /// Wait for the next change: a new version, or a change of status.
    ///
    /// Fails with [`Error::Closed`] once the follower has stopped for good.
    pub async fn next_change(&mut self) -> Result<State> {
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

    /// The arriving events, in version order, gaps included.
    ///
    /// Handed out once; later calls return `None`. A consumer that stops
    /// reading loses events rather than slowing the view down, and what it
    /// lost arrives as an [`Event::Gap`] once it reads again, because a
    /// silent jump in versions would be a lie.
    pub fn records(&mut self) -> Option<mpsc::Receiver<Event>> {
        self.records.lock().ok()?.take()
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
    /// Versions a stalled consumer missed, reported as a gap once it reads
    /// again rather than as a silent jump.
    dropped: Option<(Version, Version)>,
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
        // Frames, not events: only the view preamble can rebuild the store,
        // and an event stream has nowhere to put it.
        let frames = self.hub.subscribe_frames(&self.log, options).await?;
        futures::pin_mut!(frames);
        loop {
            let frame = tokio::select! {
                _ = self.state.closed() => return Ok(Step::Closed),
                frame = frames.next() => frame,
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
                    if !self.recover(from, to).await? {
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
                return self.recover(self.cursor, record.version - 1).await;
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

    /// Recover from a gap by refetching the view.
    ///
    /// The patches that would have carried the view from `from` to `to` are
    /// gone, so folding is not an option and the server's current view is the
    /// only truthful answer. `false` asks for a restart when the refetch
    /// cannot be trusted.
    async fn recover(&mut self, from: Version, to: Version) -> Result<bool> {
        if self.mode == Mode::Records {
            self.cursor = to;
            self.emit(Event::Gap { from, to }).await;
            self.publish(Status::Live);
            return Ok(true);
        }
        match self.hub.read(&self.log, None).await {
            Ok(view) => {
                if view.version < to {
                    // The view is behind the gap's floor, which can only mean
                    // we raced a truncation; start the whole stream over.
                    return Ok(false);
                }
                self.adopt(view.version, &view.value)?;
                // The gap the consumer is told about runs to where the view
                // actually resumed, not to where the server said it would,
                // because everything in between arrived inside the view.
                self.emit(Event::Gap {
                    from,
                    to: self.cursor,
                })
                .await;
                Ok(true)
            }
            Err(e) if e.is_transient() => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn publish(&mut self, status: Status) {
        let snapshot = self.store.as_ref().map(|s| s.snapshot());
        let version = self.cursor;
        let _ = self.versions.send(version);
        let _ = self.state.send(State {
            snapshot,
            version,
            reducible: self.mode == Mode::Reducible,
            status,
        });
    }

    /// Never blocks the fold: the view is maintained independently of whether
    /// anyone is reading the event stream. What a stalled consumer missed is
    /// handed to it as a gap, since a jump in versions it cannot see would be
    /// a lie about contiguity.
    async fn emit(&mut self, event: Event) {
        if let Some((from, to)) = self.dropped {
            if self.events.try_send(Event::Gap { from, to }).is_ok() {
                self.dropped = None;
            } else {
                self.dropped = Some((from, event.cursor()));
                return;
            }
        }
        let cursor = event.cursor();
        if self.events.try_send(event).is_err() {
            let from = self
                .dropped
                .map_or(cursor.saturating_sub(1), |(from, _)| from);
            self.dropped = Some((from, cursor));
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
