//! The per-log actor: the only place a `Log` is touched.
//!
//! The actor is generic over the data structure and storage it owns, so the
//! `Log` keeps full type information. Erasure happens one level out, at the
//! handle in the registry, which is only channels. Patches cross that line as
//! the JSON they arrived as and are decoded here, on the core that owns them.

use crate::ring::{Entry, Ring, estimate};
use lug_core::{Durability, Log, Reducible, Storage, Version, Versioned};
use lug_proto::Code;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct Failure {
    pub code: Code,
    pub message: String,
}

impl Failure {
    pub fn new(code: Code, message: impl std::fmt::Display) -> Self {
        Self { code, message: message.to_string() }
    }
}

#[derive(Debug)]
pub struct Ack {
    pub versions: Vec<Version>,
    pub synced: Version,
}

/// What a subscriber below the ring gets back.
pub enum CatchUp {
    Records(Vec<Arc<Entry>>),
    /// Everything in `(after, to]` was reclaimed; resume at `to`.
    Gap { to: Version },
}

pub enum Command {
    Append {
        patches: Vec<Value>,
        durability: Durability,
        reply: oneshot::Sender<Result<Ack, Failure>>,
    },
    View {
        at: Option<Version>,
        reply: oneshot::Sender<Result<(Version, Value), Failure>>,
    },
    CatchUp {
        after: Version,
        limit: usize,
        reply: oneshot::Sender<Result<CatchUp, Failure>>,
    },
    Checkpoint {
        reply: oneshot::Sender<Result<(), Failure>>,
    },
}

impl Command {
    fn patch_count(&self) -> usize {
        match self {
            Self::Append { patches, .. } => patches.len(),
            _ => 0,
        }
    }

    fn fail(self, failure: impl Fn() -> Failure) {
        match self {
            Self::Append { reply, .. } => drop(reply.send(Err(failure()))),
            Self::View { reply, .. } => drop(reply.send(Err(failure()))),
            Self::CatchUp { reply, .. } => drop(reply.send(Err(failure()))),
            Self::Checkpoint { reply } => drop(reply.send(Err(failure()))),
        }
    }
}

/// Counters for the group-commit story: patches folded, batches appended,
/// syncs issued. `syncs / patches` is the amortization ratio.
#[derive(Debug, Default)]
pub struct Metrics {
    pub patches: AtomicU64,
    pub batches: AtomicU64,
    pub syncs: AtomicU64,
}

impl Metrics {
    fn record(&self, patches: usize, synced: bool) {
        self.patches.fetch_add(patches as u64, Ordering::Relaxed);
        self.batches.fetch_add(1, Ordering::Relaxed);
        if synced {
            self.syncs.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.patches.load(Ordering::Relaxed),
            self.batches.load(Ordering::Relaxed),
            self.syncs.load(Ordering::Relaxed),
        )
    }
}

/// Everything the actor publishes outward, shared with the handle.
pub struct Shared {
    pub ring: Arc<Ring>,
    pub watermark: watch::Sender<Version>,
    pub oldest: AtomicU64,
    pub metrics: Metrics,
}

type LogError<D, S> =
    lug_core::Error<<D as Reducible>::Error, <S as Storage>::Error>;

pub struct Actor<D: Reducible, S: Storage<View = D::View>> {
    log: Log<D, S>,
    shared: Arc<Shared>,
    batch: usize,
    checkpoint_every: u64,
    checkpointed: Version,
}

struct Job<P> {
    raw: Vec<Value>,
    patches: Vec<P>,
    durability: Durability,
    reply: oneshot::Sender<Result<Ack, Failure>>,
}

impl<D, S> Actor<D, S>
where
    D: Reducible,
    S: Storage<View = D::View>,
{
    pub fn new(log: Log<D, S>, shared: Arc<Shared>, batch: usize, checkpoint_every: u64) -> Self {
        let watermark = log.watermark();
        shared.oldest.store(log.oldest(), Ordering::Relaxed);
        let _ = shared.watermark.send(watermark);
        Self { log, shared, batch: batch.max(1), checkpoint_every, checkpointed: watermark }
    }

    /// Take one command, then everything else already queued, then commit the
    /// whole run behind a single append. No timer: an idle log pays one
    /// record and one flush, a loaded one amortizes both over the batch.
    pub async fn run(mut self, mut rx: mpsc::Receiver<Command>) {
        let mut queue: Vec<Command> = Vec::new();
        while let Some(first) = rx.recv().await {
            let mut patches = first.patch_count();
            queue.push(first);
            while patches < self.batch {
                match rx.try_recv() {
                    Ok(command) => {
                        patches += command.patch_count();
                        queue.push(command);
                    }
                    Err(_) => break,
                }
            }
            self.drain(&mut queue);
        }
    }

    fn drain(&mut self, queue: &mut Vec<Command>) {
        let mut batch: Vec<Job<D::Patch>> = Vec::new();
        for command in queue.drain(..) {
            match command {
                Command::Append { patches, durability, reply } => {
                    match decode::<D::Patch>(&patches) {
                        Ok(decoded) => {
                            batch.push(Job { raw: patches, patches: decoded, durability, reply })
                        }
                        Err(e) => drop(reply.send(Err(Failure::new(Code::Rejected, e)))),
                    }
                }
                // A read must observe the appends queued ahead of it.
                other => {
                    self.commit(std::mem::take(&mut batch));
                    self.answer(other);
                }
            }
        }
        self.commit(batch);
    }

    /// The group commit: every queued request folded, written and synced once.
    fn commit(&mut self, mut jobs: Vec<Job<D::Patch>>) {
        if jobs.is_empty() {
            return;
        }
        let durability = jobs.iter().map(|j| j.durability).max_by_key(rank).unwrap_or_default();
        let counts: Vec<usize> = jobs.iter().map(|j| j.patches.len()).collect();
        let patches: Vec<D::Patch> =
            jobs.iter_mut().flat_map(|j| std::mem::take(&mut j.patches)).collect();
        let base = self.log.watermark();

        let views = match self.log.append(&patches, durability) {
            Ok(views) => views,
            Err(e) => return self.isolate(jobs, counts, patches, e),
        };
        self.shared.metrics.record(patches.len(), durability == Durability::Durable);

        // A patch that changed nothing mints no version, and the view it
        // returns repeats the previous one.
        let mut minted = Vec::with_capacity(views.len());
        let mut last = base;
        for view in &views {
            let version = view.version();
            minted.push((version > last).then_some(version));
            last = last.max(version);
        }

        let synced = self.log.synced();
        let mut entries = Vec::new();
        let mut acks = Vec::with_capacity(jobs.len());
        let mut offset = 0;
        for (job, count) in jobs.into_iter().zip(counts) {
            let mut versions = Vec::new();
            for (patch, minted) in job.raw.into_iter().zip(&minted[offset..offset + count]) {
                if let Some(version) = *minted {
                    versions.push(version);
                    entries.push(Arc::new(Entry {
                        version,
                        bytes: estimate(&patch),
                        patch,
                    }));
                }
            }
            offset += count;
            acks.push((job.reply, Ack { versions, synced }));
        }

        // Fan-out before acknowledgement, so a client that hears about a
        // version can always find it on a stream.
        self.publish(&entries);
        for (reply, ack) in acks {
            let _ = reply.send(Ok(ack));
        }
    }

    /// One rejected patch must not take its batchmates down with it: replay
    /// the failed run one request at a time so only the guilty one fails.
    fn isolate(
        &mut self,
        mut jobs: Vec<Job<D::Patch>>,
        counts: Vec<usize>,
        patches: Vec<D::Patch>,
        error: LogError<D, S>,
    ) {
        if jobs.len() == 1 {
            let job = jobs.remove(0);
            let _ = job.reply.send(Err(Failure::new(code_for(&error), &error)));
            return;
        }
        let mut rest = patches;
        for (job, count) in jobs.iter_mut().zip(counts) {
            job.patches = rest.drain(..count).collect();
        }
        for job in jobs {
            self.commit(vec![job]);
        }
    }

    fn publish(&mut self, entries: &[Arc<Entry>]) {
        if entries.is_empty() {
            return;
        }
        self.shared.ring.push(entries);
        let watermark = self.log.watermark();
        let _ = self.shared.watermark.send(watermark);
        if self.checkpoint_every > 0 && watermark - self.checkpointed >= self.checkpoint_every {
            match self.log.checkpoint() {
                Ok(()) => self.checkpointed = watermark,
                Err(e) => tracing::warn!(error = %e, "automatic checkpoint failed"),
            }
        }
        // After the checkpoint, because that is what reclaims segments. Read
        // before it, the published floor names versions storage has already
        // dropped.
        self.shared.oldest.store(self.log.oldest(), Ordering::Relaxed);
    }

    fn answer(&mut self, command: Command) {
        match command {
            Command::View { at, reply } => {
                let _ = reply.send(self.view(at));
            }
            Command::CatchUp { after, limit, reply } => {
                let _ = reply.send(self.catch_up(after, limit));
            }
            Command::Checkpoint { reply } => {
                let result = self
                    .log
                    .checkpoint()
                    .map_err(|e| Failure::new(Code::Storage, e))
                    .inspect(|()| self.checkpointed = self.log.watermark());
                let _ = reply.send(result);
            }
            Command::Append { .. } => unreachable!("appends are batched"),
        }
    }

    fn view(&self, at: Option<Version>) -> Result<(Version, Value), Failure> {
        let view = match at {
            Some(version) => self.log.view_at(version).ok_or_else(|| {
                Failure::new(Code::OutOfRange, format!("version {version} is not retained"))
            })?,
            None => self.log.view(),
        };
        // The frame carries the version in its own field, so serializing the
        // pointer whole would put it on the wire twice and make every client
        // reach past a wrapper for the state it wants.
        Ok((view.view.version(), view.view.state()))
    }

    fn catch_up(&self, after: Version, limit: usize) -> Result<CatchUp, Failure> {
        // Below retention is a gap, not a failure. Segment storage refuses a
        // read it can no longer serve, and handing that refusal to the
        // subscriber as Code::Storage would end a stream the protocol says
        // should resume at the oldest version still readable.
        let oldest = self.log.oldest();
        if after + 1 < oldest {
            return Ok(CatchUp::Gap { to: oldest - 1 });
        }
        let records =
            self.log.read_after(after, limit).map_err(|e| Failure::new(Code::Storage, e))?;
        match records.first() {
            Some(first) if first.version == after + 1 => {
                let mut entries = Vec::with_capacity(records.len());
                for record in records {
                    let patch: Value = serde_json::from_slice(&record.patch)
                        .map_err(|e| Failure::new(Code::Internal, e))?;
                    let bytes = record.patch.len();
                    entries.push(Arc::new(Entry { version: record.version, patch, bytes }));
                }
                Ok(CatchUp::Records(entries))
            }
            // Storage skipped ahead: the versions between were reclaimed.
            Some(first) => Ok(CatchUp::Gap { to: first.version - 1 }),
            None if after >= self.log.watermark() => Ok(CatchUp::Records(Vec::new())),
            // Nothing on disk and nothing in the ring below the cursor.
            None => {
                let resume = self.shared.ring.oldest().unwrap_or(self.log.watermark() + 1);
                Ok(CatchUp::Gap { to: resume.saturating_sub(1) })
            }
        }
    }
}

/// An actor whose storage refused to open still has to answer, so the failure
/// reaches the client that provoked it instead of hanging it forever.
pub async fn refuse(mut rx: mpsc::Receiver<Command>, error: String) {
    while let Some(command) = rx.recv().await {
        command.fail(|| Failure::new(Code::Storage, &error));
    }
}

fn rank(durability: &Durability) -> u8 {
    match durability {
        Durability::Memory => 0,
        Durability::Written => 1,
        Durability::Durable => 2,
    }
}

fn decode<P: serde::de::DeserializeOwned>(raw: &[Value]) -> Result<Vec<P>, serde_json::Error> {
    raw.iter().map(|value| serde_json::from_value(value.clone())).collect()
}

fn code_for<D, S>(error: &lug_core::Error<D, S>) -> Code {
    match error {
        lug_core::Error::Data(_) => Code::Rejected,
        lug_core::Error::Storage(_) => Code::Storage,
        lug_core::Error::Codec(_) => Code::Rejected,
        lug_core::Error::Version { .. } => Code::Internal,
    }
}
