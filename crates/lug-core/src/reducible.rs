use crate::Version;
use serde::{Serialize, de::DeserializeOwned};

/// An MVCC pointer. Immutable, O(1) to clone, safe to hand to readers that
/// outlive the writer. Serializable so it can be checkpointed into a header
/// and shipped to followers.
pub trait Versioned: Clone + Send + Sync + Serialize + DeserializeOwned + 'static {
    fn version(&self) -> Version;

    /// The materialized state this pointer names, without the version framing
    /// the pointer carries around it.
    ///
    /// Serializing the pointer whole would put the version on the wire twice,
    /// since `Response::View` already has a field for it, and would force
    /// every client to reach past a redundant wrapper to find the state it
    /// actually wants. A generic server cannot know which field to unwrap, so
    /// the structure says.
    fn state(&self) -> serde_json::Value;
}

/// A data structure that folds patches into versions.
///
/// Application is two-phase on purpose. [`stage_patch`](Reducible::stage_patch)
/// validates against current state and produces private working state that
/// nothing can observe; [`commit`](Reducible::commit) publishes it as one new
/// version. That split is what lets a log write its record to disk *before*
/// the change becomes visible, which is the whole point of a write-ahead log.
///
/// A staged value borrows nothing from the structure, so a caller may abandon
/// it by dropping it. Committing staged work built against a stale version
/// must fail.
pub trait Reducible: Send + 'static {
    /// The serializable unit of change.
    type Patch: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    /// The pointer handed out by `commit`, and the type a [`Storage`](crate::Storage)
    /// checkpoints.
    type View: Versioned;
    /// Private, unpublished working state.
    type Staged: Send;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Open working state against the current version.
    fn stage(&self) -> Self::Staged;

    /// Fold one patch into working state. Errors leave earlier staged patches
    /// intact; the caller decides whether to continue or drop.
    fn stage_patch(&self, staged: &mut Self::Staged, patch: &Self::Patch)
    -> Result<(), Self::Error>;

    /// Publish working state as one new version. Fails if the base moved.
    /// Returns `None` when nothing staged actually changed the state, in which
    /// case no version is minted.
    fn commit(&mut self, staged: Self::Staged) -> Result<Option<Self::View>, Self::Error>;

    /// The current pointer.
    fn view(&self) -> Self::View;

    /// A historical pointer, if the structure still retains it.
    fn view_at(&self, version: Version) -> Option<Self::View>;

    /// Forget every version above `version`, making it current again.
    ///
    /// Free for a partially persistent structure: the older roots are still
    /// there and nothing shares the discarded ones. A log calls this when a
    /// record fails to reach disk after the change was already folded into
    /// memory, so that memory never runs ahead of the write-ahead log.
    fn truncate(&mut self, version: Version) -> Result<(), Self::Error>;

    /// Rebuild from a checkpointed pointer. The result sits at
    /// `view.version()` and retains no history below it, which is what lets
    /// recovery skip every record the checkpoint covers.
    fn resume(view: Self::View) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn version(&self) -> Version {
        self.view().version()
    }

    /// Stage, then commit, one patch.
    fn apply(&mut self, patch: &Self::Patch) -> Result<Option<Self::View>, Self::Error> {
        let mut staged = self.stage();
        self.stage_patch(&mut staged, patch)?;
        self.commit(staged)
    }
}

/// The degenerate reducible: counts versions, keeps nothing.
///
/// Useful on its own as a plain append-only byte log, where subscribers care
/// about the patch stream and nobody needs a materialized view.
#[derive(Debug, Default)]
pub struct Noop {
    version: Version,
}

/// [`Noop`]'s view is just the counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Tick(pub Version);

impl Versioned for Tick {
    fn version(&self) -> Version {
        self.0
    }

    /// A counter materializes nothing.
    fn state(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

#[derive(Debug, thiserror::Error)]
#[error("version counter exhausted")]
pub struct Overflow;

impl Reducible for Noop {
    type Patch = serde_json::Value;
    type View = Tick;
    type Staged = usize;
    type Error = Overflow;

    fn stage(&self) -> usize {
        0
    }

    fn stage_patch(&self, staged: &mut usize, _: &serde_json::Value) -> Result<(), Overflow> {
        *staged += 1;
        Ok(())
    }

    fn commit(&mut self, staged: usize) -> Result<Option<Tick>, Overflow> {
        if staged == 0 {
            return Ok(None);
        }
        self.version = self.version.checked_add(1).ok_or(Overflow)?;
        Ok(Some(Tick(self.version)))
    }

    fn view(&self) -> Tick {
        Tick(self.version)
    }

    fn view_at(&self, version: Version) -> Option<Tick> {
        (version <= self.version).then_some(Tick(version))
    }

    fn truncate(&mut self, version: Version) -> Result<(), Overflow> {
        self.version = self.version.min(version);
        Ok(())
    }

    fn resume(view: Tick) -> Result<Self, Overflow> {
        Ok(Self { version: view.0 })
    }
}
