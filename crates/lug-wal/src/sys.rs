//! The handful of syscalls the segment format needs, wrapped once.
//!
//! Everything here is a thin call into `rustix`, which is already a safe API,
//! so this module carries no `unsafe`. It exists so the rest of the crate
//! never has to think about `EINTR`, partial writes, `IOV_MAX`, or
//! filesystems that refuse to preallocate.

use rustix::fs::{FallocateFlags, fallocate};
use rustix::io::{Errno, pwritev};
use std::fs::File;
use std::io::{self, IoSlice};

/// Linux caps a single vectored call at 1024 buffers and silently writes only
/// the first 1024 beyond that, so batches are split here rather than trusted.
const IOV_MAX: usize = 1024;

/// Write every buffer at `offset`, splitting on `IOV_MAX` and resuming after a
/// partial write. One call per 1024 buffers, never one per record.
pub fn pwritev_all(file: &File, mut bufs: &mut [IoSlice<'_>], mut offset: u64) -> io::Result<()> {
    while !bufs.is_empty() {
        fault::check()?;
        let chunk = &bufs[..bufs.len().min(IOV_MAX)];
        let written = match pwritev(file, chunk, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => n,
            Err(Errno::INTR) => continue,
            Err(e) => return Err(e.into()),
        };
        fault::wrote(file, offset, written as u64);
        offset += written as u64;
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

/// Reserve blocks and set the file length to `len`.
///
/// The length matters as much as the blocks: appending inside an already
/// allocated, already sized region makes a record write pure data, so
/// [`fdatasync`] has no inode update to drag along with it.
pub fn preallocate(file: &File, len: u64) -> io::Result<()> {
    match fallocate(file, FallocateFlags::empty(), 0, len) {
        Ok(()) => Ok(()),
        // tmpfs and network filesystems may refuse; correctness does not
        // depend on the reservation, only the cost of a flush does.
        Err(Errno::OPNOTSUPP | Errno::NOSYS) => file.set_len(len),
        Err(e) => Err(e.into()),
    }
}

/// Overwrite a range with zeros. A zero record length is the end marker, so
/// this is what makes bytes above the logical end unrecoverable again.
pub fn zero_range(file: &File, offset: u64, len: u64) -> io::Result<()> {
    const CHUNK: u64 = 64 * 1024;
    let zeros = vec![0u8; CHUNK.min(len) as usize];
    let mut done = 0;
    while done < len {
        let span = (len - done).min(CHUNK) as usize;
        pwritev_all(file, &mut [IoSlice::new(&zeros[..span])], offset + done)?;
        done += span as u64;
    }
    Ok(())
}

/// Flush data only. Safe on a preallocated file, where nothing but data moves.
pub fn fdatasync(file: &File) -> io::Result<()> {
    fault::check_sync()?;
    retry(|| rustix::fs::fdatasync(file))?;
    fault::synced(file);
    Ok(())
}

/// Flush data and metadata. Used where the size or a directory entry changed.
pub fn fsync(file: &File) -> io::Result<()> {
    retry(|| rustix::fs::fsync(file))?;
    fault::synced(file);
    Ok(())
}

/// Take the directory's exclusive advisory lock without waiting, reporting
/// `false` if someone else holds it. The lock lives on the handle, so it goes
/// away when the store is dropped or the process dies, crash included.
pub fn try_lock_dir(dir: &File) -> io::Result<bool> {
    match rustix::fs::flock(dir, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(true),
        Err(Errno::WOULDBLOCK) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Publish `to` by renaming `from` over it, both relative to an open
/// directory. `rename` is atomic, so a reader sees the old file or the new one
/// and never a half written one.
pub fn rename_at(dir: &File, from: &str, to: &str) -> io::Result<()> {
    rustix::fs::renameat(dir, from, dir, to).map_err(Into::into)
}

fn retry(mut f: impl FnMut() -> Result<(), Errno>) -> io::Result<()> {
    loop {
        match f() {
            Ok(()) => return Ok(()),
            Err(Errno::INTR) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Making a write fail exactly where it hurts, for tests that have to prove
/// the error path leaves nothing recoverable behind, and modelling a power cut
/// precisely enough to be fair to the code under test. Compiled out of any
/// build that does not ask for it.
#[cfg(feature = "fault-injection")]
pub mod fault {
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::fs::{File, OpenOptions};
    use std::io::{self, Seek, SeekFrom, Write};
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    thread_local! {
        static PLAN: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
        static SYNC_PLAN: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
        /// Byte ranges written but not yet flushed, per inode. A power cut may
        /// keep or drop any of them; nothing outside them is at risk.
        static DIRTY: RefCell<HashMap<u64, Vec<(u64, u64)>>> = RefCell::new(HashMap::new());
    }

    /// Let `pass` vectored writes through on this thread, fail the next
    /// `fail`, then get out of the way.
    pub fn fail_writes(pass: usize, fail: usize) {
        PLAN.set((pass, fail));
    }

    /// The same for data flushes, which is what a store's `sync` reaches.
    pub fn fail_syncs(pass: usize, fail: usize) {
        SYNC_PLAN.set((pass, fail));
    }

    pub fn clear() {
        PLAN.set((0, 0));
        SYNC_PLAN.set((0, 0));
        DIRTY.with_borrow_mut(|dirty| dirty.clear());
    }

    pub(super) fn check() -> io::Result<()> {
        step(&PLAN)
    }

    pub(super) fn check_sync() -> io::Result<()> {
        step(&SYNC_PLAN)
    }

    fn step(plan: &'static std::thread::LocalKey<Cell<(usize, usize)>>) -> io::Result<()> {
        match plan.get() {
            (0, 0) => Ok(()),
            (0, fail) => {
                plan.set((0, fail - 1));
                Err(io::Error::other("injected failure"))
            }
            (pass, fail) => {
                plan.set((pass - 1, fail));
                Ok(())
            }
        }
    }

    pub(super) fn wrote(file: &File, offset: u64, len: u64) {
        let Ok(meta) = file.metadata() else { return };
        DIRTY.with_borrow_mut(|dirty| dirty.entry(meta.ino()).or_default().push((offset, len)));
    }

    pub(super) fn synced(file: &File) {
        let Ok(meta) = file.metadata() else { return };
        DIRTY.with_borrow_mut(|dirty| dirty.remove(&meta.ino()));
    }

    /// Ranges of `path` that were written and never flushed.
    pub fn unsynced(path: &Path) -> Vec<(u64, u64)> {
        let Ok(meta) = std::fs::metadata(path) else { return Vec::new() };
        DIRTY.with_borrow(|dirty| dirty.get(&meta.ino()).cloned().unwrap_or_default())
    }

    /// Drop `len` bytes at `offset` on the way to the platter, as a power cut
    /// drops whichever pages had not been written back yet.
    ///
    /// Refuses a range that was already flushed, because losing those bytes
    /// would be a broken disk rather than a crash, and a test that arranges it
    /// proves nothing.
    pub fn lose(path: &Path, offset: u64, len: u64) -> io::Result<()> {
        let covered = unsynced(path)
            .iter()
            .any(|&(start, span)| offset >= start && offset + len <= start + span);
        if !covered {
            return Err(io::Error::other(format!(
                "{}: {offset}..{} was flushed, a crash cannot take it back",
                path.display(),
                offset + len
            )));
        }
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&vec![0u8; len as usize])?;
        file.sync_all()
    }

    /// Lose everything written since the last flush: the pessimistic crash.
    pub fn lose_unsynced(path: &Path) -> io::Result<()> {
        for (offset, len) in unsynced(path) {
            let mut file = OpenOptions::new().write(true).open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&vec![0u8; len as usize])?;
            file.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(not(feature = "fault-injection"))]
mod fault {
    use std::fs::File;

    #[inline]
    pub(super) fn check() -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    pub(super) fn check_sync() -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    pub(super) fn wrote(_: &File, _: u64, _: u64) {}

    #[inline]
    pub(super) fn synced(_: &File) {}
}
