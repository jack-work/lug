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
    retry(|| rustix::fs::fdatasync(file))
}

/// Flush data and metadata. Used where the size or a directory entry changed.
pub fn fsync(file: &File) -> io::Result<()> {
    retry(|| rustix::fs::fsync(file))
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
/// the error path leaves nothing recoverable behind. Compiled out of any build
/// that does not ask for it.
#[cfg(feature = "fault-injection")]
pub mod fault {
    use std::cell::Cell;
    use std::io;

    thread_local! {
        static PLAN: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    }

    /// Let `pass` vectored writes through on this thread, fail the next
    /// `fail`, then get out of the way.
    pub fn fail_writes(pass: usize, fail: usize) {
        PLAN.set((pass, fail));
    }

    pub fn clear() {
        PLAN.set((0, 0));
    }

    pub(super) fn check() -> io::Result<()> {
        match PLAN.get() {
            (0, 0) => Ok(()),
            (0, fail) => {
                PLAN.set((0, fail - 1));
                Err(io::Error::other("injected write failure"))
            }
            (pass, fail) => {
                PLAN.set((pass - 1, fail));
                Ok(())
            }
        }
    }
}

#[cfg(not(feature = "fault-injection"))]
mod fault {
    #[inline]
    pub(super) fn check() -> std::io::Result<()> {
        Ok(())
    }
}
