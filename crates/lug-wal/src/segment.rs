//! Segment file: one prologue, then records, then whatever the crash left.

use crate::Error;
use crate::format::{MAX_PAYLOAD, REC_PREFIX, SEG_MAGIC, SEG_PROLOGUE, checksum, split_prefix};
use lug_core::Version;
use std::fs::File;
use std::io::{BufReader, Read};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

/// What the on-disk name says plus what the last scan found inside.
#[derive(Debug)]
pub(crate) struct Segment {
    /// Version of the first record, and the file name.
    pub first: Version,
    pub path: PathBuf,
    /// Offset just past the last intact record.
    pub end: u64,
    /// Highest version present, `first - 1` while empty.
    pub last: Version,
}

pub(crate) struct Scan {
    pub end: u64,
    /// Stopped on a short read or a bad checksum rather than at a boundary.
    /// Normal for the segment a crash was writing into, corruption anywhere
    /// else.
    pub torn: bool,
}

/// Walk the records of a segment, handing each to `visit` until it stops, the
/// data runs out, or the data stops making sense.
///
/// A preallocated segment is zero filled past its last record, and a zero
/// prefix fails the checksum, so the tail of a live segment reads as torn.
/// That is the intended signal: it is where the next record goes.
pub(crate) fn scan<F>(path: &Path, mut visit: F) -> Result<Scan, Error>
where
    F: FnMut(Version, &[u8]) -> Result<ControlFlow<()>, Error>,
{
    let file = File::open(path).map_err(Error::at(path))?;
    let file_len = file.metadata().map_err(Error::at(path))?.len();
    let mut reader = BufReader::with_capacity(64 * 1024, file);

    let mut prologue = [0u8; SEG_PROLOGUE];
    let read = fill(&mut reader, &mut prologue).map_err(Error::at(path))?;
    if read != SEG_PROLOGUE || prologue[..8] != SEG_MAGIC {
        return Err(Error::NotASegment { path: path.into() });
    }

    let mut end = SEG_PROLOGUE as u64;
    let mut payload = Vec::new();
    loop {
        let mut prefix = [0u8; REC_PREFIX];
        match fill(&mut reader, &mut prefix).map_err(Error::at(path))? {
            // Nothing at all at a record boundary is the clean end of a
            // rotated segment, not damage.
            0 => return Ok(Scan { end, torn: false }),
            REC_PREFIX => {}
            _ => return Ok(Scan { end, torn: true }),
        }
        let (len, crc, version) = split_prefix(&prefix);
        // Zero is the end marker, which is what preallocation leaves and what
        // a failed append writes back over itself. Records carry at least one
        // byte so the two can never be confused.
        if len == 0 {
            return Ok(Scan { end, torn: false });
        }
        let room = file_len.saturating_sub(end + REC_PREFIX as u64);
        if len > MAX_PAYLOAD || u64::from(len) > room {
            return Ok(Scan { end, torn: true });
        }

        payload.clear();
        payload.resize(len as usize, 0);
        if fill(&mut reader, &mut payload).map_err(Error::at(path))? != len as usize {
            return Ok(Scan { end, torn: true });
        }
        if checksum(version, &payload) != crc {
            return Ok(Scan { end, torn: true });
        }

        end += (REC_PREFIX + payload.len()) as u64;
        if visit(version, &payload)?.is_break() {
            return Ok(Scan { end, torn: false });
        }
    }
}

/// Read until `buf` is full or the file ends, returning how much arrived. A
/// partial fill is the torn tail of an interrupted write.
fn fill(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}
