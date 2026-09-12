//! On-disk shapes: segment prologue, record framing, header file.
//!
//! Every integer is little endian. The checksum covers the version as well as
//! the payload, so a record that lands at the wrong offset with an intact
//! payload still fails.

use lug_core::Version;

pub const SEG_MAGIC: [u8; 8] = *b"LUGSEG\x00\x01";
pub const HDR_MAGIC: [u8; 8] = *b"LUGHDR\x00\x01";

/// magic, then eight reserved zero bytes.
pub const SEG_PROLOGUE: usize = 16;

/// len u32, crc u32, version u64.
pub const REC_PREFIX: usize = 16;

/// A payload larger than this is read as garbage rather than allocated. No
/// honest record approaches it; the wire protocol refuses frames at 16 MiB.
pub const MAX_PAYLOAD: u32 = 64 << 20;

/// 18 zero-padded digits plus `.seg`, named for the first record inside.
pub fn segment_name(first: Version) -> String {
    format!("{first:018}.seg")
}

pub fn parse_segment_name(name: &str) -> Option<Version> {
    let digits = name.strip_suffix(".seg")?;
    if digits.len() != 18 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// CRC-32 as `crc32fast` computes it, which is the zlib polynomial with
/// hardware carryless multiply, not the Castagnoli polynomial the spec calls
/// crc32c. The format is self consistent either way; only the name is off.
pub fn checksum(version: Version, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&version.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

pub fn record_prefix(version: Version, payload: &[u8]) -> [u8; REC_PREFIX] {
    let mut out = [0u8; REC_PREFIX];
    out[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    out[4..8].copy_from_slice(&checksum(version, payload).to_le_bytes());
    out[8..16].copy_from_slice(&version.to_le_bytes());
    out
}

/// `(payload length, checksum, version)` from a full prefix.
pub fn split_prefix(prefix: &[u8; REC_PREFIX]) -> (u32, u32, Version) {
    let len = u32::from_le_bytes(prefix[0..4].try_into().expect("4 bytes"));
    let crc = u32::from_le_bytes(prefix[4..8].try_into().expect("4 bytes"));
    let version = Version::from_le_bytes(prefix[8..16].try_into().expect("8 bytes"));
    (len, crc, version)
}

pub fn segment_prologue() -> [u8; SEG_PROLOGUE] {
    let mut out = [0u8; SEG_PROLOGUE];
    out[0..8].copy_from_slice(&SEG_MAGIC);
    out
}

/// magic, crc over the json, json length, json.
pub fn header_bytes(json: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + json.len());
    out.extend_from_slice(&HDR_MAGIC);
    out.extend_from_slice(&crc32fast::hash(json).to_le_bytes());
    out.extend_from_slice(&(json.len() as u32).to_le_bytes());
    out.extend_from_slice(json);
    out
}
