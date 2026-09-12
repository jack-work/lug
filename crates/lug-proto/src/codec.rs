//! Length-prefixed JSON framing.
//!
//! `[len: u32 be][json: len bytes]`. The length prefix is what makes this
//! cheap to read: one read for the header, one sized read for the body, no
//! scanning for delimiters and no ambiguity about newlines inside strings.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder};

/// Frames above this are refused without allocating. Generous for patches,
/// small enough that a hostile length prefix cannot exhaust memory.
pub const MAX_FRAME: u32 = 16 * 1024 * 1024;

const HEADER: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("frame of {0} bytes exceeds the {MAX_FRAME} byte limit")]
    TooLarge(u32),
    #[error("malformed frame: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Decodes `In`, encodes `Out`. The server is `Codec<Request, Response>` and
/// the client is `Codec<Response, Request>`.
pub struct Codec<In, Out> {
    marker: PhantomData<fn() -> (In, Out)>,
}

impl<In, Out> Default for Codec<In, Out> {
    fn default() -> Self {
        Self { marker: PhantomData }
    }
}

impl<In, Out> Codec<In, Out> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<In: DeserializeOwned, Out> Decoder for Codec<In, Out> {
    type Item = In;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<In>, Error> {
        if src.len() < HEADER {
            return Ok(None);
        }
        let len = u32::from_be_bytes(src[..HEADER].try_into().expect("4 bytes"));
        if len > MAX_FRAME {
            return Err(Error::TooLarge(len));
        }
        let len = len as usize;
        if src.len() < HEADER + len {
            // Ask for the whole frame at once rather than growing byte by byte.
            src.reserve(HEADER + len - src.len());
            return Ok(None);
        }
        src.advance(HEADER);
        let body = src.split_to(len);
        Ok(Some(serde_json::from_slice(&body)?))
    }
}

impl<In, Out: Serialize> Encoder<Out> for Codec<In, Out> {
    type Error = Error;

    fn encode(&mut self, item: Out, dst: &mut BytesMut) -> Result<(), Error> {
        let body = serde_json::to_vec(&item)?;
        let len = u32::try_from(body.len()).map_err(|_| Error::TooLarge(u32::MAX))?;
        if len > MAX_FRAME {
            return Err(Error::TooLarge(len));
        }
        dst.reserve(HEADER + body.len());
        dst.put_u32(len);
        dst.put_slice(&body);
        Ok(())
    }
}

/// Encode one frame standalone, for callers not driving a `Framed`.
pub fn encode<T: Serialize>(value: &T) -> Result<Bytes, Error> {
    let mut buf = BytesMut::new();
    Codec::<(), &T>::new().encode(value, &mut buf)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Request, Response};

    #[test]
    fn round_trip() {
        let mut enc = Codec::<Request, Request>::new();
        let mut buf = BytesMut::new();
        let sent = Request::Ping { id: 7 };
        enc.encode(sent.clone(), &mut buf).unwrap();
        assert_eq!(enc.decode(&mut buf).unwrap().unwrap(), sent);
        assert!(enc.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn partial_frame_waits() {
        let mut codec = Codec::<Request, Request>::new();
        let mut buf = BytesMut::new();
        codec.encode(Request::Ping { id: 1 }, &mut buf).unwrap();
        let whole = buf.split();
        let mut partial = BytesMut::from(&whole[..whole.len() - 1]);
        assert!(codec.decode(&mut partial).unwrap().is_none());
        partial.extend_from_slice(&whole[whole.len() - 1..]);
        assert!(codec.decode(&mut partial).unwrap().is_some());
    }

    #[test]
    fn oversized_length_refused_without_allocating() {
        let mut codec = Codec::<Request, Request>::new();
        let mut buf = BytesMut::new();
        buf.put_u32(MAX_FRAME + 1);
        assert!(matches!(codec.decode(&mut buf), Err(Error::TooLarge(_))));
    }

    #[test]
    fn two_frames_in_one_read() {
        let mut codec = Codec::<Response, Response>::new();
        let mut buf = BytesMut::new();
        codec.encode(Response::Pong { id: 1 }, &mut buf).unwrap();
        codec.encode(Response::End { id: 2 }, &mut buf).unwrap();
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().id(), 1);
        assert_eq!(codec.decode(&mut buf).unwrap().unwrap().id(), 2);
    }
}
