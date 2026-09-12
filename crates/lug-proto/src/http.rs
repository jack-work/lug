//! HTTP mapping of the same frames.
//!
//! The length prefix does NOT appear here. It exists to delimit frames on a
//! byte stream; HTTP already delimits them, with `Content-Length` on a call
//! and with `data:` lines on a stream. So an HTTP body is a bare JSON
//! [`Request`](crate::Request) or [`Response`](crate::Response) object, byte
//! for byte the same JSON that would sit inside a framed payload.
//!
//! Append and subscribe are separate endpoints. A caller may hold a stream
//! open and POST on another connection; setting [`SESSION_HEADER`] on the
//! POST to the session the stream was opened with ties the two together.

/// `POST`. Request body is one JSON `Request`; the response body is one JSON
/// `Response`. `Content-Type: application/json`. No streaming, no prefix.
///
/// Every request variant is valid here, including `Credit` and `Cancel`,
/// which is how a stream opened at [`STREAM`] is steered: the SSE response
/// body carries no uplink, so flow control travels over calls.
pub const CALL: &str = "/v1/call";

/// `GET`, returning `text/event-stream`. Each event is a single
/// `data: <Response as JSON>` line, in order, with no prefix.
///
/// Query parameters, mirroring [`Request::Subscribe`](crate::Request::Subscribe):
///
/// | key | required | meaning |
/// | --- | --- | --- |
/// | `id` | yes | the stream id, carried by every pushed frame |
/// | `log` | yes | log name |
/// | `from` | no, default 0 | exclusive cursor |
/// | `mode` | no, default `records` | `records` or `reducible` |
/// | `credit` | no, default 0 | initial grant; 0 means nothing is pushed until a `Credit` call arrives |
///
/// The server sends [`Response::Welcome`](crate::Response::Welcome) as the
/// first event, carrying the session to use in [`SESSION_HEADER`].
pub const STREAM: &str = "/v1/stream";

/// GET, plain JSON, no auth. For supervisors and probes.
pub const HEALTH: &str = "/health";

/// Ties a call to an open stream, so that `Credit` and `Cancel` reach the
/// right subscription. Issued by the server in the stream's `Welcome` frame.
/// Absent on a call means the call stands alone.
pub const SESSION_HEADER: &str = "x-lug-session";

/// `Authorization: Bearer <token>`.
pub const AUTH_HEADER: &str = "authorization";

/// SSE comment sent periodically so idle streams survive proxies and a dead
/// peer is noticed. Not a frame; clients ignore lines starting with `:`.
pub const KEEPALIVE: &str = ": lug\n\n";

/// How long the server waits between [`KEEPALIVE`] comments.
pub const KEEPALIVE_SECS: u64 = 15;
