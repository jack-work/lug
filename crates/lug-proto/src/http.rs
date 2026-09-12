//! HTTP mapping of the same frames.
//!
//! Append is a POST whose body is a [`Request`](crate::Request) frame and
//! whose response body is a [`Response`](crate::Response) frame. Subscribe is
//! a separate GET returning `text/event-stream`, one `Response` per `data:`
//! line. The two are separate endpoints, so a caller may hold a stream open
//! and POST on another connection, or POST and then attach to the stream it
//! already holds, by reusing `X-Lug-Session`.

/// POST a `Request` frame, get a `Response` frame. No streaming.
pub const CALL: &str = "/v1/call";

/// GET `text/event-stream`. Query: `log`, `from`, `mode`, `credit`.
/// Each event is `data: <Response as JSON>`.
pub const STREAM: &str = "/v1/stream";

/// GET, plain JSON, no auth. For supervisors and probes.
pub const HEALTH: &str = "/health";

/// Ties a POST to an already-open stream so pushed records land on the
/// connection the caller is holding. Optional; absent means the POST is
/// answered inline and nothing is pushed.
pub const SESSION_HEADER: &str = "x-lug-session";

/// `Authorization: Bearer <token>`.
pub const AUTH_HEADER: &str = "authorization";

/// SSE comment sent periodically so idle streams survive proxies and so a
/// dead peer is noticed. Not a frame; clients ignore lines starting with `:`.
pub const KEEPALIVE: &str = ": lug\n\n";
