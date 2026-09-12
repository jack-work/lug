//! The HTTP transport: calls are POSTs, subscriptions are SSE streams.
//!
//! The same frames travel here as on a socket, minus the length prefix, which
//! HTTP does not need: `Content-Length` delimits a call and `data:` lines
//! delimit a stream. An SSE response has no uplink, so `Credit` and `Cancel`
//! go out as calls carrying the session the stream announced in its
//! `Welcome`. All of that is hidden below [`Wire`], so the hub above cannot
//! tell which transport it is using.

use crate::error::{Error, Result};
use crate::transport::Wire;
use lug_proto::{Id, Mode, Request, Response, http as routes};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc, watch};

/// How long a `Credit` or `Cancel` waits for its stream to announce a
/// session. Only a startup race, and a short one.
const SESSION_WAIT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Endpoint {
    authority: String,
    token: String,
}

impl Endpoint {
    /// `http://host:port`, with or without a trailing slash.
    fn parse(base: &str, token: &str) -> Result<Self> {
        let rest = base
            .strip_prefix("http://")
            .ok_or_else(|| Error::Http(format!("{base} is not an http:// origin")))?;
        let authority = rest
            .trim_end_matches('/')
            .split('/')
            .next()
            .unwrap_or_default();
        if authority.is_empty() {
            return Err(Error::Http(format!("{base} has no host")));
        }
        let authority = if authority.contains(':') {
            authority.to_string()
        } else {
            format!("{authority}:80")
        };
        Ok(Self {
            authority,
            token: token.to_string(),
        })
    }

    async fn connect(&self) -> Result<TcpStream> {
        TcpStream::connect(&self.authority)
            .await
            .map_err(|e| Wire::connect_error(&self.authority, e))
    }
}

pub(crate) async fn dial(base: &str, token: &str, buffer: usize) -> Result<Wire> {
    let endpoint = Endpoint::parse(base, token)?;
    // Prove the daemon is there before the pool calls this slot healthy, so
    // that a dead server backs off instead of failing every call.
    drop(endpoint.connect().await?);

    let (out, out_rx) = mpsc::channel::<Request>(buffer);
    let (inbound_tx, inbound) = mpsc::channel::<Response>(buffer);
    let (dead_tx, dead_rx) = watch::channel(false);

    tokio::spawn(pump(Pump {
        endpoint,
        out: out_rx,
        inbound: inbound_tx,
        sessions: Arc::new(Sessions::default()),
        dead: Arc::new(dead_tx),
        dead_rx,
    }));

    Ok(Wire { out, inbound })
}

/// Stream ids to the session the server issued for them.
#[derive(Default)]
struct Sessions {
    map: Mutex<HashMap<Id, String>>,
    arrived: Notify,
}

impl Sessions {
    fn insert(&self, id: Id, session: String) {
        if let Ok(mut map) = self.map.lock() {
            map.insert(id, session);
        }
        self.arrived.notify_waiters();
    }

    fn remove(&self, id: Id) {
        if let Ok(mut map) = self.map.lock() {
            map.remove(&id);
        }
    }

    fn get(&self, id: Id) -> Option<String> {
        self.map.lock().ok()?.get(&id).cloned()
    }

    /// Wait for the stream with this id to announce its session.
    async fn wait(&self, id: Id) -> Option<String> {
        tokio::time::timeout(SESSION_WAIT, async {
            loop {
                // Register before looking, or a session that lands in between
                // is missed and this waits out the whole timeout.
                let arrived = self.arrived.notified();
                if let Some(session) = self.get(id) {
                    return session;
                }
                arrived.await;
            }
        })
        .await
        .ok()
    }
}

struct Pump {
    endpoint: Endpoint,
    out: mpsc::Receiver<Request>,
    inbound: mpsc::Sender<Response>,
    sessions: Arc<Sessions>,
    dead: Arc<watch::Sender<bool>>,
    dead_rx: watch::Receiver<bool>,
}

/// Fan requests out over HTTP. Each call is its own short connection, so
/// concurrency is free and nothing needs a writer lock; only subscriptions
/// hold a connection open.
async fn pump(mut p: Pump) {
    loop {
        let req = tokio::select! {
            req = p.out.recv() => match req { Some(req) => req, None => break },
            _ = wait_dead(p.dead_rx.clone()) => break,
        };
        match req {
            Request::Subscribe {
                id,
                log,
                from,
                mode,
                credit,
            } => {
                let task = StreamTask {
                    endpoint: p.endpoint.clone(),
                    inbound: p.inbound.clone(),
                    sessions: p.sessions.clone(),
                    dead: p.dead.clone(),
                    dead_rx: p.dead_rx.clone(),
                    id,
                    query: stream_query(id, &log, from, mode, credit),
                };
                tokio::spawn(task.run());
            }
            other => {
                let id = other.id();
                let needs_session =
                    matches!(other, Request::Credit { .. } | Request::Cancel { .. });
                let endpoint = p.endpoint.clone();
                let inbound = p.inbound.clone();
                let sessions = p.sessions.clone();
                let dead = p.dead.clone();
                tokio::spawn(async move {
                    let session = if needs_session {
                        sessions.wait(id).await
                    } else {
                        None
                    };
                    match call(&endpoint, &other, session.as_deref()).await {
                        Ok(Some(reply)) => {
                            let _ = inbound.send(reply).await;
                        }
                        // An empty reply is a call the server had nothing to
                        // say about; whatever it triggered rides the stream.
                        Ok(None) => {}
                        Err(e) => {
                            // A failed call is a failed connection here: there
                            // is nothing else to distinguish. Killing the wire
                            // fails every waiter at once and starts a redial.
                            tracing::debug!(id, error = %e, "http call failed");
                            let _ = dead.send(true);
                        }
                    }
                });
            }
        }
    }
    let _ = p.dead.send(true);
}

async fn wait_dead(mut rx: watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

struct StreamTask {
    endpoint: Endpoint,
    inbound: mpsc::Sender<Response>,
    sessions: Arc<Sessions>,
    dead: Arc<watch::Sender<bool>>,
    dead_rx: watch::Receiver<bool>,
    id: Id,
    query: String,
}

impl StreamTask {
    async fn run(mut self) {
        let dead = self.dead_rx.clone();
        let result = tokio::select! {
            result = self.read() => result,
            _ = wait_dead(dead) => Ok(()),
        };
        if let Err(e) = result {
            tracing::debug!(id = self.id, error = %e, "http stream ended");
            let _ = self.dead.send(true);
        }
        self.sessions.remove(self.id);
    }

    async fn read(&mut self) -> Result<()> {
        let stream = self.endpoint.connect().await?;
        let request = format!(
            "GET {path}?{query} HTTP/1.1\r\nhost: {host}\r\n{auth}: Bearer {token}\r\naccept: text/event-stream\r\n\r\n",
            path = routes::STREAM,
            query = self.query,
            host = self.endpoint.authority,
            auth = routes::AUTH_HEADER,
            token = self.endpoint.token,
        );
        let mut stream = stream;
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;

        let mut reader = BufReader::new(stream);
        let head = read_head(&mut reader).await?;
        if head.status != 200 {
            return Err(Error::Http(format!(
                "stream refused with status {}",
                head.status
            )));
        }
        let mut body = Body::new(reader, head.chunked, head.length);
        while let Some(line) = body.next_line().await? {
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let frame: Response = serde_json::from_str(data.trim())
                .map_err(|e| Error::Http(format!("bad sse frame: {e}")))?;
            if let Response::Welcome {
                session: Some(session),
                ..
            } = &frame
            {
                self.sessions.insert(self.id, session.clone());
            }
            if self.inbound.send(frame).await.is_err() {
                return Ok(());
            }
        }
        Err(Error::Http("stream closed by the server".into()))
    }
}

/// One request, one reply, on its own connection.
async fn call(
    endpoint: &Endpoint,
    req: &Request,
    session: Option<&str>,
) -> Result<Option<Response>> {
    let body = serde_json::to_vec(req).map_err(|e| Error::Http(e.to_string()))?;
    let session = session
        .map(|s| format!("{}: {s}\r\n", routes::SESSION_HEADER))
        .unwrap_or_default();
    let head = format!(
        "POST {path} HTTP/1.1\r\nhost: {host}\r\n{auth}: Bearer {token}\r\ncontent-type: application/json\r\ncontent-length: {len}\r\n{session}connection: close\r\n\r\n",
        path = routes::CALL,
        host = endpoint.authority,
        auth = routes::AUTH_HEADER,
        token = endpoint.token,
        len = body.len(),
    );
    let mut stream = endpoint.connect().await?;
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;

    let mut reader = BufReader::new(stream);
    let head = read_head(&mut reader).await?;
    let mut body = Body::new(reader, head.chunked, head.length);
    let bytes = body.read_all().await?;
    if head.status != 200 {
        return Err(Error::Http(format!(
            "status {}: {}",
            head.status,
            String::from_utf8_lossy(&bytes)
        )));
    }
    if bytes.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| Error::Http(format!("bad reply: {e}")))
}

struct Head {
    status: u16,
    chunked: bool,
    length: Option<usize>,
}

async fn read_head<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> Result<Head> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Err(Error::Http("server closed before replying".into()));
    }
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| Error::Http(format!("bad status line {line:?}")))?;

    let mut head = Head {
        status,
        chunked: false,
        length: None,
    };
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Err(Error::Http("headers truncated".into()));
        }
        let line = line.trim_end();
        if line.is_empty() {
            return Ok(head);
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim().to_ascii_lowercase(), value.trim());
        match name.as_str() {
            "content-length" => head.length = value.parse().ok(),
            "transfer-encoding" => {
                head.chunked = value.to_ascii_lowercase().contains("chunked");
            }
            _ => {}
        }
    }
}

/// A response body, chunked or not, read as lines or as a whole.
struct Body<R> {
    reader: R,
    chunked: bool,
    remaining: Option<usize>,
    buf: Vec<u8>,
    done: bool,
}

impl<R: tokio::io::AsyncBufRead + Unpin> Body<R> {
    fn new(reader: R, chunked: bool, length: Option<usize>) -> Self {
        Self {
            reader,
            chunked,
            remaining: length,
            buf: Vec::new(),
            done: false,
        }
    }

    async fn read_all(&mut self) -> Result<Vec<u8>> {
        while self.fill().await? {}
        Ok(std::mem::take(&mut self.buf))
    }

    async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(at) = self.buf.iter().position(|b| *b == b'\n') {
                let line = self.buf.drain(..=at).collect::<Vec<_>>();
                let line = String::from_utf8_lossy(&line).trim_end().to_string();
                return Ok(Some(line));
            }
            if !self.fill().await? {
                return Ok(None);
            }
        }
    }

    /// Append more decoded body bytes. `false` means end of body.
    async fn fill(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        if self.chunked {
            let mut header = String::new();
            if self.reader.read_line(&mut header).await? == 0 {
                self.done = true;
                return Ok(false);
            }
            let size =
                usize::from_str_radix(header.trim().split(';').next().unwrap_or("0").trim(), 16)
                    .map_err(|e| Error::Http(format!("bad chunk size {header:?}: {e}")))?;
            if size == 0 {
                self.done = true;
                return Ok(false);
            }
            let start = self.buf.len();
            self.buf.resize(start + size, 0);
            self.reader.read_exact(&mut self.buf[start..]).await?;
            let mut crlf = [0u8; 2];
            self.reader.read_exact(&mut crlf).await?;
            return Ok(true);
        }
        match self.remaining {
            Some(0) => {
                self.done = true;
                Ok(false)
            }
            Some(left) => {
                let start = self.buf.len();
                self.buf.resize(start + left, 0);
                self.reader.read_exact(&mut self.buf[start..]).await?;
                self.remaining = Some(0);
                Ok(true)
            }
            // No framing header at all: the body runs to end of connection,
            // which is how a simple SSE server behaves.
            None => {
                let mut chunk = [0u8; 4096];
                let read = self.reader.read(&mut chunk).await?;
                if read == 0 {
                    self.done = true;
                    return Ok(false);
                }
                self.buf.extend_from_slice(&chunk[..read]);
                Ok(true)
            }
        }
    }
}

fn stream_query(id: Id, log: &str, from: u64, mode: Mode, credit: u32) -> String {
    let mode = match mode {
        Mode::Records => "records",
        Mode::Reducible => "reducible",
    };
    format!(
        "id={id}&log={}&from={from}&mode={mode}&credit={credit}",
        escape(log)
    )
}

/// Percent-encode everything that is not plainly safe in a query value.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_parse() {
        assert_eq!(
            Endpoint::parse("http://127.0.0.1:7717", "t")
                .unwrap()
                .authority,
            "127.0.0.1:7717"
        );
        assert_eq!(
            Endpoint::parse("http://localhost/", "t").unwrap().authority,
            "localhost:80"
        );
        assert!(Endpoint::parse("127.0.0.1:7717", "t").is_err());
    }

    async fn body_of(head: &str, chunked: bool, length: Option<usize>) -> Vec<u8> {
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(head.as_bytes().to_vec()));
        Body::new(reader, chunked, length)
            .read_all()
            .await
            .expect("body")
    }

    #[tokio::test]
    async fn bodies_decode_however_they_are_framed() {
        // Content-Length, which is how a call comes back.
        assert_eq!(
            body_of("{\"t\":\"pong\"}", false, Some(12)).await,
            b"{\"t\":\"pong\"}"
        );
        // Chunked, which is how a real server sends an event stream.
        let chunked = "5\r\nhello\r\n1\r\n!\r\n0\r\n\r\n";
        assert_eq!(body_of(chunked, true, None).await, b"hello!");
        // Neither, meaning the body runs to the end of the connection.
        assert_eq!(body_of("loose", false, None).await, b"loose");
    }

    #[tokio::test]
    async fn sse_lines_come_out_of_chunks_whole() {
        let stream = "8\r\ndata: {}\r\n3\r\n\n\n:\r\n0\r\n\r\n";
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(stream.as_bytes().to_vec()));
        let mut body = Body::new(reader, true, None);
        assert_eq!(
            body.next_line().await.expect("line"),
            Some("data: {}".into())
        );
        assert_eq!(body.next_line().await.expect("line"), Some(String::new()));
    }

    #[test]
    fn log_names_survive_the_query() {
        assert_eq!(
            stream_query(3, "a/b c", 7, Mode::Reducible, 8),
            "id=3&log=a%2Fb%20c&from=7&mode=reducible&credit=8"
        );
    }
}
