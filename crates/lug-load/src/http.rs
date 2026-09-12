use crate::{
    Result, error,
    wire::{Endpoint, Event, Peer, Traffic, validate_welcome},
};
use lug_proto::{Request, Response, VERSION};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::mpsc,
    task::JoinSet,
};

struct Http {
    socket: BufReader<TcpStream>,
    traffic: Arc<Traffic>,
    chunked: bool,
    chunk_left: usize,
    remaining: Option<usize>,
    ended: bool,
    pending: Vec<u8>,
}

impl Http {
    async fn open(endpoint: &Endpoint, traffic: Arc<Traffic>) -> Result<Self> {
        let address = endpoint
            .http
            .ok_or_else(|| error("HTTP endpoint missing address"))?;
        let socket = TcpStream::connect(address).await?;
        socket.set_nodelay(true)?;
        traffic.sockets.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            socket: BufReader::new(socket),
            traffic,
            chunked: false,
            chunk_left: 0,
            remaining: None,
            ended: false,
            pending: Vec::new(),
        })
    }

    async fn line(&mut self, limit: usize) -> Result<Vec<u8>> {
        let mut line = Vec::new();
        loop {
            let available = self.socket.fill_buf().await?;
            if available.is_empty() {
                return Err(error("EOF inside HTTP line"));
            }
            let n = available
                .iter()
                .position(|b| *b == b'\n')
                .map_or(available.len(), |n| n + 1);
            if line.len() + n > limit {
                return Err(error(format!("HTTP line exceeds {limit} bytes")));
            }
            let done = available[n - 1] == b'\n';
            line.extend_from_slice(&available[..n]);
            self.socket.consume(n);
            self.traffic.rx.fetch_add(n as u64, Ordering::Relaxed);
            if done {
                return Ok(line);
            }
        }
    }

    async fn request(
        &mut self,
        endpoint: &Endpoint,
        method: &str,
        path: &str,
        session: Option<&str>,
        body: &[u8],
    ) -> Result<()> {
        let address = endpoint
            .http
            .ok_or_else(|| error("HTTP endpoint missing address"))?;
        let mut header = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
            endpoint.token,
            body.len()
        );
        if let Some(session) = session {
            if !session.bytes().all(|b| b.is_ascii_graphic()) {
                return Err(error("invalid HTTP session header"));
            }
            header.push_str(&format!(
                "{}: {session}\r\n",
                lug_proto::http::SESSION_HEADER
            ));
        }
        header.push_str("\r\n");
        self.socket.get_mut().write_all(header.as_bytes()).await?;
        self.socket.get_mut().write_all(body).await?;
        self.traffic
            .tx
            .fetch_add((header.len() + body.len()) as u64, Ordering::Relaxed);
        let status_line = self.line(8192).await?;
        let status_line = std::str::from_utf8(&status_line)?;
        let status = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("")
            .parse::<u16>()?;
        if !(200..300).contains(&status) {
            return Err(error(format!(
                "HTTP {method} {path} returned status {status}"
            )));
        }
        self.chunked = false;
        self.chunk_left = 0;
        self.remaining = None;
        self.ended = false;
        let mut header_bytes = 0;
        loop {
            let line = self.line(8192).await?;
            header_bytes += line.len();
            if header_bytes > 65536 {
                return Err(error("HTTP headers exceed 64 KiB"));
            }
            if line == b"\r\n" {
                break;
            }
            let line = std::str::from_utf8(&line)?.trim();
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    self.remaining = Some(value.trim().parse()?);
                }
                if name.eq_ignore_ascii_case("transfer-encoding") {
                    if !value.trim().eq_ignore_ascii_case("chunked") {
                        return Err(error("unsupported HTTP transfer encoding"));
                    }
                    self.chunked = true;
                }
            }
        }
        Ok(())
    }

    async fn piece(&mut self) -> Result<Vec<u8>> {
        if self.ended {
            return Ok(Vec::new());
        }
        if self.chunked && self.chunk_left == 0 {
            let line = self.line(8192).await?;
            let length = std::str::from_utf8(&line)?
                .trim()
                .split(';')
                .next()
                .ok_or_else(|| error("missing chunk size"))?;
            self.chunk_left = usize::from_str_radix(length, 16)?;
            if self.chunk_left == 0 {
                let mut total = 0;
                loop {
                    let trailer = self.line(8192).await?;
                    total += trailer.len();
                    if total > 65536 {
                        return Err(error("HTTP trailers exceed 64 KiB"));
                    }
                    if trailer == b"\r\n" {
                        break;
                    }
                }
                self.ended = true;
                return Ok(Vec::new());
            }
        }
        let available = if self.chunked {
            self.chunk_left
        } else {
            self.remaining.unwrap_or(8192)
        };
        if available == 0 {
            self.ended = true;
            return Ok(Vec::new());
        }
        let mut bytes = vec![0; available.min(8192)];
        let n = self.socket.read(&mut bytes).await?;
        self.traffic.rx.fetch_add(n as u64, Ordering::Relaxed);
        bytes.truncate(n);
        if n == 0 {
            if self.chunked || self.remaining.is_some() {
                return Err(error("truncated HTTP body"));
            }
            self.ended = true;
            return Ok(bytes);
        }
        if self.chunked {
            self.chunk_left -= n;
            if self.chunk_left == 0 {
                let mut ending = [0; 2];
                self.socket.read_exact(&mut ending).await?;
                self.traffic.rx.fetch_add(2, Ordering::Relaxed);
                if &ending != b"\r\n" {
                    return Err(error("invalid HTTP chunk terminator"));
                }
            }
        } else if let Some(remaining) = &mut self.remaining {
            *remaining -= n;
        }
        Ok(bytes)
    }

    async fn response(&mut self) -> Result<Response> {
        let mut body = Vec::new();
        loop {
            let part = self.piece().await?;
            if part.is_empty() {
                break;
            }
            if body.len() + part.len() > lug_proto::MAX_FRAME as usize {
                return Err(error("HTTP response exceeds frame limit"));
            }
            body.extend_from_slice(&part);
        }
        Ok(serde_json::from_slice(&body)?)
    }

    async fn event(&mut self) -> Result<Response> {
        loop {
            if let Some(end) = self.pending.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = self.pending.drain(..=end).collect();
                if let Some(data) = line.strip_prefix(b"data:") {
                    return Ok(serde_json::from_slice(data)?);
                }
            } else {
                let part = self.piece().await?;
                if part.is_empty() {
                    return Err(error("SSE stream ended before Cancel"));
                }
                if self.pending.len() + part.len() > lug_proto::MAX_FRAME as usize + 8192 {
                    return Err(error("SSE line exceeds frame limit"));
                }
                self.pending.extend_from_slice(&part);
            }
        }
    }

    async fn call(
        &mut self,
        endpoint: &Endpoint,
        session: Option<&str>,
        request: &Request,
    ) -> Result<Response> {
        self.request(
            endpoint,
            "POST",
            lug_proto::http::CALL,
            session,
            &serde_json::to_vec(request)?,
        )
        .await?;
        self.response().await
    }
}

fn query(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

pub async fn connect(
    endpoint: &Endpoint,
    index: usize,
    events: mpsc::Sender<Event>,
    traffic: Arc<Traffic>,
) -> Result<Peer> {
    let mut call = Http::open(endpoint, traffic.clone()).await?;
    validate_welcome(
        &call
            .call(
                endpoint,
                None,
                &Request::Hello {
                    id: 0,
                    version: VERSION,
                },
            )
            .await?,
    )?;
    let endpoint = endpoint.clone();
    let (out, mut inbox) = mpsc::channel::<Request>(256);
    let task = tokio::spawn(async move {
        let mut sessions = HashMap::<u64, (String, tokio::task::AbortHandle)>::new();
        let mut streams = JoinSet::new();
        while let Some(request) = inbox.recv().await {
            let result: Result<Option<Response>> = async {
                if let Request::Subscribe {
                    id,
                    log,
                    from,
                    mode,
                    credit,
                } = &request
                {
                    let mut stream = Http::open(&endpoint, traffic.clone()).await?;
                    let mode = match mode {
                        lug_proto::Mode::Records => "records",
                        lug_proto::Mode::Reducible => "reducible",
                    };
                    let path = format!(
                        "{}?id={id}&log={}&from={from}&mode={mode}&credit={credit}",
                        lug_proto::http::STREAM,
                        query(log)
                    );
                    stream.request(&endpoint, "GET", &path, None, &[]).await?;
                    let welcome = stream.event().await?;
                    validate_welcome(&welcome)?;
                    if welcome.id() != *id {
                        return Err(error("SSE Welcome returned the wrong stream id"));
                    }
                    let Response::Welcome {
                        session: Some(session),
                        ..
                    } = welcome
                    else {
                        return Err(error("SSE Welcome omitted session"));
                    };
                    events
                        .send(Event {
                            peer: index,
                            arrived: Instant::now(),
                            response: Ok(Response::Ok { id: *id }),
                        })
                        .await
                        .map_err(|_| {
                            error("event receiver closed while acknowledging SSE Welcome")
                        })?;
                    let events = events.clone();
                    let task = streams.spawn(async move {
                        loop {
                            let response = stream.event().await;
                            let end = response.as_ref().map_or(true, Response::is_final);
                            if events
                                .send(Event {
                                    peer: index,
                                    arrived: Instant::now(),
                                    response,
                                })
                                .await
                                .is_err()
                                || end
                            {
                                break;
                            }
                        }
                    });
                    sessions.insert(*id, (session, task));
                    Ok(None)
                } else {
                    let session = if let Request::Cancel { id } = &request {
                        sessions.remove(id).map(|(session, task)| {
                            // EOF is expected once Cancel reaches the server; its POST supplies the End acknowledgement.
                            task.abort();
                            session
                        })
                    } else {
                        sessions
                            .get(&request.id())
                            .map(|(session, _)| session.clone())
                    };
                    let response = call.call(&endpoint, session.as_deref(), &request).await?;
                    Ok(Some(response))
                }
            }
            .await;
            match result {
                Ok(None) => {}
                Ok(Some(response)) => {
                    if events
                        .send(Event {
                            peer: index,
                            arrived: Instant::now(),
                            response: Ok(response),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(e) => {
                    let _ = events
                        .send(Event {
                            peer: index,
                            arrived: Instant::now(),
                            response: Err(e),
                        })
                        .await;
                    break;
                }
            }
            while streams.try_join_next().is_some() {}
        }
    });
    Ok(Peer::from_tasks(out, vec![task]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn raw_http_mock_exercises_chunk_boundaries_and_comments() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                request.push(socket.read_u8().await.unwrap());
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            assert!(
                String::from_utf8(request)
                    .unwrap()
                    .starts_with("GET /v1/stream")
            );
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            for part in [
                b": comment\n\nda".as_slice(),
                b"ta: {\"t\":\"pong\",\"id\":9}\n\n",
            ] {
                socket
                    .write_all(format!("{:x}\r\n", part.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(part).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        let endpoint = Endpoint {
            socket: PathBuf::new(),
            http: Some(addr),
            token: Arc::new(String::new()),
        };
        let mut http = Http::open(&endpoint, Arc::new(Traffic::default()))
            .await
            .unwrap();
        http.request(&endpoint, "GET", "/v1/stream", None, &[])
            .await
            .unwrap();
        assert_eq!(http.event().await.unwrap(), Response::Pong { id: 9 });
        assert!(http.event().await.is_err());
        task.await.unwrap();
    }

    use std::path::PathBuf;

    #[test]
    fn query_escapes_path_and_header_delimiters() {
        assert_eq!(query("a&b /"), "a%26b%20%2F");
    }
}
