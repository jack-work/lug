//! End to end over the real HTTP listener: bare JSON, no length prefix.

mod common;

use common::Harness;
use lug_proto::{Request, Response, http};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// A hand-rolled HTTP/1.1 client. Small enough to be obvious, and it keeps
/// the test honest about what actually goes over the wire.
struct Http {
    addr: std::net::SocketAddr,
    token: String,
}

impl Http {
    async fn call(&self, request: &Request) -> (u16, Response) {
        self.call_with(request, None).await
    }

    async fn call_with(&self, request: &Request, session: Option<&str>) -> (u16, Response) {
        let body = serde_json::to_vec(request).expect("encode");
        let mut head = format!(
            "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             {}: Bearer {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            http::CALL,
            http::AUTH_HEADER,
            self.token,
            body.len()
        );
        if let Some(session) = session {
            head.push_str(&format!("{}: {session}\r\n", http::SESSION_HEADER));
        }
        head.push_str("\r\n");

        let mut stream = TcpStream::connect(self.addr).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("head");
        stream.write_all(&body).await.expect("body");

        let mut reader = BufReader::new(stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).await.expect("status");
        let status: u16 =
            status_line.split_whitespace().nth(1).expect("code").parse().expect("numeric");
        let mut length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("header");
            if line.trim().is_empty() {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().expect("length");
            }
        }
        let mut body = vec![0u8; length];
        tokio::io::AsyncReadExt::read_exact(&mut reader, &mut body).await.expect("body");
        (status, serde_json::from_slice(&body).expect("a Response frame"))
    }

    /// Opens the SSE stream and returns the reader plus the parsed frames as
    /// they arrive.
    async fn stream(&self, query: &str) -> Sse {
        let head = format!(
            "GET {}?{query} HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\
             {}: Bearer {}\r\n\r\n",
            http::STREAM,
            http::AUTH_HEADER,
            self.token
        );
        let mut stream = TcpStream::connect(self.addr).await.expect("connect");
        stream.write_all(head.as_bytes()).await.expect("request");
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.expect("status");
        assert!(line.starts_with("HTTP/1.1 200"), "bad status: {line}");
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).await.expect("header");
            if header.trim().is_empty() {
                break;
            }
        }
        Sse { reader }
    }
}

struct Sse {
    reader: BufReader<TcpStream>,
}

impl Sse {
    async fn next(&mut self) -> Response {
        self.try_next(common::PATIENCE).await.expect("an event")
    }

    async fn try_next(&mut self, patience: Duration) -> Option<Response> {
        loop {
            let mut line = String::new();
            let read = tokio::time::timeout(patience, self.reader.read_line(&mut line)).await;
            match read {
                Ok(Ok(0)) | Err(_) => return None,
                Ok(Err(e)) => panic!("sse read: {e}"),
                Ok(Ok(_)) => {}
            }
            if let Some(data) = line.strip_prefix("data:") {
                return Some(serde_json::from_str(data.trim()).expect("a Response frame"));
            }
        }
    }
}

async fn harness() -> (Harness, Http) {
    let lug = Harness::with(|c| c.http = Some("127.0.0.1:0".parse().expect("addr")), 1 << 16).await;
    let http = Http { addr: lug.server().http().expect("http bound"), token: lug.token.clone() };
    (lug, http)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_carry_bare_json_both_ways() {
    let (lug, http) = harness().await;

    let (status, response) =
        http.call(&Request::Create { id: 1, log: "web".into(), reducible: false }).await;
    assert_eq!(status, 200);
    assert!(matches!(response, Response::Logs { id: 1, .. }));

    let (_, response) = http
        .call(&Request::Append {
            id: 2,
            log: "web".into(),
            patches: vec![json!({ "a": 1 }), json!({ "a": 2 })],
            durability: lug_proto::Durability::Durable,
        })
        .await;
    match response {
        Response::Ack { versions, .. } => assert_eq!(versions, vec![1, 2]),
        other => panic!("expected Ack, got {other:?}"),
    }

    let (_, response) = http.call(&Request::Ping { id: 3 }).await;
    assert!(matches!(response, Response::Pong { id: 3 }));

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_is_steered_by_calls_naming_its_session() {
    let (lug, http) = harness().await;
    http.call(&Request::Create { id: 1, log: "sse".into(), reducible: false }).await;
    http.call(&Request::Append {
        id: 2,
        log: "sse".into(),
        patches: vec![json!({ "n": 1 }), json!({ "n": 2 }), json!({ "n": 3 })],
        durability: lug_proto::Durability::Durable,
    })
    .await;

    // Zero credit: the Welcome is the acknowledgement, then silence.
    let mut sse = http.stream("id=9&log=sse&from=0&credit=0").await;
    let session = match sse.next().await {
        Response::Welcome { id: 9, session, .. } => session.expect("a session name"),
        other => panic!("expected Welcome, got {other:?}"),
    };
    assert!(sse.try_next(Duration::from_millis(250)).await.is_none());

    let (status, response) =
        http.call_with(&Request::Credit { id: 9, grant: 2 }, Some(&session)).await;
    assert_eq!(status, 200);
    assert!(matches!(response, Response::Ok { id: 9 }));

    match sse.next().await {
        Response::Records { id: 9, records } => {
            assert_eq!(records.iter().map(|r| r.version).collect::<Vec<_>>(), vec![1, 2]);
        }
        other => panic!("expected Records, got {other:?}"),
    }
    assert!(sse.try_next(Duration::from_millis(250)).await.is_none());

    let (_, response) = http.call_with(&Request::Cancel { id: 9 }, Some(&session)).await;
    assert!(matches!(response, Response::End { id: 9 }));

    lug.stop().await;
}

/// The session name is a capability: it is the whole of what stops one caller
/// from steering another's subscription. Anything a caller can derive from
/// names it has already been handed must not name a live stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_guessed_session_cannot_steer_another_clients_stream() {
    let (lug, http) = harness().await;
    http.call(&Request::Create { id: 1, log: "private".into(), reducible: false }).await;
    http.call(&Request::Append {
        id: 2,
        log: "private".into(),
        patches: vec![json!({ "n": 1 })],
        durability: lug_proto::Durability::Durable,
    })
    .await;

    // Three streams of our own, to read off whatever pattern the names follow.
    let mut observed = Vec::new();
    let mut probes = Vec::new();
    for id in 10..13 {
        let mut probe = http.stream(&format!("id={id}&log=private&credit=0")).await;
        observed.push(session_of(probe.next().await));
        probes.push(probe);
    }

    // The victim opens next, on a connection we do not hold.
    let mut victim = http.stream("id=20&log=private&credit=1").await;
    let real = session_of(victim.next().await);
    assert!(matches!(victim.next().await, Response::Records { id: 20, .. }));

    for guess in successors(&observed) {
        let (status, _) = http.call_with(&Request::Cancel { id: 20 }, Some(&guess)).await;
        assert_eq!(status, 400, "session {guess} steered a stream it was never given");
        assert!(
            victim.try_next(Duration::from_millis(50)).await.is_none(),
            "guess {guess} reached the victim's stream"
        );
    }

    // The victim's own name still works, so the refusals above were the guess
    // failing and not the stream having died of something else.
    let (status, response) = http.call_with(&Request::Cancel { id: 20 }, Some(&real)).await;
    assert_eq!(status, 200);
    assert!(matches!(response, Response::End { id: 20 }));

    lug.stop().await;
}

fn session_of(response: Response) -> String {
    match response {
        Response::Welcome { session: Some(session), .. } => session,
        other => panic!("expected a Welcome carrying a session, got {other:?}"),
    }
}

/// Names a counter would hand out next: the trailing digit run of each
/// observed name, bumped, keeping its width.
fn successors(observed: &[String]) -> Vec<String> {
    let mut guesses = Vec::new();
    for name in observed {
        let digits = name.len() - name.trim_end_matches(|c: char| c.is_ascii_digit()).len();
        if digits == 0 {
            continue;
        }
        let (head, tail) = name.split_at(name.len() - digits);
        let Ok(number) = tail.parse::<u128>() else { continue };
        for step in 1..=3 {
            guesses.push(format!("{head}{:0width$}", number + step, width = digits));
        }
    }
    guesses
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_token_is_required_and_subscribe_belongs_on_the_stream() {
    let (lug, http) = harness().await;

    let wrong = Http { addr: http.addr, token: "not-the-token".into() };
    let (status, response) = wrong.call(&Request::Ping { id: 1 }).await;
    assert_eq!(status, 401);
    assert!(matches!(response, Response::Error { code: lug_proto::Code::Unauthorized, .. }));

    let (status, response) = http
        .call(&Request::Subscribe {
            id: 2,
            log: "nope".into(),
            from: 0,
            mode: lug_proto::Mode::Records,
            credit: 1,
        })
        .await;
    assert_eq!(status, 400);
    assert!(matches!(response, Response::Error { code: lug_proto::Code::Malformed, .. }));

    // Credit with no session has no stream to reach.
    let (status, _) = http.call(&Request::Credit { id: 3, grant: 1 }).await;
    assert_eq!(status, 400);

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_needs_no_token() {
    let (lug, http) = harness().await;
    let mut stream = TcpStream::connect(http.addr).await.expect("connect");
    let request =
        format!("GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", http::HEALTH);
    stream.write_all(request.as_bytes()).await.expect("request");
    let mut body = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut body).await.expect("response");
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.contains("\"status\":\"ok\""), "{body}");

    lug.stop().await;
}
