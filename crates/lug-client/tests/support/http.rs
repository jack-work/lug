//! The mock daemon over loopback HTTP: `POST /v1/call` and SSE on
//! `GET /v1/stream`, chunked, the way a real server frames it.

use super::sim::{Pusher, Sim};
use lug_proto::{Mode, Request, Response, http as routes};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};

pub const TOKEN: &str = "test-token";

pub struct Mock {
    pub sim: Arc<Sim>,
    pub base: String,
    sessions: Arc<Mutex<HashMap<String, u64>>>,
    kill: watch::Sender<u64>,
    stop: watch::Sender<bool>,
}

impl Mock {
    pub async fn start() -> Self {
        Self::start_with(Arc::new(Sim::new())).await
    }

    pub async fn start_with(sim: Arc<Sim>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let (kill, _) = watch::channel(0);
        let (stop, mut stopped) = watch::channel(false);
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        let accept_sim = sim.clone();
        let accept_sessions = sessions.clone();
        let kill_tx = kill.clone();
        tokio::spawn(async move {
            static CONN: AtomicU64 = AtomicU64::new(0);
            loop {
                let accepted = tokio::select! {
                    accepted = listener.accept() => accepted,
                    _ = stopped.changed() => return,
                };
                let Ok((stream, _)) = accepted else { return };
                let conn = CONN.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(serve(
                    stream,
                    conn,
                    accept_sim.clone(),
                    accept_sessions.clone(),
                    kill_tx.subscribe(),
                ));
            }
        });

        Self {
            sim,
            base,
            sessions,
            kill,
            stop,
        }
    }

    pub fn transport(&self) -> lug_client::Transport {
        lug_client::Transport::http(&self.base, TOKEN)
    }

    pub fn kill_connections(&self) {
        self.kill.send_modify(|n| *n += 1);
    }

    pub fn sessions_issued(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

impl Drop for Mock {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

struct Head {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
}

async fn serve(
    stream: TcpStream,
    conn: u64,
    sim: Arc<Sim>,
    sessions: Arc<Mutex<HashMap<String, u64>>>,
    mut kill: watch::Receiver<u64>,
) {
    let mut reader = BufReader::new(stream);
    let Ok(head) = read_head(&mut reader).await else {
        return;
    };

    let authorized = head
        .headers
        .get(routes::AUTH_HEADER)
        .is_some_and(|v| v == &format!("Bearer {TOKEN}"));
    if !authorized {
        let _ = write_json(reader.get_mut(), 401, "{\"t\":\"unauthorized\"}").await;
        return;
    }

    if head.path == routes::CALL && head.method == "POST" {
        let length: usize = head
            .headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or_default();
        let mut body = vec![0u8; length];
        if reader.read_exact(&mut body).await.is_err() {
            return;
        }
        let Ok(req) = serde_json::from_slice::<Request>(&body) else {
            let _ = write_json(reader.get_mut(), 400, "{}").await;
            return;
        };
        // Credit and Cancel only make sense against an open stream, so hold
        // the client to sending the session the stream announced.
        if matches!(req, Request::Credit { .. } | Request::Cancel { .. }) {
            let known = head
                .headers
                .get(routes::SESSION_HEADER)
                .is_some_and(|s| sessions.lock().unwrap().contains_key(s));
            if !known {
                let _ = write_json(reader.get_mut(), 400, "{\"t\":\"no session\"}").await;
                return;
            }
        }
        let (tx, mut rx) = mpsc::channel::<Response>(8);
        sim.handle(conn, req, Pusher(tx));
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(frame)) => {
                let body = serde_json::to_string(&frame).expect("frame serializes");
                let _ = write_json(reader.get_mut(), 200, &body).await;
            }
            // Nothing to say: the answer, if any, rides the stream instead.
            _ => {
                let _ = write_status(reader.get_mut(), 204).await;
            }
        }
        return;
    }

    if head.path == routes::STREAM && head.method == "GET" {
        let id: u64 = head
            .query
            .get("id")
            .and_then(|v| v.parse().ok())
            .unwrap_or_default();
        let log = head.query.get("log").cloned().unwrap_or_default();
        let from = head
            .query
            .get("from")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let credit = head
            .query
            .get("credit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mode = match head.query.get("mode").map(String::as_str) {
            Some("reducible") => Mode::Reducible,
            _ => Mode::Records,
        };
        let session = format!("sess-{conn}-{id}");
        sessions.lock().unwrap().insert(session.clone(), id);

        let stream = reader.get_mut();
        let headers = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\ntransfer-encoding: chunked\r\n\r\n";
        if stream.write_all(headers.as_bytes()).await.is_err() {
            return;
        }
        let welcome = Response::Welcome {
            id,
            version: lug_proto::VERSION,
            max_frame: lug_proto::MAX_FRAME,
            session: Some(session),
        };
        if write_event(stream, &welcome).await.is_err() {
            return;
        }

        let (tx, mut rx) = mpsc::channel::<Response>(1024);
        sim.handle(
            conn,
            Request::Subscribe {
                id,
                log,
                from,
                mode,
                credit,
            },
            Pusher(tx),
        );
        loop {
            let frame = tokio::select! {
                frame = rx.recv() => frame,
                _ = kill.changed() => return,
            };
            let Some(frame) = frame else { return };
            if write_event(reader.get_mut(), &frame).await.is_err() {
                return;
            }
        }
    }

    if head.path == routes::HEALTH {
        let _ = write_json(reader.get_mut(), 200, "{\"ok\":true}").await;
    }
}

async fn read_head(reader: &mut BufReader<TcpStream>) -> Result<Head, ()> {
    let mut line = String::new();
    if reader.read_line(&mut line).await.map_err(|_| ())? == 0 {
        return Err(());
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();
    let (path, raw_query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    let query = raw_query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), unescape(v)))
        .collect();
    let path = path.to_string();

    let mut headers = HashMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await.map_err(|_| ())? == 0 {
            return Err(());
        }
        let line = line.trim_end();
        if line.is_empty() {
            return Ok(Head {
                method,
                path,
                query,
                headers,
            });
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
}

fn unescape(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn write_status(stream: &mut TcpStream, status: u16) -> std::io::Result<()> {
    let head = format!("HTTP/1.1 {status} \r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.flush().await
}

async fn write_json(stream: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} \r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

/// One SSE event, in its own HTTP chunk, so the client has to dechunk.
async fn write_event(stream: &mut TcpStream, frame: &Response) -> std::io::Result<()> {
    let body = format!(
        "data: {}\n\n",
        serde_json::to_string(frame).expect("frame serializes")
    );
    stream
        .write_all(format!("{:x}\r\n", body.len()).as_bytes())
        .await?;
    stream.write_all(body.as_bytes()).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await
}
