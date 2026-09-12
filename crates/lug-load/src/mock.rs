use crate::{
    Result,
    config::Config,
    error, run,
    wire::{Endpoint, read_frame},
};
use lug_proto::{LogInfo, Record, Request, Response};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::UnixListener,
    sync::{Mutex, mpsc},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    Gap,
    Duplicate,
    Order,
    AckLoss,
    StallOnZeroCredit,
    IgnoreCredit,
    MissingSubscribeAck,
    DuplicateSubscribeAck,
}

struct Subscriber {
    id: u64,
    connection: usize,
    log: String,
    cursor: usize,
    credit: u32,
    out: mpsc::Sender<Response>,
}

#[derive(Default)]
struct State {
    logs: HashMap<String, Vec<Record>>,
    subscribers: Vec<Subscriber>,
}

struct Mock {
    endpoint: Endpoint,
    state: Arc<Mutex<State>>,
    directory: PathBuf,
    task: JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

impl Mock {
    async fn start(fault: Fault, delay: Duration) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "mock-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&directory).unwrap();
        let endpoint = Endpoint {
            socket: directory.join("lug.sock"),
            http: None,
            token: Arc::new(String::new()),
        };
        let listener = UnixListener::bind(&endpoint.socket).unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut connection = 0;
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let state = shared.clone();
                tasks.spawn(async move {
                    let (mut reader, mut writer) = socket.into_split();
                    let (out, mut inbox) = mpsc::channel(4096);
                    let writer = tokio::spawn(async move {
                        while let Some(response) = inbox.recv().await {
                            if writer
                                .write_all(&lug_proto::encode(&response).unwrap())
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                    while let Ok(body) = read_frame(&mut reader).await {
                        let request: Request = serde_json::from_slice(&body).unwrap();
                        if matches!(request, Request::Append { .. }) && !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        if handle(&mut *state.lock().await, request, connection, &out, fault)
                            .is_err()
                        {
                            break;
                        }
                    }
                    state
                        .lock()
                        .await
                        .subscribers
                        .retain(|s| s.connection != connection);
                    writer.abort();
                });
                connection += 1;
            }
        });
        Self {
            endpoint,
            state,
            directory,
            task,
        }
    }
}

fn handle(
    state: &mut State,
    request: Request,
    connection: usize,
    out: &mpsc::Sender<Response>,
    fault: Fault,
) -> Result<()> {
    let id = request.id();
    match request {
        Request::Hello { version, .. } => out.try_send(Response::Welcome {
            id,
            version,
            max_frame: lug_proto::MAX_FRAME,
            session: None,
        })?,
        Request::Create { log, .. } => {
            let records = state.logs.entry(log.clone()).or_default();
            out.try_send(Response::Logs {
                id,
                logs: vec![LogInfo {
                    name: log,
                    reducible: false,
                    version: records.len() as u64,
                    oldest: 1,
                    subscribers: 0,
                }],
            })?;
        }
        Request::Append { log, patches, .. } => {
            if fault == Fault::StallOnZeroCredit && state.subscribers.iter().any(|s| s.credit == 0)
            {
                return Ok(());
            }
            let records = state
                .logs
                .get_mut(&log)
                .ok_or_else(|| error("mock append to absent log"))?;
            let version = records.len() as u64 + 1;
            records.push(Record {
                version,
                patch: patches[0].clone(),
            });
            fanout(state, fault)?;
            if fault != Fault::AckLoss {
                out.try_send(Response::Ack {
                    id,
                    versions: vec![version],
                    synced: version,
                })?;
            }
        }
        Request::Subscribe {
            log, from, credit, ..
        } => {
            if fault != Fault::MissingSubscribeAck {
                out.try_send(Response::Ok { id })?;
            }
            if fault == Fault::DuplicateSubscribeAck {
                out.try_send(Response::Ok { id })?;
            }
            state.subscribers.push(Subscriber {
                id,
                connection,
                log,
                cursor: from as usize,
                credit,
                out: out.clone(),
            });
            fanout(state, fault)?;
        }
        Request::Credit { grant, .. } => {
            let subscriber = state
                .subscribers
                .iter_mut()
                .find(|s| s.connection == connection && s.id == id)
                .ok_or_else(|| error("mock credit for absent stream"))?;
            subscriber.credit += grant;
            out.try_send(Response::Ok { id })?;
            fanout(state, fault)?;
        }
        Request::Cancel { .. } => {
            state
                .subscribers
                .retain(|s| !(s.connection == connection && s.id == id));
            out.try_send(Response::End { id })?;
        }
        Request::Ping { .. } => out.try_send(Response::Pong { id })?,
        _ => return Err(error("mock does not implement this request")),
    }
    Ok(())
}

fn fanout(state: &mut State, fault: Fault) -> Result<()> {
    for subscriber in &mut state.subscribers {
        let records = state
            .logs
            .get(&subscriber.log)
            .ok_or_else(|| error("mock subscribe to absent log"))?;
        while subscriber.cursor < records.len()
            && (subscriber.credit > 0 || fault == Fault::IgnoreCredit)
        {
            let mut record = records[subscriber.cursor].clone();
            subscriber.cursor += 1;
            subscriber.credit = subscriber.credit.saturating_sub(1);
            if fault == Fault::Gap && record.version == 1 {
                continue;
            }
            if fault == Fault::Order && subscriber.id == 2 && record.version == 2 {
                record.patch = records[0].patch.clone();
            }
            let response = Response::Records {
                id: subscriber.id,
                records: vec![record],
            };
            subscriber.out.try_send(response.clone())?;
            if fault == Fault::Duplicate {
                subscriber.out.try_send(response)?;
            }
        }
    }
    Ok(())
}

fn config() -> Config {
    Config {
        connections: 1,
        logs: 2,
        appenders: 2,
        subscribers: 3,
        rate: 1000,
        duration: Duration::from_millis(20),
        timeout: Duration::from_millis(200),
        patch_size: 32,
        ..Config::default()
    }
}

async fn exercise(fault: Fault, delay: Duration) -> Result<run::Loaded> {
    let mock = Mock::start(fault, delay).await;
    let cfg = config();
    let names = vec!["one".to_owned(), "two".to_owned()];
    let mut pool = run::Pool::open(&mock.endpoint, 1, 1, cfg.timeout).await?;
    pool.create(&names, cfg.timeout).await?;
    let mut loaded = run::load(&mut pool, &cfg, &names, None).await?;
    run::cancel(&mut pool, &mut loaded, cfg.timeout).await?;
    pool.heartbeat(cfg.timeout).await?;
    Ok(loaded)
}

#[tokio::test]
async fn exact_multi_log_multi_subscriber_history_over_real_unix_socket() {
    let loaded = exercise(Fault::None, Duration::ZERO).await.unwrap();
    assert_eq!(loaded.metrics.scheduled, 20);
    assert_eq!(loaded.metrics.acknowledged, 20);
    assert_eq!(loaded.metrics.delivered, 60);
    loaded.ledger.finish().unwrap();
}

#[tokio::test]
async fn mock_delay_appears_as_queueing_in_intended_time_histogram() {
    let loaded = exercise(Fault::None, Duration::from_millis(3))
        .await
        .unwrap();
    assert!(loaded.metrics.append.json()["p99_us"].as_u64().unwrap() > 30_000);
}

#[tokio::test]
async fn injected_gaps_duplicates_order_changes_and_ack_loss_all_fail() {
    for fault in [
        Fault::Gap,
        Fault::Duplicate,
        Fault::Order,
        Fault::AckLoss,
        Fault::MissingSubscribeAck,
        Fault::DuplicateSubscribeAck,
    ] {
        assert!(exercise(fault, Duration::ZERO).await.is_err());
    }
}

#[tokio::test]
async fn overload_is_a_failed_run_not_silent_sample_loss() {
    let mock = Mock::start(Fault::None, Duration::from_millis(30)).await;
    let cfg = Config {
        max_inflight: 1,
        ..config()
    };
    let names = vec!["one".to_owned(), "two".to_owned()];
    let mut pool = run::Pool::open(&mock.endpoint, 1, 1, cfg.timeout)
        .await
        .unwrap();
    pool.create(&names, cfg.timeout).await.unwrap();
    let failure = run::load(&mut pool, &cfg, &names, None)
        .await
        .err()
        .unwrap();
    assert!(failure.to_string().contains("OVERLOAD"));
}

#[tokio::test]
async fn stopped_credit_probe_passes_only_when_other_work_progresses() {
    let cfg = Config {
        timeout: Duration::from_secs(2),
        ..Config::default()
    };
    let good = Mock::start(Fault::None, Duration::ZERO).await;
    run::backpressure(&good.endpoint, &cfg).await.unwrap();
    for fault in [Fault::StallOnZeroCredit, Fault::IgnoreCredit] {
        let bad = Mock::start(fault, Duration::ZERO).await;
        assert!(
            run::backpressure(&bad.endpoint, &cfg)
                .await
                .unwrap_err()
                .to_string()
                .contains("BACKPRESSURE")
        );
    }
}

#[tokio::test]
async fn replay_of_missing_acknowledged_tail_fails_loudly() {
    let mock = Mock::start(Fault::None, Duration::ZERO).await;
    let cfg = config();
    let names = vec!["one".to_owned(), "two".to_owned()];
    let mut pool = run::Pool::open(&mock.endpoint, 1, 1, cfg.timeout)
        .await
        .unwrap();
    pool.create(&names, cfg.timeout).await.unwrap();
    let mut loaded = run::load(&mut pool, &cfg, &names, None).await.unwrap();
    run::cancel(&mut pool, &mut loaded, cfg.timeout)
        .await
        .unwrap();
    drop(pool);
    run::replay(&mock.endpoint, &cfg, &names, &mut loaded.ledger)
        .await
        .unwrap();
    mock.state.lock().await.logs.get_mut("one").unwrap().pop();
    let failure = run::replay(&mock.endpoint, &cfg, &names, &mut loaded.ledger)
        .await
        .unwrap_err();
    assert!(failure.to_string().contains("DURABLE DATA LOSS"));
}

#[tokio::test]
async fn held_connections_and_short_lived_accepts_are_separate_regimes() {
    let mock = Mock::start(Fault::None, Duration::ZERO).await;
    let cfg = Config {
        connections: 128,
        parallel: 16,
        accepts: 64,
        timeout: Duration::from_secs(5),
        ..config()
    };
    let mut pool = run::Pool::open(&mock.endpoint, cfg.connections, cfg.parallel, cfg.timeout)
        .await
        .unwrap();
    assert_eq!(pool.peers.len(), 128);
    assert_eq!(run::accepts(&mock.endpoint, &cfg).await.unwrap().0, 64);
    pool.heartbeat(cfg.timeout).await.unwrap();
}

impl Mock {
    async fn http() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Endpoint {
            socket: PathBuf::new(),
            http: Some(listener.local_addr().unwrap()),
            token: Arc::new("test-only".to_owned()),
        };
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut connection = 0;
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let shared = shared.clone();
                tasks.spawn(async move {
                    let _ = serve_http(socket, shared, connection).await;
                });
                connection += 1;
            }
        });
        Self {
            endpoint,
            state,
            directory: PathBuf::new(),
            task,
        }
    }
}

async fn serve_http(
    socket: tokio::net::TcpStream,
    shared: Arc<Mutex<State>>,
    connection: usize,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    let mut socket = BufReader::new(socket);
    loop {
        let mut first = String::new();
        if socket.read_line(&mut first).await? == 0 {
            return Ok(());
        }
        let mut length = 0;
        let mut routed = connection;
        let mut auth = false;
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).await?;
            if line == "\r\n" {
                break;
            }
            let (name, value) = line
                .trim()
                .split_once(':')
                .ok_or_else(|| error("bad test HTTP header"))?;
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse()?;
            }
            if name.eq_ignore_ascii_case("authorization") {
                auth = value.trim() == "Bearer test-only";
            }
            if name.eq_ignore_ascii_case(lug_proto::http::SESSION_HEADER) {
                routed = value
                    .trim()
                    .strip_prefix("test-")
                    .ok_or_else(|| error("missing test session"))?
                    .parse()?;
            }
        }
        assert!(auth);
        let mut body = vec![0; length];
        socket.read_exact(&mut body).await?;
        let (out, mut responses) = mpsc::channel(4096);
        if first.starts_with("GET ") {
            let query = first
                .split_whitespace()
                .nth(1)
                .unwrap()
                .split_once('?')
                .unwrap()
                .1;
            let args: HashMap<_, _> = query
                .split('&')
                .map(|part| part.split_once('=').unwrap())
                .collect();
            let id = args["id"].parse()?;
            handle(
                &mut *shared.lock().await,
                Request::Subscribe {
                    id,
                    log: args["log"].to_owned(),
                    from: args["from"].parse()?,
                    mode: lug_proto::Mode::Records,
                    credit: args["credit"].parse()?,
                },
                connection,
                &out,
                Fault::None,
            )?;
            assert_eq!(responses.recv().await.unwrap(), Response::Ok { id });
            socket.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await?;
            let welcome = Response::Welcome {
                id,
                version: lug_proto::VERSION,
                max_frame: lug_proto::MAX_FRAME,
                session: Some(format!("test-{connection}")),
            };
            http_event(socket.get_mut(), &welcome).await?;
            drop(out);
            while let Some(response) = responses.recv().await {
                http_event(socket.get_mut(), &response).await?;
            }
            socket.get_mut().write_all(b"0\r\n\r\n").await?;
            return Ok(());
        }
        assert!(first.starts_with("POST /v1/call HTTP/1.1"));
        let request: Request = serde_json::from_slice(&body)?;
        handle(
            &mut *shared.lock().await,
            request,
            routed,
            &out,
            Fault::None,
        )?;
        let response = responses
            .recv()
            .await
            .ok_or_else(|| error("test call not answered"))?;
        let json = serde_json::to_vec(&response)?;
        socket.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", json.len()).as_bytes()).await?;
        socket.get_mut().write_all(&json).await?;
    }
}

async fn http_event(socket: &mut tokio::net::TcpStream, response: &Response) -> Result<()> {
    let event = format!("data: {}\n\n", serde_json::to_string(response)?);
    socket
        .write_all(format!("{:x}\r\n{event}\r\n", event.len()).as_bytes())
        .await?;
    Ok(())
}

#[tokio::test]
async fn http_sessions_carry_credit_cancel_and_exact_replay() {
    let mock = Mock::http().await;
    let cfg = Config {
        transport: crate::config::Transport::Http,
        rate: 20,
        duration: Duration::from_millis(200),
        timeout: Duration::from_secs(5),
        ..config()
    };
    let names = vec!["one".to_owned(), "two".to_owned()];
    let mut pool = run::Pool::open(&mock.endpoint, 1, 1, cfg.timeout)
        .await
        .unwrap();
    pool.create(&names, cfg.timeout).await.unwrap();
    let mut loaded = run::load(&mut pool, &cfg, &names, None).await.unwrap();
    run::cancel(&mut pool, &mut loaded, cfg.timeout)
        .await
        .unwrap();
    pool.heartbeat(cfg.timeout).await.unwrap();
    assert_eq!(loaded.metrics.acknowledged, 4);
    assert_eq!(loaded.metrics.delivered, 12);
    run::replay(&mock.endpoint, &cfg, &names, &mut loaded.ledger)
        .await
        .unwrap();
    assert!(mock.state.lock().await.subscribers.is_empty());
}
