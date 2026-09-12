use crate::{
    Result,
    config::Config,
    error, run,
    wire::{Endpoint, read_frame},
};
use lug_proto::{Record, Request, Response};
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
            state.logs.entry(log).or_default();
            out.try_send(Response::Ack {
                id,
                versions: vec![],
                synced: 0,
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
    for fault in [Fault::Gap, Fault::Duplicate, Fault::Order, Fault::AckLoss] {
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
