use crate::{
    Result,
    check::{Ledger, patch},
    config::Config,
    error,
    measure::{Metrics, offset},
    server,
    wire::{Endpoint, Event, Peer, Traffic, receive},
};
use futures::{StreamExt, stream};
use lug_proto::{Durability, Mode, Request, Response};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::mpsc,
    time::{interval, sleep_until, timeout},
};

const CREDIT: u32 = 256;
const APPEND_BASE: u64 = 1 << 32;
const CONTROL: u64 = u64::MAX - 1;
const STALLED: u64 = u64::MAX - 2;

pub struct Pool {
    pub peers: Vec<Peer>,
    events: mpsc::Receiver<Event>,
    pub traffic: Arc<Traffic>,
    confirmations: HashMap<(usize, u64), u64>,
    unconfirmed: HashSet<(usize, u64)>,
}

impl Pool {
    pub async fn open(
        endpoint: &Endpoint,
        count: usize,
        parallel: usize,
        deadline: Duration,
    ) -> Result<Self> {
        let traffic = Arc::new(Traffic::default());
        let (events, receiver) = mpsc::channel(8192);
        let work = stream::iter(0..count)
            .map(|index| {
                let events = events.clone();
                let traffic = traffic.clone();
                async move {
                    Ok::<_, anyhow::Error>((
                        index,
                        Peer::connect(endpoint, index, events, traffic).await?,
                    ))
                }
            })
            .buffer_unordered(parallel);
        tokio::pin!(work);
        let peers = timeout(deadline, async {
            let mut peers = Vec::with_capacity(count);
            while let Some(peer) = work.next().await {
                peers.push(peer?);
            }
            peers.sort_by_key(|(index, _)| *index);
            Ok::<_, anyhow::Error>(peers.into_iter().map(|(_, peer)| peer).collect())
        })
        .await
        .map_err(|_| {
            error(format!(
                "establishing {count} connections exceeded {deadline:?}"
            ))
        })??;
        Ok(Self {
            peers,
            events: receiver,
            traffic: traffic.clone(),
            confirmations: HashMap::new(),
            unconfirmed: HashSet::new(),
        })
    }

    fn expect(&mut self, peer: usize, request: &Request) {
        if matches!(request, Request::Subscribe { .. } | Request::Credit { .. }) {
            *self.confirmations.entry((peer, request.id())).or_default() += 1;
        }
        if matches!(request, Request::Subscribe { .. }) {
            self.unconfirmed.insert((peer, request.id()));
        }
    }

    fn try_send(&mut self, peer: usize, request: Request) -> Result<()> {
        self.expect(peer, &request);
        self.peers[peer].out.try_send(request).map_err(|e| {
            error(format!(
                "OVERLOAD: connection {peer} command queue: {e}; no samples dropped"
            ))
        })
    }

    async fn send(&mut self, peer: usize, request: Request) -> Result<()> {
        self.expect(peer, &request);
        self.peers[peer]
            .out
            .send(request)
            .await
            .map_err(|_| error(format!("connection {peer} writer exited")))
    }

    async fn next(&mut self) -> Result<(usize, Instant, Response)> {
        let event = receive(&mut self.events).await?;
        let response = event
            .response
            .map_err(|e| error(format!("connection {}: {e}", event.peer)))?;
        if let Response::Error { id, code, message } = &response {
            return Err(error(format!(
                "server error on connection {}, id {id}: {code:?}: {message}",
                event.peer
            )));
        }
        let key = (event.peer, response.id());
        if let Response::Ok { id } = &response {
            let remaining = self.confirmations.get_mut(&key).ok_or_else(|| {
                error(format!(
                    "unexpected Ok for connection {}, id {id}",
                    event.peer
                ))
            })?;
            if *remaining == 0 {
                return Err(error(format!(
                    "duplicate Ok for connection {}, id {id}",
                    event.peer
                )));
            }
            *remaining -= 1;
            self.unconfirmed.remove(&key);
        }
        if matches!(response, Response::Records { .. }) && self.unconfirmed.contains(&key) {
            return Err(error(format!(
                "stream {} pushed before subscription acknowledgement",
                response.id()
            )));
        }
        Ok((event.peer, event.arrived, response))
    }

    pub async fn control(&mut self, request: Request, deadline: Duration) -> Result<Response> {
        let id = request.id();
        let expects_ok = matches!(request, Request::Subscribe { .. } | Request::Credit { .. });
        timeout(deadline, async {
            self.send(0, request).await?;
            loop {
                let (_, _, response) = self.next().await?;
                if response.id() == id && (expects_ok || !matches!(response, Response::Ok { .. })) {
                    return Ok(response);
                }
                if !matches!(response, Response::Pong { .. } | Response::Ok { .. }) {
                    return Err(error(format!(
                        "unexpected control-phase response {response:?}"
                    )));
                }
            }
        })
        .await
        .map_err(|_| error(format!("control request {id} exceeded {deadline:?}")))?
    }

    pub async fn create(&mut self, names: &[String], deadline: Duration) -> Result<()> {
        for name in names {
            match self
                .control(
                    Request::Create {
                        id: CONTROL,
                        log: name.clone(),
                        reducible: false,
                    },
                    deadline,
                )
                .await?
            {
                Response::Logs { logs, .. }
                    if logs.len() == 1 && logs[0].name == *name && !logs[0].reducible => {}
                other => {
                    return Err(error(format!(
                        "Create {name:?}: unexpected response {other:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub async fn heartbeat(&mut self, deadline: Duration) -> Result<()> {
        timeout(deadline, async {
            for peer in 0..self.peers.len() {
                self.send(peer, Request::Ping { id: CONTROL }).await?;
            }
            let mut waiting: HashSet<_> = (0..self.peers.len()).collect();
            while !waiting.is_empty() || self.confirmations.values().any(|n| *n > 0) {
                let (peer, _, response) = self.next().await?;
                match response {
                    Response::Pong { id: CONTROL } if waiting.remove(&peer) => {}
                    Response::Pong { .. } | Response::Ok { .. } => {}
                    other => {
                        return Err(error(format!(
                            "unexpected held-connection heartbeat response {other:?}"
                        )));
                    }
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| error("held connections did not all answer end-of-window Ping"))?
    }
}

pub async fn accepts(endpoint: &Endpoint, cfg: &Config) -> Result<(usize, Duration)> {
    let start = Instant::now();
    let traffic = Arc::new(Traffic::default());
    let (events, _receiver) = mpsc::channel(8192);
    let work = stream::iter(0..cfg.accepts)
        .map(|index| {
            let events = events.clone();
            let traffic = traffic.clone();
            async move {
                let peer = Peer::connect(endpoint, index, events, traffic).await?;
                drop(peer);
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(cfg.parallel);
    tokio::pin!(work);
    timeout(cfg.timeout, async {
        while let Some(result) = work.next().await {
            result?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| error("short-lived accept benchmark timed out"))??;
    Ok((cfg.accepts, start.elapsed()))
}

pub struct Loaded {
    pub ledger: Ledger,
    pub metrics: Metrics,
    pub streams: HashMap<u64, usize>,
}

pub async fn load(
    pool: &mut Pool,
    cfg: &Config,
    names: &[String],
    pid: Option<u32>,
) -> Result<Loaded> {
    timeout(
        cfg.duration + cfg.timeout * 2,
        load_inner(pool, cfg, names, pid),
    )
    .await
    .map_err(|_| error("load setup or completion exceeded its bounded deadline"))?
}

async fn load_inner(
    pool: &mut Pool,
    cfg: &Config,
    names: &[String],
    pid: Option<u32>,
) -> Result<Loaded> {
    let mut ledger = Ledger::new(names.len(), cfg.patch_size, cfg.durability);
    let mut streams = HashMap::new();
    for (log, name) in names.iter().enumerate() {
        for subscriber in 0..cfg.subscribers {
            let ordinal = log * cfg.subscribers + subscriber;
            let id = ordinal as u64 + 1;
            let peer = (cfg.logs * cfg.appenders + ordinal) % pool.peers.len();
            ledger.subscribe(id, log, 0, CREDIT)?;
            pool.send(
                peer,
                Request::Subscribe {
                    id,
                    log: name.clone(),
                    from: 0,
                    mode: Mode::Records,
                    credit: CREDIT,
                },
            )
            .await?;
            streams.insert(id, peer);
        }
    }
    pool.heartbeat(cfg.timeout).await?;
    let start = Instant::now();
    let end = start + cfg.duration;
    let deadline = end + cfg.timeout;
    let before = pool.traffic.snapshot();
    let planned = cfg.planned()?;
    let lanes = cfg.logs * cfg.appenders;
    let mut sent = 0;
    let mut pending = HashSet::new();
    let mut metrics = Metrics::new()?;
    let mut sample = interval(Duration::from_millis(100));
    loop {
        if sent == planned && ledger.complete() && Instant::now() >= end {
            break;
        }
        let due = if sent < planned {
            start + offset(sent, cfg.rate)
        } else {
            end.max(Instant::now() + Duration::from_secs(86400))
        };
        tokio::select! {
            event = pool.next() => {
                let (observed_peer, arrived, response) = event?;
                match response {
                    Response::Ack { id, versions, synced } => {
                        let sequence = id.checked_sub(APPEND_BASE).ok_or_else(|| error(format!("unknown append id {id}")))?;
                        let expected_peer = (sequence as usize % lanes) % pool.peers.len();
                        if observed_peer != expected_peer { return Err(error(format!("Ack {id} arrived on connection {observed_peer}, expected {expected_peer}"))); }
                        if !pending.remove(&sequence) { return Err(error(format!("unexpected or duplicate ack {id}"))); }
                        let intended = ledger.ack(sequence, &versions, synced)?;
                        metrics.append.record(intended, arrived)?;
                        metrics.acknowledged += 1;
                        if arrived < end { metrics.in_window_acks += 1; }
                    }
                    Response::Records { id, records } => {
                        if id == STALLED { return Err(error("BACKPRESSURE FAILURE: zero-credit subscriber received records")); }
                        if streams.get(&id) != Some(&observed_peer) { return Err(error(format!("stream {id} arrived on wrong connection {observed_peer}"))); }
                        let count = u32::try_from(records.len())?;
                        for intended in ledger.records(id, &records)? { metrics.delivery.record(intended, arrived)?; }
                        metrics.delivered += u64::from(count);
                        if arrived < end { metrics.in_window_deliveries += u64::from(count); }
                        let peer = *streams.get(&id).ok_or_else(|| error(format!("unknown stream {id}")))?;
                        // Keeping the command queue bounded makes a slow harness an explicit failure, not hidden heap growth.
                        pool.try_send(peer, Request::Credit { id, grant: count })?;
                        ledger.grant(id, count)?;
                    }
                    Response::Pong { .. } | Response::Ok { .. } => {}
                    Response::Gap { id, from, to } => return Err(error(format!("GAP: stream {id} lost ({from}, {to}] despite retention disabled"))),
                    other => return Err(error(format!("unexpected load response {other:?}"))),
                }
            }
            _ = sleep_until(due.into()), if sent < planned => {
                if pending.len() >= cfg.max_inflight {
                    return Err(error(format!("OVERLOAD: {} outstanding appends hit --max-inflight {}; no samples dropped", pending.len(), cfg.max_inflight)));
                }
                let lane = sent as usize % lanes;
                let log = lane / cfg.appenders;
                let peer = lane % pool.peers.len();
                let sequence = ledger.issue(log, due);
                let request = Request::Append { id: APPEND_BASE + sequence, log: names[log].clone(), patches: vec![patch(sequence, cfg.patch_size)], durability: cfg.durability };
                pool.try_send(peer, request)?;
                pending.insert(sequence);
                sent += 1;
                metrics.scheduled += 1;
            }
            _ = sample.tick() => {
                if let Some(pid) = pid { metrics.rss_peak = metrics.rss_peak.max(server::rss(pid)?); }
            }
            _ = sleep_until(end.into()), if sent == planned && ledger.complete() => {}
            _ = sleep_until(deadline.into()) => {
                return Err(error(format!("LOAD STALLED: scheduled {sent}/{planned}, acked {}, delivered {}; drain exceeded {:?}; credit isolation or delivery invariant failed", metrics.acknowledged, metrics.delivered, cfg.timeout)));
            }
        }
    }
    ledger.finish()?;
    metrics.elapsed = start.elapsed();
    let after = pool.traffic.snapshot();
    metrics.tx_bytes = after.0 - before.0;
    metrics.rx_bytes = after.1 - before.1;
    Ok(Loaded {
        ledger,
        metrics,
        streams,
    })
}

pub async fn cancel(pool: &mut Pool, loaded: &mut Loaded, deadline: Duration) -> Result<()> {
    timeout(deadline, async {
        let mut waiting: HashSet<_> = loaded.streams.keys().copied().collect();
        for (id, peer) in &loaded.streams {
            pool.send(*peer, Request::Cancel { id: *id }).await?;
        }
        while !waiting.is_empty() {
            let (_, _, response) = pool.next().await?;
            match response {
                Response::End { id } if waiting.remove(&id) => {}
                Response::Pong { .. } | Response::Ok { .. } => {}
                Response::Records { id, records } => {
                    loaded.ledger.records(id, &records)?;
                }
                other => return Err(error(format!("unexpected cancel response {other:?}"))),
            }
        }
        loaded.ledger.finish()
    })
    .await
    .map_err(|_| error("Cancel did not return End before deadline"))?
}

pub async fn backpressure(endpoint: &Endpoint, cfg: &Config) -> Result<()> {
    let probe = Config {
        connections: 1,
        logs: 1,
        appenders: 1,
        subscribers: 1,
        patch_size: 32768,
        rate: 128,
        duration: Duration::from_secs(1),
        durability: Durability::Written,
        ..cfg.clone()
    };
    let mut pool = Pool::open(endpoint, 1, 1, cfg.timeout).await?;
    let names = vec!["credit-isolation".to_owned()];
    pool.create(&names, cfg.timeout).await?;
    pool.send(
        0,
        Request::Subscribe {
            id: STALLED,
            log: names[0].clone(),
            from: 0,
            mode: Mode::Records,
            credit: 0,
        },
    )
    .await?;
    let mut loaded = load(&mut pool, &probe, &names, None)
        .await
        .map_err(|e| error(format!("BACKPRESSURE ISOLATION FAILED: {e}")))?;
    cancel(&mut pool, &mut loaded, cfg.timeout).await?;
    timeout(cfg.timeout, async {
        pool.send(
            0,
            Request::Credit {
                id: STALLED,
                grant: 1,
            },
        )
        .await?;
        loop {
            match pool.next().await?.2 {
                Response::Records {
                    id: STALLED,
                    records,
                } if records.len() == 1
                    && records[0].version == 1
                    && records[0].patch == patch(0, probe.patch_size) =>
                {
                    break;
                }
                Response::Pong { .. } | Response::Ok { .. } => {}
                other => {
                    return Err(error(format!(
                        "stopped-credit stream failed its one-record resume: {other:?}"
                    )));
                }
            }
        }
        pool.heartbeat(cfg.timeout).await?;
        if let Ok(event) = timeout(Duration::from_millis(250), pool.next()).await {
            let (_, _, response) = event?;
            return Err(error(format!(
                "BACKPRESSURE FAILURE: frame after one-record grant was exhausted: {response:?}"
            )));
        }
        match pool
            .control(Request::Cancel { id: STALLED }, cfg.timeout)
            .await?
        {
            Response::End { id: STALLED } => Ok(()),
            other => Err(error(format!("stopped-credit Cancel: {other:?}"))),
        }
    })
    .await
    .map_err(|_| error("BACKPRESSURE FAILURE: stopped-credit subscriber did not resume"))??;
    pool.heartbeat(cfg.timeout).await?;
    eprintln!(
        "lug-load: PASS credit isolation: 128 appends and deliveries progressed beside a zero-credit stream, then that stream resumed"
    );
    Ok(())
}

pub async fn replay(
    endpoint: &Endpoint,
    cfg: &Config,
    names: &[String],
    ledger: &mut Ledger,
) -> Result<()> {
    timeout(cfg.timeout * 2, replay_inner(endpoint, cfg, names, ledger))
        .await
        .map_err(|_| error("durable replay setup or completion exceeded its bounded deadline"))?
}

async fn replay_inner(
    endpoint: &Endpoint,
    cfg: &Config,
    names: &[String],
    ledger: &mut Ledger,
) -> Result<()> {
    let mut pool = Pool::open(endpoint, 1, 1, cfg.timeout).await?;
    ledger.replay();
    for (log, name) in names.iter().enumerate() {
        let id = log as u64 + 1;
        ledger.subscribe(id, log, 0, CREDIT)?;
        pool.send(
            0,
            Request::Subscribe {
                id,
                log: name.clone(),
                from: 0,
                mode: Mode::Records,
                credit: CREDIT,
            },
        )
        .await?;
    }
    timeout(cfg.timeout, async {
        while !ledger.complete() {
            match pool.next().await?.2 {
                Response::Records { id, records } => {
                    ledger.records(id, &records)?;
                    let count = u32::try_from(records.len())?;
                    pool.send(0, Request::Credit { id, grant: count }).await?;
                    ledger.grant(id, count)?;
                }
                Response::Pong { .. } | Response::Ok { .. } => {}
                other => return Err(error(format!("DURABLE REPLAY FAILED: {other:?}"))),
            }
        }
        ledger.finish()
    })
    .await
    .map_err(|_| {
        error("DURABLE DATA LOSS: replay did not reach every acknowledged version before deadline")
    })??;
    for log in 0..names.len() {
        let id = log as u64 + 1;
        match pool.control(Request::Cancel { id }, cfg.timeout).await? {
            Response::End { .. } => {}
            other => return Err(error(format!("replay Cancel: {other:?}"))),
        }
    }
    pool.heartbeat(cfg.timeout).await?;
    Ok(())
}

pub async fn crash_probe(server: &mut server::Server, cfg: &Config, epoch: usize) -> Result<()> {
    let mut pool = Pool::open(&server.endpoint, 1, 1, cfg.timeout).await?;
    let names = vec![format!("crash-{epoch}")];
    pool.create(&names, cfg.timeout).await?;
    let mut ledger = Ledger::new(1, cfg.patch_size, Durability::Durable);
    for sequence in 0..32 {
        ledger.issue(0, Instant::now());
        let response = pool
            .control(
                Request::Append {
                    id: APPEND_BASE + sequence,
                    log: names[0].clone(),
                    patches: vec![patch(sequence, cfg.patch_size)],
                    durability: Durability::Durable,
                },
                cfg.timeout,
            )
            .await?;
        match response {
            Response::Ack {
                versions, synced, ..
            } => {
                ledger.ack(sequence, &versions, synced)?;
            }
            other => return Err(error(format!("durable crash probe append: {other:?}"))),
        }
    }
    server.crash_restart().await?;
    drop(pool);
    replay(&server.endpoint, cfg, &names, &mut ledger).await?;
    eprintln!(
        "lug-load: PASS durable crash probe: SIGKILL immediately after final Ack, all 32 exact records replayed"
    );
    Ok(())
}

pub async fn churn(endpoint: &Endpoint, cfg: &Config, names: &[String]) -> Result<()> {
    for round in 0..32 {
        let mut pool = Pool::open(endpoint, 1, 1, cfg.timeout).await?;
        let name = &names[round % names.len()];
        let id = 123;
        pool.send(
            0,
            Request::Subscribe {
                id,
                log: name.clone(),
                from: 0,
                mode: Mode::Records,
                credit: 0,
            },
        )
        .await?;
        match pool.control(Request::Cancel { id }, cfg.timeout).await? {
            Response::End { .. } => {}
            other => return Err(error(format!("churn Cancel: {other:?}"))),
        }
        pool.heartbeat(cfg.timeout).await?;
    }
    Ok(())
}
