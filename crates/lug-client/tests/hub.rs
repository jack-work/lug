//! The hub against a real socket: multiplexing, pooling, death, credit.

mod support;

use futures::StreamExt;
use lug_client::{Error, Hub, Response, Retry, Subscribe, Transport};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use support::sim::Sim;
use support::unix::Mock;

fn patch(key: &str) -> serde_json::Value {
    json!({ "Create": { key: 1 } })
}

#[tokio::test]
async fn many_calls_share_one_connection_and_come_back_matched() {
    let mock = Mock::start("mux").await;
    let hub = Hub::builder(mock.transport())
        .connections(1)
        .connect()
        .await
        .expect("connect");

    // The mock delays each read unevenly, so replies necessarily interleave
    // on the one socket. Each caller asks for a version only it asked for.
    let mut work = Vec::new();
    for version in 1..=200u64 {
        let hub = hub.clone();
        work.push(tokio::spawn(async move {
            let view = hub.read("log", Some(version)).await.expect("read");
            assert_eq!(view.version, version, "reply landed on the wrong caller");
            assert_eq!(view.value, json!({ "at": version }));
        }));
    }
    for task in work {
        task.await.expect("call task");
    }

    assert_eq!(
        mock.sim.connections_used(),
        1,
        "one connection carried all of it"
    );
}

#[tokio::test]
async fn work_spreads_over_the_pool_by_load() {
    let mock = Mock::start("pool").await;
    let hub = Hub::builder(mock.transport())
        .connections(4)
        .connect()
        .await
        .expect("connect");
    // Every slot dials at startup, so waiting for four greetings is waiting
    // for a warm pool. Connecting only waits for the first.
    support::until("pool warm", || mock.sim.connections_used() >= 4).await;

    mock.sim.swallow_pings.store(true, Ordering::Relaxed);
    let mut calls = Vec::new();
    for _ in 0..4 {
        let hub = hub.clone();
        calls.push(tokio::spawn(async move { hub.ping().await }));
    }
    support::until("four pings in flight", || {
        mock.sim
            .requests()
            .iter()
            .filter(|r| matches!(r, lug_proto::Request::Ping { .. }))
            .count()
            >= 4
    })
    .await;

    // Four concurrent calls, four connections: least-loaded must not stack
    // two of them onto one while another sits idle.
    let seen = mock.sim.seen.lock().unwrap();
    let mut per_conn = std::collections::HashMap::<u64, usize>::new();
    for (conn, req) in seen.iter() {
        if matches!(req, lug_proto::Request::Ping { .. }) {
            *per_conn.entry(*conn).or_default() += 1;
        }
    }
    drop(seen);
    for call in calls {
        call.abort();
    }
    assert_eq!(per_conn.len(), 4, "calls did not spread: {per_conn:?}");
}

#[tokio::test]
async fn a_dead_connection_fails_every_waiter_at_once() {
    let mock = Mock::start("death").await;
    let hub = Hub::builder(mock.transport())
        .connections(1)
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .expect("connect");

    mock.sim.swallow_pings.store(true, Ordering::Relaxed);
    let mut calls = Vec::new();
    for _ in 0..50 {
        let hub = hub.clone();
        calls.push(tokio::spawn(async move { hub.ping().await }));
    }
    support::until("fifty pings in flight", || {
        mock.sim
            .requests()
            .iter()
            .filter(|r| matches!(r, lug_proto::Request::Ping { .. }))
            .count()
            >= 50
    })
    .await;

    let killed = Instant::now();
    mock.kill_connections();
    for call in calls {
        let result = call.await.expect("call task");
        assert!(
            matches!(result, Err(Error::Disconnected { .. })),
            "expected a disconnect, got {result:?}"
        );
    }
    // The point: they fail on the death, not on their own 30 second deadline.
    assert!(
        killed.elapsed() < Duration::from_secs(5),
        "waiters were left hanging"
    );
}

#[tokio::test]
async fn calls_time_out_rather_than_hang() {
    let mock = Mock::start("timeout").await;
    let hub = Hub::builder(mock.transport())
        .connections(1)
        .timeout(Duration::from_millis(150))
        .connect()
        .await
        .expect("connect");
    mock.sim.swallow_pings.store(true, Ordering::Relaxed);

    let started = Instant::now();
    let result = hub.ping().await;
    assert!(
        matches!(result, Err(Error::Timeout { .. })),
        "got {result:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn the_pool_reconnects_and_serves_again() {
    let mock = Mock::start("reconnect").await;
    let hub = Hub::builder(mock.transport())
        .connections(1)
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .expect("connect");
    hub.ping().await.expect("ping before");

    mock.kill_connections();
    // No retry configured, so the first call after a kill may well fail; what
    // matters is that the pool comes back on its own.
    let _ = hub.ping().await;
    hub.with_retry(Retry::idempotent(5))
        .ping()
        .await
        .expect("ping after reconnect");
    assert!(mock.sim.connections_used() >= 2);
}

#[tokio::test]
async fn a_missing_daemon_fails_the_connect() {
    let path = support::scratch("absent").join("nothing.sock");
    let result = Hub::builder(Transport::unix(&path))
        .timeout(Duration::from_millis(200))
        .connect()
        .await;
    assert!(
        matches!(result, Err(Error::Connect { .. })),
        "got {result:?}"
    );
}

#[tokio::test]
async fn appends_and_reads_round_trip() {
    let mock = Mock::start("append").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");

    let info = hub.create("log", true).await.expect("create");
    assert_eq!(info.name, "log");

    let ack = hub
        .append("log", vec![patch("a"), patch("b")])
        .await
        .expect("append");
    assert_eq!(ack.versions, vec![1, 2]);

    let view = hub.read("log", None).await.expect("read");
    assert_eq!(view.version, 2);
    assert_eq!(view.snapshot().expect("snapshot").version(), 2);

    let logs = hub.list().await.expect("list");
    assert!(logs.iter().any(|l| l.name == "log" && l.version == 2));

    let missing = hub.append("nope", vec![patch("c")]).await;
    assert!(
        matches!(missing, Err(Error::Server { .. })),
        "got {missing:?}"
    );
}

#[tokio::test]
async fn credit_follows_the_consumer() {
    let sim = Arc::new(Sim::new());
    let patches: Vec<_> = (0..40).map(|n| patch(&format!("k{n}"))).collect();
    sim.append(&patches).expect("seed");
    let mock = Mock::start_with("credit", sim.clone()).await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");

    let window = 4;
    let mut stream = hub
        .subscribe("log", Subscribe::records().credit(window))
        .await
        .expect("subscribe");

    support::until("the first window is pushed", || {
        sim.pushed.load(Ordering::Relaxed) == window as u64
    })
    .await;
    // A stalled consumer must stall the server: no more records, no grants.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(sim.pushed.load(Ordering::Relaxed), window as u64);
    assert_eq!(sim.grants.load(Ordering::Relaxed), 0);

    let mut taken = 0;
    while taken < 20 {
        let frame = stream.next().await.expect("frame").expect("no error");
        if let Response::Records { records, .. } = frame {
            taken += records.len();
        }
    }
    assert!(
        sim.grants.load(Ordering::Relaxed) > 0,
        "draining granted no credit"
    );
    assert!(sim.pushed.load(Ordering::Relaxed) >= 20);

    // Dropping the subscription cancels it at the server.
    drop(stream);
    support::until("cancel reaches the server", || {
        sim.requests()
            .iter()
            .any(|r| matches!(r, lug_proto::Request::Cancel { .. }))
    })
    .await;
}

#[tokio::test]
async fn a_subscription_ends_when_the_server_says_so() {
    let mock = Mock::start("end").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    mock.sim.append(&[patch("a")]).expect("seed");

    let mut stream = hub
        .subscribe("log", Subscribe::records().credit(8))
        .await
        .expect("subscribe");
    let first = stream.next().await.expect("frame").expect("no error");
    assert!(matches!(first, Response::Records { .. }));

    let id = stream.id();
    mock.sim.end_stream(id);
    let end = stream.next().await;
    assert!(end.is_none(), "stream should end, got {end:?}");
}

#[tokio::test]
async fn subscribing_to_a_missing_log_fails_on_the_stream() {
    let mock = Mock::start("nolog").await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let mut stream = hub
        .subscribe("absent", Subscribe::records())
        .await
        .expect("subscribe");
    let first = stream.next().await.expect("frame");
    assert!(matches!(first, Err(Error::Server { .. })), "got {first:?}");
}
