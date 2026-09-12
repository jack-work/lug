//! The same hub over HTTP: calls as POSTs, subscriptions as SSE.
//!
//! The point of these is that nothing above the transport changes. They drive
//! the public API exactly as the unix tests do.

mod support;

use futures::StreamExt;
use lug_client::{Error, Event, Hub, Status, Subscribe, Transport};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use support::http::{Mock, TOKEN};
use support::sim::Sim;

fn patch(key: &str) -> serde_json::Value {
    json!({ "Create": { key: 1 } })
}

#[tokio::test]
async fn calls_round_trip_over_http() {
    let mock = Mock::start().await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");

    hub.ping().await.expect("ping");
    let info = hub.create("log", true).await.expect("create");
    assert_eq!(info.name, "log");

    let ack = hub
        .append("log", vec![patch("a"), patch("b")])
        .await
        .expect("append");
    assert_eq!(ack.versions, vec![1, 2]);

    let view = hub.read("log", None).await.expect("read");
    assert_eq!(view.version, 2);
    assert_eq!(view.value["a"], json!(1));

    assert!(
        hub.list()
            .await
            .expect("list")
            .iter()
            .any(|l| l.name == "log")
    );
}

#[tokio::test]
async fn many_calls_are_concurrent_over_http() {
    let mock = Mock::start().await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");

    let mut work = Vec::new();
    for version in 1..=50u64 {
        let hub = hub.clone();
        work.push(tokio::spawn(async move {
            let view = hub.read("log", Some(version)).await.expect("read");
            assert_eq!(view.version, version, "reply landed on the wrong caller");
        }));
    }
    for task in work {
        task.await.expect("call task");
    }
}

#[tokio::test]
async fn a_subscription_streams_and_grants_credit_over_sse() {
    let sim = Arc::new(Sim::new());
    let patches: Vec<_> = (0..40).map(|n| patch(&format!("k{n}"))).collect();
    sim.append(&patches).expect("seed");
    let mock = Mock::start_with(sim.clone()).await;
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
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        sim.pushed.load(Ordering::Relaxed),
        window as u64,
        "credit was not honoured"
    );

    let mut taken = 0;
    while taken < 20 {
        let event = stream.next().await.expect("event").expect("no error");
        if matches!(event, Event::Record(_)) {
            taken += 1;
        }
    }
    // Credit rode a POST carrying the session the SSE stream announced, and
    // the mock rejects a Credit that arrives without one.
    assert!(
        sim.grants.load(Ordering::Relaxed) > 0,
        "no credit was granted"
    );
    assert_eq!(mock.sessions_issued(), 1);
}

#[tokio::test]
async fn the_follower_works_over_http_too() {
    let mock = Mock::start().await;
    let hub = Hub::connect(mock.transport()).await.expect("connect");
    let mut follower = hub.follow("log").await.expect("follow");

    mock.sim
        .append(&[patch("a"), patch("b"), patch("c")])
        .expect("seed");
    let state = follower
        .wait_for(mock.sim.version())
        .await
        .expect("converge");
    assert_eq!(state.status, Status::Live);
    assert_eq!(
        state.snapshot.expect("view").root().to_json(),
        mock.sim.root()
    );
}

#[tokio::test]
async fn a_bad_token_is_refused() {
    let mock = Mock::start().await;
    let result = Hub::builder(Transport::http(&mock.base, "wrong"))
        .timeout(Duration::from_secs(2))
        .connect_lazy()
        .ping()
        .await;
    assert!(
        matches!(result, Err(Error::Disconnected { .. })),
        "got {result:?}"
    );
    assert_ne!(TOKEN, "wrong");
}

#[tokio::test]
async fn a_dead_endpoint_fails_the_connect() {
    // Bind and drop, so the port is almost certainly free and nothing answers.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    drop(listener);

    let result = Hub::builder(Transport::http(&base, TOKEN))
        .timeout(Duration::from_millis(300))
        .connect()
        .await;
    assert!(
        matches!(result, Err(Error::Connect { .. })),
        "got {result:?}"
    );
}
