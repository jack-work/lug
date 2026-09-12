//! End to end over the real unix socket.

mod common;

use common::{Harness, PATIENCE, records, versions};
use lug_proto::{Code, Durability, Mode, Request, Response};
use serde_json::json;
use std::time::Duration;

fn patches(n: usize) -> Vec<serde_json::Value> {
    (0..n).map(|i| json!({ "n": i })).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hello_append_and_read_back() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("events", false).await;

    let welcome = client.call(Request::Hello { id: 2, version: lug_proto::VERSION }).await;
    match welcome {
        Response::Welcome { version, max_frame, session, .. } => {
            assert_eq!(version, lug_proto::VERSION);
            assert_eq!(max_frame, lug_proto::MAX_FRAME);
            // A unix connection correlates itself; no session is needed.
            assert_eq!(session, None);
        }
        other => panic!("expected Welcome, got {other:?}"),
    }

    let ack = client.append(3, "events", patches(3)).await;
    assert_eq!(versions(&ack), vec![1, 2, 3]);

    client
        .send(Request::Subscribe {
            id: 4,
            log: "events".into(),
            from: 0,
            mode: Mode::Records,
            credit: 10,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 4 }));
    assert_eq!(records(&client.recv().await), vec![1, 2, 3]);

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriber_sees_live_records() {
    let lug = Harness::start().await;
    let mut owner = lug.with_log("live", false).await;

    let mut watcher = lug.client().await;
    watcher
        .send(Request::Subscribe {
            id: 1,
            log: "live".into(),
            from: 0,
            mode: Mode::Records,
            credit: 100,
        })
        .await;
    assert!(matches!(watcher.recv().await, Response::Ok { id: 1 }));

    owner.append(7, "live", patches(4)).await;
    let mut seen = Vec::new();
    while seen.len() < 4 {
        seen.extend(records(&watcher.recv().await));
    }
    assert_eq!(seen, vec![1, 2, 3, 4]);

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_credit_pushes_nothing_until_granted() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("held", false).await;
    client.append(2, "held", patches(5)).await;

    client
        .send(Request::Subscribe {
            id: 3,
            log: "held".into(),
            from: 0,
            mode: Mode::Records,
            credit: 0,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 3 }));
    assert!(client.quiet(Duration::from_millis(250)).await.is_none());

    assert!(matches!(
        client.call(Request::Credit { id: 3, grant: 2 }).await,
        Response::Ok { id: 3 }
    ));
    assert_eq!(records(&client.recv().await), vec![1, 2]);
    assert!(client.quiet(Duration::from_millis(250)).await.is_none());

    lug.stop().await;
}

/// The point of credit: one subscriber that stops granting must not touch
/// anything else on its connection, appends included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_stream_blocks_neither_its_neighbour_nor_appends() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("shared", false).await;

    for (id, credit) in [(10u64, 1u32), (11, 100_000)] {
        client
            .send(Request::Subscribe {
                id,
                log: "shared".into(),
                from: 0,
                mode: Mode::Records,
                credit,
            })
            .await;
    }
    assert!(matches!(client.recv().await, Response::Ok { id: 10 }));
    assert!(matches!(client.recv().await, Response::Ok { id: 11 }));

    let total = 500;
    for round in 0..10u64 {
        client
            .send(Request::Append {
                id: 100 + round,
                log: "shared".into(),
                patches: patches(50),
                durability: Durability::Written,
            })
            .await;
    }

    let mut acked = 0;
    let mut healthy = Vec::new();
    let mut stalled = 0;
    while acked < 10 || healthy.len() < total {
        match client.recv().await {
            Response::Ack { .. } => acked += 1,
            Response::Records { id: 11, records } => {
                healthy.extend(records.iter().map(|r| r.version))
            }
            Response::Records { id: 10, records } => stalled += records.len(),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(acked, 10);
    assert_eq!(healthy, (1..=total as u64).collect::<Vec<_>>());
    // The stalled stream got its single credit and nothing more.
    assert!(stalled <= 1, "stalled stream took {stalled} records");

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_from_a_cold_cursor_reads_storage() {
    // A ring far smaller than the history forces the catch-up path.
    let lug = Harness::with(|c| c.ring = 8, 1 << 16).await;
    let mut client = lug.with_log("history", false).await;
    for round in 0..10 {
        client.append(10 + round, "history", patches(20)).await;
    }

    client
        .send(Request::Subscribe {
            id: 5,
            log: "history".into(),
            from: 0,
            mode: Mode::Records,
            credit: 10_000,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 5 }));

    let mut seen = Vec::new();
    while seen.len() < 200 {
        match client.recv().await {
            Response::Records { records, .. } => seen.extend(records.iter().map(|r| r.version)),
            other => panic!("expected records, got {other:?}"),
        }
    }
    assert_eq!(seen, (1..=200u64).collect::<Vec<_>>());

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reclaimed_cursor_gets_a_gap_and_resumes() {
    // Storage keeps 16 records, the ring 8, so the first 100 are gone.
    let lug = Harness::with(|c| c.ring = 8, 16).await;
    let mut client = lug.with_log("short", false).await;
    for round in 0..10 {
        client.append(10 + round, "short", patches(10)).await;
    }

    client
        .send(Request::Subscribe {
            id: 6,
            log: "short".into(),
            from: 0,
            mode: Mode::Records,
            credit: 1000,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 6 }));

    let resume = match client.recv().await {
        Response::Gap { from, to, .. } => {
            assert_eq!(from, 0);
            assert!(to > 0 && to < 100, "gap to {to}");
            to
        }
        other => panic!("expected Gap, got {other:?}"),
    };
    let first = records(&client.recv().await);
    assert_eq!(first.first(), Some(&(resume + 1)));
    assert_eq!(*first.last().expect("records"), 100);

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_uid_is_refused_on_accept() {
    // Claim to be owned by a uid this test process does not have, which is
    // what the daemon's SO_PEERCRED check has to catch.
    let owner = lug_server::acl::own_uid() + 1;
    let lug = Harness::with(|c| c.owner = Some(owner), 1 << 16).await;

    let mut client = lug.client().await;
    match client.recv().await {
        Response::Error { code: Code::Unauthorized, .. } => {}
        other => panic!("expected Unauthorized, got {other:?}"),
    }
    assert!(client.try_recv().await.is_none(), "connection should be closed");

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_frame_is_refused_on_the_length_prefix() {
    use tokio::io::AsyncWriteExt;

    let lug = Harness::start().await;
    let mut raw = tokio::net::UnixStream::connect(lug.server().socket()).await.expect("connect");
    raw.write_all(&(lug_proto::MAX_FRAME + 1).to_be_bytes()).await.expect("prefix");
    raw.write_all(b"{}").await.expect("body");

    let mut framed = tokio_util::codec::Framed::new(
        raw,
        lug_proto::Codec::<Response, Request>::new(),
    );
    let response = tokio::time::timeout(PATIENCE, futures::StreamExt::next(&mut framed))
        .await
        .expect("a frame in time")
        .expect("a frame")
        .expect("decodes");
    assert!(matches!(response, Response::Error { code: Code::Malformed, .. }));

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appenders_mint_contiguous_unique_versions() {
    let lug = Harness::start().await;
    lug.with_log("busy", false).await;

    let connections = 16;
    let rounds = 25;
    let per_append = 4;
    let mut workers = Vec::new();
    for connection in 0..connections {
        let socket = lug.server().socket().to_path_buf();
        workers.push(tokio::spawn(async move {
            let mut client = common::Client::connect(&socket).await;
            let mut minted = Vec::new();
            for round in 0..rounds {
                let ack = client
                    .append(connection * 1000 + round, "busy", patches(per_append))
                    .await;
                minted.extend(versions(&ack));
            }
            minted
        }));
    }

    let mut all = Vec::new();
    for worker in workers {
        all.extend(worker.await.expect("worker"));
    }
    all.sort_unstable();
    let expected: Vec<u64> = (1..=(connections * rounds * per_append as u64)).collect();
    assert_eq!(all, expected, "versions must be contiguous and unique");

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_ends_the_stream_and_an_unknown_id_is_refused() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("cancelled", false).await;

    client
        .send(Request::Subscribe {
            id: 9,
            log: "cancelled".into(),
            from: 0,
            mode: Mode::Records,
            credit: 10,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 9 }));

    client.send(Request::Cancel { id: 9 }).await;
    assert!(matches!(client.recv_for(9).await, Response::End { id: 9 }));

    match client.call(Request::Cancel { id: 404 }).await {
        Response::Error { code: Code::BadId, .. } => {}
        other => panic!("expected BadId, got {other:?}"),
    }

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_view_needs_a_reducible_log() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("plain", false).await;

    match client.call(Request::Read { id: 2, log: "plain".into(), at: None }).await {
        Response::Error { code: Code::NotReducible, .. } => {}
        other => panic!("expected NotReducible, got {other:?}"),
    }
    client
        .send(Request::Subscribe {
            id: 3,
            log: "plain".into(),
            from: 0,
            mode: Mode::Reducible,
            credit: 1,
        })
        .await;
    match client.recv().await {
        Response::Error { code: Code::NotReducible, .. } => {}
        other => panic!("expected NotReducible, got {other:?}"),
    }

    match client.call(Request::Read { id: 4, log: "absent".into(), at: None }).await {
        Response::Error { code: Code::NoSuchLog, .. } => {}
        other => panic!("expected NoSuchLog, got {other:?}"),
    }

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reducible_log_materializes_a_view() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("store", true).await;

    let ack = client
        .append(2, "store", vec![json!({ "Create": { "a": 1 } }), json!({ "Create": { "b": 2 } })])
        .await;
    assert_eq!(versions(&ack), vec![1, 2]);

    match client.call(Request::Read { id: 3, log: "store".into(), at: None }).await {
        // The value is the view exactly as it would be checkpointed, so a
        // follower can adopt it without a second encoding.
        Response::View { version, value, .. } => {
            assert_eq!(version, 2);
            assert_eq!(value["version"], json!(2));
            assert_eq!(value["root"]["a"], json!(1));
            assert_eq!(value["root"]["b"], json!(2));
        }
        other => panic!("expected View, got {other:?}"),
    }

    // A reducible subscription opens with the view, then tails.
    client
        .send(Request::Subscribe {
            id: 4,
            log: "store".into(),
            from: 0,
            mode: Mode::Reducible,
            credit: 10,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 4 }));
    assert!(matches!(client.recv().await, Response::View { id: 4, version: 2, .. }));

    client.send(Request::Append {
        id: 5,
        log: "store".into(),
        patches: vec![json!({ "Create": { "c": 3 } })],
        durability: Durability::Written,
    })
    .await;
    assert_eq!(records(&client.recv_for(4).await), vec![3]);

    // A patch the structure rejects is the caller's fault, and says so.
    match client.append(6, "store", vec![json!({ "Nonsense": {} })]).await {
        Response::Error { code: Code::Rejected, .. } => {}
        other => panic!("expected Rejected, got {other:?}"),
    }

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creating_a_log_twice_is_idempotent_unless_the_shape_changed() {
    let lug = Harness::start().await;
    let mut client = lug.client().await;

    match client.call(Request::Create { id: 1, log: "once".into(), reducible: false }).await {
        Response::Logs { logs, .. } => {
            assert_eq!(logs.len(), 1);
            assert_eq!(logs[0].name, "once");
        }
        other => panic!("expected Logs, got {other:?}"),
    }
    assert!(matches!(
        client.call(Request::Create { id: 2, log: "once".into(), reducible: false }).await,
        Response::Logs { .. }
    ));
    match client.call(Request::Create { id: 3, log: "once".into(), reducible: true }).await {
        Response::Error { code: Code::LogExists, .. } => {}
        other => panic!("expected LogExists, got {other:?}"),
    }
    match client.call(Request::Create { id: 4, log: "../escape".into(), reducible: false }).await {
        Response::Error { code: Code::Malformed, .. } => {}
        other => panic!("expected Malformed, got {other:?}"),
    }

    match client.call(Request::List { id: 5 }).await {
        Response::Logs { logs, .. } => assert_eq!(logs.len(), 1),
        other => panic!("expected Logs, got {other:?}"),
    }

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_unlinks_the_socket() {
    let lug = Harness::start().await;
    let socket = lug.server().socket().to_path_buf();
    assert!(socket.exists());
    lug.stop().await;
    assert!(!socket.exists());
}
