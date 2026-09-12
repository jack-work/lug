mod common;

use common::{Client, Harness, versions};
use lug_proto::{Durability, Mode, Request, Response};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_cursor_crosses_the_storage_ring_seam_while_appending() {
    let rounds = std::env::var("LUG_HAMMER_ROUNDS").map_or(1, |n| n.parse().expect("round count"));
    for round in 0..rounds {
        let ring = [1, 2, 3, 8, 127, 1024][round % 6];
        let push = [1, 2, 7, 31, 128][(round / 6) % 5];
        let credit = [1, 3, 5, 32][(round / 30) % 4];
        sweep(ring, push, credit).await;
    }
    eprintln!(
        "ring sweep: {rounds} iterations, 129 cursors each, rings 1/2/3/8/127/1024, pushes 1/2/7/31/128, credit 1/3/5/32"
    );
}

async fn sweep(ring: usize, push: usize, credit: u32) {
    let lug = Harness::with(
        |c| {
            c.ring = ring;
            c.limits.push = push;
        },
        4096,
    )
    .await;
    let mut writer = lug.with_log("seam", false).await;
    assert_eq!(
        versions(
            &writer
                .append(2, "seam", (1..=128).map(|n| json!(n)).collect())
                .await
        ),
        (1..=128).collect::<Vec<_>>()
    );
    let gate = Arc::new(Barrier::new(130));
    let mut readers = Vec::new();
    for from in 0..=128 {
        let socket = lug.server().socket().to_owned();
        let gate = gate.clone();
        readers.push(tokio::spawn(async move {
            let mut client = Client::connect(&socket).await;
            gate.wait().await;
            client
                .send(Request::Subscribe {
                    id: 9,
                    log: "seam".into(),
                    from,
                    mode: Mode::Records,
                    credit,
                })
                .await;
            let mut cursor = from;
            let mut drained = 0;
            while cursor < 512 {
                match client.recv().await {
                    Response::Ok { id: 9 } => {}
                    Response::Records { id: 9, records } => {
                        for record in records {
                            assert_eq!(record.version, cursor + 1, "subscriber from {from}");
                            assert_eq!(record.patch, json!(record.version));
                            cursor = record.version;
                            drained += 1;
                        }
                        if drained >= credit {
                            client
                                .send(Request::Credit {
                                    id: 9,
                                    grant: drained,
                                })
                                .await;
                            drained = 0;
                        }
                    }
                    other => panic!("subscriber from {from} at {cursor}: {other:?}"),
                }
            }
        }));
    }
    gate.wait().await;
    for n in 129..=512 {
        assert_eq!(
            versions(&writer.append(n, "seam", vec![json!(n)]).await),
            vec![n]
        );
    }
    for reader in readers {
        reader.await.expect("subscriber");
    }
    drop(writer);
    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rejected_group_members_do_not_disturb_other_connections_versions() {
    let rounds = std::env::var("LUG_HAMMER_ROUNDS").map_or(1, |n| n.parse().expect("round count"));
    for round in 0..rounds {
        isolation(round % 2 == 1).await;
    }
    eprintln!(
        "isolation: {rounds} iterations, 16 connections, 512 requests each, alternating rejection and rejected tail"
    );
}

async fn isolation(rejected_tail: bool) {
    let succeeds = move |round| {
        if rejected_tail {
            round < 16
        } else {
            round % 2 == 0
        }
    };
    let lug = Harness::with(|c| c.ring = 2, 4096).await;
    lug.with_log("isolate", true).await;
    let mut writers = Vec::new();
    for connection in 0..16 {
        let socket = lug.server().socket().to_owned();
        writers.push(tokio::spawn(async move {
            let mut client = Client::connect(&socket).await;
            for round in 0..32 {
                let key = format!("k{connection}_{round}");
                let patches = if succeeds(round) {
                    vec![json!({ "Create": { key: round } })]
                } else {
                    vec![
                        json!({ "Create": { key: round } }),
                        json!({ "Update": { "absent": 1 } }),
                    ]
                };
                client
                    .send(Request::Append {
                        id: round,
                        log: "isolate".into(),
                        patches,
                        durability: Durability::Durable,
                    })
                    .await;
            }
            let mut minted = Vec::new();
            for round in 0..32 {
                match client.recv().await {
                    Response::Ack { id, versions, .. } if succeeds(round) => {
                        assert_eq!(id, round);
                        assert_eq!(versions.len(), 1);
                        minted.extend(versions);
                    }
                    Response::Error {
                        id,
                        code: lug_proto::Code::Rejected,
                        ..
                    } if !succeeds(round) => assert_eq!(id, round),
                    other => panic!("connection {connection}, round {round}: {other:?}"),
                }
            }
            minted
        }));
    }
    let mut minted = Vec::new();
    for writer in writers {
        minted.extend(writer.await.expect("writer"));
    }
    minted.sort_unstable();
    assert_eq!(minted, (1..=256).collect::<Vec<_>>());
    let mut reader = lug.client().await;
    reader
        .send(Request::Subscribe {
            id: 1,
            log: "isolate".into(),
            from: 0,
            mode: Mode::Records,
            credit: 1024,
        })
        .await;
    let mut cursor = 0;
    while cursor < 256 {
        match reader.recv().await {
            Response::Ok { .. } => {}
            Response::Records { records, .. } => {
                for record in records {
                    assert_eq!(record.version, cursor + 1);
                    let fields = record.patch["Create"].as_object().unwrap();
                    assert!(succeeds(fields.values().next().unwrap().as_u64().unwrap()));
                    cursor = record.version;
                }
            }
            other => panic!("isolation stream: {other:?}"),
        }
    }
    drop(reader);
    lug.stop().await;
}
