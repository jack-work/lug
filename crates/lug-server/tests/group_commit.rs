//! Group commit, measured on the real socket.
//!
//! Run it verbose to see the numbers:
//! `cargo test -p lug-server --test group_commit -- --nocapture`

mod common;

use common::{Client, Harness};
use lug_proto::{Durability, Request, Response};
use serde_json::json;
use std::time::Instant;

fn patches(n: usize) -> Vec<serde_json::Value> {
    (0..n).map(|i| json!({ "n": i, "pad": "0123456789abcdef" })).collect()
}

/// Idle, the same code path costs exactly one write and one sync, with no
/// timer holding the record back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_append_is_one_batch_and_one_sync() {
    let lug = Harness::start().await;
    let mut client = lug.with_log("quiet", false).await;

    let started = Instant::now();
    client.append(2, "quiet", patches(1)).await;
    let latency = started.elapsed();

    let (folded, batches, syncs) = lug.metrics("quiet");
    assert_eq!((folded, batches, syncs), (1, 1, 1));
    println!("idle append: {latency:?}, 1 batch, 1 sync");

    lug.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn group_commit_amortizes_the_sync_under_load() {
    let lug = Harness::start().await;
    lug.with_log("load", false).await;

    let connections = 32;
    let rounds = 100;
    let per_append = 4;
    let total = connections * rounds * per_append;

    let started = Instant::now();
    let mut workers = Vec::new();
    for connection in 0..connections {
        let socket = lug.server().socket().to_path_buf();
        workers.push(tokio::spawn(async move {
            let mut client = Client::connect(&socket).await;
            // Pipelined: the whole point is that requests queue up behind the
            // actor's mailbox and ride out together.
            for round in 0..rounds {
                client
                    .send(Request::Append {
                        id: (connection * rounds + round) as u64,
                        log: "load".into(),
                        patches: patches(per_append),
                        durability: Durability::Durable,
                    })
                    .await;
            }
            let mut acked = 0;
            while acked < rounds {
                match client.recv().await {
                    Response::Ack { versions, .. } => {
                        assert_eq!(versions.len(), per_append);
                        acked += 1;
                    }
                    other => panic!("expected Ack, got {other:?}"),
                }
            }
        }));
    }
    for worker in workers {
        worker.await.expect("worker");
    }
    let elapsed = started.elapsed();

    let (folded, batches, syncs) = lug.metrics("load");
    assert_eq!(folded as usize, total);
    let per_sync = folded as f64 / syncs as f64;
    let syncs_per_append = syncs as f64 / folded as f64;
    println!(
        "{total} patches over {connections} connections in {elapsed:?}: \
         {batches} batches, {syncs} syncs, {per_sync:.1} patches per sync, \
         {syncs_per_append:.4} syncs per append, \
         {:.0} patches/s",
        total as f64 / elapsed.as_secs_f64()
    );

    // Every patch paying for its own sync would mean no amortization at all.
    assert!(
        syncs_per_append < 0.5,
        "group commit did not amortize: {syncs} syncs for {folded} patches"
    );

    lug.stop().await;
}
