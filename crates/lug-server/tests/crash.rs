//! The daemon as a process: started from a config file, killed uncleanly,
//! and expected to come back with every acknowledged append intact.

mod common;

use common::Client;
use lug_proto::{Durability, Mode, Request, Response};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Daemon {
    child: Child,
}

impl Daemon {
    fn start(config: &Path, socket: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_lug-server"))
            .arg("--config")
            .arg(config)
            .spawn()
            .expect("spawn lug-server");
        let daemon = Self { child };
        // A stale socket file survives a SIGKILL, so wait for one that
        // actually answers rather than for the path to exist.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if std::os::unix::net::UnixStream::connect(socket).is_ok() {
                return daemon;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("no daemon answering at {socket:?}");
    }

    fn kill_hard(&mut self) {
        // No SIGTERM: nothing gets to checkpoint or flush on the way out.
        let _ = Command::new("kill").arg("-9").arg(self.child.id().to_string()).status();
        let _ = self.child.wait();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_config(dir: &Path) -> (PathBuf, PathBuf) {
    config_with(dir, 10_000)
}

fn config_with(dir: &Path, checkpoint_every: u64) -> (PathBuf, PathBuf) {
    let config = dir.join("lug.toml");
    let run = dir.join("run");
    std::fs::write(
        &config,
        format!(
            "data = {:?}\nrun = {:?}\nsocket = \"lug.sock\"\nsegment = \"4MiB\"\n\
             ring = 1024\ncheckpoint_every = {checkpoint_every}\n",
            dir.join("data"),
            run,
        ),
    )
    .expect("config");
    (config, run.join("lug.sock"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_then_restart_recovers_every_acknowledged_append() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (config, socket) = write_config(dir.path());

    let mut daemon = Daemon::start(&config, &socket);
    let mut client = Client::connect(&socket).await;
    client.call(Request::Create { id: 1, log: "notes".into(), reducible: true }).await;
    client.call(Request::Create { id: 2, log: "events".into(), reducible: false }).await;

    let patches = vec![
        json!({ "Create": { "title": "lug", "tags": [] } }),
        json!({ "Create": { "author": { "name": "Gluck" } } }),
        json!({ "Update": { "title": "lug: a little log" } }),
    ];
    let ack = client
        .call(Request::Append {
            id: 3,
            log: "notes".into(),
            patches: patches.clone(),
            durability: Durability::Durable,
        })
        .await;
    assert_eq!(common::versions(&ack), vec![1, 2, 3]);
    let ack = client
        .call(Request::Append {
            id: 4,
            log: "events".into(),
            patches: (0..5).map(|n| json!({ "tick": n })).collect(),
            durability: Durability::Durable,
        })
        .await;
    assert_eq!(common::versions(&ack), vec![1, 2, 3, 4, 5]);

    drop(client);
    daemon.kill_hard();
    assert!(!socket.exists() || std::fs::metadata(&socket).is_ok());

    let _restarted = Daemon::start(&config, &socket);
    let mut client = Client::connect(&socket).await;

    match client.call(Request::List { id: 1 }).await {
        Response::Logs { logs, .. } => {
            let mut names: Vec<_> = logs.iter().map(|l| l.name.clone()).collect();
            names.sort();
            assert_eq!(names, vec!["events".to_string(), "notes".to_string()]);
            let notes = logs.iter().find(|l| l.name == "notes").expect("notes");
            assert!(notes.reducible);
            assert_eq!(notes.version, 3);
        }
        other => panic!("expected Logs, got {other:?}"),
    }

    match client.call(Request::Read { id: 2, log: "notes".into(), at: None }).await {
        Response::View { version, value, .. } => {
            assert_eq!(version, 3);
            assert_eq!(value["root"]["title"], json!("lug: a little log"));
            assert_eq!(value["root"]["author"]["name"], json!("Gluck"));
        }
        other => panic!("expected View, got {other:?}"),
    }

    // The record stream survived too, not just the folded view.
    client
        .send(Request::Subscribe {
            id: 3,
            log: "events".into(),
            from: 0,
            mode: Mode::Records,
            credit: 100,
        })
        .await;
    assert!(matches!(client.recv().await, Response::Ok { id: 3 }));
    assert_eq!(common::records(&client.recv().await), vec![1, 2, 3, 4, 5]);

    // Appends continue from where recovery left off. The live subscription
    // above is pushing on the same connection, so pick the Ack out by id.
    client
        .send(Request::Append {
            id: 4,
            log: "events".into(),
            patches: vec![json!({ "tick": 5 })],
            durability: Durability::Durable,
        })
        .await;
    assert_eq!(common::versions(&client.recv_for(4).await), vec![6]);
}

#[test]
fn check_validates_the_config_and_exits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (config, _) = write_config(dir.path());
    let status = Command::new(env!("CARGO_BIN_EXE_lug-server"))
        .arg("--config")
        .arg(&config)
        .arg("--check")
        .status()
        .expect("run");
    assert!(status.success());

    // An http address that is not loopback must be refused, not bound.
    std::fs::write(
        &config,
        format!(
            "data = {:?}\nrun = {:?}\nhttp = \"8.8.8.8:7717\"\n",
            dir.path().join("data"),
            dir.path().join("run")
        ),
    )
    .expect("config");
    let status = Command::new(env!("CARGO_BIN_EXE_lug-server"))
        .arg("--config")
        .arg(&config)
        .arg("--check")
        .status()
        .expect("run");
    assert!(!status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_unlinks_the_socket() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (config, socket) = write_config(dir.path());
    let mut daemon = Daemon::start(&config, &socket);

    let mut client = Client::connect(&socket).await;
    client.call(Request::Create { id: 1, log: "bye".into(), reducible: false }).await;
    drop(client);

    let _ = Command::new("kill").arg(daemon.child.id().to_string()).status();
    let deadline = Instant::now() + Duration::from_secs(10);
    while socket.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(!socket.exists(), "SIGTERM should unlink the socket");
    let _ = daemon.child.wait();
}

/// Recovery has to agree with the checkpoints taken along the way: the header
/// covers the versions below it and the records above it are replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoints_taken_along_the_way_recover_to_the_same_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (config, socket) = config_with(dir.path(), 5);

    let mut daemon = Daemon::start(&config, &socket);
    let mut client = Client::connect(&socket).await;
    client.call(Request::Create { id: 1, log: "counted".into(), reducible: true }).await;
    for round in 0..23u64 {
        let ack = client
            .call(Request::Append {
                id: 100 + round,
                log: "counted".into(),
                patches: vec![json!({ "Create": { format!("k{round}"): round } })],
                durability: Durability::Written,
            })
            .await;
        assert_eq!(common::versions(&ack), vec![round + 1]);
    }
    drop(client);
    daemon.kill_hard();

    let _restarted = Daemon::start(&config, &socket);
    let mut client = Client::connect(&socket).await;
    match client.call(Request::Read { id: 1, log: "counted".into(), at: None }).await {
        Response::View { version, value, .. } => {
            assert_eq!(version, 23);
            assert_eq!(value["root"]["k0"], json!(0));
            assert_eq!(value["root"]["k22"], json!(22));
        }
        other => panic!("expected View, got {other:?}"),
    }
}
