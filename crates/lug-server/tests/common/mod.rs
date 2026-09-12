#![allow(dead_code)] // each test binary uses a different slice of this helper

//! A real client over the real socket. Nothing here mocks the daemon.

use futures::{SinkExt, StreamExt};
use lug_proto::{Codec, Id, Request, Response};
use lug_server::config::{Config, Limits};
use lug_server::{MemoryFactory, Segments, Server};
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio_util::codec::Framed;

pub const PATIENCE: Duration = Duration::from_secs(5);

pub struct Harness {
    pub server: Option<Server>,
    pub dir: tempfile::TempDir,
    pub token: String,
}

impl Harness {
    pub async fn start() -> Self {
        Self::with(|_| {}, 1 << 16).await
    }

    /// `retain` is how many records the in-memory storage keeps, which is what
    /// makes retention and Gap reachable from a test.
    pub async fn with(tweak: impl FnOnce(&mut Config), retain: usize) -> Self {
        let (mut harness, config) = Self::configured(tweak);
        harness.server =
            Some(Server::start(config, MemoryFactory::new(retain)).await.expect("server starts"));
        harness
    }

    fn configured(tweak: impl FnOnce(&mut Config)) -> (Self, Config) {
        let dir = tempfile::tempdir().expect("tempdir");
        let token = "s3cr3t-not-logged".to_string();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, format!("{token}\n")).expect("token file");

        let mut config = Config {
            data: dir.path().join("data"),
            run: dir.path().join("run"),
            socket: PathBuf::from("lug.sock"),
            token: Some(token_path),
            cores: Some(2),
            limits: Limits { outbox: 64, ..Limits::default() },
            ..Config::default()
        };
        tweak(&mut config);
        (Self { server: None, dir, token: token.clone() }, config)
    }

    /// The daemon on segment files, where a sync is a real fdatasync.
    pub async fn durable() -> Self {
        Self::durable_with(|_| {}).await
    }

    /// Segment files with the config tweaked, for the retention behaviour that
    /// only the real storage has: a read below `oldest` is refused there, where
    /// [`MemoryFactory`] quietly serves what it still holds.
    pub async fn durable_with(tweak: impl FnOnce(&mut Config)) -> Self {
        let (mut harness, config) = Self::configured(tweak);
        let storage = Segments::new(config.data.clone(), config.segment.0);
        harness.server =
            Some(Server::start(config, storage).await.expect("server starts"));
        harness
    }

    pub fn server(&self) -> &Server {
        self.server.as_ref().expect("server running")
    }

    pub async fn client(&self) -> Client {
        Client::connect(self.server().socket()).await
    }

    /// A connected client with the log created and a handle to its metrics.
    pub async fn with_log(&self, name: &str, reducible: bool) -> Client {
        let mut client = self.client().await;
        client.call(Request::Create { id: 1, log: name.into(), reducible }).await;
        client
    }

    pub fn metrics(&self, log: &str) -> (u64, u64, u64) {
        self.server().logs().get(log).expect("log exists").shared.metrics.snapshot()
    }

    pub async fn stop(mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown().await.expect("clean shutdown");
        }
    }
}

pub struct Client {
    framed: Framed<UnixStream, Codec<Response, Request>>,
}

impl Client {
    pub async fn connect(socket: &std::path::Path) -> Self {
        let stream = UnixStream::connect(socket).await.expect("connect");
        Self { framed: Framed::new(stream, Codec::new()) }
    }

    pub async fn send(&mut self, request: Request) {
        self.framed.send(request).await.expect("send");
    }

    pub async fn recv(&mut self) -> Response {
        self.try_recv().await.expect("a frame")
    }

    pub async fn try_recv(&mut self) -> Option<Response> {
        match tokio::time::timeout(PATIENCE, self.framed.next()).await {
            Ok(Some(Ok(response))) => Some(response),
            Ok(Some(Err(e))) => panic!("decode: {e}"),
            Ok(None) => None,
            Err(_) => None,
        }
    }

    /// Nothing should arrive within `patience`.
    pub async fn quiet(&mut self, patience: Duration) -> Option<Response> {
        match tokio::time::timeout(patience, self.framed.next()).await {
            Ok(Some(Ok(response))) => Some(response),
            _ => None,
        }
    }

    pub async fn call(&mut self, request: Request) -> Response {
        self.send(request).await;
        self.recv().await
    }

    /// Next frame carrying this id, skipping frames for other streams.
    pub async fn recv_for(&mut self, id: Id) -> Response {
        loop {
            let response = self.recv().await;
            if response.id() == id {
                return response;
            }
        }
    }

    pub async fn append(&mut self, id: Id, log: &str, patches: Vec<serde_json::Value>) -> Response {
        self.call(Request::Append {
            id,
            log: log.into(),
            patches,
            durability: lug_proto::Durability::Durable,
        })
        .await
    }

    pub fn inner(&mut self) -> &mut Framed<UnixStream, Codec<Response, Request>> {
        &mut self.framed
    }
}

pub fn versions(response: &Response) -> Vec<u64> {
    match response {
        Response::Ack { versions, .. } => versions.clone(),
        other => panic!("expected Ack, got {other:?}"),
    }
}

pub fn records(response: &Response) -> Vec<u64> {
    match response {
        Response::Records { records, .. } => records.iter().map(|r| r.version).collect(),
        other => panic!("expected Records, got {other:?}"),
    }
}
