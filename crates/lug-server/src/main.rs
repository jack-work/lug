//! `lug-server`: foreground, logs to stderr, never forks. The supervisor owns
//! the process.

use lug_server::config::Config;
use lug_server::{Segments, Server};
use tokio::signal::unix::{SignalKind, signal};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LUG_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let (config, check) = match Config::from_args(std::env::args().skip(1)) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("lug-server: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = config.validate() {
        eprintln!("lug-server: {e}");
        std::process::exit(2);
    }
    if check {
        println!("ok: socket {:?}, data {:?}", config.socket_path(), config.data);
        return Ok(());
    }

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run(config))
}

async fn run(config: Config) -> anyhow::Result<()> {
    let storage = Segments::new(config.data.clone(), config.segment.0);
    let server = Server::start(config, storage).await?;
    tracing::info!(socket = ?server.socket(), http = ?server.http(), "lug-server ready");

    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = term.recv() => tracing::info!("SIGTERM"),
        _ = int.recv() => tracing::info!("SIGINT"),
    }
    server.shutdown().await?;
    tracing::info!("stopped");
    Ok(())
}
