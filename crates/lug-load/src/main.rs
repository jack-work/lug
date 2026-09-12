mod check;
mod config;
mod http;
mod measure;
#[cfg(test)]
mod mock;
mod run;
mod server;
mod wire;

use config::{Config, Transport};
use lug_proto::Durability;
use serde_json::{Value, json};
use std::{process::ExitCode, time::Instant};

pub type Result<T> = anyhow::Result<T>;
pub fn error(message: impl Into<String>) -> anyhow::Error {
    anyhow::anyhow!(message.into())
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print!("{}", config::HELP);
        return ExitCode::SUCCESS;
    }
    if args.iter().any(|arg| arg == "--version") {
        println!("lug-load {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let cfg = match Config::parse(args) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("lug-load: {e}\nTry lug-load --help");
            return ExitCode::from(2);
        }
    };
    match execute(&cfg).await {
        Ok(Some(report)) => {
            report_output(&report, cfg.json, cfg.smoke);
            ExitCode::SUCCESS
        }
        Ok(None) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("lug-load: FAIL: {e:#}");
            if cfg.json {
                println!(
                    "{}",
                    json!({"schema": 1, "status": "failed", "error": format!("{e:#}")})
                );
            }
            if e.to_string() == "interrupted by SIGINT" {
                ExitCode::from(130)
            } else if e.to_string() == "interrupted by SIGTERM" {
                ExitCode::from(143)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}

async fn execute(cfg: &Config) -> Result<Option<Value>> {
    let limits = server::raise_nofile()?;
    eprintln!(
        "lug-load: RLIMIT_NOFILE soft {} -> {}, hard {} (child inherits it)",
        limits.before, limits.soft, limits.hard
    );
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let started = tokio::select! {
        result = server::Server::start(cfg) => result?,
        _ = interrupt.recv() => return Err(error("interrupted by SIGINT")),
        _ = terminate.recv() => return Err(error("interrupted by SIGTERM")),
    };
    let Some(mut child) = started else {
        let reason = format!(
            "server executable {} is missing; no load or conformance measurements were made",
            cfg.server.display()
        );
        eprintln!("lug-load: SKIP: {reason}");
        if cfg.json {
            println!(
                "{}",
                json!({"schema": 1, "status": "skipped", "reason": reason, "rlimit_nofile": {"before": limits.before, "soft": limits.soft, "hard": limits.hard}})
            );
        } else {
            println!("status  skipped\nreason  {reason}");
        }
        return Ok(None);
    };
    let outcome = tokio::select! {
        result = benchmark(&mut child, cfg, &limits) => result,
        _ = interrupt.recv() => Err(error("interrupted by SIGINT")),
        _ = terminate.recv() => Err(error("interrupted by SIGTERM")),
    };
    let cleanup = child.shutdown().await;
    if let Err(e) = &cleanup {
        eprintln!("lug-load: cleanup: {e:#}");
    }
    let report = outcome?;
    cleanup?;
    Ok(Some(report))
}

async fn benchmark(
    child: &mut server::Server,
    cfg: &Config,
    limits: &server::Limits,
) -> Result<Value> {
    let (_, cold_rss) = child.quiescent().await?;
    run::backpressure(&child.endpoint, cfg).await?;
    let (accept_count, accept_time) = run::accepts(&child.endpoint, cfg).await?;
    let accept_rate = if accept_count == 0 {
        None
    } else {
        Some(accept_count as f64 / accept_time.as_secs_f64())
    };
    let epochs = if cfg.soak { cfg.soak_cycles } else { 1 };
    let mut reports = Vec::new();
    for epoch in 0..epochs {
        let names: Vec<_> = (0..cfg.logs)
            .map(|log| format!("load-{epoch}-{log}"))
            .collect();
        {
            let mut setup = run::Pool::open(&child.endpoint, 1, 1, cfg.timeout).await?;
            setup.create(&names, cfg.timeout).await?;
        }
        let (_, baseline_rss) = child.quiescent().await?;
        let start = Instant::now();
        let mut pool =
            run::Pool::open(&child.endpoint, cfg.connections, cfg.parallel, cfg.timeout).await?;
        let establish_seconds = start.elapsed().as_secs_f64();
        let held_rss = server::rss(child.pid()?)?;
        eprintln!(
            "lug-load: epoch {epoch}: {} connections established in {establish_seconds:.3}s",
            pool.peers.len()
        );
        let mut loaded = run::load(&mut pool, cfg, &names, Some(child.pid()?)).await?;
        loaded.metrics.rss_peak = loaded.metrics.rss_peak.max(held_rss);
        run::cancel(&mut pool, &mut loaded, cfg.timeout).await?;
        pool.heartbeat(cfg.timeout).await?;
        let held_sockets = cfg.connections
            + if cfg.transport == Transport::Http {
                cfg.logs * cfg.subscribers
            } else {
                0
            };
        let measured = loaded.metrics.json(cfg.duration);
        drop(pool);
        let (fd_baseline, _) = child.quiescent().await?;
        let mut fd_after = fd_baseline;
        if cfg.soak {
            // New logs and WAL segments legitimately keep descriptors; only connection churn uses this fixed baseline.
            for _ in 0..4 {
                run::churn(&child.endpoint, cfg, &names).await?;
                fd_after = child.quiescent().await?.0;
                measure::check_fd_growth(fd_baseline, fd_after, cfg.fd_slack)?;
            }
        }
        run::crash_probe(child, cfg, epoch).await?;
        let replayed = cfg.durability == Durability::Durable;
        if replayed {
            run::replay(&child.endpoint, cfg, &names, &mut loaded.ledger).await?;
        }
        reports.push(json!({
            "epoch": epoch,
            "held_connections": cfg.connections,
            "held_transport_sockets_including_sse": held_sockets,
            "held_connections_answered_final_ping": cfg.connections,
            "establishment_seconds": establish_seconds,
            "establishment_handshakes_per_sec": cfg.connections as f64 / establish_seconds,
            "server_rss_warm_baseline_bytes": baseline_rss,
            "server_rss_connections_only_bytes": held_rss,
            "server_rss_incremental_per_call_socket_estimate_bytes": held_rss.saturating_sub(baseline_rss) as f64 / cfg.connections as f64,
            "measurement": measured,
            "contiguous_exact_subscriber_order": "passed",
            "durable_workload_replayed_after_sigkill": replayed,
            "durable_crash_probe": "passed",
            "fd_churn_checked": cfg.soak,
            "fd_quiescent_baseline": fd_baseline,
            "fd_quiescent_after_churn": fd_after,
        }));
        eprintln!(
            "lug-load: epoch {epoch}: PASS exact delivery to every subscriber{}",
            if replayed {
                " and durable replay after SIGKILL"
            } else {
                " (weak-durability workload recovery not asserted)"
            }
        );
    }
    Ok(json!({
        "schema": 1, "status": "passed",
        "mode": if cfg.smoke { "smoke" } else if cfg.soak { "soak" } else { "load" },
        "transport": if cfg.transport == Transport::Unix { "unix" } else { "http" },
        "single_unix_listener": true,
        "rlimit_nofile": {"before": limits.before, "soft": limits.soft, "hard": limits.hard},
        "workload": {"logs": cfg.logs, "appenders_per_log": cfg.appenders, "subscribers_per_log": cfg.subscribers,
            "patch_size_bytes": cfg.patch_size, "durability": cfg.durability, "target_appends_per_sec": cfg.rate,
            "duration_seconds": cfg.duration.as_secs_f64(), "soak": cfg.soak},
        "short_lived_connections": accept_count,
        "accepts_per_sec": accept_rate,
        "accept_seconds": accept_time.as_secs_f64(),
        "accept_parallelism": cfg.parallel,
        "server_rss_cold_bytes": cold_rss,
        "credit_isolation": "passed",
        "epochs": reports,
    }))
}

fn report_output(report: &Value, as_json: bool, smoke: bool) {
    if as_json {
        println!("{report}");
        return;
    }
    if smoke {
        let measured = &report["epochs"][0]["measurement"];
        println!("PASS smoke: one log, one appender, one subscriber");
        println!(
            "{} appends acknowledged, {} exact subscriber deliveries",
            measured["acknowledged_appends"], measured["subscriber_records"]
        );
        println!("Credit isolation and durable SIGKILL/restart replay passed");
        println!(
            "Append latency: p50={} us, p99={} us",
            measured["append_latency"]["p50_us"], measured["append_latency"]["p99_us"]
        );
        return;
    }
    println!("{:<48} value", "metric");
    println!("{:<48} {}", "status", report["status"]);
    println!(
        "{:<48} {}",
        "short-lived accepts/sec (Hello completed)", report["accepts_per_sec"]
    );
    println!(
        "{:<48} {}",
        "server RSS cold fixed baseline (bytes)", report["server_rss_cold_bytes"]
    );
    if let Some(epochs) = report["epochs"].as_array() {
        for epoch in epochs {
            println!("{:<48} {}", "epoch", epoch["epoch"]);
            for (label, key) in [
                ("held connections (final Ping checked)", "held_connections"),
                (
                    "held sockets including HTTP SSE",
                    "held_transport_sockets_including_sse",
                ),
                ("establishment seconds", "establishment_seconds"),
                (
                    "RSS warm fixed baseline (bytes)",
                    "server_rss_warm_baseline_bytes",
                ),
                (
                    "RSS with call sockets, before load (bytes)",
                    "server_rss_connections_only_bytes",
                ),
                (
                    "RSS incremental per call socket (estimate)",
                    "server_rss_incremental_per_call_socket_estimate_bytes",
                ),
                ("fd baseline after warmup", "fd_quiescent_baseline"),
                (
                    "fd after connection/subscription churn",
                    "fd_quiescent_after_churn",
                ),
            ] {
                println!("{label:<48} {}", epoch[key]);
            }
            let m = &epoch["measurement"];
            for (label, key) in [
                ("appends/sec including drain", "appends_per_sec"),
                (
                    "appends/sec inside scheduling window",
                    "window_appends_per_sec",
                ),
                (
                    "subscriber records/sec including drain",
                    "subscriber_records_per_sec",
                ),
                ("wire bytes/sec (tx + rx)", "bytes_per_sec"),
                (
                    "RSS sampled load peak, total bytes",
                    "server_rss_peak_bytes",
                ),
                (
                    "elapsed seconds including drain",
                    "elapsed_seconds_including_drain",
                ),
            ] {
                println!("{label:<48} {}", m[key]);
            }
            for (label, key) in [
                ("append latency us", "append_latency"),
                (
                    "publish-to-subscriber latency us",
                    "publish_to_subscriber_latency",
                ),
            ] {
                println!(
                    "{label:<48} p50={} p99={} p999={}",
                    m[key]["p50_us"], m[key]["p99_us"], m[key]["p999_us"]
                );
            }
        }
    }
    println!(
        "{:<48} passed",
        "credit isolation and exact-delivery assertions"
    );
}
