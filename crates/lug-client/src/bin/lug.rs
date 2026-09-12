//! `lug`: drive a lug daemon from a shell.
//!
//! Built to live in a pipe. Record streams are JSON Lines, flushed per line,
//! so `lug tail notes | jq` behaves. Diagnostics go to stderr, data to
//! stdout, colour only when a person is watching.

use clap::{ArgAction, Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use lug_client::{Durability, Error, Event, Hub, Subscribe, Transport, Version};
use serde_json::Value;
use std::io::{BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

/// 0 success, 1 failure, 2 misuse. Clap already exits 2 on a bad flag.
const FAILURE: u8 = 1;

#[derive(Parser)]
#[command(
    name = "lug",
    version,
    about = "Talk to a lug daemon",
    after_help = "\
Examples:
  lug ls
  lug create notes --reducible
  echo '{\"Create\":{\"title\":\"lug\"}}' | lug append notes -
  lug read notes
  lug tail notes --from 0 --follow=false | jq

Exit codes: 0 ok, 1 failed, 2 bad usage.",
    disable_help_subcommand = false
)]
struct Cli {
    #[command(flatten)]
    at: Endpoint,

    /// Machine-readable output where there is a human format.
    #[arg(long, global = true)]
    json: bool,

    #[arg(long, global = true, value_name = "WHEN", default_value = "auto")]
    color: Color,

    /// Seconds to wait on any single call. Streaming is not bounded by it.
    #[arg(long, global = true, value_name = "SECS", default_value_t = 10)]
    timeout: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct Endpoint {
    /// Unix socket to connect to.
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "LUG_SOCKET",
        default_value = "/run/lug/lug.sock"
    )]
    socket: PathBuf,

    /// Loopback HTTP origin instead of the socket, such as http://127.0.0.1:7717.
    #[arg(long, global = true, value_name = "URL", env = "LUG_HTTP")]
    http: Option<String>,

    /// File holding the bearer token. A token in argv is visible in `ps` to
    /// everyone on the box, so it is not accepted there.
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        env = "LUG_TOKEN_FILE",
        requires = "http"
    )]
    token_file: Option<PathBuf>,
}

impl Endpoint {
    fn transport(&self) -> Result<Transport, Fail> {
        match &self.http {
            Some(base) => {
                let token = match &self.token_file {
                    Some(path) => std::fs::read_to_string(path)
                        .map_err(|e| {
                            Fail::new(format!(
                                "cannot read the token file {}: {e}",
                                path.display()
                            ))
                        })?
                        .trim()
                        .to_string(),
                    None => String::new(),
                };
                Ok(Transport::http(base, token))
            }
            None => Ok(Transport::unix(&self.socket)),
        }
    }

    fn describe(&self) -> String {
        match &self.http {
            Some(base) => base.clone(),
            None => self.socket.display().to_string(),
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Color {
    Auto,
    Always,
    Never,
}

#[derive(Subcommand)]
enum Command {
    /// List logs.
    #[command(alias = "list")]
    Ls,

    /// Create a log. Succeeds if it already exists with the same shape.
    Create {
        log: String,
        /// Fold patches into a materialized view, rather than keeping a plain
        /// record stream.
        #[arg(long)]
        reducible: bool,
    },

    /// Append patches read as JSON, one per line.
    Append {
        log: String,
        /// `-` or omitted reads stdin.
        #[arg(value_name = "FILE")]
        file: Option<String>,
        /// How far an append must get before it is acknowledged.
        #[arg(long, value_enum, default_value = "written")]
        durability: Sync_,
    },

    /// Stream records as JSON Lines.
    Tail {
        log: String,
        /// Exclusive cursor. 0 replays everything retained.
        #[arg(long, default_value_t = 0)]
        from: Version,
        /// Keep streaming after catching up. `--follow=false` prints the
        /// history and exits.
        #[arg(
            long,
            num_args = 0..=1,
            action = ArgAction::Set,
            default_value_t = true,
            default_missing_value = "true"
        )]
        follow: bool,
        /// Records the server may push before waiting for more credit.
        #[arg(long, default_value_t = 256)]
        credit: u32,
    },

    /// Print a materialized view.
    Read {
        log: String,
        /// Read a past version instead of the current one.
        #[arg(long, value_name = "N")]
        at: Option<Version>,
    },

    /// Print the materialized view again every time it changes.
    Follow { log: String },

    /// Check that the daemon is answering.
    Ping,
}

#[derive(Clone, Copy, ValueEnum)]
#[value(rename_all = "lower")]
enum Sync_ {
    Memory,
    Written,
    Durable,
}

impl From<Sync_> for Durability {
    fn from(value: Sync_) -> Self {
        match value {
            Sync_::Memory => Self::Memory,
            Sync_::Written => Self::Written,
            Sync_::Durable => Self::Durable,
        }
    }
}

/// A failure with something useful to say about it.
struct Fail {
    message: String,
}

impl Fail {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("lug: cannot start the runtime: {e}");
            return ExitCode::from(FAILURE);
        }
    };
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(fail) => {
            eprintln!("lug: {}", fail.message);
            ExitCode::from(FAILURE)
        }
    }
}

async fn run(cli: Cli) -> Result<(), Fail> {
    let transport = cli.at.transport()?;
    let hub = Hub::builder(transport)
        .timeout(Duration::from_secs(cli.timeout.max(1)))
        // A command is one short conversation; a pool of one keeps startup to
        // a single connection.
        .connections(1)
        .connect()
        .await
        .map_err(|e| match e {
            Error::Connect { .. } => Fail::new(format!(
                "no daemon at {}: {e}\n      is lug-server running? try --socket <path> or --http <url>",
                cli.at.describe()
            )),
            other => Fail::new(other.to_string()),
        })?;

    let out = Output {
        json: cli.json,
        color: paint(cli.color),
    };
    match cli.command {
        Command::Ls => ls(&hub, &out).await,
        Command::Create { log, reducible } => create(&hub, &out, &log, reducible).await,
        Command::Append {
            log,
            file,
            durability,
        } => append(&hub, &out, &log, file.as_deref(), durability.into()).await,
        Command::Tail {
            log,
            from,
            follow,
            credit,
        } => tail(&hub, &log, from, follow, credit).await,
        Command::Read { log, at } => read(&hub, &out, &log, at).await,
        Command::Follow { log } => follow_view(&hub, &out, &log).await,
        Command::Ping => {
            hub.ping().await.map_err(failed("ping"))?;
            out.line("pong")
        }
    }
}

struct Output {
    json: bool,
    color: bool,
}

impl Output {
    fn line(&self, text: impl AsRef<str>) -> Result<(), Fail> {
        let mut stdout = std::io::stdout().lock();
        match writeln!(stdout, "{}", text.as_ref()).and_then(|()| stdout.flush()) {
            Ok(()) => Ok(()),
            // The reader went away, as `| head` does. Nothing to report.
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
            Err(e) => Err(Fail::new(format!("cannot write to stdout: {e}"))),
        }
    }

    fn bold(&self, text: &str) -> String {
        if self.color {
            format!("\x1b[1m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    /// Pretty for a person, compact for a pipe.
    fn value(&self, value: &Value) -> String {
        if self.color && !self.json {
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        } else {
            value.to_string()
        }
    }
}

fn paint(when: Color) -> bool {
    match when {
        Color::Always => true,
        Color::Never => false,
        Color::Auto => {
            std::io::stdout().is_terminal()
                && std::env::var_os("NO_COLOR").is_none()
                && std::env::var("TERM").map(|t| t != "dumb").unwrap_or(true)
        }
    }
}

fn failed(what: &'static str) -> impl Fn(Error) -> Fail {
    move |e| Fail::new(format!("{what} failed: {e}"))
}

async fn ls(hub: &Hub, out: &Output) -> Result<(), Fail> {
    let logs = hub.list().await.map_err(failed("ls"))?;
    if out.json {
        for log in &logs {
            out.line(serde_json::to_string(log).unwrap_or_default())?;
        }
        return Ok(());
    }
    if logs.is_empty() {
        return out.line("no logs");
    }
    let width = logs.iter().map(|l| l.name.len()).max().unwrap_or(4).max(4);
    out.line(out.bold(&format!(
        "{:<width$}  {:>9}  {:>7}  {:>6}  {:>4}",
        "NAME",
        "REDUCIBLE",
        "VERSION",
        "OLDEST",
        "SUBS",
        width = width
    )))?;
    for log in &logs {
        out.line(format!(
            "{:<width$}  {:>9}  {:>7}  {:>6}  {:>4}",
            log.name,
            log.reducible,
            log.version,
            log.oldest,
            log.subscribers,
            width = width
        ))?;
    }
    Ok(())
}

async fn create(hub: &Hub, out: &Output, log: &str, reducible: bool) -> Result<(), Fail> {
    let info = hub.create(log, reducible).await.map_err(failed("create"))?;
    if out.json {
        return out.line(serde_json::to_string(&info).unwrap_or_default());
    }
    let shape = if info.reducible { "reducible" } else { "plain" };
    out.line(format!(
        "created {} ({shape}, version {})",
        info.name, info.version
    ))
}

async fn append(
    hub: &Hub,
    out: &Output,
    log: &str,
    file: Option<&str>,
    durability: Durability,
) -> Result<(), Fail> {
    let patches = read_patches(file)?;
    if patches.is_empty() {
        return Err(Fail::new(
            "no patches on stdin; each line must be one JSON patch",
        ));
    }
    let count = patches.len();
    let ack = hub
        .append_with(log, patches, durability)
        .await
        .map_err(failed("append"))?;
    if out.json {
        return out.line(
            serde_json::json!({ "versions": ack.versions, "synced": ack.synced }).to_string(),
        );
    }
    match ack.versions.last() {
        Some(version) => out.line(format!(
            "appended {count} patch{} through version {version}",
            if count == 1 { "" } else { "es" }
        )),
        None => out.line(format!("appended {count}, nothing changed")),
    }
}

/// One JSON patch per line, from a file or stdin. `-` means stdin, and so
/// does saying nothing.
fn read_patches(file: Option<&str>) -> Result<Vec<Value>, Fail> {
    let source: Box<dyn BufRead> = match file {
        None | Some("-") => Box::new(std::io::stdin().lock()),
        Some(path) => {
            let file = std::fs::File::open(path)
                .map_err(|e| Fail::new(format!("cannot open {path}: {e}")))?;
            Box::new(std::io::BufReader::new(file))
        }
    };
    let mut patches = Vec::new();
    for (n, line) in source.lines().enumerate() {
        let line = line.map_err(|e| Fail::new(format!("cannot read input: {e}")))?;
        if line.trim().is_empty() {
            continue;
        }
        let patch = serde_json::from_str(&line)
            .map_err(|e| Fail::new(format!("line {}: not valid JSON: {e}", n + 1)))?;
        patches.push(patch);
    }
    Ok(patches)
}

async fn read(hub: &Hub, out: &Output, log: &str, at: Option<Version>) -> Result<(), Fail> {
    let view = hub.read(log, at).await.map_err(failed("read"))?;
    if out.json {
        return out
            .line(serde_json::json!({ "version": view.version, "value": view.value }).to_string());
    }
    let body = match view.snapshot() {
        Ok(snapshot) => snapshot.root().to_json(),
        // Not a shape the follower understands, so print what arrived.
        Err(_) => view.value.clone(),
    };
    out.line(out.value(&body))
}

async fn tail(hub: &Hub, log: &str, from: Version, follow: bool, credit: u32) -> Result<(), Fail> {
    // Without --follow there has to be a finish line, and the log's current
    // version is it: everything up to it, then out.
    let target = if follow {
        None
    } else {
        let logs = hub.list().await.map_err(failed("tail"))?;
        let info = logs
            .iter()
            .find(|l| l.name == log)
            .ok_or_else(|| Fail::new(format!("no log named {log}")))?;
        if info.version <= from {
            return Ok(());
        }
        Some(info.version)
    };

    let mut stream = hub
        .subscribe(log, Subscribe::records().from(from).credit(credit))
        .await
        .map_err(failed("subscribe"))?;
    let mut stdout = std::io::stdout().lock();

    loop {
        let frame = tokio::select! {
            frame = stream.next() => frame,
            // A tail is meant to be interrupted, so that is a success.
            _ = interrupted() => return Ok(()),
        };
        let Some(frame) = frame else { return Ok(()) };
        match frame.map_err(failed("tail"))? {
            Event::Record(record) => {
                let line = serde_json::to_string(&record).unwrap_or_default();
                // Flushed per record, or a pipe sees nothing until the buffer
                // happens to fill.
                match writeln!(stdout, "{line}").and_then(|()| stdout.flush()) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
                    Err(e) => return Err(Fail::new(format!("cannot write to stdout: {e}"))),
                }
                if target.is_some_and(|target| record.version >= target) {
                    return Ok(());
                }
            }
            // A gap is data too: say what is missing rather than letting the
            // versions jump silently.
            Event::Gap { from, to } => {
                eprintln!("lug: gap, versions {from}..{to} were reclaimed by the server");
                if target.is_some_and(|target| to >= target) {
                    return Ok(());
                }
            }
        }
    }
}

async fn follow_view(hub: &Hub, out: &Output, log: &str) -> Result<(), Fail> {
    let follower = hub.follow(log).await.map_err(failed("follow"))?;
    let mut changed = follower.changed();
    loop {
        if let Some(view) = follower.view() {
            out.line(out.value(&view.value))?;
        }
        tokio::select! {
            advanced = changed.changed() => {
                if advanced.is_err() {
                    return Err(Fail::new("the follower stopped"));
                }
            }
            _ = interrupted() => return Ok(()),
        }
    }
}

/// Resolves on SIGINT or SIGTERM. Being interrupted is how a tail normally
/// ends, so it is not an error and does not deserve a backtrace.
async fn interrupted() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(term) => term,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
