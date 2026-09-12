//! Disposable line-mode TUI. All state and batch rules live in cavlc.
use cavlc::{Batch, Patch, Store};
use serde_json::json;
use std::{
    env,
    io::{self, BufRead, IsTerminal, Write},
    process::ExitCode,
};

const HELP: &str = "cavlc-demo: in-memory AVL/MVCC playground
Usage: cavlc-demo [--plain]

Enter one object patch per line (not RFC 6902).
Create a key: {\"Create\":{\"count\":0}}
Update it:   {\"Update\":{\"count\":1}}
Delete it:   {\"Delete\":[\"count\"]}
Create can initialize a whole subtree: {\"Create\":{\"profile\":{\"name\":\"Gluck\"}}}
Update requires an existing path. Delete removes the whole subtree.

:batch         start a private batch
:apply         publish it as one version
:abort         discard it
:show [VERSION] inspect current or historical state
:log            show committed JSON log records
:help           show this help
:quit           exit; all state is discarded

The current committed snapshot is rendered after every command.
A batch also shows its private working snapshot.
--plain disables screen clearing. Piped input emits JSON Lines.
--help, -h show help; --version prints the version.
";

struct Demo {
    store: Store,
    pending: Option<Batch>,
}
impl Demo {
    fn new() -> Self {
        Self {
            store: Store::new(),
            pending: None,
        }
    }
    fn execute(&mut self, line: &str) -> Result<serde_json::Value, String> {
        match line {
            ":batch" => {
                if self.pending.is_some() {
                    return Err("batch already open".into());
                }
                self.pending = Some(self.store.begin_batch());
                Ok(json!({"message":"batch opened"}))
            }
            ":apply" => {
                let batch = self.pending.take().ok_or("no batch open")?;
                let result = self.store.apply_batch(batch).map_err(|e| e.to_string())?;
                Ok(
                    json!({"message": if result.record.is_some() {"committed"} else {"no change"}, "record":result.record}),
                )
            }
            ":abort" => {
                self.pending.take().ok_or("no batch open")?;
                Ok(json!({"message":"batch aborted"}))
            }
            ":log" => Ok(json!({"log":self.store.log()})),
            ":show" => Ok(json!({"message":"current snapshot"})),
            ":help" => Ok(json!({"help":HELP})),
            _ if line.starts_with(":show ") => {
                let version = line[6..]
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| "use :show VERSION")?;
                let snapshot = self.store.snapshot_at(version).ok_or("unknown version")?;
                Ok(json!({"historical":{"version":version,"snapshot":snapshot.root()}}))
            }
            _ if line.starts_with(':') => Err("unknown command; try :help".into()),
            _ => {
                let patch: Patch =
                    serde_json::from_str(line).map_err(|e| format!("invalid patch JSON: {e}"))?;
                if let Some(batch) = self.pending.as_mut() {
                    batch.apply(&patch).map_err(|e| e.to_string())?;
                    Ok(json!({"message":"staged, not committed"}))
                } else {
                    let result = self.store.apply(&patch).map_err(|e| e.to_string())?;
                    Ok(
                        json!({"message":if result.record.is_some() {"committed"} else {"no change"},"record":result.record}),
                    )
                }
            }
        }
    }
    fn response(&self, result: Result<serde_json::Value, String>) -> serde_json::Value {
        let current = self.store.snapshot();
        let mut response = match result {
            Ok(detail) => json!({"ok":true,"detail":detail}),
            Err(error) => json!({"ok":false,"error":error}),
        };
        response["version"] = json!(current.version());
        response["snapshot"] = json!(current.root());
        if let Some(batch) = &self.pending {
            response["batch"] = json!({"base":batch.base().version(),"snapshot":batch.root()});
        }
        response
    }
    fn render(
        &self,
        out: &mut impl Write,
        clear: bool,
        response: &serde_json::Value,
    ) -> io::Result<()> {
        if clear {
            write!(out, "\x1b[2J\x1b[H")?;
        }
        let snapshot = self.store.snapshot();
        let map = snapshot.root().as_object().unwrap();
        writeln!(
            out,
            "cavlc | version {} | {} keys | AVL height {} | {} commits",
            snapshot.version(),
            map.len(),
            map.height(),
            self.store.log().len()
        )?;
        writeln!(
            out,
            "CURRENT SNAPSHOT\n{}",
            serde_json::to_string_pretty(snapshot.root())?
        )?;
        if let Some(batch) = &self.pending {
            writeln!(
                out,
                "WORKING SNAPSHOT (base {})\n{}",
                batch.base().version(),
                serde_json::to_string_pretty(batch.root())?
            )?;
        }
        if let Some(error) = response.get("error") {
            writeln!(out, "error: {}", error.as_str().unwrap_or("unknown"))?;
        }
        if let Some(detail) = response.get("detail") {
            if let Some(help) = detail.get("help") {
                writeln!(out, "{}", help.as_str().unwrap())?;
            } else if let Some(message) = detail.get("message") {
                writeln!(out, "{}", message.as_str().unwrap())?;
            } else {
                writeln!(out, "{}", serde_json::to_string_pretty(detail)?)?;
            }
        }
        write!(
            out,
            "\nJSON patch or :help | :batch :apply :abort :show :log :quit\n> "
        )?;
        out.flush()
    }
}

fn run(plain: bool) -> io::Result<bool> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let interactive = stdin.is_terminal() && stdout.is_terminal();
    let clear = interactive && !plain && env::var("TERM").as_deref() != Ok("dumb");
    let mut out = stdout.lock();
    let mut demo = Demo::new();
    let mut failed = false;
    if interactive {
        demo.render(&mut out, clear, &json!({}))?;
    }
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line == ":quit" {
            break;
        }
        if line.is_empty() {
            if interactive {
                write!(out, "> ")?;
                out.flush()?;
            }
            continue;
        }
        let result = demo.execute(line);
        failed |= result.is_err();
        if let (false, Err(error)) = (interactive, &result) {
            eprintln!("error: {error}");
        }
        let response = demo.response(result);
        if interactive {
            demo.render(&mut out, clear, &response)?;
        } else {
            writeln!(out, "{response}")?;
            out.flush()?;
        }
    }
    if interactive {
        writeln!(out)?;
    }
    Ok(!interactive && failed)
}

fn main() -> ExitCode {
    let mut plain = false;
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--plain" => plain = true,
            "--help" | "-h" => {
                print!("{HELP}");
                return ExitCode::SUCCESS;
            }
            "--version" => {
                println!("cavlc-demo {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            _ => {
                eprintln!("unknown argument: {arg}; use --help");
                return ExitCode::from(2);
            }
        }
    }
    match run(plain) {
        Ok(false) => ExitCode::SUCCESS,
        Ok(true) => ExitCode::FAILURE,
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
