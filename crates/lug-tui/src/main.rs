use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Result, bail};
use lug_tui::app::{self, Mode};
use lug_tui::follow::Follow;
use lug_tui::script::Script;

const USAGE: &str = "\
lug-tui: watch a lug log

usage:
    lug-tui --socket <path> <log> [--reducible]
    lug-tui --script <file> [--reducible] [<name>]

    --socket <path>   follow a log on a running daemon
    --script <file>   follow a file of JSON lines, tailed live, for testing
    --reducible       render the materialized view instead of the records
    <log>             the log to follow, and what the footer calls it

keys:
    up down pgup pgdn   scroll        home end   top, bottom
    q esc ctrl-c        quit
";

fn main() -> ExitCode {
    match start() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("lug-tui: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn start() -> Result<()> {
    let Some(args) = parse(std::env::args().skip(1))? else {
        print!("{USAGE}");
        return Ok(());
    };

    let source: Box<dyn Follow> = match args.source {
        Source::Script(path) => Box::new(Script::open(&path)?),
        Source::Socket(path) => {
            // The one seam the client plugs into: lug_client::Follower behind
            // the same three questions the script answers.
            bail!("following {} needs lug-client, which is not wired in yet", path.display())
        }
    };

    let mode = if args.reducible {
        // A log with no view has nothing to render as a tree, and saying so
        // beats painting an empty one.
        if !source.reducible() {
            bail!("{} is not reducible, drop --reducible to tail it", args.name);
        }
        if source.view().is_none() {
            bail!("{} is reducible but the source has no view yet", args.name);
        }
        Mode::Reducible
    } else {
        Mode::Log
    };

    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
    runtime.block_on(app::run(args.name, mode, source))
}

#[derive(Debug)]
struct Args {
    source: Source,
    reducible: bool,
    name: String,
}

#[derive(Debug)]
enum Source {
    Script(PathBuf),
    Socket(PathBuf),
}

/// `None` asks for the usage text.
fn parse(args: impl Iterator<Item = String>) -> Result<Option<Args>> {
    let mut script = None;
    let mut socket = None;
    let mut reducible = false;
    let mut name = None;

    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--reducible" => reducible = true,
            "--script" => {
                let Some(path) = args.next() else { bail!("--script wants a path") };
                script = Some(PathBuf::from(path));
            }
            "--socket" => {
                let Some(path) = args.next() else { bail!("--socket wants a path") };
                socket = Some(PathBuf::from(path));
            }
            other if other.starts_with('-') => bail!("unknown flag {other}"),
            other => name = Some(other.to_owned()),
        }
    }

    let source = match (script, socket) {
        (Some(_), Some(_)) => bail!("--script and --socket are two different logs, pick one"),
        (Some(path), None) => Source::Script(path),
        (None, Some(path)) => Source::Socket(path),
        (None, None) => bail!("no log to follow: pass --socket <path> or --script <file>"),
    };

    // A script names its log in the footer after the file, since there is no
    // daemon to have named it.
    let name = name.unwrap_or_else(|| match &source {
        Source::Script(path) => {
            path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "log".into())
        }
        Source::Socket(_) => "log".into(),
    });
    Ok(Some(Args { source, reducible, name }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_args(args: &[&str]) -> Result<Option<Args>> {
        parse(args.iter().map(|a| a.to_string()))
    }

    #[test]
    fn the_two_sources_are_exclusive() {
        let error = parse_args(&["--script", "a.jsonl", "--socket", "/run/lug.sock"])
            .expect_err("two sources were accepted");
        assert!(error.to_string().contains("pick one"), "{error}");
    }

    #[test]
    fn a_log_with_no_source_is_refused() {
        let error = parse_args(&["orders"]).expect_err("a nameless source was accepted");
        assert!(error.to_string().contains("--socket"), "{error}");
    }

    #[test]
    fn a_socket_takes_the_log_name_from_the_argument() {
        let args = parse_args(&["--socket", "/run/lug/lug.sock", "orders"])
            .expect("parsing")
            .expect("arguments");
        assert!(matches!(args.source, Source::Socket(_)));
        assert_eq!(args.name, "orders");
    }

    #[test]
    fn a_script_falls_back_to_the_file_name() {
        let args =
            parse_args(&["--script", "/tmp/orders.jsonl"]).expect("parsing").expect("arguments");
        assert_eq!(args.name, "orders");
    }
}
