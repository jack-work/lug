use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Result, bail};
use lug_tui::app::{self, Mode};
use lug_tui::follow::Follow;
use lug_tui::script::Script;

const USAGE: &str = "\
lug-tui: watch a lug log

usage:
    lug-tui --script <file> [--reducible] [<name>]

    --script <file>   read the log from a file of JSON lines, tailed live
    --reducible       render the materialized view instead of the records
    <name>            what to call the log in the footer

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

    let source = Script::open(&args.script)?;
    let mode = if args.reducible {
        // A non-reducible log has no view to render, and saying so beats
        // painting an empty tree.
        if source.view().is_none() {
            bail!("{} is not reducible, drop --reducible to tail it", args.name);
        }
        Mode::Reducible
    } else {
        Mode::Log
    };

    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;
    runtime.block_on(app::run(args.name, mode, Box::new(source)))
}

struct Args {
    script: PathBuf,
    reducible: bool,
    name: String,
}

/// `None` asks for the usage text.
fn parse(args: impl Iterator<Item = String>) -> Result<Option<Args>> {
    let mut script = None;
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
            other if other.starts_with('-') => bail!("unknown flag {other}"),
            other => name = Some(other.to_owned()),
        }
    }

    let Some(script) = script else {
        bail!(
            "no source: lug-client is not wired in yet, so a log must come from --script <file>"
        )
    };
    let name = name.unwrap_or_else(|| {
        script.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "log".into())
    });
    Ok(Some(Args { script, reducible, name }))
}
