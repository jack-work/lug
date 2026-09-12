//! A [`Follow`] source driven by a file of JSON lines.
//!
//! The client is not wired up yet, and a viewer that can only be exercised
//! against a live daemon cannot be tested at all. A script is a source the
//! tests can append to while the real binary is running in a real terminal,
//! which is the only way to watch a repaint happen.
//!
//! First line is the header: `{"reducible": true, "view": {...}}`. Every line
//! after it is one record: `{"version": 1, "patch": {...}, "view": {...}}`,
//! where `view` is the state after that patch and is ignored on a
//! non-reducible log. A reclaimed range is `{"gap": {"from": 39, "to": 91},
//! "view": {...}}`, the view being what a real follower refetches once it
//! knows the patches it missed are unrecoverable. The file is tailed, so lines
//! appended later arrive later.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use lug_proto::{Event, Record, Version};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::follow::{Follow, View};

/// How long the tail thread waits after reaching the end of the file.
const IDLE: Duration = Duration::from_millis(30);

const CAPACITY: usize = 1024;

#[derive(Debug, Default, Deserialize)]
struct Header {
    #[serde(default)]
    reducible: bool,
    #[serde(default)]
    view: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Entry {
    Record {
        version: Version,
        patch: Value,
        #[serde(default)]
        view: Option<Value>,
    },
    Gap {
        gap: Range,
        #[serde(default)]
        view: Option<Value>,
    },
}

#[derive(Debug, Deserialize)]
struct Range {
    from: Version,
    to: Version,
}

pub struct Script {
    view: Arc<Mutex<Option<View>>>,
    version: watch::Receiver<Version>,
    events: Option<mpsc::Receiver<Event>>,
}

impl Script {
    pub fn open(path: &Path) -> Result<Self> {
        let (header, offset) = read_header(path)?;
        let view = header.view.map(|value| View { version: 0, value });
        if header.reducible && view.is_none() {
            bail!("script header says reducible but carries no initial view");
        }

        let view = Arc::new(Mutex::new(view));
        let (version_tx, version_rx) = watch::channel(0);
        let (event_tx, event_rx) = mpsc::channel(CAPACITY);

        let tail = Tailer {
            path: path.to_path_buf(),
            offset,
            reducible: header.reducible,
            view: Arc::clone(&view),
            version: version_tx,
            events: event_tx,
        };
        std::thread::Builder::new()
            .name("lug-tui-script".into())
            .spawn(move || tail.run())
            .context("spawning the script tail thread")?;

        Ok(Self {
            view,
            version: version_rx,
            events: Some(event_rx),
        })
    }
}

impl Follow for Script {
    fn view(&self) -> Option<View> {
        self.view.lock().expect("script view mutex poisoned").clone()
    }

    fn changed(&self) -> watch::Receiver<Version> {
        self.version.clone()
    }

    fn events(&mut self) -> Option<mpsc::Receiver<Event>> {
        self.events.take()
    }
}

/// Reads just the header, and reports the byte offset the records start at.
fn read_header(path: &Path) -> Result<(Header, u64)> {
    let file = File::open(path).with_context(|| format!("opening script {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 => break,
            _ => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
        }
    }
    if line.is_empty() {
        bail!("script {} is empty, it needs a header line", path.display());
    }
    let offset = line.len() as u64;
    let header: Header = serde_json::from_slice(&line)
        .with_context(|| format!("parsing the header of {}", path.display()))?;
    Ok((header, offset))
}

struct Tailer {
    path: PathBuf,
    offset: u64,
    reducible: bool,
    view: Arc<Mutex<Option<View>>>,
    version: watch::Sender<Version>,
    events: mpsc::Sender<Event>,
}

impl Tailer {
    fn run(self) {
        let Ok(file) = File::open(&self.path) else { return };
        let mut reader = BufReader::new(file);
        if std::io::Seek::seek(reader.get_mut(), std::io::SeekFrom::Start(self.offset)).is_err() {
            return;
        }

        // A partial line is normal: the writer may be mid-append.
        let mut pending = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let read = match reader.read(&mut chunk) {
                Ok(n) => n,
                Err(_) => return,
            };
            if read == 0 {
                if self.events.is_closed() {
                    return;
                }
                std::thread::sleep(IDLE);
                continue;
            }
            pending.extend_from_slice(&chunk[..read]);
            while let Some(end) = pending.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = pending.drain(..=end).collect();
                if !self.emit(&line) {
                    return;
                }
            }
        }
    }

    /// False once the viewer has hung up.
    fn emit(&self, line: &[u8]) -> bool {
        if line.iter().all(u8::is_ascii_whitespace) {
            return true;
        }
        let Ok(entry) = serde_json::from_slice::<Entry>(line) else {
            return true;
        };
        let (event, view) = match entry {
            Entry::Record { version, patch, view } => {
                (Event::Record(Record { version, patch }), view)
            }
            Entry::Gap { gap, view } => (Event::Gap { from: gap.from, to: gap.to }, view),
        };
        let version = event.cursor();

        // The event goes out first: the viewer has to know a gap happened
        // before it is handed the view that came after it.
        if self.events.blocking_send(event).is_err() {
            return false;
        }
        if self.reducible && let Some(value) = view {
            let mut slot = self.view.lock().expect("script view mutex poisoned");
            *slot = Some(View { version, value });
        }
        self.version.send(version).is_ok()
    }
}
