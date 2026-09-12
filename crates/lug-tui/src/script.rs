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
//! non-reducible log. The file is tailed, so lines appended later arrive later.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use lug_proto::{Record, Version};
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
struct Entry {
    version: Version,
    patch: Value,
    #[serde(default)]
    view: Option<Value>,
}

pub struct Script {
    view: Arc<Mutex<Option<View>>>,
    version: watch::Receiver<Version>,
    records: Option<mpsc::Receiver<Record>>,
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
        let (record_tx, record_rx) = mpsc::channel(CAPACITY);

        let tail = Tailer {
            path: path.to_path_buf(),
            offset,
            reducible: header.reducible,
            view: Arc::clone(&view),
            version: version_tx,
            records: record_tx,
        };
        std::thread::Builder::new()
            .name("lug-tui-script".into())
            .spawn(move || tail.run())
            .context("spawning the script tail thread")?;

        Ok(Self { view, version: version_rx, records: Some(record_rx) })
    }
}

impl Follow for Script {
    fn view(&self) -> Option<View> {
        self.view.lock().expect("script view mutex poisoned").clone()
    }

    fn changed(&self) -> watch::Receiver<Version> {
        self.version.clone()
    }

    fn records(&mut self) -> Option<mpsc::Receiver<Record>> {
        self.records.take()
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
    records: mpsc::Sender<Record>,
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
                if self.records.is_closed() {
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
        if self.reducible && let Some(value) = entry.view {
            let mut slot = self.view.lock().expect("script view mutex poisoned");
            *slot = Some(View { version: entry.version, value });
        }
        let record = Record { version: entry.version, patch: entry.patch };
        if self.records.blocking_send(record).is_err() {
            return false;
        }
        // After the view, so a woken reducible viewer never reads stale state.
        self.version.send(entry.version).is_ok()
    }
}
