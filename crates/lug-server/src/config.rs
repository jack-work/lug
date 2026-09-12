//! Configuration: a TOML file, with a flag override for every key.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

pub const USAGE: &str = "\
lug-server [--config <path>] [--check]
           [--data <dir>] [--run <dir>] [--socket <name>] [--http <addr>]
           [--token <path>] [--allow-uid <uid>] [--segment <size>]
           [--ring <n>] [--checkpoint-every <n>] [--connections <n>]";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Segment directories, one per log.
    pub data: PathBuf,
    /// Created 0700 before the socket is bound.
    pub run: PathBuf,
    /// Relative to `run`.
    pub socket: PathBuf,
    /// Omit to disable HTTP.
    pub http: Option<SocketAddr>,
    /// Bearer token file, never logged.
    pub token: Option<PathBuf>,
    /// Additional uids beyond the daemon's own.
    pub allow_uid: Vec<u32>,
    pub segment: Size,
    /// Records held in memory per log for fan-out.
    pub ring: usize,
    /// Versions between automatic checkpoints. 0 disables them.
    pub checkpoint_every: u64,

    /// Per-log uid lists, consulted for uids in `allow_uid`.
    pub acl: HashMap<String, Vec<u32>>,
    /// Owning uid. Defaults to the daemon's own, and exists so tests can
    /// drive the peer credential check from the wrong side.
    pub owner: Option<u32>,
    /// Actor runtimes. Defaults to the core count.
    pub cores: Option<usize>,
    pub limits: Limits,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data: PathBuf::from("/var/lib/lug"),
            run: PathBuf::from("/run/lug"),
            socket: PathBuf::from("lug.sock"),
            http: None,
            token: None,
            allow_uid: Vec::new(),
            segment: Size(64 * 1024 * 1024),
            ring: 4096,
            checkpoint_every: 10_000,
            acl: HashMap::new(),
            owner: None,
            cores: None,
            limits: Limits::default(),
        }
    }
}

/// Queue and frame bounds. Every one of them is a ceiling on heap a client
/// can cause the daemon to hold.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct Limits {
    /// Log actor mailbox.
    pub inbox: usize,
    /// Patches per group commit.
    pub batch: usize,
    /// Records held per log for fan-out, taken from `ring`.
    pub ring: usize,
    /// Frames queued toward one connection.
    pub outbox: usize,
    /// Replies a connection may be waiting on at once.
    pub pending: usize,
    /// Records in one pushed frame.
    pub push: usize,
    /// Bytes in one pushed frame.
    pub push_bytes: usize,
    /// Accepted connections held at once. Past this the daemon refuses with
    /// Code::Backpressure rather than queueing, because a queue here only
    /// moves the failure somewhere harder to see.
    pub connections: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            inbox: 4096,
            batch: 1024,
            ring: 4096,
            outbox: 1024,
            pending: 1024,
            push: 512,
            push_bytes: 4 * 1024 * 1024,
            connections: 16384,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("reading {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("parsing {path}: {source}")]
    Parse { path: PathBuf, source: toml::de::Error },
    #[error("{0}")]
    Invalid(String),
    #[error("bad value for --{flag}: {value}")]
    Flag { flag: String, value: String },
    #[error("unknown argument {0}\n{USAGE}")]
    Unknown(String),
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| Error::Read { path: path.into(), source })?;
        toml::from_str(&text).map_err(|source| Error::Parse { path: path.into(), source })
    }

    /// Parse the command line. Returns the config and whether `--check` was
    /// asked for.
    pub fn from_args<I: IntoIterator<Item = String>>(args: I) -> Result<(Self, bool), Error> {
        let mut args = args.into_iter().peekable();
        let mut overrides: Vec<(String, String)> = Vec::new();
        let mut path = None;
        let mut check = false;

        while let Some(arg) = args.next() {
            let Some(flag) = arg.strip_prefix("--") else {
                return Err(Error::Unknown(arg));
            };
            // Both spellings, because the TOML keys use underscores and the
            // flags read better with dashes.
            let flag = flag.replace('-', "_");
            if flag == "check" {
                check = true;
                continue;
            }
            let value = args.next().ok_or_else(|| Error::Flag {
                flag: flag.clone(),
                value: "missing".into(),
            })?;
            if flag == "config" {
                path = Some(PathBuf::from(value));
            } else {
                overrides.push((flag, value));
            }
        }

        let mut config = match &path {
            Some(path) => Self::load(path)?,
            None => Self::default(),
        };
        for (flag, value) in overrides {
            config.set(&flag, &value)?;
        }
        Ok((config, check))
    }

    fn set(&mut self, flag: &str, value: &str) -> Result<(), Error> {
        let bad = |value: &str| Error::Flag { flag: flag.to_string(), value: value.to_string() };
        match flag {
            "data" => self.data = value.into(),
            "run" => self.run = value.into(),
            "socket" => self.socket = value.into(),
            "http" => self.http = Some(value.parse().map_err(|_| bad(value))?),
            "token" => self.token = Some(value.into()),
            "allow_uid" => self.allow_uid.push(value.parse().map_err(|_| bad(value))?),
            "segment" => self.segment = Size(parse_size(value).ok_or_else(|| bad(value))?),
            "ring" => self.ring = value.parse().map_err(|_| bad(value))?,
            "checkpoint_every" => self.checkpoint_every = value.parse().map_err(|_| bad(value))?,
            "owner" => self.owner = Some(value.parse().map_err(|_| bad(value))?),
            "cores" => self.cores = Some(value.parse().map_err(|_| bad(value))?),
            "connections" => self.limits.connections = value.parse().map_err(|_| bad(value))?,
            other => return Err(Error::Unknown(format!("--{other}"))),
        }
        Ok(())
    }

    pub fn socket_path(&self) -> PathBuf {
        self.run.join(&self.socket)
    }

    /// Everything that can be checked without binding anything.
    pub fn validate(&self) -> Result<(), Error> {
        if self.socket.is_absolute() {
            return Err(Error::Invalid(format!(
                "socket {:?} must be relative to run",
                self.socket
            )));
        }
        if self.ring == 0 {
            return Err(Error::Invalid("ring must hold at least one record".into()));
        }
        if self.segment.0 < 4096 {
            return Err(Error::Invalid(format!("segment {} is too small", self.segment.0)));
        }
        if let Some(http) = self.http {
            if !http.ip().is_loopback() {
                return Err(Error::Invalid(format!("http {http} is not loopback")));
            }
            let Some(token) = &self.token else {
                return Err(Error::Invalid("http needs a token file".into()));
            };
            // Read it now so a missing or unreadable token fails at --check
            // rather than on the first request. The value is never logged.
            read_token(token)?;
        }
        Ok(())
    }

    pub fn limits(&self) -> Limits {
        Limits { ring: self.ring, ..self.limits }
    }
}

pub fn read_token(path: &Path) -> Result<String, Error> {
    let token = std::fs::read_to_string(path)
        .map_err(|source| Error::Read { path: path.into(), source })?;
    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(Error::Invalid(format!("token file {path:?} is empty")));
    }
    Ok(token)
}

/// A byte size, written either as a number or as `64MiB`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Size(pub u64);

impl Serialize for Size {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Written {
            Bytes(u64),
            Text(String),
        }
        match Written::deserialize(deserializer)? {
            Written::Bytes(bytes) => Ok(Size(bytes)),
            Written::Text(text) => parse_size(&text)
                .map(Size)
                .ok_or_else(|| serde::de::Error::custom(format!("bad size {text:?}"))),
        }
    }
}

fn parse_size(text: &str) -> Option<u64> {
    let text = text.trim();
    let digits = text.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = text[digits.len()..].trim();
    let scale = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        _ => return None,
    };
    digits.trim().parse::<u64>().ok()?.checked_mul(scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_keys_round_trip() {
        let config: Config = toml::from_str(
            r#"
            data = "/var/lib/lug"
            run = "/run/lug"
            socket = "lug.sock"
            http = "127.0.0.1:7717"
            token = "/etc/lug/token"
            allow_uid = [1001]
            segment = "64MiB"
            ring = 4096
            checkpoint_every = 10000
            "#,
        )
        .expect("config parses");
        assert_eq!(config.segment, Size(64 * 1024 * 1024));
        assert_eq!(config.allow_uid, vec![1001]);
        assert_eq!(config.socket_path(), PathBuf::from("/run/lug/lug.sock"));
        assert_eq!(config.http.map(|a| a.port()), Some(7717));
    }

    #[test]
    fn flags_override_the_file() {
        let args = ["--run", "/tmp/lug", "--ring", "8", "--allow-uid", "7", "--check"];
        let (config, check) =
            Config::from_args(args.iter().map(|s| s.to_string())).expect("args parse");
        assert!(check);
        assert_eq!(config.run, PathBuf::from("/tmp/lug"));
        assert_eq!(config.ring, 8);
        assert_eq!(config.allow_uid, vec![7]);
    }

    #[test]
    fn http_off_loopback_is_refused() {
        let mut config = Config { http: Some("8.8.8.8:80".parse().unwrap()), ..Config::default() };
        assert!(config.validate().is_err());
        config.http = None;
        assert!(config.validate().is_ok());
    }
}
