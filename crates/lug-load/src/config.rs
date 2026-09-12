use crate::{Result, error};
use lug_proto::Durability;
use std::{path::PathBuf, time::Duration};

pub const HELP: &str = "lug-load: raw-socket load and conformance tests for lug-server

Usage: lug-load [OPTIONS]

  --server PATH          Server executable [sibling lug-server, then PATH]
  --connections N        Held transport connections [10000]
  --logs N               Independent Noop logs [1]
  --appenders-per-log N   Writer lanes per log [1]
  --subscribers-per-log N  Checked streams per log [1]
  --patch-size N         JSON string payload bytes, minimum 16 [256]
  --durability MODE      memory, written, durable [durable]
  --rate N               Global scheduled appends per second [1000]
  --duration SECONDS     Fixed scheduling window, accepts decimals [10]
  --transport MODE       unix or http [unix]
  --accepts N            Separate short-lived handshake count [1000]
  --parallel N           Parallel connection setup/accept workers [128]
  --max-inflight N       Hard limit on outstanding appends [65536]
  --timeout SECONDS      Startup, probe and drain deadline [30]
  --smoke               One-connection end-to-end check, a few seconds
  --soak                Repeat measured epochs with crash/replay and fd checks
  --soak-cycles N        Number of epochs in soak mode [10]
  --fd-slack N           Allowed post-churn fd growth [0]
  --json                Emit one schema-versioned JSON object
  -h, --help            Show help
  --version             Show version

Examples:
  lug-load --connections 10000 --rate 10000 --duration 60
  lug-load --soak --connections 100 --soak-cycles 20 --json

Every successful run includes a same-connection credit isolation probe.
Durable runs include SIGKILL and exact acknowledged-record replay.
Missing server executable is a reported skip, not a measurement.
";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Unix,
    Http,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub server: PathBuf,
    pub connections: usize,
    pub logs: usize,
    pub appenders: usize,
    pub subscribers: usize,
    pub patch_size: usize,
    pub durability: Durability,
    pub rate: u64,
    pub duration: Duration,
    pub transport: Transport,
    pub accepts: usize,
    pub parallel: usize,
    pub max_inflight: usize,
    pub timeout: Duration,
    pub smoke: bool,
    pub soak: bool,
    pub soak_cycles: usize,
    pub fd_slack: usize,
    pub json: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: std::env::current_exe()
                .ok()
                .and_then(|exe| exe.parent().map(|dir| dir.join("lug-server")))
                .filter(|path| path.is_file())
                .unwrap_or_else(|| PathBuf::from("lug-server")),
            connections: 10_000,
            logs: 1,
            appenders: 1,
            subscribers: 1,
            patch_size: 256,
            durability: Durability::Durable,
            rate: 1000,
            duration: Duration::from_secs(10),
            transport: Transport::Unix,
            accepts: 1000,
            parallel: 128,
            max_inflight: 65536,
            timeout: Duration::from_secs(30),
            smoke: false,
            soak: false,
            soak_cycles: 10,
            fd_slack: 0,
            json: false,
        }
    }
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let args: Vec<_> = args.into_iter().collect();
        let mut cfg = if args.iter().any(|arg| arg == "--smoke") {
            Self {
                smoke: true,
                connections: 1,
                rate: 20,
                duration: Duration::from_secs(1),
                accepts: 4,
                parallel: 1,
                ..Self::default()
            }
        } else {
            Self::default()
        };
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--smoke" => cfg.smoke = true,
                "--soak" => cfg.soak = true,
                "--json" => cfg.json = true,
                "--" => {
                    if let Some(arg) = args.next() {
                        return Err(error(format!("unexpected positional argument {arg:?}")));
                    }
                    break;
                }
                _ => {
                    let value = args
                        .next()
                        .ok_or_else(|| error(format!("missing value for {flag}")))?;
                    let number = || {
                        value
                            .parse::<usize>()
                            .map_err(|_| error(format!("invalid {flag}: {value:?}")))
                    };
                    match flag.as_str() {
                        "--server" => cfg.server = value.into(),
                        "--connections" => cfg.connections = number()?,
                        "--logs" => cfg.logs = number()?,
                        "--appenders-per-log" => cfg.appenders = number()?,
                        "--subscribers-per-log" => cfg.subscribers = number()?,
                        "--patch-size" => cfg.patch_size = number()?,
                        "--rate" => cfg.rate = number()? as u64,
                        "--accepts" => cfg.accepts = number()?,
                        "--parallel" => cfg.parallel = number()?,
                        "--max-inflight" => cfg.max_inflight = number()?,
                        "--soak-cycles" => cfg.soak_cycles = number()?,
                        "--fd-slack" => cfg.fd_slack = number()?,
                        "--duration" | "--timeout" => {
                            let n = value
                                .parse::<f64>()
                                .map_err(|_| error(format!("invalid {flag}: {value:?}")))?;
                            if !n.is_finite() || n <= 0.0 || n > 86400.0 {
                                return Err(error(format!(
                                    "{flag} must be in (0, 86400], got {value}"
                                )));
                            }
                            let duration = Duration::from_secs_f64(n);
                            if duration.is_zero() {
                                return Err(error(format!(
                                    "{flag} is below clock resolution: {value}"
                                )));
                            }
                            if flag == "--duration" {
                                cfg.duration = duration;
                            } else {
                                cfg.timeout = duration;
                            }
                        }
                        "--transport" => {
                            cfg.transport = match value.as_str() {
                                "unix" => Transport::Unix,
                                "http" => Transport::Http,
                                _ => return Err(error(format!("invalid transport {value:?}"))),
                            }
                        }
                        "--durability" => {
                            cfg.durability = match value.as_str() {
                                "memory" => Durability::Memory,
                                "written" => Durability::Written,
                                "durable" => Durability::Durable,
                                _ => return Err(error(format!("invalid durability {value:?}"))),
                            }
                        }
                        _ => return Err(error(format!("unknown option {flag:?}"))),
                    }
                }
            }
        }
        if cfg.smoke
            && (cfg.connections != 1 || cfg.logs != 1 || cfg.appenders != 1 || cfg.subscribers != 1)
        {
            return Err(error(
                "--smoke requires one connection, log, appender and subscriber; omit it for a load run",
            ));
        }
        if cfg.smoke && cfg.soak {
            return Err(error("--smoke and --soak are separate modes"));
        }
        for (name, value) in [
            ("connections", cfg.connections),
            ("logs", cfg.logs),
            ("appenders-per-log", cfg.appenders),
            ("subscribers-per-log", cfg.subscribers),
            ("rate", cfg.rate as usize),
            ("parallel", cfg.parallel),
            ("max-inflight", cfg.max_inflight),
            ("soak-cycles", cfg.soak_cycles),
        ] {
            if value == 0 {
                return Err(error(format!("--{name} must be positive, got 0")));
            }
        }
        if !(16..=8 * 1024 * 1024).contains(&cfg.patch_size) {
            return Err(error(format!(
                "--patch-size must be 16..8388608, got {}",
                cfg.patch_size
            )));
        }
        cfg.logs
            .checked_mul(cfg.appenders)
            .and_then(|_| cfg.logs.checked_mul(cfg.subscribers))
            .ok_or_else(|| error("log/worker count overflow"))?;
        if cfg.planned()? > u64::MAX - (1 << 32) - 16 {
            return Err(error("workload exhausts protocol request ids"));
        }
        if cfg.planned()? == 0 {
            return Err(error(
                "rate times duration must schedule at least one append",
            ));
        }
        Ok(cfg)
    }

    pub fn planned(&self) -> Result<u64> {
        u64::try_from(self.duration.as_nanos() * u128::from(self.rate) / 1_000_000_000)
            .map_err(|_| error("rate times duration exceeds u64"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parse(s: &str) -> Result<Config> {
        Config::parse(s.split_whitespace().map(str::to_owned))
    }

    #[test]
    fn reject_invalid_workloads() {
        for args in [
            "--rate 0",
            "--duration NaN",
            "--duration 0.00000000001",
            "--logs 0",
            "--transport udp",
            "--patch-size 8",
            "--max-inflight 0",
            "--typo 1",
        ] {
            assert!(parse(args).is_err(), "{args}");
        }
    }

    #[test]
    fn smoke_is_small_and_distinct_from_soak() {
        let cfg = parse("--smoke").unwrap();
        assert_eq!(cfg.connections, 1);
        assert_eq!(cfg.logs * cfg.appenders * cfg.subscribers, 1);
        assert_eq!(cfg.planned().unwrap(), 20);
        assert!(parse("--smoke --soak").is_err());
    }

    #[test]
    fn fractional_window_has_exact_schedule_count() {
        assert_eq!(
            parse("--rate 10 --duration 0.25")
                .unwrap()
                .planned()
                .unwrap(),
            2
        );
        assert_eq!(Config::default().connections, 10_000);
    }
}
