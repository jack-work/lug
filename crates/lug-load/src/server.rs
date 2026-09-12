use crate::{
    Result,
    config::{Config, Transport},
    error,
    wire::Endpoint,
};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::time::{Instant, interval, timeout};

pub struct Limits {
    pub before: u64,
    pub soft: u64,
    pub hard: u64,
}

pub fn raise_nofile() -> Result<Limits> {
    // RLIMIT_NOFILE is inherited by the child, so both halves of a socket test get the same ceiling.
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let before = limit.rlim_cur;
        limit.rlim_cur = limit.rlim_max;
        if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
            eprintln!(
                "lug-load: could not raise RLIMIT_NOFILE: {}",
                std::io::Error::last_os_error()
            );
        }
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Limits {
            before,
            soft: limit.rlim_cur,
            hard: limit.rlim_max,
        })
    }
}

fn signal(pid: u32, signal: i32) -> Result<()> {
    let pid = libc::pid_t::try_from(pid)?;
    // The PID comes from our unreaped Child, so it cannot have been reused for another process.
    if unsafe { libc::kill(pid, signal) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

pub fn rss(pid: u32) -> Result<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| error(format!("/proc/{pid}/status omitted VmRSS")))?
        .parse::<u64>()?;
    Ok(kib * 1024)
}

pub fn fd_count(pid: u32) -> Result<usize> {
    Ok(fs::read_dir(format!("/proc/{pid}/fd"))?
        .collect::<std::io::Result<Vec<_>>>()?
        .len())
}

pub struct Server {
    child: Option<Child>,
    pub endpoint: Endpoint,
    directory: PathBuf,
    config: PathBuf,
    executable: PathBuf,
    timeout: Duration,
}

impl Server {
    pub async fn start(cfg: &Config) -> Result<Option<Self>> {
        let mut random = [0u8; 32];
        fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let nonce: String = random[..8].iter().map(|b| format!("{b:02x}")).collect();
        let directory =
            std::env::temp_dir().join(format!("lug-load-{}-{nonce}", std::process::id()));
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let run = directory.join("run");
        fs::DirBuilder::new().mode(0o700).create(&run)?;
        let http = if cfg.transport == Transport::Http {
            // Reserving a loopback port avoids a fixed-port collision; startup still detects the close/bind race.
            Some(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?)
        } else {
            None
        };
        let token: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let endpoint = Endpoint {
            socket: run.join("lug.sock"),
            http,
            token: Arc::new(token),
        };
        let mut server = Self {
            child: None,
            endpoint,
            config: directory.join("server.toml"),
            directory,
            executable: cfg.server.clone(),
            timeout: cfg.timeout,
        };
        let mut config = toml::map::Map::new();
        config.insert(
            "data".into(),
            toml::Value::String(server.directory.join("data").display().to_string()),
        );
        config.insert("run".into(), toml::Value::String(run.display().to_string()));
        config.insert("socket".into(), toml::Value::String("lug.sock".into()));
        config.insert("ring".into(), toml::Value::Integer(4096));
        // Replay assertions need records retained rather than replaced by a Noop checkpoint.
        config.insert("checkpoint_every".into(), toml::Value::Integer(i64::MAX));
        if let Some(http) = http {
            let token_path = server.directory.join("bearer");
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&token_path)?
                .write_all(server.endpoint.token.as_bytes())?;
            config.insert("http".into(), toml::Value::String(http.to_string()));
            config.insert(
                "token".into(),
                toml::Value::String(token_path.display().to_string()),
            );
        }
        fs::write(&server.config, toml::to_string(&config)?)?;
        match server.spawn() {
            Ok(()) => {
                server.ready().await?;
                Ok(Some(server))
            }
            Err(e)
                if e.downcast_ref::<std::io::Error>()
                    .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn spawn(&mut self) -> Result<()> {
        self.child = Some(
            Command::new(&self.executable)
                .arg("--config")
                .arg(&self.config)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        Ok(())
    }

    pub fn pid(&self) -> Result<u32> {
        self.child
            .as_ref()
            .map(Child::id)
            .ok_or_else(|| error("server has no live child"))
    }

    async fn ready(&mut self) -> Result<()> {
        timeout(self.timeout, async {
            let mut tick = interval(Duration::from_millis(10));
            loop {
                tick.tick().await;
                if let Some(status) = self
                    .child
                    .as_mut()
                    .ok_or_else(|| error("missing child"))?
                    .try_wait()?
                {
                    return Err(error(format!("server exited during startup with {status}")));
                }
                if self.endpoint.socket.exists() {
                    if tokio::net::UnixStream::connect(&self.endpoint.socket)
                        .await
                        .is_err()
                    {
                        continue;
                    }
                    if let Some(address) = self.endpoint.http {
                        if tokio::net::TcpStream::connect(address).await.is_err() {
                            continue;
                        }
                    }
                    return Ok(());
                }
            }
        })
        .await
        .map_err(|_| {
            error(format!(
                "server socket {} did not become ready within {:?}",
                self.endpoint.socket.display(),
                self.timeout
            ))
        })?
    }

    pub async fn crash_restart(&mut self) -> Result<()> {
        let mut child = self
            .child
            .take()
            .ok_or_else(|| error("cannot crash absent server"))?;
        child.kill()?;
        let status = child.wait()?;
        use std::os::unix::process::ExitStatusExt;
        if status.signal() != Some(libc::SIGKILL) {
            return Err(error(format!("crash expected SIGKILL, got {status}")));
        }
        // A killed daemon cannot unlink its pathname. Removing only our owned socket permits a clean bind.
        match fs::remove_file(&self.endpoint.socket) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        self.spawn()?;
        self.ready().await
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        let Some(child) = &mut self.child else {
            return Ok(());
        };
        if child.try_wait()?.is_none() {
            signal(child.id(), libc::SIGTERM)?;
            let stopped = timeout(Duration::from_secs(5), async {
                let mut tick = interval(Duration::from_millis(20));
                loop {
                    tick.tick().await;
                    if child.try_wait()?.is_some() {
                        return Ok::<_, std::io::Error>(());
                    }
                }
            })
            .await;
            if stopped.is_err() {
                child.kill()?;
                child.wait()?;
                return Err(error(
                    "server ignored SIGTERM for 5 seconds; killed during cleanup",
                ));
            }
            stopped??;
        }
        self.child = None;
        Ok(())
    }

    pub async fn quiescent(&self) -> Result<(usize, u64)> {
        let pid = self.pid()?;
        let deadline = Instant::now() + self.timeout;
        let mut tick = interval(Duration::from_millis(50));
        let mut previous = None;
        let mut same = 0;
        loop {
            tick.tick().await;
            let count = fd_count(pid)?;
            if previous == Some(count) {
                same += 1;
            } else {
                same = 0;
            }
            if same >= 4 {
                return Ok((count, rss(pid)?));
            }
            previous = Some(count);
            if Instant::now() >= deadline {
                return Err(error(format!("server {pid} fds never quiesced")));
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}
