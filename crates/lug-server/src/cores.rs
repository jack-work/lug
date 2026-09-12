//! Shared-nothing placement: each log actor lives on one core's runtime.
//!
//! A log actor does blocking storage work, so it gets a single-threaded
//! runtime of its own rather than a slot on a work-stealing pool where it
//! would stall unrelated connections. `hash(name)` picks the core, so a log
//! stays put for the life of the process.

use std::future::Future;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::thread::JoinHandle;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

type Task = std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub struct Cores {
    lanes: Vec<mpsc::Sender<Task>>,
    threads: Vec<JoinHandle<()>>,
}

impl Cores {
    pub fn new(count: usize) -> std::io::Result<Self> {
        let count = count.max(1);
        let mut lanes = Vec::with_capacity(count);
        let mut threads = Vec::with_capacity(count);
        for core in 0..count {
            // Bounded: a flood of log creations must not grow the heap.
            let (tx, mut rx) = mpsc::channel::<Task>(64);
            let thread = std::thread::Builder::new()
                .name(format!("lug-core-{core}"))
                .spawn(move || {
                    let runtime = match Builder::new_current_thread().enable_all().build() {
                        Ok(runtime) => runtime,
                        Err(e) => {
                            tracing::error!(error = %e, core, "core runtime failed to start");
                            return;
                        }
                    };
                    runtime.block_on(async move {
                        while let Some(task) = rx.recv().await {
                            tokio::spawn(task);
                        }
                    });
                })?;
            lanes.push(tx);
            threads.push(thread);
        }
        Ok(Self { lanes, threads })
    }

    pub fn spawn(
        &self,
        key: &str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), String> {
        let lane = &self.lanes[self.lane(key)];
        lane.try_send(Box::pin(task)).map_err(|e| format!("core runtime for {key}: {e}"))
    }

    fn lane(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % self.lanes.len() as u64) as usize
    }
}

impl Drop for Cores {
    fn drop(&mut self) {
        self.lanes.clear();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
