//! Shared test scaffolding: a mock daemon on a real unix socket and on real
//! loopback HTTP, both driven by the same [`sim::Sim`].

#![allow(dead_code)]

pub mod http;
pub mod sim;
pub mod unix;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A private directory for one test's socket. Real path, real permissions,
/// no abstract namespace.
pub fn scratch(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("lug-test-{}-{tag}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch directory");
    dir
}

/// Poll a condition until it holds. Used instead of a fixed sleep, which is
/// either flaky or slow.
pub async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
