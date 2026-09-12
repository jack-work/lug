//! The viewer driven the way a person drives it: the real binary, in a real
//! terminal, asserted against what is actually on the screen.
//!
//! A render-buffer assertion only knows what the program decided to paint. It
//! cannot see a leaked alternate screen, a swallowed cursor, or a resize that
//! leaves the previous width's text behind.

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_lug-tui");
const TIMEOUT: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);

fn tmux_present() -> bool {
    Command::new("tmux").arg("-V").output().is_ok_and(|o| o.status.success())
}

/// Skips the body of a test when tmux is missing, loudly enough to notice.
macro_rules! need_tmux {
    () => {
        if !tmux_present() {
            eprintln!("skipping: tmux is not installed");
            return;
        }
    };
}

struct Script {
    path: PathBuf,
}

impl Script {
    fn new(name: &str, header: &str) -> Self {
        let path = std::env::temp_dir()
            .join(format!("lug-tui-{}-{}-{}.jsonl", std::process::id(), name, now_nanos()));
        std::fs::write(&path, format!("{header}\n")).expect("writing the script header");
        Self { path }
    }

    fn append(&self, line: &str) {
        use std::io::Write;
        let mut file =
            std::fs::OpenOptions::new().append(true).open(&self.path).expect("appending");
        writeln!(file, "{line}").expect("appending");
    }

    fn arg(&self) -> String {
        self.path.display().to_string()
    }
}

impl Drop for Script {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn now_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before the epoch")
        .as_nanos()
}

struct Pane {
    session: String,
}

impl Pane {
    /// Runs `argv` as the session's own command, so nothing but the viewer is
    /// on the screen.
    fn run(name: &str, width: u16, height: u16, argv: &[&str]) -> Self {
        let pane = Self::open(name, width, height, argv);
        pane.resize(width, height);
        pane
    }

    /// Runs a shell, so what the terminal looks like after the viewer exits is
    /// observable.
    fn shell(name: &str, width: u16, height: u16) -> Self {
        Self::open(name, width, height, &["/bin/sh"])
    }

    fn open(name: &str, width: u16, height: u16, argv: &[&str]) -> Self {
        let session = format!("lugtui-{}-{}-{}", std::process::id(), name, now_nanos() % 100_000);
        let mut command = Command::new("tmux");
        command
            .args(["new-session", "-d", "-s", &session, "-x", &width.to_string()])
            .args(["-y", &height.to_string()])
            .arg("--")
            .args(argv);
        let status = command.status().expect("tmux new-session");
        assert!(status.success(), "tmux new-session failed");
        Self { session }
    }

    /// The pane is not the window: a status line, or its absence, moves the
    /// count by one. Ask, measure, correct, and only then believe the size.
    fn resize(&self, width: u16, height: u16) {
        let mut request = (width as i32, height as i32);
        for _ in 0..4 {
            let status = Command::new("tmux")
                .args([
                    "resize-window",
                    "-t",
                    &self.session,
                    "-x",
                    &request.0.to_string(),
                    "-y",
                    &request.1.to_string(),
                ])
                .status()
                .expect("tmux resize-window");
            assert!(status.success(), "tmux resize-window failed");
            let actual = (self.measure("#{pane_width}"), self.measure("#{pane_height}"));
            if actual == (width as i32, height as i32) {
                return;
            }
            request.0 += width as i32 - actual.0;
            request.1 += height as i32 - actual.1;
        }
        panic!("could not get a {width}x{height} pane out of tmux");
    }

    fn measure(&self, format: &str) -> i32 {
        self.display(format).parse().expect("a tmux dimension")
    }

    fn display(&self, format: &str) -> String {
        let out = Command::new("tmux")
            .args(["display", "-p", "-t", &self.session, format])
            .output()
            .expect("tmux display");
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn capture(&self) -> String {
        let out = Command::new("tmux")
            .args(["capture-pane", "-p", "-t", &self.session])
            .output()
            .expect("tmux capture-pane");
        String::from_utf8_lossy(&out.stdout).trim_end().to_owned()
    }

    fn send(&self, keys: &str) {
        let status = Command::new("tmux")
            .args(["send-keys", "-t", &self.session, keys])
            .status()
            .expect("tmux send-keys");
        assert!(status.success(), "tmux send-keys failed");
    }

    fn type_line(&self, text: &str) {
        // One character per read, the way a person types.
        for ch in text.chars() {
            let status = Command::new("tmux")
                .args(["send-keys", "-t", &self.session, "-l", &ch.to_string()])
                .status()
                .expect("tmux send-keys");
            assert!(status.success(), "tmux send-keys failed");
        }
        self.send("Enter");
    }

    fn wait_for(&self, needle: &str) -> String {
        self.wait_until(|screen| screen.contains(needle), &format!("{needle:?} to appear"))
    }

    fn wait_gone(&self, needle: &str) -> String {
        self.wait_until(|screen| !screen.contains(needle), &format!("{needle:?} to go away"))
    }

    fn wait_until(&self, predicate: impl Fn(&str) -> bool, what: &str) -> String {
        let deadline = Instant::now() + TIMEOUT;
        let mut screen = self.capture();
        while !predicate(&screen) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}; screen was:\n{screen}"
            );
            std::thread::sleep(POLL);
            screen = self.capture();
        }
        screen
    }

    /// Returns once two captures in a row agree, so nothing half-painted is
    /// mistaken for a finished frame.
    fn wait_stable(&self) -> String {
        let deadline = Instant::now() + TIMEOUT;
        let mut previous = self.capture();
        loop {
            std::thread::sleep(POLL);
            let screen = self.capture();
            if screen == previous {
                return screen;
            }
            assert!(Instant::now() < deadline, "screen never settled; last:\n{screen}");
            previous = screen;
        }
    }
}

impl Drop for Pane {
    fn drop(&mut self) {
        let _ = Command::new("tmux").args(["kill-session", "-t", &self.session]).status();
    }
}

const HEADER_PLAIN: &str = r#"{"reducible": false}"#;
const HEADER_REDUCIBLE: &str = r#"{"reducible": true, "view": {"count": 0, "orders": {}}}"#;

fn record(version: u64, patch: &str) -> String {
    format!(r#"{{"version": {version}, "patch": {patch}}}"#)
}

fn record_with_view(version: u64, patch: &str, view: &str) -> String {
    format!(r#"{{"version": {version}, "patch": {patch}, "view": {view}}}"#)
}

#[test]
fn it_starts_and_paints() {
    need_tmux!();
    let script = Script::new("start", HEADER_PLAIN);
    script.append(&record(1, r#"{"op": "set", "k": "alpha"}"#));
    let pane = Pane::run("start", 80, 12, &[BIN, "--script", &script.arg(), "orders"]);

    let screen = pane.wait_for("alpha");
    let patch = r#"{"k": "alpha", "op": "set"}"#;
    assert!(screen.contains(patch), "the patch is not on screen as JSON:\n{screen}");
    let footer = screen.lines().last().expect("a footer line").to_owned();
    assert!(footer.contains("orders"), "footer lost the log name: {footer:?}");
    assert!(footer.contains("v1"), "footer lost the version: {footer:?}");
    assert!(footer.contains("tail"), "footer lost the mode: {footer:?}");
}

#[test]
fn log_mode_tails_arriving_records() {
    need_tmux!();
    let script = Script::new("tails", HEADER_PLAIN);
    script.append(&record(1, r#"{"first": true}"#));
    let pane = Pane::run("tails", 80, 10, &[BIN, "--script", &script.arg(), "orders"]);
    pane.wait_for("first");

    script.append(&record(2, r#"{"second": true}"#));
    let screen = pane.wait_for("second");

    let body: Vec<&str> = screen.lines().filter(|l| !l.trim().is_empty()).collect();
    let last = body[body.len() - 2];
    assert!(last.contains("second"), "newest record is not at the bottom:\n{screen}");
    assert!(body[body.len() - 1].contains("v2"), "version did not advance:\n{screen}");
}

#[test]
fn scrolling_up_pauses_the_tail_and_end_resumes_it() {
    need_tmux!();
    let script = Script::new("pause", HEADER_PLAIN);
    for v in 1..=8 {
        script.append(&record(v, &format!(r#"{{"n": {v}}}"#)));
    }
    let pane = Pane::run("pause", 60, 6, &[BIN, "--script", &script.arg(), "orders"]);
    pane.wait_for(r#"{"n": 8}"#);

    pane.send("Up");
    let paused = pane.wait_for("tail paused");
    let frozen: Vec<String> = body_of(&paused);

    script.append(&record(9, r#"{"n": 9}"#));
    // Give the record time to arrive and be wrongly painted.
    std::thread::sleep(Duration::from_millis(400));
    let after = pane.capture();
    assert_eq!(frozen, body_of(&after), "a paused viewport moved under an arrival");
    assert!(!after.contains(r#"{"n": 9}"#), "a paused viewport showed the new record");

    pane.send("End");
    let resumed = pane.wait_for(r#"{"n": 9}"#);
    assert!(!resumed.contains("paused"), "End did not resume following:\n{resumed}");
}

#[test]
fn reducible_mode_repaints_with_the_changed_subtree_lit() {
    need_tmux!();
    let script = Script::new("reduce", HEADER_REDUCIBLE);
    let pane =
        Pane::run("reduce", 60, 12, &[BIN, "--script", &script.arg(), "--reducible", "orders"]);
    pane.wait_for("count: 0");

    script.append(&record_with_view(
        1,
        r#"{"op": "set"}"#,
        r#"{"count": 1, "orders": {"a1": {"total": 42}}}"#,
    ));

    let lit = pane.wait_for("total: 42");
    assert!(lit.contains("v1"), "version indicator did not advance:\n{lit}");
    assert!(lit.contains("count: 1"), "the changed leaf did not repaint:\n{lit}");
    let bars: Vec<&str> = lit.lines().filter(|l| l.starts_with('▌')).collect();
    assert!(
        bars.iter().any(|l| l.contains("count: 1")) && bars.iter().any(|l| l.contains("total: 42")),
        "the changed subtree was not lit:\n{lit}"
    );
    assert!(
        !lit.lines().any(|l| l.starts_with('▌') && l.contains("orders {1}")),
        "a container that only gained a child was lit:\n{lit}"
    );

    let calm = pane.wait_gone("▌");
    assert!(calm.contains("total: 42"), "the view vanished with the highlight:\n{calm}");
}

#[test]
fn reducible_mode_is_refused_on_a_plain_log() {
    let script = Script::new("refuse", HEADER_PLAIN);
    let out = Command::new(BIN)
        .args(["--script", &script.arg(), "--reducible", "orders"])
        .output()
        .expect("running lug-tui");
    assert!(!out.status.success(), "a plain log accepted --reducible");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(stderr.lines().count(), 1, "the refusal is not one line: {stderr:?}");
    assert!(stderr.contains("not reducible"), "unhelpful refusal: {stderr:?}");
}

#[test]
fn resizing_rewraps_without_leaving_the_old_width_behind() {
    need_tmux!();
    let patch =
        r#"{"note": "a patch long enough that it must wrap when it gets narrow", "n": 1}"#;
    let script = Script::new("resize", HEADER_PLAIN);
    script.append(&record(1, patch));
    let pane = Pane::run("resize", 100, 10, &[BIN, "--script", &script.arg(), "orders"]);
    pane.wait_for("must wrap");

    for width in [46u16, 72, 100] {
        pane.resize(width, 10);
        let screen = pane.wait_stable();
        for line in screen.lines() {
            assert!(
                line.chars().count() <= width as usize,
                "a line outran the {width} column pane:\n{screen}"
            );
        }
        // Spaces are dropped: a break at one puts it in the dead column at the
        // end of a row, where the terminal does not keep it. Everything else
        // must still be there exactly once, in order.
        assert_eq!(
            reassemble(&screen),
            r#"{"n":1,"note":"apatchlongenoughthatitmustwrapwhenitgetsnarrow"}"#,
            "the record did not survive the resize to {width}:\n{screen}"
        );
        let footer = screen.lines().last().unwrap_or_default();
        assert!(footer.contains("orders"), "the footer went missing at {width}:\n{screen}");
    }
}

#[test]
fn quitting_restores_the_terminal() {
    need_tmux!();
    let script = Script::new("quit", HEADER_PLAIN);
    script.append(&record(1, r#"{"k": "alpha"}"#));
    let pane = Pane::shell("quit", 80, 12);
    pane.type_line("echo MARKER-BEFORE");
    pane.wait_for("MARKER-BEFORE");

    pane.type_line(&format!("{BIN} --script {} orders", script.arg()));
    pane.wait_for("alpha");
    assert_eq!(pane.display("#{alternate_on}"), "1", "the viewer never took the alternate screen");

    pane.send("q");
    pane.wait_until(|_| pane.display("#{alternate_on}") == "0", "the alternate screen to be left");

    let screen = pane.wait_stable();
    assert!(screen.contains("MARKER-BEFORE"), "the shell's screen did not come back:\n{screen}");
    assert!(!screen.contains("q quit"), "the viewer's footer leaked into the shell:\n{screen}");
    assert_eq!(pane.display("#{cursor_flag}"), "1", "the cursor was left hidden");

    // A restored terminal is one that still echoes.
    pane.type_line("echo MARKER-AFTER");
    pane.wait_for("MARKER-AFTER");
}

/// The screen's record text with the version gutter and wrap indent removed.
fn reassemble(screen: &str) -> String {
    screen
        .lines()
        .filter(|line| !line.contains("q quit"))
        .map(|line| line.chars().skip(8).filter(|c| !c.is_whitespace()).collect::<String>())
        .collect()
}

fn body_of(screen: &str) -> Vec<String> {
    screen.lines().filter(|l| !l.contains("q quit")).map(str::to_owned).collect()
}
