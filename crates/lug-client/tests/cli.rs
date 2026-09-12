//! The `lug` binary, driven the way a shell drives it.

mod support;

use serde_json::{Value, json};
use std::process::Stdio;
use std::time::Duration;
use support::unix::Mock;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

fn lug(mock: &Mock) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lug"));
    cmd.arg("--socket").arg(mock.path());
    cmd.kill_on_drop(true);
    cmd
}

async fn output(mut cmd: Command) -> (bool, String, String) {
    let out = cmd.output().await.expect("run lug");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[tokio::test]
async fn create_append_read_round_trip() {
    let mock = Mock::start("cli-basics").await;

    let (ok, out, err) = output({
        let mut cmd = lug(&mock);
        cmd.args(["create", "notes", "--reducible"]);
        cmd
    })
    .await;
    assert!(ok, "create failed: {err}");
    assert!(out.contains("created notes"), "{out}");

    // The demo pipes one patch per invocation, so that is what is tested.
    for patch in [
        r#"{"Create":{"title":"lug","tags":[]}}"#,
        r#"{"Create":{"author":{"name":"Gluck"}}}"#,
        r#"{"Update":{"title":"lug: a little log"}}"#,
    ] {
        let mut child = lug(&mock)
            .args(["append", "notes", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn append");
        let mut stdin = child.stdin.take().expect("stdin");
        stdin
            .write_all(patch.as_bytes())
            .await
            .expect("write patch");
        stdin.write_all(b"\n").await.expect("newline");
        drop(stdin);
        let status = child.wait().await.expect("append");
        assert!(status.success(), "append failed for {patch}");
    }

    let (ok, out, err) = output({
        let mut cmd = lug(&mock);
        cmd.args(["--json", "read", "notes"]);
        cmd
    })
    .await;
    assert!(ok, "read failed: {err}");
    let view: Value = serde_json::from_str(out.trim()).expect("read prints one JSON object");
    assert_eq!(view["version"], json!(3));
    assert_eq!(view["value"]["title"], json!("lug: a little log"));
    assert_eq!(view["value"]["author"]["name"], json!("Gluck"));
}

#[tokio::test]
async fn tail_without_follow_prints_history_and_exits() {
    let mock = Mock::start("cli-tail").await;
    for n in 0..4 {
        mock.sim
            .append(&[json!({ "Create": { format!("k{n}"): n } })])
            .expect("seed");
    }

    let (ok, out, err) = output({
        let mut cmd = lug(&mock);
        cmd.args(["tail", "log", "--from", "0", "--follow=false"]);
        cmd
    })
    .await;
    assert!(ok, "tail failed: {err}");

    // One JSON object per line, in version order, so `| jq` works.
    let versions: Vec<u64> = out
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("a record per line")["version"]
                .as_u64()
                .expect("version")
        })
        .collect();
    assert_eq!(versions, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn a_live_tail_streams_and_ends_on_a_signal() {
    let mock = Mock::start("cli-live").await;
    mock.sim
        .append(&[json!({ "Create": { "a": 1 } })])
        .expect("seed");

    let mut child = lug(&mock)
        .args(["tail", "log", "--from", "0"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn tail");
    let mut lines = BufReader::new(child.stdout.take().expect("stdout")).lines();

    let first = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("a line within five seconds")
        .expect("read")
        .expect("a line");
    assert_eq!(
        serde_json::from_str::<Value>(&first).expect("json")["version"],
        json!(1)
    );

    // Written after the tail attached: it has to arrive without another
    // invocation, and it has to be flushed rather than sitting in a buffer.
    mock.sim
        .append(&[json!({ "Create": { "b": 2 } })])
        .expect("append");
    let second = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("a second line")
        .expect("read")
        .expect("a line");
    assert_eq!(
        serde_json::from_str::<Value>(&second).expect("json")["version"],
        json!(2)
    );

    let pid = child.id().expect("pid");
    std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM");
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("tail exits promptly")
        .expect("wait");
    // Being interrupted is how a tail ends, so it is not a failure.
    assert!(status.success(), "tail exited with {status}");
}

#[tokio::test]
async fn ls_has_a_human_table_and_a_json_mode() {
    let mock = Mock::start("cli-ls").await;
    mock.sim
        .append(&[json!({ "Create": { "a": 1 } })])
        .expect("seed");

    let (ok, out, err) = output({
        let mut cmd = lug(&mock);
        cmd.arg("ls");
        cmd
    })
    .await;
    assert!(ok, "ls failed: {err}");
    assert!(out.contains("NAME"), "{out}");
    assert!(out.contains("log"), "{out}");

    let (ok, out, _) = output({
        let mut cmd = lug(&mock);
        cmd.args(["--json", "ls"]);
        cmd
    })
    .await;
    assert!(ok);
    for line in out.lines() {
        let info: Value = serde_json::from_str(line).expect("one log per line");
        assert!(info["name"].is_string());
    }
}

#[tokio::test]
async fn errors_go_to_stderr_with_a_nonzero_status() {
    let mock = Mock::start("cli-errors").await;

    let (ok, out, err) = output({
        let mut cmd = lug(&mock);
        cmd.args(["read", "absent"]);
        cmd
    })
    .await;
    assert!(!ok, "a missing log must fail");
    assert!(out.is_empty(), "nothing on stdout: {out}");
    assert!(err.contains("no log absent"), "{err}");

    // Nothing listening at all is the other common mistake, and it should say
    // so rather than hang.
    let missing = support::scratch("cli-absent").join("nothing.sock");
    let out = Command::new(env!("CARGO_BIN_EXE_lug"))
        .arg("--socket")
        .arg(&missing)
        .args(["--timeout", "1", "ls"])
        .output()
        .await
        .expect("run lug");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("no daemon at"), "{err}");
}

#[tokio::test]
async fn bad_usage_exits_two() {
    let out = Command::new(env!("CARGO_BIN_EXE_lug"))
        .arg("--nonsense")
        .output()
        .await
        .expect("run lug");
    assert_eq!(out.status.code(), Some(2), "clap's convention for misuse");
}

#[tokio::test]
async fn the_same_verbs_work_over_http_with_a_token_file() {
    let mock = support::http::Mock::start().await;
    let dir = support::scratch("cli-http");
    let token = dir.join("token");
    std::fs::write(&token, format!("{}\n", support::http::TOKEN)).expect("write token");

    let (ok, out, err) = output({
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_lug"));
        cmd.args(["--http", &mock.base]);
        cmd.arg("--token-file").arg(&token);
        cmd.args(["create", "notes", "--reducible"]);
        cmd.kill_on_drop(true);
        cmd
    })
    .await;
    assert!(ok, "create over http failed: {err}");
    assert!(out.contains("created notes"), "{out}");

    let mut child = Command::new(env!("CARGO_BIN_EXE_lug"))
        .args(["--http", &mock.base])
        .arg("--token-file")
        .arg(&token)
        .args(["append", "notes", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn append");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin
        .write_all(b"{\"Create\":{\"title\":\"over http\"}}\n")
        .await
        .expect("write patch");
    drop(stdin);
    assert!(child.wait().await.expect("append").success());

    let (ok, out, err) = output({
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_lug"));
        cmd.args(["--http", &mock.base]);
        cmd.arg("--token-file").arg(&token);
        cmd.args(["--json", "read", "notes"]);
        cmd.kill_on_drop(true);
        cmd
    })
    .await;
    assert!(ok, "read over http failed: {err}");
    let view: Value = serde_json::from_str(out.trim()).expect("one JSON object");
    assert_eq!(view["value"]["title"], json!("over http"));
}
