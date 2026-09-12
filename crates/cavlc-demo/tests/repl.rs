use serde_json::Value;
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn run(input: &str) -> (std::process::ExitStatus, Vec<Value>, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cavlc-demo"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(!text.contains('\x1b'));
    let responses = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (
        output.status,
        responses,
        String::from_utf8(output.stderr).unwrap(),
    )
}

#[test]
fn renders_current_and_pending_snapshots_after_every_update() {
    let (status, lines, errors) = run(r#"{"Create":{"count":1}}
:batch
{"Update":{"count":2}}
{"Create":{"name":"Gluck"}}
:apply
:show 1
:log
:quit
"#);
    assert!(status.success(), "{errors}");
    assert_eq!(lines.len(), 7);
    assert_eq!(lines[0]["snapshot"]["count"], 1);
    assert_eq!(lines[2]["snapshot"]["count"], 1);
    assert_eq!(lines[2]["batch"]["snapshot"]["count"], 2);
    assert_eq!(lines[3]["batch"]["snapshot"]["name"], "Gluck");
    assert_eq!(lines[4]["snapshot"]["count"], 2);
    assert_eq!(lines[4]["version"], 2);
    assert_eq!(lines[5]["detail"]["historical"]["snapshot"]["count"], 1);
    assert_eq!(lines[5]["snapshot"]["count"], 2);
    assert_eq!(lines[6]["detail"]["log"].as_array().unwrap().len(), 2);
    assert!(
        lines
            .iter()
            .all(|line| line["ok"] == true && line["snapshot"].is_object())
    );
}

#[test]
fn errors_leave_snapshot_intact_and_input_continues() {
    let (status, lines, errors) =
        run("not json\n{\"Create\":{\"a\":1}}\n{\"Update\":{\"missing\":99}}\n:show\n");
    assert_eq!(status.code(), Some(1));
    assert!(!errors.is_empty());
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0]["version"], 0);
    assert_eq!(lines[2]["snapshot"], lines[1]["snapshot"]);
    assert_eq!(lines[2]["version"], 1);
    assert_eq!(lines[3]["ok"], true);
}

#[test]
fn abort_discards_work_and_exit_flags_are_predictable() {
    let (status, lines, _) = run(":batch\n{\"Create\":{\"a\":1}}\n:abort\n");
    assert!(status.success());
    assert_eq!(lines[2]["version"], 0);
    assert_eq!(lines[2]["snapshot"], serde_json::json!({}));
    for flag in ["--help", "--version"] {
        assert!(
            Command::new(env!("CARGO_BIN_EXE_cavlc-demo"))
                .arg(flag)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
    assert_eq!(
        Command::new(env!("CARGO_BIN_EXE_cavlc-demo"))
            .arg("--bogus")
            .output()
            .unwrap()
            .status
            .code(),
        Some(2)
    );
}

#[test]
fn create_then_update_and_delete_without_old_values() {
    let (status, lines, _) = run(r#"{"Update":{"count":1}}
{"Create":{"count":0}}
{"Update":{"count":2}}
{"Delete":["count"]}
"#);
    assert_eq!(status.code(), Some(1));
    assert_eq!(lines[0]["ok"], false);
    assert_eq!(lines[0]["version"], 0);
    assert_eq!(lines[1]["snapshot"]["count"], 0);
    assert_eq!(lines[2]["snapshot"]["count"], 2);
    assert_eq!(lines[3]["snapshot"], serde_json::json!({}));
    assert_eq!(lines[3]["version"], 3);
}

#[test]
fn initialized_children_nested_delete_and_no_resurrection() {
    let (status, lines, _) = run(
        r#"{"Create":{"profile":{"nested":{"name":"Gluck","remove":true}}}}
{"Update":{"profile":{"Update":{"nested":{"Update":{"name":"Figaro"}}}}}}
{"Update":{"profile":{"Update":{"nested":{"Delete":["remove"]}}}}}
{"Delete":["profile"]}
{"Update":{"profile":{"Create":{"late":true}}}}
"#,
    );
    assert_eq!(status.code(), Some(1));
    assert_eq!(lines[0]["ok"], true);
    assert_eq!(
        lines[0]["snapshot"],
        serde_json::json!({"profile":{"nested":{"name":"Gluck","remove":true}}})
    );
    assert_eq!(
        lines[2]["snapshot"],
        serde_json::json!({"profile":{"nested":{"name":"Figaro"}}})
    );
    assert_eq!(lines[3]["snapshot"], serde_json::json!({}));
    assert_eq!(lines[4]["ok"], false);
    assert_eq!(lines[4]["version"], 4);
    assert_eq!(lines[4]["snapshot"], serde_json::json!({}));
}
