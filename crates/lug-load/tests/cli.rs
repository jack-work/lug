use std::process::Command;

#[test]
fn missing_server_is_a_machine_readable_skip_not_a_measurement() {
    let output = Command::new(env!("CARGO_BIN_EXE_lug-load"))
        .args([
            "--smoke",
            "--server",
            "/definitely-absent-lug-load-test-server",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "skipped");
    assert!(report.get("epochs").is_none());
    assert!(report["rlimit_nofile"]["soft"].as_u64().unwrap() > 0);
    assert!(String::from_utf8(output.stderr).unwrap().contains("SKIP"));
}

#[test]
fn a_present_but_exiting_server_is_not_a_skip() {
    let output = Command::new(env!("CARGO_BIN_EXE_lug-load"))
        .args(["--smoke", "--server", "/bin/false", "--json"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "failed");
    assert!(report["error"].as_str().unwrap().contains("startup"));
}

#[test]
fn help_and_invalid_flag_exit_codes() {
    let help = Command::new(env!("CARGO_BIN_EXE_lug-load"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8(help.stdout).unwrap().contains("--smoke"));
    let invalid = Command::new(env!("CARGO_BIN_EXE_lug-load"))
        .args(["--rate", "0"])
        .output()
        .unwrap();
    assert_eq!(invalid.status.code(), Some(2));
}
