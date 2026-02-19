use std::path::PathBuf;
use std::process::Command;

use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::time::{Duration, sleep};

struct MockServer {
    socket_path: PathBuf,
    _tempdir: TempDir,
}

async fn start_echo_server() -> MockServer {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    wr.write_all(line.as_bytes()).await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    MockServer {
        socket_path,
        _tempdir: tempdir,
    }
}

/// A mock server that, for each incoming line, sends several fake update lines
/// (TDLib envelope with payloads that have no matching @extra) before echoing
/// the original line back.
async fn start_noisy_echo_server() -> MockServer {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    // Send fake updates before the real response
                    let updates = [
                        r#"{"type":"tdlib","payload":{"@type":"updateUser"}}"#,
                        r#"{"type":"tdlib","payload":{"@type":"updateChat","@extra":"unrelated"}}"#,
                        r#"{"type":"tdlib","payload":{"@type":"updateOption"}}"#,
                    ];
                    for update in &updates {
                        wr.write_all(update.as_bytes()).await.unwrap();
                        wr.write_all(b"\n").await.unwrap();
                    }
                    // Echo back the original line
                    wr.write_all(line.as_bytes()).await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    MockServer {
        socket_path,
        _tempdir: tempdir,
    }
}

fn tdctl_cmd(socket_path: &PathBuf) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tdctl"));
    cmd.arg("--socket").arg(socket_path);
    cmd
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_single_json_arg() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["raw", r#"{"a":1}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response, serde_json::json!({"a": 1}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_single_json_stdin() {
    let server = start_echo_server().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_tdctl"))
        .arg("--socket")
        .arg(&server.socket_path)
        .arg("raw")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"{\"b\":2}\n").unwrap();
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response, serde_json::json!({"b": 2}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_multiple_json_stdin() {
    let server = start_echo_server().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_tdctl"))
        .arg("--socket")
        .arg(&server.socket_path)
        .arg("raw")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().unwrap();
        stdin
            .write_all(b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n")
            .unwrap();
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());

    let responses: Vec<serde_json::Value> = output
        .stdout
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0], serde_json::json!({"x": 1}));
    assert_eq!(responses[1], serde_json::json!({"x": 2}));
    assert_eq!(responses[2], serde_json::json!({"x": 3}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_empty_lines_skipped() {
    let server = start_echo_server().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_tdctl"))
        .arg("--socket")
        .arg(&server.socket_path)
        .arg("raw")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"\n{\"a\":1}\n\n\n{\"a\":2}\n\n").unwrap();
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());

    let responses: Vec<serde_json::Value> = output
        .stdout
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0], serde_json::json!({"a": 1}));
    assert_eq!(responses[1], serde_json::json!({"a": 2}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdlib_raw_envelope() {
    let server = start_noisy_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["tdlib", "raw", r#"{"@type":"getMe","@extra":"1"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    // User's @extra should be preserved as-is
    assert_eq!(
        response,
        serde_json::json!({"@type": "getMe", "@extra": "1"})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdlib_raw_auto_extra() {
    let server = start_noisy_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["tdlib", "raw", r#"{"@type":"getMe"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "getMe");
    // No user-provided @extra, so it should be stripped from the response
    assert!(response.get("@extra").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdesktop_raw_envelope() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["tdesktop", "raw", r#"{"action":"quit"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response, serde_json::json!({"action": "quit"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdlib_raw_account_flag() {
    // Server that copies the envelope's "account" field into the payload before echoing
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    let mut envelope: serde_json::Value =
                        serde_json::from_str(line.trim()).unwrap();
                    let account = envelope["account"].clone();
                    envelope["payload"]["account"] = account;
                    let response = serde_json::to_string(&envelope).unwrap();
                    wr.write_all(response.as_bytes()).await.unwrap();
                    wr.write_all(b"\n").await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    let output = tdctl_cmd(&socket_path)
        .args(["-a", "3", "tdesktop", "raw", r#"{"@type":"getMe"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "getMe");
    assert_eq!(response["account"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdesktop_list_accounts() {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                    let response = serde_json::json!({
                        "type": envelope["type"],
                        "payload": {
                            "command": "listAccounts",
                            "accounts": [
                                {"index": 0, "first_name": "John", "last_name": "Doe", "username": "johndoe", "phone": "1234567890"},
                                {"index": 1, "first_name": "Jane", "username": "jane"},
                                {"index": 2}
                            ]
                        }
                    });
                    let response_str = serde_json::to_string(&response).unwrap();
                    wr.write_all(response_str.as_bytes()).await.unwrap();
                    wr.write_all(b"\n").await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    let output = tdctl_cmd(&socket_path)
        .args(["list-accounts"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "account 0\tJohn Doe\t@johndoe\t+1234567890");
    assert_eq!(lines[1], "account 1\tJane\t@jane");
    assert_eq!(lines[2], "account 2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_invalid_json_error() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["raw", "not-json"])
        .output()
        .unwrap();

    assert!(!output.status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_empty_stdin_exits_cleanly() {
    let server = start_echo_server().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_tdctl"))
        .arg("--socket")
        .arg(&server.socket_path)
        .arg("raw")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}

async fn start_export_server() -> MockServer {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();

                // Read the export request
                let n = reader.read_line(&mut line).await.unwrap();
                if n == 0 {
                    return;
                }

                let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                let account = envelope
                    .get("account")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                // Send exportStarted
                let started = serde_json::json!({
                    "type": "tdesktop",
                    "payload": {"command": "exportStarted", "account": account}
                });
                wr.write_all(serde_json::to_string(&started).unwrap().as_bytes())
                    .await
                    .unwrap();
                wr.write_all(b"\n").await.unwrap();
                wr.flush().await.unwrap();

                // Send progress updates
                let updates = [
                    serde_json::json!({
                        "type": "tdesktop",
                        "payload": {
                            "command": "exportProgress", "account": account,
                            "state": "processing", "step": "Dialogs",
                            "entity_type": "Chat", "entity_name": "John Doe",
                            "entity_index": 1, "entity_count": 3,
                            "item_index": 50, "item_count": 100
                        }
                    }),
                    serde_json::json!({
                        "type": "tdesktop",
                        "payload": {
                            "command": "exportProgress", "account": account,
                            "state": "processing", "step": "Dialogs",
                            "entity_type": "Chat", "entity_name": "Photos",
                            "entity_index": 2, "entity_count": 3,
                            "item_index": 5, "item_count": 20,
                            "bytes_loaded": 524288, "bytes_count": 2097152,
                            "bytes_name": "photo.jpg"
                        }
                    }),
                    serde_json::json!({
                        "type": "tdesktop",
                        "payload": {
                            "command": "exportProgress", "account": account,
                            "state": "finished",
                            "path": "/tmp/export/test",
                            "files_count": 42,
                            "bytes_count": 52428800
                        }
                    }),
                ];

                for update in &updates {
                    wr.write_all(serde_json::to_string(update).unwrap().as_bytes())
                        .await
                        .unwrap();
                    wr.write_all(b"\n").await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    MockServer {
        socket_path,
        _tempdir: tempdir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_export_progress() {
    let server = start_export_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["export", "/tmp/test"])
        .output()
        .unwrap();

    let stderr = String::from_utf8(output.stderr).unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );
    assert!(stderr.contains("Export started"));
    assert!(stderr.contains("[1/3] Chat \"John Doe\": 50/100 items"));
    assert!(stderr.contains("photo.jpg"));
    assert!(stderr.contains("Export finished: 42 files"));
}

async fn start_slow_export_server() -> MockServer {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();

                // Read the export request
                let n = reader.read_line(&mut line).await.unwrap();
                if n == 0 {
                    return;
                }

                let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                let account = envelope
                    .get("account")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                // Send exportStarted
                let started = serde_json::json!({
                    "type": "tdesktop",
                    "payload": {"command": "exportStarted", "account": account}
                });
                wr.write_all(serde_json::to_string(&started).unwrap().as_bytes())
                    .await
                    .unwrap();
                wr.write_all(b"\n").await.unwrap();
                wr.flush().await.unwrap();

                // Send slow progress updates, checking for cancelExport between each
                let mut cancelled_by_client = false;
                for i in 1..=20 {
                    sleep(Duration::from_millis(50)).await;

                    // Check if client sent cancelExport
                    line.clear();
                    let cancel_check = tokio::time::timeout(
                        Duration::from_millis(1),
                        reader.read_line(&mut line),
                    )
                    .await;
                    if let Ok(Ok(n)) = cancel_check {
                        if n > 0 {
                            cancelled_by_client = true;
                            break;
                        }
                    }

                    let update = serde_json::json!({
                        "type": "tdesktop",
                        "payload": {
                            "command": "exportProgress", "account": account,
                            "state": "processing", "step": "Dialogs",
                            "entity_type": "Chat", "entity_name": "Test",
                            "entity_index": 1, "entity_count": 1,
                            "item_index": i, "item_count": 100
                        }
                    });
                    if wr
                        .write_all(serde_json::to_string(&update).unwrap().as_bytes())
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if wr.write_all(b"\n").await.is_err() {
                        return;
                    }
                    if wr.flush().await.is_err() {
                        return;
                    }
                }

                if !cancelled_by_client {
                    // Wait for cancelExport
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        return;
                    }
                }

                // Send cancelled
                let cancelled = serde_json::json!({
                    "type": "tdesktop",
                    "payload": {
                        "command": "exportProgress", "account": account,
                        "state": "cancelled"
                    }
                });
                wr.write_all(serde_json::to_string(&cancelled).unwrap().as_bytes())
                    .await
                    .unwrap();
                wr.write_all(b"\n").await.unwrap();
                wr.flush().await.unwrap();
            });
        }
    });

    MockServer {
        socket_path,
        _tempdir: tempdir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_export_cancel() {
    let server = start_slow_export_server().await;
    let child = Command::new(env!("CARGO_BIN_EXE_tdctl"))
        .arg("--socket")
        .arg(&server.socket_path)
        .args(["export", "/tmp/test"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let pid = child.id() as i32;

    // Wait a bit for some progress, then send SIGINT
    sleep(Duration::from_millis(200)).await;
    unsafe {
        libc::kill(pid, libc::SIGINT);
    }

    let output = child.wait_with_output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );
    assert!(stderr.contains("Export started"));
    assert!(stderr.contains("Cancelling export"));
    assert!(stderr.contains("Export cancelled"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mtp_raw_envelope() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["mtp", "raw", r#"{"@type":"help.getNearestDc"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response, serde_json::json!({"@type": "help.getNearestDc"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mtp_raw_stdin() {
    let server = start_echo_server().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_tdctl"))
        .arg("--socket")
        .arg(&server.socket_path)
        .args(["mtp", "raw"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().unwrap();
        stdin
            .write_all(b"{\"@type\":\"help.getNearestDc\"}\n{\"@type\":\"help.getConfig\"}\n")
            .unwrap();
    }
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());

    let responses: Vec<serde_json::Value> = output
        .stdout
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();

    assert_eq!(responses.len(), 2);
    assert_eq!(
        responses[0],
        serde_json::json!({"@type": "help.getNearestDc"})
    );
    assert_eq!(responses[1], serde_json::json!({"@type": "help.getConfig"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mtp_raw_account_flag() {
    // Server that copies the envelope's "account" field into the payload before echoing
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    let mut envelope: serde_json::Value =
                        serde_json::from_str(line.trim()).unwrap();
                    let account = envelope["account"].clone();
                    envelope["payload"]["account"] = account;
                    let response = serde_json::to_string(&envelope).unwrap();
                    wr.write_all(response.as_bytes()).await.unwrap();
                    wr.write_all(b"\n").await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    let output = tdctl_cmd(&socket_path)
        .args(["-a", "2", "mtp", "raw", r#"{"@type":"help.getNearestDc"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "help.getNearestDc");
    assert_eq!(response["account"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_error_response_exit_code() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args([
            "mtp",
            "raw",
            r#"{"@type":"error","code":400,"message":"INPUT_METHOD_INVALID"}"#,
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "error");
    assert_eq!(response["code"], 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdlib_error_response_exit_code() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args([
            "tdlib",
            "raw",
            r#"{"@type":"error","code":400,"message":"Bad Request"}"#,
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_error_response_exit_code() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["raw", r#"{"@type":"error","code":400,"message":"test"}"#])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "error");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mtp_raw_skips_unsolicited_updates() {
    let server = start_noisy_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["mtp", "raw", r#"{"@type":"help.getNearestDc"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    // Should get the mtp response, not a tdlib update
    assert_eq!(response["@type"], "help.getNearestDc");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdesktop_raw_skips_unsolicited_updates() {
    let server = start_noisy_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["tdesktop", "raw", r#"{"command":"listAccounts"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["command"], "listAccounts");
}

/// A mock server that echoes the raw envelope JSON (not just the payload).
async fn start_envelope_echo_server() -> MockServer {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    // Parse envelope, copy "account" (or lack thereof) into payload, echo back
                    let mut envelope: serde_json::Value =
                        serde_json::from_str(line.trim()).unwrap();
                    let has_account = envelope.get("account").is_some();
                    envelope["payload"]["has_account"] = serde_json::json!(has_account);
                    if has_account {
                        let account = envelope["account"].clone();
                        envelope["payload"]["account"] = account;
                    }
                    let response = serde_json::to_string(&envelope).unwrap();
                    wr.write_all(response.as_bytes()).await.unwrap();
                    wr.write_all(b"\n").await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    MockServer {
        socket_path,
        _tempdir: tempdir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_account_omitted_by_default() {
    let server = start_envelope_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["mtp", "raw", r#"{"@type":"help.getNearestDc"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "help.getNearestDc");
    assert_eq!(response["has_account"], false);
    assert!(response.get("account").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_account_included_when_specified() {
    let server = start_envelope_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["-a", "2", "mtp", "raw", r#"{"@type":"help.getNearestDc"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["@type"], "help.getNearestDc");
    assert_eq!(response["has_account"], true);
    assert_eq!(response["account"], 2);
}

/// A mock TDLib server that handles searchPublicChat and getChatHistory requests.
/// Returns fake messages for getChatHistory, with pagination support.
async fn start_tdlib_chat_server() -> MockServer {
    let tempdir = TempDir::new().unwrap();
    let socket_path = tempdir.path().join("test.sock");

    let listener = UnixListener::bind(&socket_path).unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.into_split();
                let mut reader = BufReader::new(rd);
                let mut line = String::new();
                loop {
                    line.clear();
                    let n = reader.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
                    let payload = &envelope["payload"];
                    let extra = payload.get("@extra").cloned();
                    let req_type = payload.get("@type").and_then(|v| v.as_str()).unwrap_or("");

                    let response_payload = match req_type {
                        "searchPublicChat" => {
                            let mut resp = serde_json::json!({
                                "@type": "chat",
                                "id": -1001234567890_i64,
                                "title": "Test Channel",
                            });
                            if let Some(e) = &extra {
                                resp["@extra"] = e.clone();
                            }
                            resp
                        }
                        "getChatHistory" => {
                            let from_id = payload
                                .get("from_message_id")
                                .and_then(|v| v.as_i64())
                                .unwrap_or(0);

                            // Generate messages: IDs 5,4,3,2,1
                            // from_id=0 means start from latest (return 5,4,3)
                            // from_id=3 means return 2,1
                            // from_id=1 means return empty (exhausted)
                            let all_messages: Vec<serde_json::Value> = (1..=5)
                                .rev()
                                .map(|i| {
                                    serde_json::json!({
                                        "@type": "message",
                                        "id": i,
                                        "chat_id": -1001234567890_i64,
                                        "date": 1708185600 + i * 60,
                                        "author_signature": "Pavel Durov",
                                        "content": {
                                            "@type": "messageText",
                                            "text": {
                                                "@type": "formattedText",
                                                "text": format!("Message {i}"),
                                            }
                                        },
                                        "interaction_info": {
                                            "view_count": i * 1000,
                                        }
                                    })
                                })
                                .collect();

                            let messages: Vec<serde_json::Value> = if from_id == 0 {
                                // First batch: return first 3 (IDs 5,4,3)
                                all_messages.into_iter().take(3).collect()
                            } else {
                                // Return messages with ID < from_id
                                all_messages
                                    .into_iter()
                                    .filter(|m| {
                                        m.get("id").and_then(|v| v.as_i64()).unwrap_or(0) < from_id
                                    })
                                    .collect()
                            };

                            let mut resp = serde_json::json!({
                                "@type": "messages",
                                "total_count": messages.len(),
                                "messages": messages,
                            });
                            if let Some(e) = &extra {
                                resp["@extra"] = e.clone();
                            }
                            resp
                        }
                        _ => {
                            let mut resp = serde_json::json!({
                                "@type": "error",
                                "code": 404,
                                "message": "Not Found",
                            });
                            if let Some(e) = &extra {
                                resp["@extra"] = e.clone();
                            }
                            resp
                        }
                    };

                    let response = serde_json::json!({
                        "type": "tdlib",
                        "payload": response_payload,
                    });
                    let response_str = serde_json::to_string(&response).unwrap();
                    wr.write_all(response_str.as_bytes()).await.unwrap();
                    wr.write_all(b"\n").await.unwrap();
                    wr.flush().await.unwrap();
                }
            });
        }
    });

    MockServer {
        socket_path,
        _tempdir: tempdir,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_history_json() {
    let server = start_tdlib_chat_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["get-history", "-1001234567890", "--json"])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );

    let messages: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(messages.len(), 5);
    assert_eq!(messages[0]["id"], 5);
    assert_eq!(messages[4]["id"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_history_human() {
    let server = start_tdlib_chat_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["get-history", "-1001234567890"])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );

    assert!(stdout.contains("#5"), "should contain message ID #5");
    assert!(stdout.contains("#1"), "should contain message ID #1");
    assert!(stdout.contains("Pavel Durov"), "should contain author");
    assert!(stdout.contains("Message 5"), "should contain message text");
    assert!(stdout.contains("views"), "should contain view count");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_history_limit() {
    let server = start_tdlib_chat_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["get-history", "-1001234567890", "--json", "--limit", "2"])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );

    let messages: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["id"], 5);
    assert_eq!(messages[1]["id"], 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_history_username() {
    let server = start_tdlib_chat_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["get-history", "@testchannel", "--json", "--limit", "1"])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );

    let messages: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(messages.len(), 1);
    // Chat ID should be from the resolved searchPublicChat
    assert_eq!(messages[0]["chat_id"], -1001234567890_i64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_history_pagination() {
    let server = start_tdlib_chat_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["get-history", "-1001234567890", "--json"])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        output.status.success(),
        "exit status: {:?}\nstderr: {stderr}\nstdout: {stdout}",
        output.status
    );

    let messages: Vec<serde_json::Value> = stdout
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    // Server returns 3 in first batch (5,4,3), then 2 in second batch (2,1)
    assert_eq!(
        messages.len(),
        5,
        "should fetch all 5 messages across 2 batches"
    );
    // Verify ordering: newest first
    let ids: Vec<i64> = messages.iter().map(|m| m["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![5, 4, 3, 2, 1]);
}
