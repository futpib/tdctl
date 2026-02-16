use std::path::PathBuf;
use std::process::Command;

use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

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
        stdin.write_all(b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n").unwrap();
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
        stdin
            .write_all(b"\n{\"a\":1}\n\n\n{\"a\":2}\n\n")
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
    assert_eq!(responses[0], serde_json::json!({"a": 1}));
    assert_eq!(responses[1], serde_json::json!({"a": 2}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tdlib_raw_envelope() {
    let server = start_echo_server().await;
    let output = tdctl_cmd(&server.socket_path)
        .args(["tdlib", "raw", r#"{"method":"getMe"}"#])
        .output()
        .unwrap();

    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response, serde_json::json!({"method": "getMe"}));
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
