use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;

static EXTRA_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Parser)]
#[command(about = "CLI client for tdesktop Unix socket API")]
struct Cli {
    /// Path to the tdesktop Unix socket
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

    /// Account index
    #[arg(short, long, global = true, default_value_t = 0)]
    account: u32,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send raw JSON to the socket
    Raw {
        /// JSON to send (reads from stdin if omitted)
        json: Option<String>,
    },

    /// TDLib commands
    Tdlib {
        #[command(subcommand)]
        command: TdlibCommand,
    },

    /// Tdesktop commands
    Tdesktop {
        #[command(subcommand)]
        command: TdesktopCommand,
    },
}

#[derive(Subcommand)]
enum TdlibCommand {
    /// Send raw JSON wrapped in a tdlib envelope
    Raw {
        /// JSON to send as payload (reads from stdin if omitted)
        json: Option<String>,
    },
}

#[derive(Subcommand)]
enum TdesktopCommand {
    /// Send raw JSON wrapped in a tdesktop envelope
    Raw {
        /// JSON to send as payload (reads from stdin if omitted)
        json: Option<String>,
    },
}

fn default_socket_path() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("tdesktop.sock");
    }

    let uid = unsafe { libc::getuid() };
    PathBuf::from(format!("/tmp/tdesktop-{uid}/tdesktop.sock"))
}

fn parse_json(text: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let value: serde_json::Value = serde_json::from_str(text.trim())?;
    Ok(value)
}

async fn send_and_receive(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    message: &serde_json::Value,
    envelope: &Envelope,
    expected_extra: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let line = serde_json::to_string(message)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    match envelope {
        Envelope::None => {
            let mut response = String::new();
            reader.read_line(&mut response).await?;
            print!("{response}");
        }
        Envelope::Tdesktop => {
            let mut response = String::new();
            reader.read_line(&mut response).await?;
            let parsed: serde_json::Value = serde_json::from_str(response.trim())?;
            let payload = &parsed["payload"];
            println!("{}", serde_json::to_string(payload)?);
        }
        Envelope::Tdlib => {
            loop {
                let mut response = String::new();
                let n = reader.read_line(&mut response).await?;
                if n == 0 {
                    return Err("unexpected EOF while waiting for response".into());
                }
                let parsed: serde_json::Value = serde_json::from_str(response.trim())?;
                let mut payload = parsed["payload"].clone();
                if let Some(extra) = expected_extra {
                    let matches = payload
                        .get("@extra")
                        .and_then(|v| v.get("tdctl"))
                        .and_then(|v| v.as_str())
                        == Some(extra);
                    if matches {
                        strip_extra(&mut payload);
                        println!("{}", serde_json::to_string(&payload)?);
                        break;
                    }
                } else {
                    strip_extra(&mut payload);
                    println!("{}", serde_json::to_string(&payload)?);
                    break;
                }
            }
        }
    }

    Ok(())
}

enum Envelope {
    None,
    Tdlib,
    Tdesktop,
}

fn inject_extra(envelope: &Envelope, payload: &mut serde_json::Value) -> Option<String> {
    if !matches!(envelope, Envelope::Tdlib) {
        return None;
    }
    let tdctl_id = format!("tdctl-{}", EXTRA_COUNTER.fetch_add(1, Ordering::Relaxed));
    let mut extra_obj = serde_json::json!({ "tdctl": tdctl_id });
    if let Some(user_extra) = payload.get("@extra") {
        extra_obj["user"] = user_extra.clone();
    }
    payload["@extra"] = extra_obj;
    Some(tdctl_id)
}

fn strip_extra(payload: &mut serde_json::Value) {
    let Some(extra) = payload.get("@extra") else {
        return;
    };
    if let Some(user_extra) = extra.get("user") {
        payload["@extra"] = user_extra.clone();
    } else {
        payload.as_object_mut().unwrap().remove("@extra");
    }
}

fn wrap(envelope: &Envelope, account: u32, payload: serde_json::Value) -> serde_json::Value {
    match envelope {
        Envelope::None => payload,
        Envelope::Tdlib => {
            serde_json::json!({ "type": "tdlib", "account": account, "payload": payload })
        }
        Envelope::Tdesktop => {
            serde_json::json!({ "type": "tdesktop", "account": account, "payload": payload })
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let socket_path = cli.socket.unwrap_or_else(default_socket_path);
    let account = cli.account;

    let (json_arg, envelope) = match cli.command {
        Command::Raw { json } => (json, Envelope::None),
        Command::Tdlib { command } => match command {
            TdlibCommand::Raw { json } => (json, Envelope::Tdlib),
        },
        Command::Tdesktop { command } => match command {
            TdesktopCommand::Raw { json } => (json, Envelope::Tdesktop),
        },
    };

    let stream = UnixStream::connect(&socket_path).await?;
    let (rd, wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let mut writer = BufWriter::new(wr);

    match json_arg {
        Some(text) => {
            let mut payload = parse_json(&text)?;
            let extra = inject_extra(&envelope, &mut payload);
            let message = wrap(&envelope, account, payload);
            send_and_receive(&mut reader, &mut writer, &message, &envelope, extra.as_deref())
                .await?;
        }
        None => {
            use std::io::BufRead;
            let stdin = std::io::stdin().lock();
            for line in stdin.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let mut payload = parse_json(&line)?;
                let extra = inject_extra(&envelope, &mut payload);
                let message = wrap(&envelope, account, payload);
                send_and_receive(&mut reader, &mut writer, &message, &envelope, extra.as_deref())
                    .await?;
            }
        }
    }

    Ok(())
}
