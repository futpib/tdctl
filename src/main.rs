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

    /// List available accounts
    ListAccounts,

    /// Export data from tdesktop
    Export {
        /// Output directory for exported data
        path: PathBuf,

        /// Export format: json, html, or html_and_json
        #[arg(long, default_value = "json")]
        format: String,

        /// Data types to export (defaults to all)
        #[arg(long = "type", value_delimiter = ',')]
        types: Vec<String>,

        /// Media types to download
        #[arg(long = "media-type", value_delimiter = ',')]
        media_types: Vec<String>,

        /// Media size limit in bytes
        #[arg(long)]
        media_size_limit: Option<u64>,

        /// Export messages after this Unix timestamp
        #[arg(long)]
        from_date: Option<i64>,

        /// Export messages before this Unix timestamp
        #[arg(long)]
        till_date: Option<i64>,
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
        Envelope::Tdlib => loop {
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
        },
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

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn format_export_progress(payload: &serde_json::Value) {
    let state = payload.get("state").and_then(|v| v.as_str()).unwrap_or("");
    match state {
        "processing" => {
            let entity_index = payload
                .get("entity_index")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let entity_count = payload
                .get("entity_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let entity_name = payload
                .get("entity_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let entity_type = payload
                .get("entity_type")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let item_index = payload
                .get("item_index")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let item_count = payload
                .get("item_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let label = if !entity_name.is_empty() {
                format!("{entity_type} \"{entity_name}\"")
            } else {
                entity_type.to_string()
            };

            let mut line =
                format!("[{entity_index}/{entity_count}] {label}: {item_index}/{item_count} items");

            if let Some(bytes_name) = payload.get("bytes_name").and_then(|v| v.as_str()) {
                let bytes_loaded = payload
                    .get("bytes_loaded")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let bytes_count = payload
                    .get("bytes_count")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                line.push_str(&format!(
                    " ({} {}/{})",
                    bytes_name,
                    format_bytes(bytes_loaded),
                    format_bytes(bytes_count),
                ));
            }

            eprintln!("{line}");
        }
        "finished" => {
            let path = payload.get("path").and_then(|v| v.as_str()).unwrap_or("");
            let files_count = payload
                .get("files_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let bytes_count = payload
                .get("bytes_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            eprintln!(
                "Export finished: {} files, {} written to {}",
                files_count,
                format_bytes(bytes_count),
                path,
            );
        }
        "error" => {
            let message = payload
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            if let Some(path) = payload.get("path").and_then(|v| v.as_str()) {
                eprintln!("Export error: {message} ({path})");
            } else {
                eprintln!("Export error: {message}");
            }
        }
        "cancelled" => {
            eprintln!("Export cancelled");
        }
        _ => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let socket_path = cli.socket.unwrap_or_else(default_socket_path);
    let account = cli.account;

    let (json_arg, envelope) = match &cli.command {
        Command::Raw { json } => (json.clone(), Envelope::None),
        Command::Tdlib { command } => match command {
            TdlibCommand::Raw { json } => (json.clone(), Envelope::Tdlib),
        },
        Command::Tdesktop { command } => match command {
            TdesktopCommand::Raw { json } => (json.clone(), Envelope::Tdesktop),
        },
        Command::ListAccounts => (None, Envelope::Tdesktop),
        Command::Export { .. } => (None, Envelope::Tdesktop),
    };

    let stream = UnixStream::connect(&socket_path).await?;
    let (rd, wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let mut writer = BufWriter::new(wr);

    if let Command::Export {
        path,
        format,
        types,
        media_types,
        media_size_limit,
        from_date,
        till_date,
    } = cli.command
    {
        let mut settings = serde_json::json!({
            "path": path.to_string_lossy(),
            "format": format,
        });

        if !types.is_empty() {
            settings["types"] = serde_json::json!(types);
        }

        if !media_types.is_empty() || media_size_limit.is_some() {
            let mut media = serde_json::Map::new();
            if !media_types.is_empty() {
                media.insert("types".to_string(), serde_json::json!(media_types));
            }
            if let Some(limit) = media_size_limit {
                media.insert("size_limit".to_string(), serde_json::json!(limit));
            }
            settings["media"] = serde_json::Value::Object(media);
        }

        if let Some(from) = from_date {
            settings["from_date"] = serde_json::json!(from);
        }
        if let Some(till) = till_date {
            settings["till_date"] = serde_json::json!(till);
        }

        let payload = serde_json::json!({"command": "export", "settings": settings});
        let message = wrap(&envelope, account, payload);
        let line = serde_json::to_string(&message)?;
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        // Read first response — expect exportStarted
        let mut response = String::new();
        reader.read_line(&mut response).await?;
        let parsed: serde_json::Value = serde_json::from_str(response.trim())?;
        let first_payload = &parsed["payload"];

        let command = first_payload
            .get("command")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if command != "exportStarted" {
            let state = first_payload
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if state == "error" {
                format_export_progress(first_payload);
            } else {
                eprintln!(
                    "Export error: unexpected response: {}",
                    serde_json::to_string(first_payload)?
                );
            }
            std::process::exit(1);
        }

        eprintln!("Export started");

        // Streaming loop
        let mut cancelled = false;
        loop {
            let mut line = String::new();
            tokio::select! {
                result = reader.read_line(&mut line) => {
                    let n = result?;
                    if n == 0 {
                        eprintln!("Connection closed");
                        break;
                    }
                    let parsed: serde_json::Value = serde_json::from_str(line.trim())?;
                    let payload = &parsed["payload"];
                    let state = payload.get("state").and_then(|v| v.as_str()).unwrap_or("");

                    format_export_progress(payload);

                    match state {
                        "finished" | "cancelled" | "error" => break,
                        _ => {}
                    }
                }
                _ = tokio::signal::ctrl_c(), if !cancelled => {
                    cancelled = true;
                    eprintln!("Cancelling export...");
                    let cancel_payload = serde_json::json!({"command": "cancelExport"});
                    let cancel_message = wrap(&Envelope::Tdesktop, account, cancel_payload);
                    let cancel_line = serde_json::to_string(&cancel_message)?;
                    writer.write_all(cancel_line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                }
            }
        }

        return Ok(());
    }

    if matches!(cli.command, Command::ListAccounts) {
        let payload = serde_json::json!({"command": "listAccounts"});
        let message = wrap(&envelope, account, payload);
        let line = serde_json::to_string(&message)?;
        writer.write_all(line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        let mut response = String::new();
        reader.read_line(&mut response).await?;
        let parsed: serde_json::Value = serde_json::from_str(response.trim())?;
        let payload = &parsed["payload"];

        if let Some(accounts) = payload.get("accounts").and_then(|v| v.as_array()) {
            for account in accounts {
                let index = account.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
                let mut parts = vec![format!("account {index}")];

                if let Some(name) = account.get("first_name").and_then(|v| v.as_str()) {
                    let full_name = match account.get("last_name").and_then(|v| v.as_str()) {
                        Some(last) => format!("{name} {last}"),
                        None => name.to_string(),
                    };
                    parts.push(full_name);
                }

                if let Some(username) = account.get("username").and_then(|v| v.as_str()) {
                    parts.push(format!("@{username}"));
                }

                if let Some(phone) = account.get("phone").and_then(|v| v.as_str()) {
                    parts.push(format!("+{phone}"));
                }

                println!("{}", parts.join("\t"));
            }
        }

        return Ok(());
    }

    match json_arg {
        Some(text) => {
            let mut payload = parse_json(&text)?;
            let extra = inject_extra(&envelope, &mut payload);
            let message = wrap(&envelope, account, payload);
            send_and_receive(
                &mut reader,
                &mut writer,
                &message,
                &envelope,
                extra.as_deref(),
            )
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
                send_and_receive(
                    &mut reader,
                    &mut writer,
                    &message,
                    &envelope,
                    extra.as_deref(),
                )
                .await?;
            }
        }
    }

    Ok(())
}
