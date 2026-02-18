#![feature(async_iterator, gen_blocks)]

use std::async_iter::AsyncIterator;
use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{Local, TimeZone};
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
    #[arg(short, long, global = true)]
    account: Option<u32>,

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

    /// MTP (raw Telegram API) commands
    Mtp {
        #[command(subcommand)]
        command: MtpCommand,
    },

    /// Browse chat message history
    GetHistory {
        /// Chat ID or @username
        #[arg(allow_hyphen_values = true)]
        chat: String,

        /// Start from this message ID (0 = latest)
        #[arg(long, default_value_t = 0)]
        from: i64,

        /// Maximum messages to fetch (0 = unlimited)
        #[arg(long, default_value_t = 0)]
        limit: u64,

        /// Output raw JSON (one message per line)
        #[arg(long)]
        json: bool,
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

#[derive(Subcommand)]
enum MtpCommand {
    /// Send raw JSON wrapped in an mtp envelope
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

fn is_error_payload(payload: &serde_json::Value) -> bool {
    payload.get("@type").and_then(|v| v.as_str()) == Some("error")
}

/// Returns true if the response was an error.
async fn send_and_receive(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    message: &serde_json::Value,
    envelope: &Envelope,
    expected_extra: Option<&str>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let line = serde_json::to_string(message)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let is_error = match envelope {
        Envelope::None => {
            let mut response = String::new();
            reader.read_line(&mut response).await?;
            let is_err = serde_json::from_str::<serde_json::Value>(response.trim())
                .map(|v| is_error_payload(&v))
                .unwrap_or(false);
            print!("{response}");
            is_err
        }
        Envelope::Tdesktop | Envelope::Mtp => {
            let expected_type = match envelope {
                Envelope::Tdesktop => "tdesktop",
                Envelope::Mtp => "mtp",
                _ => unreachable!(),
            };
            loop {
                let mut response = String::new();
                let n = reader.read_line(&mut response).await?;
                if n == 0 {
                    return Err("unexpected EOF while waiting for response".into());
                }
                let parsed: serde_json::Value = serde_json::from_str(response.trim())?;
                if parsed.get("type").and_then(|v| v.as_str()) != Some(expected_type) {
                    continue;
                }
                let payload = &parsed["payload"];
                println!("{}", serde_json::to_string(payload)?);
                break is_error_payload(payload);
            }
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
                    let is_err = is_error_payload(&payload);
                    println!("{}", serde_json::to_string(&payload)?);
                    break is_err;
                }
            } else {
                strip_extra(&mut payload);
                let is_err = is_error_payload(&payload);
                println!("{}", serde_json::to_string(&payload)?);
                break is_err;
            }
        },
    };

    Ok(is_error)
}

enum Envelope {
    None,
    Tdlib,
    Tdesktop,
    Mtp,
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

fn wrap(
    envelope: &Envelope,
    account: Option<u32>,
    payload: serde_json::Value,
) -> serde_json::Value {
    match envelope {
        Envelope::None => payload,
        Envelope::Tdlib | Envelope::Tdesktop | Envelope::Mtp => {
            let type_str = match envelope {
                Envelope::Tdlib => "tdlib",
                Envelope::Tdesktop => "tdesktop",
                Envelope::Mtp => "mtp",
                _ => unreachable!(),
            };
            let mut msg = serde_json::json!({ "type": type_str, "payload": payload });
            if let Some(account) = account {
                msg["account"] = serde_json::json!(account);
            }
            msg
        }
    }
}

async fn send_tdlib_request(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    account: Option<u32>,
    mut payload: serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let extra = inject_extra(&Envelope::Tdlib, &mut payload);
    let expected_extra = extra.as_deref().unwrap();
    let message = wrap(&Envelope::Tdlib, account, payload);

    let line = serde_json::to_string(&message)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    loop {
        let mut response = String::new();
        let n = reader.read_line(&mut response).await?;
        if n == 0 {
            return Err("unexpected EOF while waiting for response".into());
        }
        let parsed: serde_json::Value = serde_json::from_str(response.trim())?;
        let mut payload = parsed["payload"].clone();
        let matches = payload
            .get("@extra")
            .and_then(|v| v.get("tdctl"))
            .and_then(|v| v.as_str())
            == Some(expected_extra);
        if matches {
            strip_extra(&mut payload);
            return Ok(payload);
        }
    }
}

async fn resolve_chat(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    account: Option<u32>,
    chat: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    if let Ok(id) = chat.parse::<i64>() {
        return Ok(id);
    }

    let username = chat.strip_prefix('@').unwrap_or(chat);
    let payload = serde_json::json!({
        "@type": "searchPublicChat",
        "username": username,
    });
    let response = send_tdlib_request(reader, writer, account, payload).await?;
    if is_error_payload(&response) {
        let message = response
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        return Err(format!("Failed to resolve chat '{}': {}", chat, message).into());
    }
    response
        .get("id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| format!("No id in searchPublicChat response for '{}'", chat).into())
}

#[derive(Default)]
struct NameCache {
    users: HashMap<i64, String>,
    chats: HashMap<i64, String>,
}

fn primary_username(response: &serde_json::Value) -> Option<&str> {
    response
        .get("usernames")
        .and_then(|v| v.get("active_usernames"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
}

fn format_display_name(id: i64, handle: Option<&str>, full_name: &str) -> String {
    let mut parts = vec![format!("#{id}")];
    if let Some(h) = handle {
        parts.push(format!("@{h}"));
    }
    if !full_name.is_empty() {
        parts.push(format!("({full_name})"));
    }
    parts.join(" ")
}

impl NameCache {
    async fn resolve_user(
        &mut self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
        writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
        account: Option<u32>,
        user_id: i64,
    ) -> String {
        if let Some(name) = self.users.get(&user_id) {
            return name.clone();
        }
        let payload = serde_json::json!({
            "@type": "getUser",
            "user_id": user_id,
        });
        let name = match send_tdlib_request(reader, writer, account, payload).await {
            Ok(response) if !is_error_payload(&response) => {
                let first = response
                    .get("first_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let last = response
                    .get("last_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let full_name = if last.is_empty() {
                    first.to_string()
                } else {
                    format!("{first} {last}")
                };
                let handle = primary_username(&response);
                format_display_name(user_id, handle, &full_name)
            }
            _ => format!("#{user_id}"),
        };
        self.users.insert(user_id, name.clone());
        name
    }

    async fn resolve_chat(
        &mut self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
        writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
        account: Option<u32>,
        chat_id: i64,
    ) -> String {
        if let Some(name) = self.chats.get(&chat_id) {
            return name.clone();
        }
        let payload = serde_json::json!({
            "@type": "getChat",
            "chat_id": chat_id,
        });
        let name = match send_tdlib_request(reader, writer, account, payload).await {
            Ok(response) if !is_error_payload(&response) => {
                let title = response
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                format_display_name(chat_id, None, title)
            }
            _ => format!("#{chat_id}"),
        };
        self.chats.insert(chat_id, name.clone());
        name
    }

    async fn resolve_sender(
        &mut self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
        writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
        account: Option<u32>,
        sender_id: &serde_json::Value,
    ) -> Option<String> {
        let sender_type = sender_id.get("@type").and_then(|v| v.as_str())?;
        match sender_type {
            "messageSenderUser" => {
                let user_id = sender_id.get("user_id").and_then(|v| v.as_i64())?;
                Some(self.resolve_user(reader, writer, account, user_id).await)
            }
            "messageSenderChat" => {
                let chat_id = sender_id.get("chat_id").and_then(|v| v.as_i64())?;
                Some(self.resolve_chat(reader, writer, account, chat_id).await)
            }
            _ => None,
        }
    }

    async fn resolve_origin(
        &mut self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
        writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
        account: Option<u32>,
        origin: &serde_json::Value,
    ) -> Option<String> {
        let origin_type = origin.get("@type").and_then(|v| v.as_str())?;
        match origin_type {
            "messageOriginUser" => {
                let user_id = origin.get("sender_user_id").and_then(|v| v.as_i64())?;
                Some(self.resolve_user(reader, writer, account, user_id).await)
            }
            "messageOriginHiddenUser" => Some(origin.get("sender_name")?.as_str()?.to_string()),
            "messageOriginChat" => {
                let chat_id = origin.get("sender_chat_id").and_then(|v| v.as_i64())?;
                Some(self.resolve_chat(reader, writer, account, chat_id).await)
            }
            "messageOriginChannel" => {
                let chat_id = origin.get("chat_id").and_then(|v| v.as_i64())?;
                Some(self.resolve_chat(reader, writer, account, chat_id).await)
            }
            _ => None,
        }
    }
}

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD_CYAN: &str = "\x1b[1;36m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_BOLD_YELLOW: &str = "\x1b[1;33m";
const ANSI_BOLD_MAGENTA: &str = "\x1b[1;35m";

fn format_message_human(
    message: &serde_json::Value,
    sender_name: Option<&str>,
    forward_origin_name: Option<&str>,
    color: bool,
) -> String {
    let mut parts = Vec::new();
    let mut plain_parts = Vec::new();

    let id = message.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
    let id_str = format!("#{id}");
    plain_parts.push(id_str.clone());
    if color {
        parts.push(format!("{ANSI_BOLD_CYAN}{id_str}{ANSI_RESET}"));
    } else {
        parts.push(id_str);
    }

    if let Some(date) = message.get("date").and_then(|v| v.as_i64()) {
        let dt = Local
            .timestamp_opt(date, 0)
            .single()
            .map(|d| d.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_default();
        plain_parts.push(dt.clone());
        if color {
            parts.push(format!("{ANSI_DIM}{dt}{ANSI_RESET}"));
        } else {
            parts.push(dt);
        }
    }

    let author = sender_name
        .or_else(|| {
            message
                .get("author_signature")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("");
    if !author.is_empty() {
        plain_parts.push(author.to_string());
        if color {
            parts.push(format!("{ANSI_BOLD_YELLOW}{author}{ANSI_RESET}"));
        } else {
            parts.push(author.to_string());
        }
    }

    let edited = message
        .get("edit_date")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        > 0;
    if edited {
        plain_parts.push("(edited)".to_string());
        if color {
            parts.push(format!("{ANSI_DIM}(edited){ANSI_RESET}"));
        } else {
            parts.push("(edited)".to_string());
        }
    }

    let forwarded = message.get("forward_info").is_some() && !message["forward_info"].is_null();
    if forwarded {
        let fwd_label = match forward_origin_name {
            Some(name) => format!("forwarded from {name}"),
            None => "forwarded".to_string(),
        };
        plain_parts.push(fwd_label.clone());
        if color {
            parts.push(format!("{ANSI_DIM}{fwd_label}{ANSI_RESET}"));
        } else {
            parts.push(fwd_label);
        }
    }

    let view_count = message
        .get("interaction_info")
        .and_then(|v| v.get("view_count"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let mut header = parts.join("  ");
    if view_count > 0 {
        let formatted_views = format_view_count(view_count);
        let visual_len = plain_parts.join("  ").len();
        let padding = if visual_len < 60 {
            " ".repeat(60 - visual_len)
        } else {
            "  ".to_string()
        };
        if color {
            header.push_str(&format!(
                "{}{ANSI_DIM}{} views{ANSI_RESET}",
                padding, formatted_views
            ));
        } else {
            header.push_str(&format!("{}{} views", padding, formatted_views));
        }
    }

    let content = format_message_content(message.get("content"), color);
    if content.is_empty() {
        header
    } else {
        format!("{}\n{}", header, content)
    }
}

fn format_view_count(count: i64) -> String {
    if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{},{:03}", count / 1000, count % 1000)
    } else {
        count.to_string()
    }
}

fn format_message_content(content: Option<&serde_json::Value>, color: bool) -> String {
    let Some(content) = content else {
        return String::new();
    };

    let content_type = content.get("@type").and_then(|v| v.as_str()).unwrap_or("");

    let color_tag = |tag: &str| -> String {
        if color {
            format!("{ANSI_BOLD_MAGENTA}{tag}{ANSI_RESET}")
        } else {
            tag.to_string()
        }
    };

    match content_type {
        "messageText" => content
            .get("text")
            .and_then(|v| v.get("text"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "messagePhoto" => {
            let caption = get_caption(content);
            let tag = color_tag("[Photo]");
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageVideo" => {
            let video = content.get("video");
            let filename = video
                .and_then(|v| v.get("file_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let size = video
                .and_then(|v| v.get("video"))
                .and_then(|v| v.get("expected_size"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let tag = if !filename.is_empty() && size > 0 {
                color_tag(&format!("[Video: {}, {}]", filename, format_bytes(size)))
            } else if !filename.is_empty() {
                color_tag(&format!("[Video: {}]", filename))
            } else {
                color_tag("[Video]")
            };
            let caption = get_caption(content);
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageDocument" => {
            let doc = content.get("document");
            let filename = doc
                .and_then(|v| v.get("file_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let size = doc
                .and_then(|v| v.get("document"))
                .and_then(|v| v.get("expected_size"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let tag = if !filename.is_empty() && size > 0 {
                color_tag(&format!("[Document: {}, {}]", filename, format_bytes(size)))
            } else if !filename.is_empty() {
                color_tag(&format!("[Document: {}]", filename))
            } else {
                color_tag("[Document]")
            };
            let caption = get_caption(content);
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageAudio" => {
            let title = content
                .get("audio")
                .and_then(|v| v.get("title"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tag = if !title.is_empty() {
                color_tag(&format!("[Audio: {}]", title))
            } else {
                color_tag("[Audio]")
            };
            let caption = get_caption(content);
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageAnimation" => {
            let caption = get_caption(content);
            let tag = color_tag("[GIF]");
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageVoiceNote" => {
            let caption = get_caption(content);
            let tag = color_tag("[Voice note]");
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageVideoNote" => color_tag("[Video note]"),
        "messagePoll" => {
            let question = content
                .get("poll")
                .and_then(|v| v.get("question"))
                .and_then(|v| v.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !question.is_empty() {
                color_tag(&format!("[Poll: {}]", question))
            } else {
                color_tag("[Poll]")
            }
        }
        "messageSticker" => {
            let emoji = content
                .get("sticker")
                .and_then(|v| v.get("emoji"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !emoji.is_empty() {
                color_tag(&format!("[Sticker: {}]", emoji))
            } else {
                color_tag("[Sticker]")
            }
        }
        other => color_tag(&format!("[{}]", other)),
    }
}

fn get_caption(content: &serde_json::Value) -> String {
    content
        .get("caption")
        .and_then(|v| v.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
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
        Command::Mtp { command } => match command {
            MtpCommand::Raw { json } => (json.clone(), Envelope::Mtp),
        },
        Command::GetHistory { .. } => (None, Envelope::Tdlib),
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

    if let Command::GetHistory {
        chat,
        from,
        limit,
        json,
    } = cli.command
    {
        let chat_id = resolve_chat(&mut reader, &mut writer, account, &chat).await?;

        let use_pager = !json && std::io::stdout().is_terminal();
        let mut pager_child = None;
        let mut output: Box<dyn Write> = if use_pager {
            let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| "less -R".to_string());
            let parts: Vec<&str> = pager_cmd.split_whitespace().collect();
            let child = std::process::Command::new(parts[0])
                .args(&parts[1..])
                .stdin(std::process::Stdio::piped())
                .spawn()?;
            pager_child = Some(child);
            Box::new(pager_child.as_mut().unwrap().stdin.take().unwrap())
        } else {
            Box::new(std::io::stdout().lock())
        };

        let messages = async gen {
            let mut from_message_id = from;
            let mut remaining = limit;
            let mut name_cache = NameCache::default();

            loop {
                let batch_limit = if remaining > 0 {
                    remaining.min(100)
                } else {
                    100
                };
                let payload = serde_json::json!({
                    "@type": "getChatHistory",
                    "chat_id": chat_id,
                    "from_message_id": from_message_id,
                    "offset": 0,
                    "limit": batch_limit,
                    "only_local": false,
                });
                let response = send_tdlib_request(&mut reader, &mut writer, account, payload).await;
                let response = match response {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("Error fetching history: {}", e);
                        std::process::exit(1);
                    }
                };
                if is_error_payload(&response) {
                    let message = response
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    eprintln!("Error fetching history: {}", message);
                    std::process::exit(1);
                }

                let batch = response
                    .get("messages")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                if batch.is_empty() {
                    break;
                }

                from_message_id = batch
                    .last()
                    .and_then(|m| m.get("id"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                for msg in batch {
                    let sender_name = if let Some(sender_id) = msg.get("sender_id") {
                        name_cache
                            .resolve_sender(&mut reader, &mut writer, account, sender_id)
                            .await
                    } else {
                        None
                    };

                    let forward_origin_name = msg
                        .get("forward_info")
                        .filter(|v| !v.is_null())
                        .and_then(|v| v.get("origin"));
                    let forward_origin_name = if let Some(origin) = forward_origin_name {
                        name_cache
                            .resolve_origin(&mut reader, &mut writer, account, origin)
                            .await
                    } else {
                        None
                    };

                    yield (msg, sender_name, forward_origin_name);
                    if remaining > 0 {
                        remaining -= 1;
                        if remaining == 0 {
                            return;
                        }
                    }
                }
            }
        };

        let mut messages = pin!(messages);
        let mut first = true;

        while let Some((msg, sender_name, forward_origin_name)) =
            std::future::poll_fn(|cx| messages.as_mut().poll_next(cx)).await
        {
            if json {
                if writeln!(output, "{}", serde_json::to_string(&msg)?).is_err() {
                    break;
                }
            } else {
                let formatted = format_message_human(
                    &msg,
                    sender_name.as_deref(),
                    forward_origin_name.as_deref(),
                    use_pager,
                );
                let result = if first {
                    first = false;
                    write!(output, "{}", formatted)
                } else {
                    write!(output, "\n{}", formatted)
                };
                if result.is_err() {
                    break;
                }
            }
        }

        // Ensure trailing newline for json mode
        if json {
            let _ = output.flush();
        } else {
            let _ = writeln!(output);
        }

        drop(output);
        if let Some(mut child) = pager_child {
            let _ = child.wait();
        }

        return Ok(());
    }

    let mut had_error = false;
    match json_arg {
        Some(text) => {
            let mut payload = parse_json(&text)?;
            let extra = inject_extra(&envelope, &mut payload);
            let message = wrap(&envelope, account, payload);
            had_error |= send_and_receive(
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
                had_error |= send_and_receive(
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

    if had_error {
        std::process::exit(1);
    }

    Ok(())
}
