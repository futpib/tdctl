#![feature(async_iterator, gen_blocks)]

mod ids;

use ids::{MtpMessageId, MtpPeerId, TdlibDialogId, TdlibMessageId};
use std::async_iter::AsyncIterator;
use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{Local, NaiveTime, TimeZone};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;

static EXTRA_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Parser)]
#[command(about = "CLI client for tdesktop Unix socket API")]
struct Cli {
    /// Path to the tdesktop Unix socket
    #[arg(long, global = true, env = "TDCTL_SOCKET")]
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

        /// Show messages after this date (e.g. "2025-01-01", "last monday")
        #[arg(long, alias = "since")]
        after: Option<String>,

        /// Show messages before this date (e.g. "2025-01-01", "last monday")
        #[arg(long, alias = "until")]
        before: Option<String>,

        /// Output raw JSON (one message per line)
        #[arg(long)]
        json: bool,
    },

    /// Send a message to a chat
    SendMessage {
        /// Chat ID or @username
        #[arg(allow_hyphen_values = true)]
        chat: String,

        /// Message text (reads from stdin if omitted and no files given)
        text: Option<String>,

        /// Photo file to attach (can be repeated)
        #[arg(long = "photo", value_name = "PATH")]
        photos: Vec<PathBuf>,

        /// Video file to attach (can be repeated)
        #[arg(long = "video", value_name = "PATH")]
        videos: Vec<PathBuf>,

        /// Document file to attach (can be repeated)
        #[arg(long = "document", value_name = "PATH")]
        documents: Vec<PathBuf>,

        /// Audio file to attach (can be repeated)
        #[arg(long = "audio", value_name = "PATH")]
        audios: Vec<PathBuf>,

        /// Generic file to attach, sent as document (can be repeated)
        #[arg(long = "file", value_name = "PATH")]
        files: Vec<PathBuf>,

        /// MTP message ID to reply to
        #[arg(long)]
        reply_to: Option<i64>,
    },

    /// Search for chats by name or username
    SearchChats {
        /// Search query
        query: String,

        /// Maximum results to show (default 20)
        #[arg(long, default_value_t = 20)]
        limit: u32,
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

fn parse_flood_wait(payload: &serde_json::Value) -> Option<u64> {
    if !is_error_payload(payload) {
        return None;
    }
    let message = payload.get("message").and_then(|v| v.as_str())?;
    let seconds = message.strip_prefix("FLOOD_WAIT_")?;
    seconds.parse().ok()
}

async fn send_and_receive(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    message: &serde_json::Value,
    envelope: &Envelope,
    expected_extra: Option<&str>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let line = serde_json::to_string(message)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    match envelope {
        Envelope::None => {
            let mut response = String::new();
            reader.read_line(&mut response).await?;
            let payload: serde_json::Value = serde_json::from_str(response.trim())?;
            Ok(payload)
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
                break Ok(parsed["payload"].clone());
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
                    break Ok(payload);
                }
            } else {
                strip_extra(&mut payload);
                break Ok(payload);
            }
        },
    }
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

async fn send_tdlib_request_with_retry(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    account: Option<u32>,
    payload: serde_json::Value,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    loop {
        let response = send_tdlib_request(reader, writer, account, payload.clone()).await?;
        if let Some(seconds) = parse_flood_wait(&response) {
            eprintln!("Rate limited, waiting {seconds}s...");
            tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
            continue;
        }
        return Ok(response);
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
    let response = send_tdlib_request_with_retry(reader, writer, account, payload).await?;
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

fn format_display_name(
    id: impl std::fmt::Display,
    handle: Option<&str>,
    full_name: &str,
) -> String {
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
        let name = match send_tdlib_request_with_retry(reader, writer, account, payload).await {
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
                format_display_name(ids::MtpUserId(user_id), handle, &full_name)
            }
            _ => format!("#{}", ids::MtpUserId(user_id)),
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
        let mtp_peer: MtpPeerId = TdlibDialogId(chat_id).into();
        let name = match send_tdlib_request_with_retry(reader, writer, account, payload).await {
            Ok(response) if !is_error_payload(&response) => {
                let title = response.get("title").and_then(|v| v.as_str()).unwrap_or("");
                format_display_name(&mtp_peer, None, title)
            }
            _ => format!("#{mtp_peer}"),
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

fn entity_type_ansi(entity_type: &serde_json::Value) -> Option<&'static str> {
    let type_str = entity_type.get("@type").and_then(|v| v.as_str())?;
    match type_str {
        "textEntityTypeBold" => Some("\x1b[1m"),
        "textEntityTypeItalic" => Some("\x1b[3m"),
        "textEntityTypeUnderline" => Some("\x1b[4m"),
        "textEntityTypeStrikethrough" => Some("\x1b[9m"),
        "textEntityTypeCode" | "textEntityTypePre" | "textEntityTypePreCode" => Some("\x1b[7m"),
        "textEntityTypeUrl" => Some("\x1b[4m"),
        "textEntityTypeSpoiler" => Some("\x1b[8m"),
        "textEntityTypeBlockQuote" | "textEntityTypeExpandableBlockQuote" => Some("\x1b[2m"),
        _ => None,
    }
}

fn entity_type_markdown(entity_type: &serde_json::Value) -> Option<&'static str> {
    let type_str = entity_type.get("@type").and_then(|v| v.as_str())?;
    match type_str {
        "textEntityTypeBold" => Some("**"),
        "textEntityTypeItalic" => Some("_"),
        "textEntityTypeStrikethrough" => Some("~~"),
        "textEntityTypeCode" => Some("`"),
        "textEntityTypePre" | "textEntityTypePreCode" => Some("```"),
        "textEntityTypeSpoiler" => Some("||"),
        _ => None,
    }
}

fn format_formatted_text(formatted_text: &serde_json::Value, color: bool) -> String {
    let text = formatted_text
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let entities = formatted_text.get("entities").and_then(|v| v.as_array());
    let entities = match entities {
        Some(e) if !e.is_empty() => e,
        _ => return text.to_string(),
    };

    // Build UTF-16 offset to byte offset mapping
    let mut utf16_to_byte: Vec<usize> = Vec::new();
    let mut byte_pos = 0;
    for ch in text.chars() {
        let utf16_len = ch.len_utf16();
        for _ in 0..utf16_len {
            utf16_to_byte.push(byte_pos);
        }
        byte_pos += ch.len_utf8();
    }
    utf16_to_byte.push(byte_pos); // sentinel for end-of-string

    struct Event {
        byte_pos: usize,
        is_end: bool,
        entity_idx: usize,
        ansi_code: &'static str,
        md_marker: &'static str,
        text_url: Option<String>,
    }

    let mut events: Vec<Event> = Vec::new();
    for (i, entity) in entities.iter().enumerate() {
        let offset = entity.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let length = entity.get("length").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let etype = entity.get("type").unwrap_or(&serde_json::Value::Null);

        let ansi_code = entity_type_ansi(etype).unwrap_or("");
        let md_marker = entity_type_markdown(etype).unwrap_or("");

        let text_url =
            if etype.get("@type").and_then(|v| v.as_str()) == Some("textEntityTypeTextUrl") {
                etype
                    .get("url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            } else {
                None
            };

        let start = if offset < utf16_to_byte.len() {
            utf16_to_byte[offset]
        } else {
            byte_pos
        };
        let end_utf16 = offset + length;
        let end = if end_utf16 < utf16_to_byte.len() {
            utf16_to_byte[end_utf16]
        } else {
            byte_pos
        };

        events.push(Event {
            byte_pos: start,
            is_end: false,
            entity_idx: i,
            ansi_code,
            md_marker,
            text_url: text_url.clone(),
        });
        events.push(Event {
            byte_pos: end,
            is_end: true,
            entity_idx: i,
            ansi_code,
            md_marker,
            text_url,
        });
    }

    // Sort: by byte_pos, then ends before starts (so closing happens before opening at same position)
    events.sort_by(|a, b| {
        a.byte_pos
            .cmp(&b.byte_pos)
            .then_with(|| a.is_end.cmp(&b.is_end).reverse())
    });

    let mut result = String::new();
    let mut prev = 0;

    if color {
        let mut active: Vec<(usize, &str)> = Vec::new();
        for event in &events {
            if event.byte_pos > prev {
                result.push_str(&text[prev..event.byte_pos]);
            }
            prev = event.byte_pos;

            if event.is_end {
                if let Some(url) = &event.text_url {
                    result.push_str(&format!(" ({})", url));
                }
                active.retain(|(idx, _)| *idx != event.entity_idx);
                result.push_str(ANSI_RESET);
                for (_, code) in &active {
                    result.push_str(code);
                }
            } else if !event.ansi_code.is_empty() {
                result.push_str(event.ansi_code);
                active.push((event.entity_idx, event.ansi_code));
            }
        }
    } else {
        for event in &events {
            if event.byte_pos > prev {
                result.push_str(&text[prev..event.byte_pos]);
            }
            prev = event.byte_pos;

            if event.is_end {
                if let Some(url) = &event.text_url {
                    result.push_str(&format!("]({})", url));
                } else {
                    result.push_str(event.md_marker);
                }
            } else if event.text_url.is_some() {
                result.push('[');
            } else {
                result.push_str(event.md_marker);
            }
        }
    }

    if prev < text.len() {
        result.push_str(&text[prev..]);
    }

    result
}

fn format_message_human(
    message: &serde_json::Value,
    sender_name: Option<&str>,
    forward_origin_name: Option<&str>,
    color: bool,
) -> String {
    let mut parts = Vec::new();
    let mut plain_parts = Vec::new();

    let tdlib_id = TdlibMessageId(message.get("id").and_then(|v| v.as_i64()).unwrap_or(0));
    let mtp_id: MtpMessageId = tdlib_id.into();
    let id_str = format!("#{mtp_id}");
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

    let content = format_message_content(mtp_id, message.get("content"), color);
    if content.is_empty() {
        header
    } else {
        format!("{}\n{}", header, content)
    }
}

struct ResolvedMessage {
    msg: serde_json::Value,
    sender_name: Option<String>,
    forward_origin_name: Option<String>,
}

fn get_media_album_id(msg: &serde_json::Value) -> i64 {
    msg.get("media_album_id")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

fn content_type_label(content: Option<&serde_json::Value>) -> &'static str {
    let Some(content) = content else {
        return "Unknown";
    };
    match content.get("@type").and_then(|v| v.as_str()).unwrap_or("") {
        "messagePhoto" => "Photo",
        "messageVideo" => "Video",
        "messageDocument" => "Document",
        "messageAudio" => "Audio",
        _ => "Media",
    }
}

fn format_album_human(messages: &[ResolvedMessage], color: bool) -> String {
    let first = &messages[0];

    let mut parts = Vec::new();
    let mut plain_parts = Vec::new();

    let tdlib_id = TdlibMessageId(first.msg.get("id").and_then(|v| v.as_i64()).unwrap_or(0));
    let mtp_id: MtpMessageId = tdlib_id.into();
    let id_str = format!("#{mtp_id}");
    plain_parts.push(id_str.clone());
    if color {
        parts.push(format!("{ANSI_BOLD_CYAN}{id_str}{ANSI_RESET}"));
    } else {
        parts.push(id_str);
    }

    if let Some(date) = first.msg.get("date").and_then(|v| v.as_i64()) {
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

    let author = first
        .sender_name
        .as_deref()
        .or_else(|| {
            first
                .msg
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

    let forwarded = first.msg.get("forward_info").is_some() && !first.msg["forward_info"].is_null();
    if forwarded {
        let fwd_label = match &first.forward_origin_name {
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

    let view_count = messages
        .iter()
        .map(|m| {
            m.msg
                .get("interaction_info")
                .and_then(|v| v.get("view_count"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
        })
        .max()
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

    let items: Vec<String> = messages
        .iter()
        .map(|m| {
            let tdlib_id = TdlibMessageId(m.msg.get("id").and_then(|v| v.as_i64()).unwrap_or(0));
            let mtp_id: MtpMessageId = tdlib_id.into();
            let label = content_type_label(m.msg.get("content"));
            format!("{label} #{mtp_id}")
        })
        .collect();
    let album_tag = format!("[{}]", items.join(", "));
    let album_tag = if color {
        format!("{ANSI_BOLD_MAGENTA}{album_tag}{ANSI_RESET}")
    } else {
        album_tag
    };

    let caption = messages
        .iter()
        .map(|m| get_caption_from_message(&m.msg, color))
        .find(|c| !c.is_empty())
        .unwrap_or_default();

    if caption.is_empty() {
        format!("{header}\n{album_tag}")
    } else {
        format!("{header}\n{album_tag}\n{caption}")
    }
}

fn get_caption_from_message(msg: &serde_json::Value, color: bool) -> String {
    let ft = msg
        .get("content")
        .and_then(|c| c.get("caption"))
        .unwrap_or(&serde_json::Value::Null);
    format_formatted_text(ft, color)
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

fn format_message_content(
    message_id: MtpMessageId,
    content: Option<&serde_json::Value>,
    color: bool,
) -> String {
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

    let id = format!("#{message_id}");

    match content_type {
        "messageText" => {
            let ft = content.get("text").unwrap_or(&serde_json::Value::Null);
            format_formatted_text(ft, color)
        }
        "messagePhoto" => {
            let caption = format_caption(content, color);
            let tag = color_tag(&format!("[Photo {id}]"));
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
                color_tag(&format!(
                    "[Video {id}: {}, {}]",
                    filename,
                    format_bytes(size)
                ))
            } else if !filename.is_empty() {
                color_tag(&format!("[Video {id}: {}]", filename))
            } else {
                color_tag(&format!("[Video {id}]"))
            };
            let caption = format_caption(content, color);
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
                color_tag(&format!(
                    "[Document {id}: {}, {}]",
                    filename,
                    format_bytes(size)
                ))
            } else if !filename.is_empty() {
                color_tag(&format!("[Document {id}: {}]", filename))
            } else {
                color_tag(&format!("[Document {id}]"))
            };
            let caption = format_caption(content, color);
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
                color_tag(&format!("[Audio {id}: {}]", title))
            } else {
                color_tag(&format!("[Audio {id}]"))
            };
            let caption = format_caption(content, color);
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageAnimation" => {
            let caption = format_caption(content, color);
            let tag = color_tag(&format!("[GIF {id}]"));
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageVoiceNote" => {
            let caption = format_caption(content, color);
            let tag = color_tag(&format!("[Voice note {id}]"));
            if caption.is_empty() {
                tag
            } else {
                format!("{}\n{}", tag, caption)
            }
        }
        "messageVideoNote" => color_tag(&format!("[Video note {id}]")),
        "messagePoll" => {
            let question = content
                .get("poll")
                .and_then(|v| v.get("question"))
                .and_then(|v| v.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !question.is_empty() {
                color_tag(&format!("[Poll {id}: {}]", question))
            } else {
                color_tag(&format!("[Poll {id}]"))
            }
        }
        "messageSticker" => {
            let emoji = content
                .get("sticker")
                .and_then(|v| v.get("emoji"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !emoji.is_empty() {
                color_tag(&format!("[Sticker {id}: {}]", emoji))
            } else {
                color_tag(&format!("[Sticker {id}]"))
            }
        }
        other => color_tag(&format!("[{other} {id}]")),
    }
}

fn format_caption(content: &serde_json::Value, color: bool) -> String {
    let ft = content.get("caption").unwrap_or(&serde_json::Value::Null);
    format_formatted_text(ft, color)
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

fn parse_date_arg(s: &str, end_of_day: bool) -> Result<i64, Box<dyn std::error::Error>> {
    let now = Local::now().naive_local();
    let result = human_date_parser::from_human_time(s, now)?;
    let naive_dt = match result {
        human_date_parser::ParseResult::DateTime(dt) => dt,
        human_date_parser::ParseResult::Date(d) => {
            let time = if end_of_day {
                NaiveTime::from_hms_opt(23, 59, 59).unwrap()
            } else {
                NaiveTime::from_hms_opt(0, 0, 0).unwrap()
            };
            d.and_time(time)
        }
        human_date_parser::ParseResult::Time(t) => now.date().and_time(t),
    };
    let local_dt = Local
        .from_local_datetime(&naive_dt)
        .single()
        .ok_or_else(|| format!("Ambiguous or invalid local time: {}", naive_dt))?;
    Ok(local_dt.timestamp())
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
        Command::SendMessage { .. } => (None, Envelope::Tdlib),
        Command::SearchChats { .. } => (None, Envelope::Tdlib),
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

    if let Command::SearchChats { query, limit } = cli.command {
        let server_payload = serde_json::json!({
            "@type": "searchChatsOnServer",
            "query": query,
            "limit": limit,
        });
        let server_response =
            send_tdlib_request_with_retry(&mut reader, &mut writer, account, server_payload)
                .await?;

        let public_payload = serde_json::json!({
            "@type": "searchPublicChats",
            "query": query,
        });
        let public_response =
            send_tdlib_request_with_retry(&mut reader, &mut writer, account, public_payload)
                .await?;

        let mut seen = std::collections::HashSet::new();
        let mut chat_ids: Vec<i64> = Vec::new();

        for response in [&server_response, &public_response] {
            if is_error_payload(response) {
                continue;
            }
            if let Some(ids) = response.get("chat_ids").and_then(|v| v.as_array()) {
                for id in ids {
                    if let Some(id) = id.as_i64()
                        && seen.insert(id)
                    {
                        chat_ids.push(id);
                    }
                }
            }
        }

        chat_ids.truncate(limit as usize);

        for chat_id in chat_ids {
            let chat_payload = serde_json::json!({
                "@type": "getChat",
                "chat_id": chat_id,
            });
            let chat_response =
                send_tdlib_request_with_retry(&mut reader, &mut writer, account, chat_payload)
                    .await?;
            if is_error_payload(&chat_response) {
                continue;
            }

            let title = chat_response
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let mtp_peer: MtpPeerId = TdlibDialogId(chat_id).into();

            let (username, member_count, type_label) = match chat_response
                .get("type")
                .and_then(|v| v.get("@type"))
                .and_then(|v| v.as_str())
            {
                Some("chatTypePrivate") => {
                    let user_id = chat_response
                        .get("type")
                        .and_then(|v| v.get("user_id"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let user_payload = serde_json::json!({
                        "@type": "getUser",
                        "user_id": user_id,
                    });
                    let user_response = send_tdlib_request_with_retry(
                        &mut reader,
                        &mut writer,
                        account,
                        user_payload,
                    )
                    .await?;
                    if !is_error_payload(&user_response) {
                        let label = match user_response
                            .get("type")
                            .and_then(|v| v.get("@type"))
                            .and_then(|v| v.as_str())
                        {
                            Some("userTypeBot") => Some("bot"),
                            _ => None,
                        };
                        (
                            primary_username(&user_response).map(|s| s.to_string()),
                            None,
                            label,
                        )
                    } else {
                        (None, None, None)
                    }
                }
                Some("chatTypeSupergroup") => {
                    let supergroup_id = chat_response
                        .get("type")
                        .and_then(|v| v.get("supergroup_id"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let sg_payload = serde_json::json!({
                        "@type": "getSupergroup",
                        "supergroup_id": supergroup_id,
                    });
                    let sg_response = send_tdlib_request_with_retry(
                        &mut reader,
                        &mut writer,
                        account,
                        sg_payload,
                    )
                    .await?;
                    if !is_error_payload(&sg_response) {
                        let count = sg_response
                            .get("member_count")
                            .and_then(|v| v.as_i64())
                            .filter(|&c| c > 0);
                        let is_channel = sg_response
                            .get("is_channel")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let is_forum = sg_response
                            .get("is_forum")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        let label = if is_forum {
                            "forum"
                        } else if is_channel {
                            "channel"
                        } else {
                            "group"
                        };
                        (
                            primary_username(&sg_response).map(|s| s.to_string()),
                            count,
                            Some(label),
                        )
                    } else {
                        (None, None, None)
                    }
                }
                Some("chatTypeBasicGroup") => {
                    let basic_group_id = chat_response
                        .get("type")
                        .and_then(|v| v.get("basic_group_id"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let bg_payload = serde_json::json!({
                        "@type": "getBasicGroup",
                        "basic_group_id": basic_group_id,
                    });
                    let bg_response = send_tdlib_request_with_retry(
                        &mut reader,
                        &mut writer,
                        account,
                        bg_payload,
                    )
                    .await?;
                    if !is_error_payload(&bg_response) {
                        let count = bg_response
                            .get("member_count")
                            .and_then(|v| v.as_i64())
                            .filter(|&c| c > 0);
                        (None, count, Some("group"))
                    } else {
                        (None, None, Some("group"))
                    }
                }
                _ => (None, None, None),
            };

            let mut display = format_display_name(&mtp_peer, username.as_deref(), title);
            if let Some(label) = type_label {
                display.push_str(&format!(" [{label}]"));
            }
            if let Some(count) = member_count {
                display.push_str(&format!(" [{count} members]"));
            }
            println!("{display}");
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

    if let Command::SendMessage {
        chat,
        text,
        photos,
        videos,
        documents,
        audios,
        files,
        reply_to,
    } = cli.command
    {
        let chat_id = resolve_chat(&mut reader, &mut writer, account, &chat).await?;

        #[derive(Clone, Copy)]
        enum MediaType {
            Photo,
            Video,
            Document,
            Audio,
        }

        let mut media: Vec<(MediaType, PathBuf)> = Vec::new();
        for p in photos {
            media.push((MediaType::Photo, p.canonicalize()?));
        }
        for p in videos {
            media.push((MediaType::Video, p.canonicalize()?));
        }
        for p in documents {
            media.push((MediaType::Document, p.canonicalize()?));
        }
        for p in audios {
            media.push((MediaType::Audio, p.canonicalize()?));
        }
        for p in files {
            media.push((MediaType::Document, p.canonicalize()?));
        }

        let text = match text {
            Some(t) => Some(t),
            None if media.is_empty() && !std::io::stdin().is_terminal() => {
                let mut buf = String::new();
                std::io::Read::read_to_string(&mut std::io::stdin().lock(), &mut buf)?;
                if buf.is_empty() {
                    return Err("no message text or files provided".into());
                }
                Some(buf)
            }
            None if media.is_empty() => {
                return Err("no message text or files provided".into());
            }
            None => None,
        };

        let formatted_text = text.map(|t| {
            serde_json::json!({
                "@type": "formattedText",
                "text": t,
            })
        });

        let build_input_message_content = |media_type: MediaType,
                                           path: &PathBuf,
                                           caption: Option<&serde_json::Value>|
         -> serde_json::Value {
            let empty_text = serde_json::json!({"@type": "formattedText", "text": ""});
            let caption = caption.unwrap_or(&empty_text);
            match media_type {
                MediaType::Photo => serde_json::json!({
                    "@type": "inputMessagePhoto",
                    "photo": {
                        "@type": "inputFileLocal",
                        "path": path.to_string_lossy(),
                    },
                    "caption": caption,
                }),
                MediaType::Video => serde_json::json!({
                    "@type": "inputMessageVideo",
                    "video": {
                        "@type": "inputFileLocal",
                        "path": path.to_string_lossy(),
                    },
                    "caption": caption,
                }),
                MediaType::Document => serde_json::json!({
                    "@type": "inputMessageDocument",
                    "document": {
                        "@type": "inputFileLocal",
                        "path": path.to_string_lossy(),
                    },
                    "caption": caption,
                }),
                MediaType::Audio => serde_json::json!({
                    "@type": "inputMessageAudio",
                    "audio": {
                        "@type": "inputFileLocal",
                        "path": path.to_string_lossy(),
                    },
                    "caption": caption,
                }),
            }
        };

        let reply_to_value = reply_to.map(|id| {
            let tdlib_id: TdlibMessageId = MtpMessageId(id).into();
            serde_json::json!({
                "@type": "inputMessageReplyToMessage",
                "message_id": tdlib_id.0,
            })
        });

        let payload = if media.is_empty() {
            let mut msg = serde_json::json!({
                "@type": "sendMessage",
                "chat_id": chat_id,
                "input_message_content": {
                    "@type": "inputMessageText",
                    "text": formatted_text,
                },
            });
            if let Some(reply) = &reply_to_value {
                msg["reply_to"] = reply.clone();
            }
            msg
        } else if media.len() == 1 {
            let (mt, path) = &media[0];
            let content = build_input_message_content(*mt, path, formatted_text.as_ref());
            let mut msg = serde_json::json!({
                "@type": "sendMessage",
                "chat_id": chat_id,
                "input_message_content": content,
            });
            if let Some(reply) = &reply_to_value {
                msg["reply_to"] = reply.clone();
            }
            msg
        } else {
            let contents: Vec<serde_json::Value> = media
                .iter()
                .enumerate()
                .map(|(i, (mt, path))| {
                    let caption = if i == 0 {
                        formatted_text.as_ref()
                    } else {
                        None
                    };
                    build_input_message_content(*mt, path, caption)
                })
                .collect();
            let mut msg = serde_json::json!({
                "@type": "sendMessageAlbum",
                "chat_id": chat_id,
                "input_message_contents": contents,
            });
            if let Some(reply) = &reply_to_value {
                msg["reply_to"] = reply.clone();
            }
            msg
        };

        let response =
            send_tdlib_request_with_retry(&mut reader, &mut writer, account, payload).await?;
        if is_error_payload(&response) {
            let message = response
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            eprintln!("Error: {}", message);
            std::process::exit(1);
        }

        let response_type = response.get("@type").and_then(|v| v.as_str()).unwrap_or("");
        match response_type {
            "message" => {
                let tdlib_id =
                    TdlibMessageId(response.get("id").and_then(|v| v.as_i64()).unwrap_or(0));
                let mtp_id: MtpMessageId = tdlib_id.into();
                println!("{mtp_id}");
            }
            "messages" => {
                if let Some(messages) = response.get("messages").and_then(|v| v.as_array()) {
                    for msg in messages {
                        let tdlib_id =
                            TdlibMessageId(msg.get("id").and_then(|v| v.as_i64()).unwrap_or(0));
                        let mtp_id: MtpMessageId = tdlib_id.into();
                        println!("{mtp_id}");
                    }
                }
            }
            _ => {
                println!("{}", serde_json::to_string(&response)?);
            }
        }

        return Ok(());
    }

    if let Command::GetHistory {
        chat,
        from,
        limit,
        after,
        before,
        json,
    } = cli.command
    {
        let after_ts = after
            .as_deref()
            .map(|s| parse_date_arg(s, false))
            .transpose()?;
        let before_ts = before
            .as_deref()
            .map(|s| parse_date_arg(s, true))
            .transpose()?;

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

        let initial_from = if from == 0 {
            if let Some(ts) = before_ts {
                let payload = serde_json::json!({
                    "@type": "getChatMessageByDate",
                    "chat_id": chat_id,
                    "date": ts,
                });
                let response =
                    send_tdlib_request_with_retry(&mut reader, &mut writer, account, payload)
                        .await?;
                if is_error_payload(&response) {
                    let message = response
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    return Err(format!("Error finding message by date: {}", message).into());
                }
                response.get("id").and_then(|v| v.as_i64()).unwrap_or(0)
            } else {
                0
            }
        } else {
            from
        };

        let messages = async gen {
            let mut from_message_id = initial_from;
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
                let response =
                    send_tdlib_request_with_retry(&mut reader, &mut writer, account, payload).await;
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

                    let msg_date = msg.get("date").and_then(|v| v.as_i64()).unwrap_or(0);
                    yield (msg, sender_name, forward_origin_name);
                    if let Some(after) = after_ts
                        && msg_date < after
                    {
                        return;
                    }
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
        let mut album_buf: Vec<ResolvedMessage> = Vec::new();
        let mut album_id: i64 = 0;

        let flush_album =
            |buf: &mut Vec<ResolvedMessage>, first: &mut bool, output: &mut dyn Write| -> bool {
                if buf.is_empty() {
                    return true;
                }
                let formatted = format_album_human(buf, use_pager);
                let result = if *first {
                    *first = false;
                    write!(output, "{}", formatted)
                } else {
                    write!(output, "\n{}", formatted)
                };
                buf.clear();
                result.is_ok()
            };

        while let Some((msg, sender_name, forward_origin_name)) =
            std::future::poll_fn(|cx| messages.as_mut().poll_next(cx)).await
        {
            if json {
                if writeln!(output, "{}", serde_json::to_string(&msg)?).is_err() {
                    break;
                }
            } else {
                let msg_album_id = get_media_album_id(&msg);
                let resolved = ResolvedMessage {
                    msg,
                    sender_name,
                    forward_origin_name,
                };

                if msg_album_id != 0 && msg_album_id == album_id {
                    album_buf.push(resolved);
                } else {
                    if !flush_album(&mut album_buf, &mut first, output.as_mut()) {
                        break;
                    }
                    if msg_album_id != 0 {
                        album_id = msg_album_id;
                        album_buf.push(resolved);
                    } else {
                        album_id = 0;
                        let formatted = format_message_human(
                            &resolved.msg,
                            resolved.sender_name.as_deref(),
                            resolved.forward_origin_name.as_deref(),
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
            }
        }

        if !json {
            flush_album(&mut album_buf, &mut first, output.as_mut());
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
            let response = send_and_receive(
                &mut reader,
                &mut writer,
                &message,
                &envelope,
                extra.as_deref(),
            )
            .await?;
            println!("{}", serde_json::to_string(&response)?);
            had_error |= is_error_payload(&response);
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
                let response = send_and_receive(
                    &mut reader,
                    &mut writer,
                    &message,
                    &envelope,
                    extra.as_deref(),
                )
                .await?;
                println!("{}", serde_json::to_string(&response)?);
                had_error |= is_error_payload(&response);
            }
        }
    }

    if had_error {
        std::process::exit(1);
    }

    Ok(())
}
