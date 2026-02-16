use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;

#[derive(Parser)]
#[command(about = "CLI client for tdesktop Unix socket API")]
struct Cli {
    /// Path to the tdesktop Unix socket
    #[arg(long, global = true)]
    socket: Option<PathBuf>,

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
) -> Result<(), Box<dyn std::error::Error>> {
    let line = serde_json::to_string(message)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let mut response = String::new();
    reader.read_line(&mut response).await?;

    print!("{response}");

    Ok(())
}

enum Envelope {
    None,
    Tdlib,
    Tdesktop,
}

fn wrap(envelope: &Envelope, payload: serde_json::Value) -> serde_json::Value {
    match envelope {
        Envelope::None => payload,
        Envelope::Tdlib => serde_json::json!({ "type": "tdlib", "payload": payload }),
        Envelope::Tdesktop => serde_json::json!({ "type": "tdesktop", "payload": payload }),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let socket_path = cli.socket.unwrap_or_else(default_socket_path);

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
            let message = wrap(&envelope, parse_json(&text)?);
            send_and_receive(&mut reader, &mut writer, &message).await?;
        }
        None => {
            use std::io::BufRead;
            let stdin = std::io::stdin().lock();
            for line in stdin.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let message = wrap(&envelope, parse_json(&line)?);
                send_and_receive(&mut reader, &mut writer, &message).await?;
            }
        }
    }

    Ok(())
}
