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

fn read_json(arg: Option<String>) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let text = match arg {
        Some(text) => text,
        None => {
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };

    let value: serde_json::Value = serde_json::from_str(text.trim())?;
    Ok(value)
}

async fn send_and_receive(
    socket_path: &PathBuf,
    message: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let stream = UnixStream::connect(socket_path).await?;
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    let line = serde_json::to_string(&message)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let mut response = String::new();
    reader.read_line(&mut response).await?;

    print!("{response}");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let socket_path = cli.socket.unwrap_or_else(default_socket_path);

    let message = match cli.command {
        Command::Raw { json } => read_json(json)?,
        Command::Tdlib { command } => match command {
            TdlibCommand::Raw { json } => {
                let payload = read_json(json)?;
                serde_json::json!({ "type": "tdlib", "payload": payload })
            }
        },
        Command::Tdesktop { command } => match command {
            TdesktopCommand::Raw { json } => {
                let payload = read_json(json)?;
                serde_json::json!({ "type": "tdesktop", "payload": payload })
            }
        },
    };

    send_and_receive(&socket_path, message).await?;

    Ok(())
}
