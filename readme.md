# tdctl

CLI client for [tdesktop](https://github.com/futpib/tdesktop) Unix socket API.

## Usage

```
tdctl [OPTIONS] <COMMAND>
```

### Global options

- `--socket <SOCKET>` — Path to the tdesktop Unix socket (env: `TDCTL_SOCKET`)
- `-a, --account <ACCOUNT>` — Account index
- `--config <PATH>` — Path to the config file (env: `TDCTL_CONFIG`)

If `--socket` is not specified and `TDCTL_SOCKET` is not set, the default path is `$XDG_RUNTIME_DIR/tdesktop.sock` or `/tmp/tdesktop-<uid>/tdesktop.sock`.

### Commands

#### `get-history`

Browse chat message history.

```
tdctl get-history [OPTIONS] <CHAT>
```

`<CHAT>` is a numeric chat ID or `@username`.

- `--from <ID>` — Start from this message ID (0 = latest)
- `--limit <N>` — Maximum messages to fetch (0 = unlimited)
- `--after <DATE>` / `--since <DATE>` — Show messages after this date
- `--before <DATE>` / `--until <DATE>` — Show messages before this date
- `--json` — Output raw JSON (one message per line)
- `--mark-read` / `--no-mark-read` — Mark the fetched messages as read on the
  server, or not. Overrides the config (see [Configuration](#configuration)).

Date arguments accept natural language (e.g. `"2025-01-01"`, `"last monday"`).

#### `listen`

Stream new incoming messages in real time (TDLib `updateNewMessage`). The socket
server already broadcasts every TDLib update to all clients; this consumes that
stream and prints messages matching the filters as they arrive — no polling, no
re-fetching. Ideal for a DM monitor.

```
tdctl listen [OPTIONS]
```

- `--chat <ID>` — Only show messages in these chat IDs (repeatable; default: all)
- `--from <ID>` — Only show messages from these sender user IDs (repeatable)
- `--include-outgoing` — Include your own outgoing messages (default: incoming only)
- `--count <N>` — Exit after this many matching messages (0 = unlimited)
- `--timeout <S>` — Exit after this many seconds (0 = run until killed)
- `--json` — Emit one JSON message object per line instead of a compact human line
- `--mark-read` / `--no-mark-read` — Mark each matched (incoming) message as read
  on the server, or not. Overrides the config (see [Configuration](#configuration)).

```
tdctl listen --from 499210827 --json              # watch one sender, JSON out
tdctl listen --chat -100123 --count 1 --timeout 30 # wait for one msg, then exit
```

#### `download`

Download media from a message to a file. Works for photos, videos, documents,
audio, animations, voice/video notes, and stickers.

```
tdctl download [OPTIONS] <CHAT> <MESSAGE_ID>
```

`<CHAT>` is a numeric chat ID or `@username`. `<MESSAGE_ID>` is the message ID as
printed by `get-history`.

- `-o, --output <PATH>` — Where to write the file. If `PATH` is an existing
  directory, the media is saved inside it under its original file name. Use `-`
  to stream the raw bytes to stdout. If omitted, the file is downloaded into
  TDLib's cache and its path is printed.
- `--mark-read` / `--no-mark-read` — Mark the message as read on the server, or
  not. Overrides the config (see [Configuration](#configuration)).

The command blocks until the download finishes. The resulting file path is
printed to stdout; progress and status are written to stderr.

```
tdctl download @user 185 -o ./photo.jpg
tdctl download @user 185 -o ~/Downloads/   # keep the original file name
tdctl download @user 185 -o - > image.jpg  # stream bytes to a file
tdctl download @user 185                    # just print the cache path
```

#### `send-message`

Send a message to a chat.

```
tdctl send-message [OPTIONS] <CHAT> [TEXT]
```

`<CHAT>` is a numeric chat ID or `@username`.

- `--photo <PATH>` — Photo file to attach (can be repeated)
- `--video <PATH>` — Video file to attach (can be repeated)
- `--document <PATH>` — Document file to attach (can be repeated)
- `--audio <PATH>` — Audio file to attach (can be repeated)
- `--file <PATH>` — Generic file, sent as document (can be repeated)
- `--reply-to <ID>` — MTP message ID to reply to

If no text argument is given and no files are specified, text is read from stdin. Multiple files are sent as an album; text becomes the caption on the first item.

Prints the MTP message ID(s) of sent messages on success.

```
tdctl send-message @user "Hello"
echo "Hello" | tdctl send-message @user
tdctl send-message @user "Caption" --photo ./img.jpg
tdctl send-message @user --photo a.jpg --photo b.jpg --video c.mp4
tdctl send-message @user --document ./report.pdf
tdctl send-message @user "Reply" --reply-to 42
```

#### `search-chats`

Search for chats by name or username. Combines server-side and public search results.

```
tdctl search-chats [OPTIONS] <QUERY>
```

- `--limit <N>` — Maximum results to show (default: 20)

```
tdctl search-chats "linux"
tdctl search-chats "durov" --limit 5
```

#### `list-accounts`

List available accounts.

```
tdctl list-accounts
```

#### `export`

Export data from tdesktop.

```
tdctl export [OPTIONS] <PATH>
```

- `--format <FORMAT>` — Export format: `json`, `html`, or `html_and_json` (default: `json`)
- `--type <TYPES>` — Data types to export (comma-separated, defaults to all)
- `--media-type <TYPES>` — Media types to download (comma-separated)
- `--media-size-limit <BYTES>` — Media size limit in bytes
- `--from-date <TIMESTAMP>` — Export messages after this Unix timestamp
- `--till-date <TIMESTAMP>` — Export messages before this Unix timestamp

#### `raw`, `tdlib raw`, `tdesktop raw`, `mtp raw`

Send raw JSON to the socket, optionally wrapped in a TDLib, tdesktop, or MTP envelope. Reads from stdin if no argument is given.

```
tdctl raw '{"@type": "..."}'
tdctl tdlib raw '{"@type": "getMe"}'
tdctl mtp raw '{"@type": "..."}'
```

## Configuration

tdctl reads an optional TOML config file. An explicit `--config <PATH>` (or the
`TDCTL_CONFIG` env var) is used directly; otherwise tdctl follows the XDG Base
Directory Specification, searching `$XDG_CONFIG_HOME/tdctl/config.toml` (default
`~/.config/tdctl/config.toml`) and then each `$XDG_CONFIG_DIRS` entry (default
`/etc/xdg/tdctl/config.toml`), using the first that exists. A missing file is
fine; a malformed one warns and falls back to defaults.

### Marking messages as read

The commands that read message content — `get-history`, `listen`, and
`download` — can mark those messages as read on the server (TDLib
`viewMessages` with `force_read`). Whether they do is resolved by precedence,
highest first:

1. The `--mark-read` / `--no-mark-read` CLI flag
2. The per-command `mark_read` config key
3. The global `mark_read` config key
4. The built-in default, which is **on** (mark read)

```toml
# ~/.config/tdctl/config.toml

# Global default for all read-marking commands (built-in default: true).
mark_read = true

# Optional per-command overrides of the global default.
[get-history]
mark_read = false

[listen]
mark_read = true

[download]
mark_read = false
```
