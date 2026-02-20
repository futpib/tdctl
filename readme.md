# tdctl

CLI client for [tdesktop](https://github.com/futpib/tdesktop) Unix socket API.

## Usage

```
tdctl [OPTIONS] <COMMAND>
```

### Global options

- `--socket <SOCKET>` — Path to the tdesktop Unix socket (env: `TDCTL_SOCKET`)
- `-a, --account <ACCOUNT>` — Account index

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

Date arguments accept natural language (e.g. `"2025-01-01"`, `"last monday"`).

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
