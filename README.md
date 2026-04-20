# hearthstore

A disk-backed, open-source drop-in replacement for the Cloud Firestore and Cloud Datastore emulators.

The official Firestore emulator stores all data in JVM heap memory, making it impractical for large datasets or long-running development sessions. hearthstore uses SQLite with WAL mode and memory-mapped I/O: the OS page cache handles what fits in RAM, the rest stays on disk. Memory usage scales with the working set, not total data size.

## Features

- **Firestore Native API** — full gRPC + WebChannel (Firebase JS SDK) + REST support
- **Cloud Datastore API** — gRPC and REST, compatible with all Datastore client libraries

## Quick start

```bash
go install github.com/magnus-rattlehead/hearthstore/cmd/server@latest
hearthstore
```

Or build from source:

```bash
git clone https://github.com/magnus-rattlehead/hearthstore
cd hearthstore
go build -o hearthstore ./cmd/server
./hearthstore
```

Point your Firestore client at it:

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
```

Point your Datastore client at it:

```bash
export DATASTORE_EMULATOR_HOST=localhost:8456
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8080` | gRPC listen port for the Firestore Native API (also serves WebChannel and REST on the same port) |
| `-web-port` | `0` | Secondary HTTP port for gRPC-Web + WebChannel + REST (0 = disabled) |
| `-datastore-addr` | `:8456` | Listen address for the Cloud Datastore API (gRPC + REST) |
| `-data-dir` | `~/.hearthstore` | Directory for SQLite database files |
| `-mode` | `both` | Which APIs to serve: `firestore`, `datastore`, or `both` |
| `-log-level` | `info` | Structured log verbosity: `debug`, `info`, `warn`, or `error` |
| `-index-config` | _(none)_ | Path to a Datastore `index.yaml` for composite index configuration |

### Data directory

The data directory is resolved in this order:
1. `-data-dir` flag
2. `HEARTHSTORE_DATA_DIR` environment variable
3. `~/.hearthstore`
4. `./data` (fallback if home directory is unavailable)

## Architecture

hearthstore multiplexes gRPC (HTTP/2), WebChannel long-poll streams (for the Firebase JS SDK), and the Firestore REST API all on the same port.

Storage uses two SQLite connection pools against a single WAL-mode database:
- One write connection (serializes all mutations)
- Up to four read connections (concurrent reads never block writers in WAL mode)

See `DESIGN.md` for full architecture details.

## Status

Early development. The Firestore Native and Cloud Datastore APIs are substantially complete for typical emulator use cases. See `TODO.md` for flagged implementation gaps.
