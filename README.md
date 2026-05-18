# hearthstore

A disk-backed, open-source drop-in replacement for the Cloud Firestore and Cloud Datastore emulators.

The official Firestore emulator stores all data in JVM heap memory, making it impractical for large datasets or long-running development sessions. hearthstore uses SQLite with WAL mode and memory-mapped I/O: the OS page cache handles what fits in RAM, the rest stays on disk. Memory usage scales with the working set, not total data size.

## Features

- **Firestore Native API** — full gRPC + WebChannel (Firebase JS SDK) + REST support
- **Cloud Datastore API** — gRPC and REST, compatible with all Datastore client libraries
- **Persistent storage** — SQLite on disk; data survives restarts, no size limits imposed by RAM
- **Dashboard** — live metrics, queue depths, runtime stats, and recent RPC history at `/_/dashboard`

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

Open the dashboard in a browser:

```
http://localhost:8080/_/dashboard
http://localhost:8456/_/dashboard
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
| `-reindex-ds` | `false` | Rebuild the Datastore field index from stored entities, then serve normally |

### Data directory

The data directory is resolved in this order:
1. `-data-dir` flag
2. `HEARTHSTORE_DATA_DIR` environment variable
3. `~/.hearthstore`
4. `./data` (fallback if home directory is unavailable)

## Architecture

hearthstore multiplexes gRPC (HTTP/2), WebChannel long-poll streams (for the Firebase JS SDK), and the Firestore REST API all on the same port. The Datastore API runs on a separate port and multiplexes gRPC and REST the same way.

Storage uses three SQLite connection pools against a single WAL-mode database:
- **wdb** — one write connection; all mutations are serialized here
- **rdb** — unlimited read connections; SQLite WAL allows arbitrary concurrent readers without blocking writers; idle connections are pooled up to `max(4, GOMAXPROCS)`
- **cpdb** — one dedicated checkpoint connection; keeps WAL checkpoints off the write path so commits never stall

Non-transactional writes are coalesced by a group-commit loop: concurrent RPCs are batched into a single SQLite transaction, amortizing fsync cost across callers. Transactional commits bypass the batch loop via a priority queue and run in isolation.

See `DESIGN.md` for full architecture details.

## Status

The Firestore Native and Cloud Datastore APIs are substantially complete. The full `googleapis/nodejs-firestore` system test suite passes. See `TODO.md` for remaining gaps and planned work.
