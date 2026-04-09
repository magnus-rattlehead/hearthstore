# hearthstore

A disk-backed, open-source drop-in replacement for the [Cloud Firestore emulator](https://firebase.google.com/docs/emulator-suite/connect_firestore).

The official emulator stores all data in JVM heap memory, making it impractical
for large datasets. hearthstore uses SQLite with memory-mapped I/O — the OS
page cache handles what fits in RAM, the rest stays on disk.

## Status

Early development. See [notes/design.md](notes/design.md) for architecture and
[internal/server/server.go](internal/server/server.go) for RPC implementation
status.

## Usage

```bash
hearthstore --port 8080 --data-dir ./data
```

Point any Firestore client at it via the standard environment variable:

```bash
export FIRESTORE_EMULATOR_HOST=localhost:8080
```

## Building

```bash
go build ./cmd/server
```

## Design

See [notes/design.md](notes/design.md).
