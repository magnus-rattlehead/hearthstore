# hearthstore

A disk-backed, open-source drop-in replacement for the Cloud Firestore and Cloud Datastore emulators.

The official Firestore emulator stores all data in JVM heap memory, making it impractical for large datasets or long-running development sessions. hearthstore uses SQLite with WAL mode and memory-mapped I/O: the OS page cache handles what fits in RAM, the rest stays on disk.

## Features

- **Firestore Native API**, full gRPC + WebChannel (Firebase JS SDK) + REST support
- **Cloud Datastore API**, gRPC and REST, compatible with all Datastore client libraries
- **Dashboard**, live metrics, storage queue/checkpoint timings, runtime stats, and recent RPC history at `/_/dashboard`

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

hearthstore runs non-blocking SQLite WAL checkpoints periodically so active
readers do not stall writes. On clean shutdown it performs a final truncating
checkpoint to reclaim the WAL file.

### Datastore composite indexes

Pass a standard Datastore `index.yaml` with `-index-config`. Configured indexes
are materialized before the Datastore listener starts, with build progress in
the structured log and dashboard. Index entries are maintained atomically with
entity writes.

When a complex query has no configured index, hearthstore runs it through the
generic query path and builds the required composite index in the background.
Discovered definitions are written to `<data-dir>/index.generated.yaml`; the
supplied file is never modified. Later queries use a direct ordered index scan
once the index is ready. The Datastore Admin `CreateIndex`, `DeleteIndex`,
`ListIndexes`, and `GetIndex` RPCs use the same index catalog.

Composite indexes trade additional disk usage and write work for predictable
query latency. Array-valued properties can produce multiple index entries, so
hearthstore enforces Datastore-style exploding-index size limits.

Query cursors use a versioned format. Composite cursors contain the index ID,
generation, encoded index key, and entity-path tie-breaker, allowing subsequent
pages to seek directly into the same index. Cursors produced by older
hearthstore versions are not supported.

## Status

The Firestore Native and Cloud Datastore APIs are substantially complete. The full `googleapis/nodejs-firestore` system test suite passes.
