# hearthstore — Design

## Problem

The official Cloud Firestore emulator (closed-source Java JAR) stores all data in JVM heap memory. For large datasets this causes memory exhaustion, long GC pauses, and no disk paging — the entire dataset must fit in RAM.

There is no open-source, disk-backed alternative.

## Goal

A drop-in replacement for the Firestore and Datastore emulators that:

- Implements `google.firestore.v1.Firestore` (gRPC + WebChannel + REST) and the Cloud Datastore API (gRPC + REST)
- Stores data on disk — memory usage scales with the working set, not total data size
- Is compatible with all official client libraries without client-side changes
- Targets local development and CI integration testing

## Non-goals (v1)

- Security rules enforcement
- Composite index enforcement (queries work without pre-defined indexes)
- Full production parity

## Architecture

```
Browser SDK (JS)            Node / Go / Python SDK
     │                              │
     │  WebChannel long-poll        │  gRPC (HTTP/2)
     │  REST (/v1/...)              │
     └──────────────────────────────┘
                      │
                      │  port 8080 (multiplexed)
                      ▼
           hearthstore server (Go)
                      │
          ┌───────────┴────────────┐
          │  Firestore Native API  │  Cloud Datastore API
          │  (internal/server)     │  (internal/datastore) — port 8456
          └───────────┬────────────┘
                      │
               internal/storage
                      │
               SQLite (WAL + mmap)
```

A single port multiplexes gRPC (detected by `Content-Type: application/grpc`), WebChannel (paths ending in `/channel`), and REST (`/v1/...` paths). The Firebase JS browser SDK sends all traffic — including streaming Listen and Write RPCs — to one address, so multiplexing on one port avoids client configuration complexity.

An optional second port (`--web-port`) exposes gRPC-Web for environments that need it separately.

## Storage layer

SQLite in WAL mode with memory-mapped I/O (`PRAGMA mmap_size=8GiB`). The OS page cache handles paging transparently — hot data stays in RAM, cold data is evicted automatically.

Two connection pools share a single file:
- **wdb**: one write connection (serialises all mutations)
- **rdb**: up to 4 read connections (WAL allows concurrent reads without blocking writes)

### Key tables

| Table | Purpose |
|-------|---------|
| `documents` | Firestore documents (serialized proto blobs) |
| `field_index` | Denormalized scalar values for SQL filter pushdown |
| `document_changes` | Append-only changelog for Listen resume tokens |
| `ds_documents` | Datastore entities |
| `ds_field_index` | Denormalized Datastore property values for filter pushdown |
| `ds_document_changes` | Append-only changelog for Datastore snapshot reads |
| `ds_id_sequences` | Auto-allocated ID counters per kind |

### Query execution

Firestore `StructuredQuery` filters are translated to SQL EXISTS subqueries against `field_index` by `internal/query`. The same pattern applies for Datastore queries via `ds_field_index`. Types that cannot be expressed in SQL (GeoPoint, KeyValue, EntityValue filters) fall back to in-process Go evaluation on the reduced result set.

Datastore `RunQuery` uses **keyset pagination** when `limit > 0` and no snapshot read / DISTINCT is required: `ORDER BY`, the cursor condition, and `LIMIT` are pushed entirely into SQLite. Each page is O(page-size) rather than O(table-size). Sort columns are auto-detected from `ds_field_index` on first use; if a sort field has no indexed data the query falls back to the Go-side path. Cursors are URL-safe base64-encoded JSON (`{"p": path, "s": [{c, v}, ...]}`); plain-path cursors from earlier versions are accepted for backward compatibility.

### Listen (real-time)

Document writes publish to an in-process pub/sub channel. Watch streams receive change events and deliver `DocumentChange` messages to the client. Resume tokens encode the last-seen change log sequence number; on reconnect the server replays only the diff.

## Firestore RPC status

| RPC | Status |
|-----|--------|
| GetDocument | done |
| ListDocuments | done |
| CreateDocument | done |
| UpdateDocument | done |
| DeleteDocument | done |
| BatchGetDocuments | done |
| BatchWrite | done |
| RunQuery | done |
| RunAggregationQuery | done |
| BeginTransaction | done |
| Commit | done |
| Rollback | done |
| ListCollectionIds | done |
| Listen | done |
| PartitionQuery | stub (returns empty) |

## Datastore API status

Lookup, RunQuery, RunAggregationQuery, BeginTransaction, Commit, Rollback, AllocateIds, ReserveIds — all done. Composite index config loaded from `index.yaml` via `--index-config`.

## Stack

- **Language:** Go
- **gRPC:** `google.golang.org/grpc`
- **Protobuf:** `google.golang.org/protobuf`
- **Firestore protos:** `cloud.google.com/go/firestore/apiv1/firestorepb`
- **Datastore protos:** `cloud.google.com/go/datastore/apiv1/datastorepb`
- **SQLite:** `modernc.org/sqlite` (pure Go, no CGo)
