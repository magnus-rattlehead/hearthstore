# hearthstore — Design Notes

## Problem

The official Cloud Firestore emulator (closed-source Java JAR) stores all data
in JVM heap memory. For large datasets (companies with millions of entities),
this causes:

- Memory exhaustion (16–32 GB heap for a single large company)
- Long GC pauses during writes
- No disk paging — all data must fit in RAM

There is no open-source, disk-backed alternative to the Firestore emulator.

## Goal

A drop-in replacement for the Firestore emulator that:

- Implements the same Firestore gRPC API (`google.firestore.v1.Firestore`)
- Stores documents on disk — memory usage scales with working set, not total data
- Is compatible with existing Firestore client libraries (Python, Go, Node, etc.)
  without any client-side changes
- Targets local development and integration testing use cases

## Non-goals (v1)

- Security rules enforcement
- Real-time listeners (`Listen` RPC / `onSnapshot`)
- Composite index enforcement (queries work without pre-defined indexes)
- Full production parity

## Architecture

```
Client (any Firestore SDK)
  │
  │  gRPC (port 8080 by default)
  ▼
hearthstore server (Go)
  │
  ├── Document store  ──► SQLite (WAL mode, mmap)
  └── Field index     ──► SQLite index tables
```

### Storage layer

SQLite in WAL mode with memory-mapped I/O (`PRAGMA mmap_size`). The OS page
cache handles paging transparently — when the dataset fits in RAM it behaves
like an in-memory store; when it doesn't, cold pages are evicted automatically.

Documents are stored as serialized Firestore `Value` protobufs, keyed by full
document path.

Schema sketch:

```sql
CREATE TABLE documents (
    project     TEXT NOT NULL,
    database    TEXT NOT NULL,
    path        TEXT NOT NULL,   -- e.g. "companies/123/users/456"
    collection  TEXT NOT NULL,   -- leaf collection name, for ListDocuments
    data        BLOB NOT NULL,   -- serialized google.firestore.v1.Document proto
    create_time TEXT NOT NULL,
    update_time TEXT NOT NULL,
    deleted     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (project, database, path)
);

CREATE TABLE field_index (
    project         TEXT NOT NULL,
    database        TEXT NOT NULL,
    collection_path TEXT NOT NULL,  -- full path to collection
    doc_path        TEXT NOT NULL,
    field_path      TEXT NOT NULL,  -- dot-separated field path
    -- Normalized value columns for efficient range queries
    value_string    TEXT,
    value_int       INTEGER,
    value_double    REAL,
    value_bool      INTEGER,
    value_bytes     BLOB,
    value_null      INTEGER,
    value_ref       TEXT,
    FOREIGN KEY (project, database, doc_path)
        REFERENCES documents (project, database, path)
);

CREATE INDEX idx_field ON field_index
    (project, database, collection_path, field_path, value_string, value_int, value_double);
```

### Query layer

Firestore queries always target a single collection (or collection group).
Execution plan:

1. Resolve collection path from request
2. Translate `StructuredQuery.where` filters to SQL WHERE clauses via field_index
3. Apply `order_by` as SQL ORDER BY
4. Apply `limit` / `offset`
5. Fetch matching document paths, load from documents table
6. Return as `RunQueryResponse` stream

Supported filter operators (v1):
- `EQUAL`, `NOT_EQUAL`
- `LESS_THAN`, `LESS_THAN_OR_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`
- `ARRAY_CONTAINS`
- `IN`, `NOT_IN`, `ARRAY_CONTAINS_ANY`

### Transactions

SQLite provides ACID transactions. Firestore transaction semantics:
- `BeginTransaction` → SQLite `BEGIN`
- reads within transaction → `SELECT ... FOR READ` (snapshot isolation via WAL)
- `Commit` with writes → apply writes then `COMMIT`
- `Rollback` → SQLite `ROLLBACK`

## Write throughput

SQLite WAL mode sustains ~100k–500k simple writes/second on modern NVMe.
Batch writes (`BatchWrite`) are wrapped in a single SQLite transaction,
amortising fsync cost across the batch.

If write throughput proves insufficient, the storage backend can be swapped
to RocksDB (LSM tree, designed for write-heavy workloads).

## Supported RPCs (v1 target)

| RPC                    | Status   |
|------------------------|----------|
| GetDocument            | planned  |
| ListDocuments          | planned  |
| CreateDocument         | planned  |
| UpdateDocument         | planned  |
| DeleteDocument         | planned  |
| BatchGetDocuments      | planned  |
| BatchWrite             | planned  |
| RunQuery               | planned  |
| RunAggregationQuery    | planned  |
| BeginTransaction       | planned  |
| Commit                 | planned  |
| Rollback               | planned  |
| ListCollectionIds      | planned  |
| Listen                 | deferred |
| PartitionQuery         | deferred |

## Stack

- **Language:** Go
- **gRPC:** `google.golang.org/grpc`
- **Protobuf:** `google.golang.org/protobuf` + `cloud.google.com/go/firestore/apiv1/firestorepb`
- **SQLite:** `modernc.org/sqlite` (pure Go, no CGo required)
- **Proto definitions:** `google.golang.org/genproto/googleapis/firestore/v1`

## Usage (planned)

```bash
# Start server
hearthstore --port 8080 --data-dir ./data

# Point any Firestore client at it
export FIRESTORE_EMULATOR_HOST=localhost:8080
```

Compatible with the standard `FIRESTORE_EMULATOR_HOST` environment variable
used by all official Firestore SDKs.
