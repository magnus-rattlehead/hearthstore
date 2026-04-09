# Origin & Context

## The problem we hit

hearthstore was conceived while working on a company data import pipeline for a
local development environment. The pipeline imports entities from BigQuery into
a local Firestore emulator — companies, users, calls, contacts, and ~80 other
entity types.

For large companies the official emulator consistently ran out of heap:

```
java.lang.OutOfMemoryError: Java heap space
```

We tried the obvious JVM mitigations:

- Pre-allocated heap (`-Xms16g -Xmx16g`) to eliminate resize pauses
- G1GC with tuned parameters (IHOP=30, G1ReservePercent=20, ParallelRefProcEnabled)
- `-XX:+UseStringDeduplication` to reduce duplicate string overhead (entity
  property names repeat millions of times across stored documents)

These helped with GC pause latency and reduced some overhead, but didn't solve
the root problem: the emulator is fundamentally an in-memory store. Its
resident memory equals the sum of all stored documents. There is no disk
paging.

## Why a wrapper isn't straightforward

The emulator is a closed-source JAR. We can patch the shell runner script
(JVM flags) but not the emulator itself.

A "disk paging wrapper" that sits in front of the emulator and evicts cold
documents would require implementing Firestore's query semantics — filters,
ordering, composite indexes — for the evicted portion. Any document not in the
emulator would be invisible to queries. This is effectively reimplementing the
emulator.

## The actual idea

Since we'd have to implement query semantics anyway, the cleaner approach is to
skip the official emulator entirely and build a Firestore-compatible gRPC server
backed by a disk store.

Key insight: SQLite with `PRAGMA mmap_size` lets the OS manage paging
transparently. When the dataset fits in RAM it behaves like an in-memory store.
When it doesn't, the OS evicts cold pages automatically. This is fundamentally
better than the JVM model where the GC can never page anything out.

## Performance concern

The main concern raised: would disk-backed writes be too slow compared to the
in-memory emulator?

For **reads during development**: SQLite with a warm OS page cache is
effectively memory-speed for hot data. Real Firestore has 10–100ms network
latency; SQLite point reads are ~0.1ms. Not a practical concern.

For **writes during import**: SQLite in WAL mode adds ~1–5ms per transaction
vs. ~100μs for in-memory JVM writes. For a bulk import with tens of thousands
of batches this is measurable — roughly 2–5× slower import times.

Mitigation:
- `PRAGMA mmap_size` + `PRAGMA cache_size` — when data fits in RAM, SQLite
  behaves like an in-memory store
- `BatchWrite` maps to a single SQLite transaction, amortising fsync cost
- RocksDB as an alternative backend if write throughput proves insufficient
  (LSM tree, used in CockroachDB/TiKV, designed for write-heavy workloads)

The trade-off: import is a one-time cost; the development workflow (queries,
targeted reads/writes) is what matters day-to-day, and that would be fine.

## What's making contacts so expensive in the emulator

In the import pipeline, contacts have two fields — `phones_details` and
`emails_details` — that are large `JsonProperty` blobs (serialized JSON stored
directly on the entity). For a company with hundreds of thousands of contacts,
the cumulative size of these blobs alone can be several GB in the emulator heap.

The bare contact entity (name, raw phone/email fields) is ~100–200 bytes.
With full detail blobs it can be 1–5 KB per entity. At 500k contacts that's
the difference between ~100 MB and ~2.5 GB just for contacts.

This is a concrete example of the class of problem hearthstore targets: data
that fits fine on disk but exhausts an in-memory emulator.

## Name

`hearthstore` — hearth (fire reference, kin to Firestore) + store (storage).
Short, memorable, unique.
