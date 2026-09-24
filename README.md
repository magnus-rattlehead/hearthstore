# Hearthstore

Hearthstore is a disk-backed replacement for Google's Datastore emulator, serving the Datastore gRPC and REST APIs.

## Building

Requires Go 1.24 or newer.

```sh
git clone https://github.com/magnus-rattlehead/hearthstore.git
cd hearthstore
go build -o hearthstore ./cmd/server
```

## Usage

Start the server:

```sh
./hearthstore
```

Point your Datastore client at it:

```sh
export DATASTORE_EMULATOR_HOST=localhost:8456
export DATASTORE_PROJECT_ID=my-project-id
```

Replace `my-project-id` with your application's project ID. Clients already configured for the official emulator should work without code changes.

Data is stored in `~/.hearthstore`. The dashboard is at <http://localhost:8456/_/dashboard>.

Arguments:

- `-datastore-addr=localhost:8081` — change the listen address; update `DATASTORE_EMULATOR_HOST` to match.
- `-data-dir` — use a different storage directory.
- `-index-config` — load a Datastore `index.yaml`.
- `-import-data` — import a local Datastore export before serving.
- `-export-on-exit` — export to a local directory on `SIGINT` or `SIGTERM`.

Import/export requires `-project-id`; `-database-id` defaults to `(default)`. Imports overwrite matching keys, retain unrelated entities, and remap keys to the selected project and database.

Run `./hearthstore -h` for all options, including cache, query memory, and concurrency settings.

## Differences

- **Datastore only.** No Firestore Native API, Firebase emulator features, or GQL queries.
- **Always strongly consistent and disk-backed.** No eventual-consistency simulation or in-memory mode. Clean shutdown flushes acknowledged writes; a crash may lose the newest writes.
- **Automatic composite indexes.** Missing indexes are built and persisted; queries wait instead of returning a missing-index error. Generated indexes are not added to `index.yaml`.
- **Optimistic transactions.** Clients must retry `ABORTED` transactions. Query transactions conservatively conflict with any concurrent write to the queried kind in the same partition.
- **Local import/export only.** Local transfers use Google's Datastore export format for exchange with the official emulator. The production Datastore Admin `ImportEntities` and `ExportEntities` RPCs, which use Cloud Storage and long-running operations, are not implemented. Imported entities receive new local timestamps and versions.
- **Storage and cursors can become incompatible across versions.** There is no in-place storage migration. Startup asks before deleting incompatible data; declining or running non-interactively leaves it untouched and exits. To preserve data, reimport the original Datastore export into a new directory. Restart queries with incompatible cursors.

## Raising an issue

Open a [GitHub issue](https://github.com/magnus-rattlehead/hearthstore/issues) with:

- What you expected and what happened.
- A minimal reproduction, including the query or mutation and sample data.
- Your Hearthstore commit, OS, client library version, startup flags, and relevant logs.
- For compatibility issues, the result from Google's emulator or managed Datastore, if available.

## AI disclosure

Hearthstore is developed with AI assistance.
