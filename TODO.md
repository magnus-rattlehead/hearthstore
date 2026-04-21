# hearthstore — TODO

## Real-world validation

- **Run app test suites against hearthstore**: point existing apps' integration test suites at hearthstore (via `FIRESTORE_EMULATOR_HOST` / `DATASTORE_EMULATOR_HOST`) and compare results against the official emulator.
- **nodejs-firestore system tests**: `npm run system-test` in the `googleapis/nodejs-firestore` repo exercises the full Node.js SDK surface. Track pass rate and close remaining gaps.
- **nodejs-datastore system tests**: same for `googleapis/nodejs-datastore`.

## Distribution

- **Docker image + release workflow**: Add a `Dockerfile` and GitHub Actions release pipeline. Needed for drop-in CI replacement of `gcr.io/google.com/cloudsdktool/cloud-sdk`.

## Benchmarks

- Measure RSS at 10k / 100k / 1M documents vs the official Firestore emulator.
- Measure p50/p99 query latency for equality and range queries.

## Known gaps

- **GeoPoint filters**: fall back to in-process Go evaluation; no SQL pushdown. Needs a dedicated test to ensure correctness.
- **`__name__` ORDER BY**: `__key__` is not stored in `ds_field_index`, so keyset pagination falls back to Go-side processing for queries that ORDER BY `__key__`. Cursor positioning across pages may behave differently from Datastore in edge cases.
- **Keyset pagination + DESC + NULL sort values**: when a kind has entities where a DESC-sorted field is absent (NULL in `ds_field_index`), those entities appear after all non-null values in SQLite's DESC ordering but are not included in keyset cursor conditions. Queries against uniformly-populated kinds are unaffected.
- **Multi-database**: Firestore supports named databases (`projects/p/databases/mydb`); the path parser should be tested with non-`(default)` names.
- **`POST /reset` endpoint**: a reset endpoint that clears all data without restarting would be useful for test suite teardown.
