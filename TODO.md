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
- **`POST /reset` endpoint**: a reset endpoint that clears all data without restarting would be useful for test suite teardown.
