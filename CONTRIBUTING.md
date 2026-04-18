# Contributing

## Development setup

```bash
git clone https://github.com/magnus-rattlehead/hearthstore
cd hearthstore
go build ./...
go test ./...
```

The server can be run directly without a separate install step:

```bash
go run ./cmd/server --port 8080
```

## Testing

All changes must include tests. Write tests **before** implementation — the test defines the contract, the implementation satisfies it.

```bash
# Unit + integration tests
go test ./...

# Run a specific package
go test ./internal/storage/...
go test ./internal/server/...

# Run with verbose output
go test -v ./...
```

Integration tests start an in-process server and exercise it through the real Firestore/Datastore client SDKs. They live in `tests/` and require no external services.

## Code style

- Standard `gofmt` / `goimports` formatting
- Comments only where the code is not self-evident: exported function docstrings, explanations of non-obvious values or algorithms, protocol notes
- No speculative abstractions — implement exactly what the test requires

## Submitting changes

1. Fork the repo and create a branch from `main`
2. Write tests first
3. Ensure `go build ./...` and `go test ./...` pass cleanly
4. Open a pull request with a clear description of what and why
