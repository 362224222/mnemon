# Mnemon — Project Guidelines

## Development

- **Build**: `go build -o mnemon ./cmd/mnemon` and `go build -o mnemond ./cmd/mnemond`
- **Install**: `make install && mnemon setup`
- **Test**: `make test`
- **Integration**: `make test-integration` for CLI E2E and mnemond Agency boundaries
- **Dependencies**: `modernc.org/sqlite`, `spf13/cobra`, `google/uuid`
- **Optional**: Ollama with `nomic-embed-text` for embedding support
