# Git Workflow for asterisk-host

This repository contains the Slidebolt Asterisk host. It owns the connection to Asterisk (ARI), all call state and the safety limits, and exposes them as a versioned REST API with an event stream, the same shape as `zigbee-host`. It produces a standalone binary, and holds no behavior: what happens and when is decided by whoever calls the API.

## Dependencies
- **Internal:**
  - `sb-contract`: Core interfaces and shared structures.
  - `sb-media-sdk`: Contract with `sb-media`, which serves streams.
  - `sb-messenger-sdk`: Shared messaging interfaces.
  - `sb-runtime`: Core execution environment.
- **External:**
  - `github.com/gorilla/websocket`: ARI events.
  - Asterisk with ARI enabled, at runtime.

## Build Process
- **Type:** Go Application (Service).
- **Command:** `go build -o asterisk-host ./cmd/asterisk-host`
- **Validation:**
  - Unit tests with a fake ARI: `go test ./...`
  - Against a real Asterisk: `go test -tags integration -v ./internal/ari/...`
  - End to end: see `sb-e2e`.
