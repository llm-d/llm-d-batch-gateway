# Integration Tests

Integration tests validate feature-level behavior through the real HTTP stack — routing, middleware, and handlers — using in-memory mock backends or real external services. Tests that require external services (Docker, S3) gracefully skip when those services are unavailable.

## What they cover

- **Batch workflows**: create, retrieve, list, cancel, validation errors
- **File workflows**: upload, download, retrieve, list, delete, validation errors
- **Multi-tenant isolation**: resources created by one tenant are invisible to another
- **Cross-cutting behavior**: security headers, request ID propagation, 404 JSON format
- **Inference client**: HTTP client integration with containerized mock server (Docker or Podman)
- **S3 client**: file store operations against a real S3-compatible service (SeaweedFS in the dev deployment)
- **GCS client**: file store operations against real GCS or a GCS emulator
  - `make test-integration-gcs` auto-provisions a [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) container in-process and points the SDK at it via `STORAGE_EMULATOR_HOST`; no GCP credentials needed.
  - `make test-integration` runs the same tests against real GCS when `GCS_TEST_BUCKET`/`GCS_TEST_PROJECT` are set (ADC).

## Prerequisites

- Go 1.26+
- Docker or Podman (optional, for inference client tests and the GCS emulator)
- S3-compatible service (optional, for S3 tests — set `S3_TEST_ENDPOINT`)
- GCS-compatible service (optional, for GCS tests — `make test-integration-gcs` provisions fake-gcs-server automatically; or set `GCS_TEST_BUCKET`/`GCS_TEST_PROJECT` to use real GCS via ADC, or `STORAGE_EMULATOR_HOST` to point the SDK at a manually-started emulator)

## Run the tests

```bash
make test-integration
```

Or directly:

```bash
go test -v -tags=integration -count=1 ./test/integration/...
```

## How they work

Each API test calls `newTestServer(t)` which spins up an `httptest.Server` wired with:

- The same `http.ServeMux` and middleware chain as the production server (`Recovery → RequestMiddleware → SecurityHeaders`)
- In-memory mock DB clients (batch and file metadata)
- A local-filesystem file store under `t.TempDir()`

Tests make real HTTP requests and validate responses as a client would. No state persists between tests — each `newTestServer` call creates a fresh, isolated instance.

## Build tag

All files carry `//go:build integration`. They are excluded from `make test` (unit-only) and run with `make test-integration`.
