# Batch Gateway

[![Go Report Card](https://goreportcard.com/badge/github.com/llm-d/llm-d-batch-gateway)](https://goreportcard.com/report/github.com/llm-d/llm-d-batch-gateway)
[![Go Version](https://img.shields.io/badge/Go-1.27.1-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Join Slack](https://img.shields.io/badge/Join_Slack-blue?logo=slack)](https://llm-d.slack.com/archives/C0AA8772H7T)
[![apiserver](https://img.shields.io/github/v/release/llm-d/llm-d-batch-gateway?label=apiserver)](https://github.com/llm-d/llm-d-batch-gateway/pkgs/container/batch-gateway-apiserver)
[![processor](https://img.shields.io/github/v/release/llm-d/llm-d-batch-gateway?label=processor)](https://github.com/llm-d/llm-d-batch-gateway/pkgs/container/batch-gateway-processor)
[![gc](https://img.shields.io/github/v/release/llm-d/llm-d-batch-gateway?label=gc)](https://github.com/llm-d/llm-d-batch-gateway/pkgs/container/batch-gateway-gc)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fllm-d%2Fllm-d-batch-gateway.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fllm-d%2Fllm-d-batch-gateway?ref=badge_shield)

## Overview

Batch Gateway is a high-performance system for processing large-scale batch inference jobs in Kubernetes environments. It provides an OpenAI-compatible API for submitting, tracking, and managing batch inference jobs.

The system is designed to facilitate efficient processing of batch workloads in combination with interactive workloads. It minimizes interference with interactive workloads while satisfying batch jobs' service level objectives (SLOs).

### Use Cases

- Inferencing large datasets.
- Generating embeddings for large corpora.
- Model evaluations and testing.
- Offline analysis and batch processing.
- Cost-optimized inference using differential billing for batch vs. interactive workloads.

## Key Features

### Batch Processing

- **OpenAI API Compatibility**: Full schema parity with OpenAI's `/v1/batches` and `/v1/files` endpoints.
- **Large-Scale Processing**: Support for up to 50,000 requests per job.
- **Progress Tracking**: Real-time job status progress updates.
- **Job Management and Control**: Enables to manage and control batch jobs, before, during, and after their processing.
- **Model-Aware Scheduling**: Groups and orders requests by model and system-prompt hash for prefix cache optimization, keeping KV-cache entries hot and avoiding eviction-triggered prefill reconstruction.
- **Adaptive Concurrency Control**: AIMD-based flow control dynamically adjusts batch request dispatch rate based on downstream success/failure signals, protecting interactive traffic without manual tuning.

### System Design

- **Deployment Flexibility**: Separate API server, batch processor, and request dispatcher components for independent scaling.
- **Pluggable Storage Backends**: Supports pluggable storage backends for files, metadata, and queues.
- **Fault Tolerance**: Automatic recovery from batch processor crashes.

### Operations

- **Kubernetes Native**: Helm charts with OpenShift compatibility.
- **Observability**: Prometheus metrics and Open Telemetry integration.
- **Health Checks**: Liveness and readiness probes for the system components.
- **Security**: TLS support, non-root execution, capability dropping, read-only filesystem. Processor connections to HTTPS llm-d Routers can use custom CAs and mTLS; see [Processor inference TLS](docs/guides/processor-inference-tls.md). Gateway deployments: batch API admission and per-model inference authorization are enforced on separate routes; see **Security boundary** in the [Kubernetes](docs/guides/deploy-k8s.md#15-security-boundary-batch-route-vs-batch-llm-route-vs-llm-route) deployment guide.

## Architecture

### High-Level System Design

![Architecture Diagram](docs/design/diagrams/arch_1.png)

### Components

1. **API Server** ([`batch-gateway-apiserver`](cmd/apiserver))
   - Handles REST API requests for batch job submission, management and tracking, as well as file management.
   - Exposes OpenAI-compatible `/v1/batches` and `/v1/files` endpoints.

2. **Batch Processor** ([`batch-gateway-processor`](cmd/batch-processor))
   - Pulls a batch job from a priority queue, and gets its associated file of inference requests.
   - Pre-processes the batch file and builds per-model execution plans.
   - Sends downstream individual inference requests from the batch file, with per-model and global concurrency control.
   - Writes results to an output file.
   - Updates job status.
   - Listens to job events (e.g. cancellation) during job processing.

3. **Data Layer**
   - Manages batch input and output files, batch jobs' and files' metadata, priority queue, events and status mechanisms.
   - Supports pluggable backends.
   - Backends available out of the box:
     - Job and file metadata storage: `PostgreSQL`.
     - Priority queue, event channels, and status updates: `Redis` or `Valkey`.
     - File storage: `S3`, `filesystem`.

4. **Batch Dispatcher** *(optional)* — implemented in [llm-d/llm-d-async](https://github.com/llm-d/llm-d-async)
   - An optional component that provides metrics-driven flow control for batch request dispatching.
   - Monitors downstream inference system metrics (e.g. queue depth, latency, utilization) and dynamically adjusts dispatch flow to minimize interference with interactive requests while meeting batch job SLOs.
   - Not required for batch processing — the batch processor includes built-in AIMD-based adaptive concurrency control.

### Processing Flow

```text
User → API Server → PostgreSQL (metadata) + Redis/Valkey (queue) + S3 (input file)
                         ↓
                  Priority Queue
                         ↓
                  Batch Processor (pulls jobs)
                         ↓
              Ingestion
                  - Obtain input file
                  - Parse model IDs and system prompts
                  - Build per-model plans
                  - Write plans to local disk
                         ↓
              Execution
                  - Launch per-model goroutines
                  - Acquire global & per-model semaphores
                  - Read requests from plan files
                  - Send to llm-d Router
                  - Write results to output file
                         ↓
                  Upload Results to S3
                         ↓
                  Update Job Status
```

### Design Documents

For detailed architecture information see the [design directory](docs/design/).

## Repository Structure

```text
batch-gateway/
├── benchmarks/                   # Benchmark tooling, profiles, and results
├── cmd/                          # Application entry points
│   ├── apiserver/                # API server binary
│   ├── batch-processor/          # Batch processor binary
│   └── batch-gc/                 # Garbage collector binary
├── charts/                       # Helm charts
│   └── batch-gateway/            # Kubernetes deployment manifests
├── docker/                       # Dockerfiles
├── docs/                         # Documentation
│   ├── design/                   # Architecture and design documents
│   └── guides/                   # Developer and user guides
├── examples/                     # Deployment examples
│   ├── deploy-demo/              # Demo deployment resources
│   └── poc/                      # Proof-of-concept local demo
├── internal/                     # Private application code
│   ├── apiserver/                # API server implementation
│   ├── database/                 # Database clients (PostgreSQL, Redis)
│   ├── files_store/              # File storage clients (S3, filesystem)
│   ├── gc/                       # Garbage collector implementation
│   ├── processor/                # Batch processor implementation
│   ├── shared/                   # Shared types and utilities
│   └── util/                     # Common utilities (logging, TLS, etc.)
├── pkg/                          # Public library code
│   └── clients/                  # Reusable client libraries
├── scripts/                      # Development and deployment scripts
├── test/                         # Test suites
│   ├── e2e/                      # End-to-end tests
│   ├── integration/              # Integration tests
│   └── regression/               # Regression tests
├── tools/                        # Development tooling
├── Makefile                      # Build and development targets
└── go.mod                        # Go module dependencies
```

### Key Directories

- **`cmd/`**: Contains `main.go` entry points for the components' binaries.
- **`internal/`**: All private application code, organized by component.
- **`pkg/`**: Public library code.
- **`charts/`**: Helm chart for deploying the components in Kubernetes.
- **`docs/`**: Contains architecture documents and development / usage guides.
- **`examples/`**: Deployment examples and proof-of-concept demos.
- **`test/`**: Unit, integration, regression, and E2E test suites.

## Getting Started

### Prerequisites

- Go 1.27.1 or later.
- PostgreSQL 12+ (for metadata storage).
- Redis 6+ or Valkey 8+ (for job queue).
- S3-compatible object storage or local filesystem.
- Docker or Podman (for containerized deployment).
- Kubernetes 1.19+ and Helm 3.0+ (for Kubernetes deployment).

### Local Development

#### 1. Build Binaries

```bash
# Build all the components
make build

# Or build individually
make build-apiserver
make build-processor
make build-gc
```

#### 2. Run Tests

```bash
# Run unit tests
make test

# Run unit tests with coverage
make test-coverage

# Run integration tests
make test-integration

# Run unit tests and integration tests
make test-all

# Run E2E tests (requires a kind cluster)
make test-e2e
```

#### 3. Run Locally

Configure the components via YAML configuration files (see `cmd/apiserver/config.yaml` and `cmd/batch-processor/config.yaml` for examples).

```bash
# Run API server
make run-apiserver

# Run processor (in another terminal)
make run-processor

# Run gc (in another terminal)
make run-gc

# Or with verbose logging
make run-apiserver-dev
make run-processor-dev
make run-gc-dev
```

### Kubernetes Deployment

#### Quick Start with Kind

**Prerequisites:** see [Development Guide prerequisites](docs/guides/development.md#prerequisites).

Deploy to a local Kind cluster for development:

```bash
# Creates cluster, builds images, and deploys with Helm
make dev-deploy

# Or deploy a specific release version from GHCR
IMAGE_TAG=v0.1.0 SKIP_BUILD=true make dev-deploy
```

For detailed instructions, see [Development Guide](docs/guides/development.md).

#### Production Deployment

```bash
# Install all components (apiserver, processor, gc) with defaults
helm install batch-gateway ./charts/batch-gateway

# Scale processor replicas
helm install batch-gateway ./charts/batch-gateway \
  --set processor.replicaCount=3
```

See [Helm Chart README](charts/batch-gateway/README.md) for full configuration options.

### Docker Images

```bash
# Build all images
make image-build

# Or build individually
make image-build-apiserver
make image-build-processor
make image-build-gc
```

Images are published to:

- `ghcr.io/llm-d/batch-gateway-apiserver`
- `ghcr.io/llm-d/batch-gateway-processor`
- `ghcr.io/llm-d/batch-gateway-gc`

## API Usage

### Submit a Batch Job

```bash
# 1. Upload input file
curl -X POST http://localhost:8000/v1/files \
  -H "Content-Type: multipart/form-data" \
  -F "file=@batch_requests.jsonl" \
  -F "purpose=batch"

# Response: {"id": "file_abc123", ...}

# 2. Create batch job
curl -X POST http://localhost:8000/v1/batches \
  -H "Content-Type: application/json" \
  -d '{
    "input_file_id": "file_abc123",
    "endpoint": "/v1/chat/completions",
    "completion_window": "24h"
  }'

# Response: {"id": "batch_xyz789", "status": "validating", ...}
```

### Check Job Status

```bash
curl http://localhost:8000/v1/batches/batch_xyz789

# Response includes status: validating, in_progress, finalizing, completed, failed, expired, cancelled
```

### Retrieve Results

```bash
# Get output file ID from batch status
curl http://localhost:8000/v1/batches/batch_xyz789 | jq -r '.output_file_id'

# Download results
curl http://localhost:8000/v1/files/file-output123/content > results.jsonl
```

For complete API documentation, see the [OpenAI Batch API reference](https://platform.openai.com/docs/guides/batch).

## Configuration

### API Server Configuration

See configuration example in `cmd/apiserver/config.yaml`.

### Batch Processor Configuration

See configuration example in `cmd/batch-processor/config.yaml`.

### Garbage Collector Configuration

See configuration example in `cmd/batch-gc/config.yaml`.

## Monitoring

### Metrics

The batch gateway components expose Prometheus metrics for monitoring.
For a complete list of available metrics, see [docs/guides/metrics.md](docs/guides/metrics.md).

### Health Checks

**API Server:**

- Health: `GET /health` (port 8081).
- Readiness: `GET /ready` (port 8081).

**Processor:**

- Health: `GET /health` (port 9090).
- Readiness: `GET /ready` (port 9090).

### Profiling (pprof)

Both the API server and processor support [Go pprof](https://go.dev/blog/pprof) profiling endpoints on their observability ports. Pprof is controlled by the `enable_pprof` config option (or `config.enablePprof` in Helm values) and is **disabled by default**.

**Enable via Helm:**

```yaml
apiserver:
  config:
    enablePprof: true
processor:
  config:
    enablePprof: true
```

**Usage (with dev-deploy port-forwards):**

```bash
# API Server (port 8081)
go tool pprof http://localhost:8081/debug/pprof/profile?seconds=30  # CPU
go tool pprof http://localhost:8081/debug/pprof/heap                # Heap
go tool pprof http://localhost:8081/debug/pprof/allocs              # Allocs
go tool pprof http://localhost:8081/debug/pprof/goroutine           # Goroutine

# Processor (port 9090)
go tool pprof http://localhost:9090/debug/pprof/profile?seconds=30  # CPU
go tool pprof http://localhost:9090/debug/pprof/heap                # Heap
go tool pprof http://localhost:9090/debug/pprof/allocs              # Allocs
go tool pprof http://localhost:9090/debug/pprof/goroutine           # Goroutine
```

All pprof endpoints are served on the observability port (not the API port), so they are not exposed to external traffic.

## Development

### Code Quality

```bash
# Run all CI checks
make ci

# Or run individual checks:
make fmt   # Format code only
make lint  # Run linter only (requires golangci-lint)
make vet   # Run static analysis only
make ci    # Run fmt + vet + lint + test
```

### Install Development Tools

```bash
make install-tools
```

This installs:

- `golangci-lint` - Linting and static analysis
- `goimports` - Import formatting and organization
- `gosec` - Security vulnerability scanner

### Project Structure Conventions

- Use `internal/` for all private code (not intended for external import).
- Place shared types in `internal/shared/`.
- Keep component-specific code in dedicated subdirectories (`internal/apiserver/`, `internal/processor/`).
- Write unit tests alongside implementation files (`*_test.go`).
- Place E2E tests in `test/e2e/`.

## Contributing

Contributions are welcome! Please ensure:

1. New features include tests and documentation.
2. CI checks pass: `make ci`.
3. E2E tests pass: `make test-e2e`.
4. Commits are signed off (`git commit -s`) and follow conventional commit format.
5. Code follows project [contributing guidelines](CONTRIBUTING.md).

## Security

Batch-gateway implements defense-in-depth security across multiple layers:

- **Authentication & authorization**: delegated to the gateway layer (Kuadrant/Authorino); supports API key, ServiceAccount token, and OpenShift user token modes.
- **Multi-tenancy isolation**: all data access scoped to the authenticated tenant; cross-tenant requests return 404.
- **TLS/mTLS**: optional TLS on the API server (TLS 1.2+); processor supports custom CAs and mTLS for outbound inference connections.
- **Input validation**: strict JSON decoding, file size and line count limits.
- **Pod hardening**: non-root execution (UID 65532), read-only root filesystem, all Linux capabilities dropped, no privilege escalation, seccomp enabled, OpenShift SCC compatible.
- **Secret management**: Kubernetes secrets mounted read-only; read via `os.OpenInRoot()` to prevent path traversal.
- **Security headers**: `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection` on every response.

For the full overview, see the [Security Guide](docs/guides/security.md). To report vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

```text
http://www.apache.org/licenses/LICENSE-2.0
```

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fllm-d%2Fllm-d-batch-gateway.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fllm-d%2Fllm-d-batch-gateway?ref=badge_large)

## Related Projects

- [llm-d-router](https://github.com/llm-d/llm-d-router) - llm-d inference router.
- [llm-d-async](https://github.com/llm-d/llm-d-async) - Metrics-driven batch dispatch flow control (optional).

## Support

For help and support:

- Open an issue on GitHub.
- Review the [design documentation](docs/design/).
- Review the [development and usage guides](docs/guides/).
