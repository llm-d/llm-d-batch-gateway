# Batch Gateway Benchmark

Demonstrates that batch-gateway with gated dispatch **protects interactive workload latency** while enabling batch workloads to make **steady progress toward SLO targets** on shared GPU infrastructure.

## Overview

The benchmark compares dispatch strategies under a realistic traffic pattern: interactive requests arrive in waves (bursts that saturate the inference server alternating with near-idle periods), while a high-volume batch workload is always available. The key finding is that gated dispatch fills spare capacity with batch work during idle periods and backs off during bursts, without degrading interactive quality.

## Scenarios

| # | Scenario | Dispatch Mode | Description |
|---|----------|---------------|-------------|
| 0 | Interactive only | N/A | Only interactive traffic, no batch. Establishes the ideal latency baseline. |
| 1 | No batch-gateway | N/A | Batch prompts sent as regular requests via guidellm. What a user does without batch-gateway. |
| 2 | Ungated batch | sync, aggressive | Batch-gateway with high concurrency (global=200, perEndpoint=100), AIMD disabled. No flow control. |
| 3 | AIMD only | sync, AIMD enabled | Processor reactively adjusts concurrency based on 429/5xx backpressure. Single layer. |
| 4 | AIMD + flow control | sync, AIMD + Router | Two layers: Router prioritizes interactive (priority 100) over batch (priority -1, sheddable); AIMD adjusts on top. |
| 5 | Async processor | async, endpoint-scrape gate | Async dispatch via llm-d-async with endpoint-scrape gating on vLLM queue depth. |

### How to read results

- **Scenario 0** = ideal interactive latency with no batch interference
- **Scenarios 1-2** = how much batch degrades interactive without gating
- **Scenarios 3-5** = how gated dispatch protects interactive while batch still makes progress
- **Scenario 3 vs 4** = incremental value of Router flow control on top of AIMD
- **Scenario 5** = async dispatch path — batch goes through Redis queues + async-processor instead of sync HTTP

## Prerequisites

- Kubernetes cluster with GPU node(s) (e.g., 1x NVIDIA A100)
- `kubectl` configured with access to the cluster
- `helm` 3.x
- `python3` with `faker` and `pyyaml` installed (`pip3 install faker pyyaml`)
- [guidellm](https://github.com/vllm-project/guidellm) (container image used automatically)

Optional (for development with local checkouts instead of OCI charts):
- [llm-d](https://github.com/llm-d/llm-d) repo cloned locally
- [llm-d-router](https://github.com/llm-d/llm-d-router) repo cloned locally

## Quick Start

### 1. Generate batch input prompts

```bash
python3 benchmarks/generate_prompts.py \
    --num-requests 1000 \
    --num-system-prompts 5 \
    --prompt-tokens 256 \
    --model "Qwen/Qwen3-8B" \
    --multi-job \
    --output-dir benchmarks/results/
```

### 2. Deploy infrastructure for a scenario

```bash
export KUBE_CONTEXT=my-context
export SCENARIO=2

./benchmarks/setup.sh
```

To use local repo checkouts instead of published OCI charts (for development):

```bash
export LLM_D_REPO=/path/to/llm-d
export ROUTER_REPO=/path/to/llm-d-router
./benchmarks/setup.sh
```

### 3. Run the benchmark

```bash
python3 benchmarks/benchmark.py \
    --context my-context \
    --scenarios 0 2 3 4 \
    --burst-rate 15 --idle-rate 1 \
    --burst-seconds 90 --idle-seconds 90 \
    --cycles 3 \
    --results-dir ./benchmarks/results/run-01
```

### 4. View the report

```bash
open benchmarks/results/run-01/report.html
```

### 5. Teardown

```bash
# Single scenario
KUBE_CONTEXT=my-context SCENARIO=2 ./benchmarks/teardown.sh

# All scenarios
KUBE_CONTEXT=my-context SCENARIO=all ./benchmarks/teardown.sh
```

## Local Testing (Kind + inference-sim)

The full pipeline can be validated locally without a GPU using [`llm-d-inference-sim`](https://github.com/llm-d/llm-d-inference-sim). This deploys inference-sim (a synthetic vLLM-compatible server), Redis, PostgreSQL, and batch-gateway into a local Kind cluster.

### Prerequisites

1. A Kind cluster with batch-gateway images pre-loaded:

```bash
make dev-deploy
```

This creates the Kind cluster, builds batch-gateway images, and loads them into the cluster. Alternatively, create a cluster manually (`kind create cluster`) and load images with `kind load docker-image`.

### Running

```bash
make benchmark-local
```

This runs the full pipeline end-to-end:
1. Deploys infrastructure (`setup.sh` with `MODE=sim`) — Redis, PostgreSQL, inference-sim, batch-gateway
2. Generates prompts (`generate_prompts.py` with fixed ISL for fast local runs)
3. Runs the benchmark (`benchmark.py`)
4. Produces a report and metadata JSON in `benchmarks/results/latest/`

Expected output:

```
=== Benchmark local e2e (MODE=sim, scenario 2) ===
Step 1/4: Setting up infrastructure...
Step 2/4: Generating prompts...
Step 3/4: Running benchmark...
Step 4/4: Done!
Report: benchmarks/results/latest/report.html
Metadata: benchmarks/results/latest/run-metadata.json
```

### Cleanup

```bash
make benchmark-local-teardown
```

This tears down the benchmark namespace while leaving the Kind cluster intact.

### Configuration

The simulator behaviour is controlled via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MODE` | `gpu` | Set to `sim` to use inference-sim (automatic in `make benchmark-local`) |
| `SIM_IMAGE` | `ghcr.io/llm-d/llm-d-inference-sim:latest` | Inference-sim container image |
| `SIM_TTFT` | `50ms` | Simulated time-to-first-token |
| `SIM_ITL` | `20ms` | Simulated inter-token latency |

Metrics from sim mode won't reflect real GPU performance, but orchestration, metric collection, and report generation can be validated end-to-end.

> **Note:** Batch-gateway images must be built and loaded into Kind before running. If you see `ImagePullBackOff` errors, run `make dev-deploy` to rebuild and reload images.

## Traffic Pattern

### Interactive workload

Generated by guidellm in alternating burst/idle phases:

| Phase | Rate | Duration | Purpose |
|-------|------|----------|---------|
| Burst | Saturating (e.g., 15 req/s) | 60-120s | Peak interactive demand — GPU near capacity |
| Idle | Low (e.g., 1 req/s) | 60-120s | Off-peak — spare capacity for batch |

Multiple cycles (3-5) to show the pattern repeating reliably.

### Batch workload

3 concurrent batch jobs submitted before interactive traffic:

| Job | Requests | SLO Window | Purpose |
|-----|----------|------------|---------|
| A | 1000 | 30m (tight) | Should complete first via SLO-deadline ordering |
| B | 1000 | 2h (moderate) | Completes second |
| C | 1000 | 24h (relaxed) | Completes last |

All use the same model as interactive traffic with 5 distinct system prompts for prefix-cache evaluation.

## Prompt Generation

`generate_prompts.py` creates batch input files with:

- **Faker-based random text** of configurable token length
- **System prompt diversity** — 3-5 distinct personas distributed across requests (round-robin)
- **Reproducible** via `--seed` flag

The system-prompt diversity enables testing the batch processor's FNV-32a hash sorting, which groups same-system-prompt requests for prefix-cache efficiency.

## Interpreting Results

### Batch completion timeline

The key chart shows batch requests completed over time with burst/idle phase bands overlaid:

- **Gated scenarios (3-4):** Staircase pattern — progress during idle, flat during burst
- **Ungated scenarios (1-2):** Linear progress regardless of interactive load
- **SLO ordering:** Job A (tight deadline) completes before B before C

### Interactive latency

Compare TTFT p99 during burst phases across scenarios:

- Should be close to scenario 0 baseline in scenarios 3-4
- Should be significantly degraded in scenarios 1-2

## Configuration Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--context` | (required) | kubectl context |
| `--scenarios` | `[2]` | Scenario numbers to run |
| `--model` | `Qwen/Qwen3-8B` | Model name |
| `--burst-rate` | `15` | Requests/s during burst |
| `--idle-rate` | `1` | Requests/s during idle |
| `--burst-seconds` | `90` | Burst phase duration |
| `--idle-seconds` | `90` | Idle phase duration |
| `--cycles` | `3` | Number of burst/idle cycles |
| `--batch-size` | `1000` | Requests per batch job |
| `--num-jobs` | `3` | Concurrent batch jobs |
| `--prompt-tokens` | `256` | Input tokens per prompt |
| `--num-system-prompts` | `5` | Distinct system prompts |
| `--results-dir` | `benchmarks/results/latest` | Output directory |
| `--target` | `http://llm-d-inference-gateway-istio` | Inference gateway URL |

## Environment Variables (setup.sh / teardown.sh)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KUBE_CONTEXT` | Yes | — | kubectl context |
| `SCENARIO` | Yes | — | Scenario number (0-5) |
| `LLM_D_REPO` | No | — | Path to llm-d checkout (overrides downloading from tag) |
| `ROUTER_REPO` | No | — | Path to llm-d-router checkout (overrides OCI chart) |
| `ROUTER_CHART_VERSION` | No | `0.9.2` | OCI chart version for llm-d-router |
| `LLM_D_TAG` | No | `v0.7.0` | Git tag for llm-d guide values (used in OCI mode) |
| `NAMESPACE` | No | `batch-bench-s${SCENARIO}` | Override namespace |
| `MODEL` | No | `Qwen/Qwen3-8B` | Model to serve |
| `MODEL_REVISION` | No | — | HuggingFace model revision/commit-sha to pin for reproducibility |
| `GUIDE_NAME` | No | `optimized-baseline` | Inference pool name |
| `BG_IMAGE_REPO` | No | — | Batch-gateway image repo override |
| `BG_IMAGE_TAG` | No | — | Batch-gateway image tag override |
| `DISPATCHER_VERSION` | No | `v0.9.1` | llm-d-async image version (scenario 5) |
| `DISPATCHER_CHART` | No | OCI chart | Async-processor Helm chart reference |
| `DISPATCHER_CHART_VERSION` | No | `v0.9.1` | llm-d-async chart version |

## Directory Structure

```
benchmarks/
├── benchmark.py              # Orchestrator (scenario selection, monitoring, report)
├── generate_prompts.py       # Faker-based prompt generation with system-prompt diversity
├── setup.sh                  # Deploy full stack per scenario
├── teardown.sh               # Cleanup
├── README.md                 # This file
├── helm-values/              # Per-scenario Helm value overrides
├── manifests/                # K8s manifests (guidellm jobs, vLLM overlay, PVCs)
├── profiles/                 # Canonical parameter profiles
└── results/                  # gitignored — local run output
```

## Related

- [Issue #491](https://github.com/llm-d/llm-d-batch-gateway/issues/491) — Full benchmark spec
- [Flow Control Setup Guide](../docs/guides/flow-control-setup.md) — AIMD + Router configuration
- [Batch Processor Architecture](../docs/design/batch_processor_architecture.md) — Processor internals
