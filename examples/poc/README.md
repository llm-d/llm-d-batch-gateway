# Batch Gateway Demo Overview

This directory contains demo files for testing the Batch Gateway system.

## Files

- **batch_input_short.jsonl**: Short batch input file with requests split across `sim-model` and `sim-model-b`.
- **batch_input_long.jsonl**: Longer batch input file with requests split across `sim-model` and `sim-model-b`.
- **demo.http**: REST Client file for VS Code REST Client plugin, with two demo sequences.
- **curl_demo.md**: Demo guide using curl commands from the command line, with detailed examples and demo sequences.

## Prerequisites

1. **Install required tools**: see [Development guide prerequisites](../../docs/guides/development.md#prerequisites).

2. **Deploy the Batch Gateway**:

   ```bash
   make dev-deploy
   ```

   To deploy a specific release version from GHCR instead of building locally:

   ```bash
   IMAGE_TAG=v0.1.0 SKIP_BUILD=true make dev-deploy
   ```

   This will start:
   - API Server at <https://localhost:8000>
   - Processor at <http://localhost:9090>
   - Garbage Collector (GC) — runs periodic cleanup of expired batches and files
   - Jaeger UI at <http://localhost:16686>
   - Prometheus UI at <http://localhost:9091>
   - Grafana UI at <http://localhost:3000> (anonymous admin access, no login required)
   - SeaweedFS (S3-compatible storage) at <http://localhost:9002>
   - Metrics endpoints at <http://localhost:8081/metrics> (API) and <http://localhost:9090/metrics> (Processor)

3. **Choose Your Demo Tool**:
   - **Using demo.http**: Install the REST Client for Visual Studio Code extension (Ctrl+Shift+X / Cmd+Shift+X)
   - **Using curl_demo.md**: Ensure `curl` and `jq` are available on your system

## Architecture

The demo environment runs the following components in a Kubernetes cluster (kind):

```text
┌─────────────────────────────────────────────────────────────────────┐
│                   Kubernetes Cluster (kind)                         │
│                                                                     │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────┐ │
│  │   API Server         │  │   Processor          │  │   GC       │ │
│  │  (batch-gateway-     │  │  (batch-gateway-     │  │  (batch-   │ │
│  │   apiserver)         │  │   processor)         │  │  gateway-  │ │
│  │                      │  │                      │  │  gc)       │ │
│  │  • REST API (8000)   │  │  • Polling worker    │  │            │ │
│  │  • Metrics (8081)    │  │  • Metrics (9090)    │  │  • Expired │ │
│  └──────────┬───────────┘  └──────────┬───────────┘  │   cleanup  │ │
│             │                         │              └──────┬─────┘ │
│             │                         │                     │       │
│             ├─────────────┬───────────┤─────────────────────┤       │
│             │             │           │                             │
│  ┌──────────▼─────────┐   │   ┌───────▼──────────────────────┐      │
│  │   PostgreSQL       │   │   │   Redis                      │      │
│  │                    │   │   │                              │      │
│  │  • Batch metadata  │   │   │  • Priority queue            │      │
│  │  • File metadata   │   │   │  • Progress tracking         │      │
│  │  • Persistent DB   │   │   │  • Event exchange            │      │
│  └────────────────────┘   │   └──────────────────────────────┘      │
│                           │                                         │
│           ┌───────────────▼─────────────────────┐                   │
│           │   SeaweedFS (S3-compatible storage) │                   │
│           │                                     │                   │
│           │  • Batch input files (.jsonl)       │                   │
│           │  • Batch output files (results)     │                   │
│           │  • Error files (failed requests)    │                   │
│           │  • API (9002)                       │                   │
│           └─────────────────────────────────────┘                   │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │   Model Inference Services                               │       │
│  │                                                          │       │
│  │   ┌────────────────────┐       ┌────────────────────┐    │       │
│  │   │ vLLM Simulator     │       │ vLLM Simulator B   │    │       │
│  │   │  (sim-model)       │       │  (sim-model-b)     │    │       │
│  │   │                    │       │                    │    │       │
│  │   │ • 50ms TTFT        │       │ • 200ms TTFT       │    │       │
│  │   │ • 100ms token      │       │ • 500ms token      │    │       │
│  │   └─────────▲──────────┘       └─────────▲──────────┘    │       │
│  │             │                            │               │       │
│  └─────────────┼────────────────────────────┼───────────────┘       │
│                │                            │                       │
│                └────────────┬───────────────┘                       │
│                             │                                       │
│                             │                                       │
│                Inference requests from Processor                    │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │   Observability                                          │       │
│  │                                                          │       │
│  │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │       │
│  │   │ Jaeger       │  │ Prometheus   │  │ Grafana      │   │       │
│  │   │              │  │              │  │              │   │       │
│  │   │ • Traces     │  │ • Metrics    │  │ • Dashboards │   │       │
│  │   │ • UI (16686) │  │ • UI (9091)  │  │ • UI (3000)  │   │       │
│  │   └──────▲───────┘  └──────▲───────┘  └──────▲───────┘   │       │
│  │          │ Traces          │ Scrapes         │ Queries   │       │
│  │          └─────────────────┴─────────────────┘           │       │
│  │                                                          │       │
│  │                      All components                      │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                     │
└─────────────────────────────▲───────────────────────────────────────┘
                              │
                              │ kind extraPortMappings
                              │ (NodePort → localhost)
                              │
                      ┌───────┴──────────┐
                      │  localhost       │
                      │                  │
                      │  :8000  (API)    │
                      │  :8081  (Obs)    │
                      │  :9090  (Proc)   │
                      │  :9091  (Prom)   │
                      │  :3000  (Grafana)│
                      │  :9002  (S3)     │
                      │  :16686 (Jaeger) │
                      └──────────────────┘
```

**Request Flow:**

1. **Create Batch**: User → API Server → PostgreSQL (metadata) + Redis (queue) + SeaweedFS (input file)
2. **Process Batch**: Processor polls Redis → reads batch from PostgreSQL → reads input from SeaweedFS → sends requests to vLLM simulators → writes results to SeaweedFS → updates PostgreSQL + Redis
3. **Retrieve Results**: User → API Server → PostgreSQL (batch status) + SeaweedFS (output file)
4. **Garbage Collection**: GC periodically scans for expired batches and files → deletes from PostgreSQL + SeaweedFS
5. **Monitor**: All components send traces to Jaeger; metrics exposed on /metrics endpoints, collected by Prometheus, and visualized in Grafana dashboards

## Demo Sequences

### Sequence 1: Complete Batch Processing Flow

This demo shows the full lifecycle of a batch job:

1. **Upload batch input file**
2. **Create batch job** specifying the input file
3. **Monitor batch status** by polling the batch endpoint
4. **Download results** when processing completes
5. **View system metrics** from API server and processor

### Sequence 2: Batch Cancellation Flow

This demo shows how to cancel a running batch job:

1. **Upload batch input file**
2. **Create batch job**
3. **Check initial status**
4. **Cancel the batch**
5. **Verify cancelled status**
6. **Download partial results** (completed requests before cancellation)

## Batch Status Flow

```text
validating → in_progress → finalizing → completed
            ↓
         cancelling → cancelled
            ↓
         failed
```

## Expected Timings

- **File upload**: < 1 second
- **Batch creation**: < 1 second
- **Short input file**: ~5-10 seconds (depends on mock simulator settings)
- **Long input file**: ~15-30 seconds (depends on mock simulator settings)

## Request Format

Each line in the JSONL files follows the OpenAI Batch API format:

```json
{
  "custom_id": "req-001",
  "method": "POST",
  "url": "/v1/chat/completions",
  "body": {
    "model": "sim-model",
    "max_tokens": 100,
    "messages": [
      {"role": "user", "content": "What is machine learning?"}
    ]
  }
}
```

## Inference Requests

Both input files contain inference requests that are interweaved between two models:

- **sim-model**: Odd-numbered requests (1, 3, 5, ...)
- **sim-model-b**: Even-numbered requests (2, 4, 6, ...)

Both models are mock simulators configured in the dev deployment to demonstrate multi-model routing.

## Monitoring

### Jaeger Traces

Open <http://localhost:16686> in your browser to view distributed traces:

- Select service: `batch-gateway`
- Search by batch ID to see the full request flow
- View span details to see timing and errors

### Grafana Dashboards

Open <http://localhost:3000> in your browser (anonymous admin access, no login required):

- Pre-configured Prometheus data source
- Batch Gateway dashboards are auto-loaded from the Helm chart

### Prometheus Metrics

Prometheus automatically scrapes metrics from the components.

**Using Prometheus UI**:

1. Open <http://localhost:9091> in your browser
2. Navigate to Graph tab
3. Enter a metric name in the expression browser (see [metrics guide](../../docs/guides/metrics.md) for available names)
4. Click "Execute" to see current values or "Graph" for time-series visualization

**Direct access to raw metrics** (useful for debugging):

- API Server metrics endpoint: <http://localhost:8081/metrics>
- Processor metrics endpoint: <http://localhost:9090/metrics>

**Key metrics**: See the [metrics guide](../../docs/guides/metrics.md) for the full list of available metric names.

### Health Endpoints

- API Server Health: <http://localhost:8081/health>
- Processor Health: <http://localhost:9090/health>

## Troubleshooting

### Connection Refused

- Ensure the batch gateway is deployed: `make dev-deploy`
- Verify the kind cluster is running: `kind get clusters`
- Check that services are up: `kubectl get pods -n default`
- The kind cluster maps NodePort services directly to localhost (no separate port-forward needed)

### TLS Certificate Errors

- The demo uses self-signed certificates
- REST Client and cURL commands use `-k` / insecure mode for testing
- This is normal for local development

### No Results After Completion

- Check that mock models are configured in the gateway
- View processor logs: `kubectl logs -l app.kubernetes.io/component=processor -n default`
- Check Jaeger traces for errors

### Batch Stuck in Processing

- View processor metrics to see if it's processing: <http://localhost:9090/metrics>
- Check processor health: <http://localhost:9090/health>
- View processor logs for errors
