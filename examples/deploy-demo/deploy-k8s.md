# Demo Scripts

One-click deployment script for batch-gateway on Kubernetes. Supports `install`, `test`, and `uninstall` commands.

## Overview

**Prerequisites**:

- **Tools**: `kubectl`, `helm`, `git`, `curl`, `jq`, `yq`.
- **Cluster access**: You must be logged in to the target cluster. Use `kubectl config current-context` (or `oc whoami` on OpenShift) to verify.

## Usage

### install

```bash
bash examples/deploy-demo/deploy-k8s.sh install
```

#### Components Installed

| Component | Details |
|-----------|---------|
| cert-manager | TLS certificate management |
| Istio | Service mesh + ingress gateway (HTTPS:443) |
| llm-d stack | llm-d Router (gateway mode) + vllm-sim (single model, default: random) |
| Kuadrant | Auth + rate limiting (installed via Helm) |
| Redis | Exchange backend (Bitnami Helm chart, configurable via `BATCH_EXCHANGE_CLIENT_TYPE`) |
| PostgreSQL | Batch metadata store (Bitnami Helm chart) |
| MinIO | S3-compatible file storage (when `BATCH_STORAGE_TYPE=s3`) |
| Internal Gateway | ClusterIP gateway for batch processor → LLM inference (bypasses rate limits, preserves AuthPolicy) |
| InferenceObjective | GIE flow control CRDs — priority-based dispatch (interactive=100, batch=-1). Enabled by default (`ENABLE_FLOW_CONTROL=true`) |
| batch-gateway | apiserver + processor + gc (Helm chart) |
| async-processor | llm-d-async dispatcher for async dispatch mode (enabled by default, `ENABLE_DISPATCHER=true`). Routes requests through Redis queues → Internal Gateway → EPP |
| Prometheus | Scrapes EPP + vLLM metrics for the async dispatch budget (when `ENABLE_DISPATCHER=true`) |

#### Routing & Policies

**External Gateway** (`istio-gateway`, HTTPS:443):

| HTTPRoute | Backend | Auth | Rate Limit |
|-----------|---------|------|------------|
| `llm-route` | InferencePool (direct inference) | kubernetesTokenReview + SubjectAccessReview (model-level authz) | 500 tokens/1min per user on `/v1/chat/completions` only (TokenRateLimitPolicy) |
| `batch-route` | batch-gateway apiserver | kubernetesTokenReview only (no authz) | 20 req/1min per user (RateLimitPolicy) |

**Internal Gateway** (`batch-internal-gateway`, ClusterIP, HTTP:80):

| HTTPRoute | Backend | Auth | Rate Limit |
|-----------|---------|------|------------|
| `batch-llm-route` | InferencePool (batch processor access) | kubernetesTokenReview + SubjectAccessReview (model-level authz) | — (none, by design) |

Batch-route has no authorization — model-level authz is enforced downstream when the batch processor forwards requests through the Internal Gateway's `batch-llm-route`.

#### Install Examples

| Mode | Command |
|------|---------|
| Local chart (default) | `bash examples/deploy-demo/deploy-k8s.sh install` |
| Older commit chart (HTTP sync) | `BATCH_DEV_VERSION=1f925ff ENABLE_DISPATCHER=false ENABLE_FLOW_CONTROL=false bash examples/deploy-demo/deploy-k8s.sh install` |
| Released OCI chart | `BATCH_RELEASE_VERSION=v0.5.0 bash examples/deploy-demo/deploy-k8s.sh install` |
| Custom images (HTTP sync) | `BATCH_IMAGE_TAG=v0.2.0` <br> `ENABLE_DISPATCHER=false` <br> `BATCH_APISERVER_REPO=ghcr.io/llm-d/batch-gateway-apiserver` <br> `BATCH_PROCESSOR_REPO=ghcr.io/llm-d/batch-gateway-processor` <br> `BATCH_GC_REPO=ghcr.io/llm-d/batch-gateway-gc` <br> `bash examples/deploy-demo/deploy-k8s.sh install` |
| Explicit HTTP sync (no dispatcher) | `ENABLE_DISPATCHER=false bash examples/deploy-demo/deploy-k8s.sh install` |

> `BATCH_RELEASE_VERSION` and `BATCH_DEV_VERSION` cannot be used together. See [Environment Variables](#environment-variables) for common parameters.

Async dispatch uses `processor.config.asyncDispatch.models` for model-to-pool mappings and flow-control objectives. Setting `ENABLE_DISPATCHER=false` explicitly sets `processor.config.dispatchMode=sync` and retains HTTP routing through `modelGateways`. API clients and the direct inference HTTP path are unchanged.

Released charts `v0.3.0` and `v0.4.0` use the older async `modelGateways` layout; the script handles those exact versions. Charts `v0.1.0` and `v0.2.0` require `ENABLE_DISPATCHER=false` because they do not support async dispatch. For `v0.1.0`, also set `ENABLE_FLOW_CONTROL=false` because its objective configuration predates per-model objectives. Arbitrary older commit charts or mismatched image/chart versions are not auto-detected.

### test

```bash
bash examples/deploy-demo/deploy-k8s.sh test
```

Use the same environment overrides for `install` and `test`, including `ENABLE_DISPATCHER=false` for an HTTP sync deployment and `BATCH_RELEASE_VERSION` for a released chart.

Creates temporary ServiceAccounts (authorized + unauthorized) with short-lived tokens and runs the following test groups:

| # | Test Group | What it verifies |
|---|------------|------------------|
| 1 | LLM Authn | Unauthenticated → 401, authenticated → 200 |
| 2 | LLM Authz | Unauthorized → 403, authorized → 200 |
| 3 | LLM Token Rate Limit | Repeated requests → 429 |
| 4 | Batch Authn | Unauthenticated → 401, authenticated → 200 |
| 5 | Batch Authz | Unauthorized user's batch → requests rejected with 403 by Internal Gateway |
| 6 | Batch Lifecycle | File upload → batch create → poll → completed → download output |
| 7 | Batch Request Rate Limit | Rapid requests → 429 |
| 8 | Flow Control (if enabled) | EPP metrics: interactive requests at priority 0, batch at priority -1, pool saturation metric present |

Internal Gateway isolation is also verified before tests run (service type is ClusterIP, no Route/Ingress exposes it).

### uninstall

```bash
bash examples/deploy-demo/deploy-k8s.sh uninstall
```

Default `uninstall` removes the batch-gateway footprint and associated gateway/policy resources:

- Dispatcher Helm release (if deployed)
- Helm releases and CRs in `BATCH_NAMESPACE` (`batch-route` HTTPRoute, Redis, PostgreSQL, MinIO)
- Both Gateways: `GATEWAY_NAME` and `BATCH_INTERNAL_GATEWAY_NAME`
- DestinationRule `${BATCH_INSTANCE_NAME}-backend-tls`
- Internal Gateway resources (`batch-llm-route`, `batch-llm-route-auth`) in `LLM_NAMESPACE`
- Kuadrant policies (`llm-route-auth`, `batch-route-auth`, `inference-token-limit`, `batch-ratelimit`)
- Demo RBAC (test ServiceAccounts, Role, RoleBinding) in `LLM_NAMESPACE`
- `BATCH_NAMESPACE` itself

It does **not** remove Kuadrant, Istio, cert-manager, operators, or cluster-wide CRDs—so other teams' platform pieces stay.

> **Note**: The default uninstall deletes the Gateway named `GATEWAY_NAME` (default: `istio-gateway`). If this Gateway is shared with other teams, override `GATEWAY_NAME` or remove the Gateway deletion from the script before running.

**Do not use `UNINSTALL_ALL=1` on shared production or multi-team clusters** — that mode tears down operators and platform components others may depend on.

**Full teardown** (throwaway / dedicated demo cluster only) — prefix the command with `UNINSTALL_ALL=1`:

```bash
UNINSTALL_ALL=1 bash examples/deploy-demo/deploy-k8s.sh uninstall
```

Use that only on **ephemeral or dedicated** demo clusters. See [issue #309](https://github.com/llm-d/llm-d-batch-gateway/issues/309) for background.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_INSTANCE_NAME` | `batch-gateway` | Helm release / instance name |
| `BATCH_RELEASE_VERSION` | — | Install from released OCI chart (e.g. `v1.0.0`). Cannot be used with `BATCH_DEV_VERSION` |
| `BATCH_DEV_VERSION` | `local` | Image tag / commit SHA. `local` uses local chart + `latest` image. Cannot be used with `BATCH_RELEASE_VERSION` |
| `BATCH_IMAGE_TAG` | — | Override image tag for all components. Takes precedence over `BATCH_RELEASE_VERSION` / `BATCH_DEV_VERSION` derived tags |
| `BATCH_APISERVER_REPO` | — | Override apiserver image repository |
| `BATCH_PROCESSOR_REPO` | — | Override processor image repository |
| `BATCH_GC_REPO` | — | Override gc image repository |
| `BATCH_DB_TYPE` | `postgresql` | Database backend: `postgresql` or `redis` |
| `BATCH_STORAGE_TYPE` | `s3` | File storage: `fs` or `s3` |
| `MINIO_BUCKET` | `llm-d-batch-gateway` | MinIO bucket name (also used as the S3 `bucket` and `prefix` config values) |
| `MINIO_REGION` | `us-east-1` | S3 region for MinIO |
| `DEMO_TLS_INSECURE_SKIP_VERIFY` | `1` | Disables TLS certificate verification for processor → model gateway and Istio Gateway → batch apiserver (**demo/lab only**, [CWE-295](https://cwe.mitre.org/data/definitions/295.html)). Default `1` since demo scripts use self-signed certs. Set to `0` if you have trusted CA certs. |
| `BATCH_NAMESPACE` | `batch-api` | Namespace for batch-gateway |
| `LLM_NAMESPACE` | `llm` | Namespace for model serving |
| `BATCH_EXCHANGE_CLIENT_TYPE` | `redis` | Exchange backend type (`redis` or `valkey`) |
| `GW_REQUEST_TIMEOUT` | `5m` | Model gateway HTTP request timeout |
| `GW_MAX_RETRIES` | `3` | Model gateway max retries |
| `GW_INITIAL_BACKOFF` | `1s` | Model gateway initial retry backoff |
| `GW_MAX_BACKOFF` | `60s` | Model gateway max retry backoff |
| `GATEWAY_NAME` | `istio-gateway` | Gateway resource name |
| `GATEWAY_NAMESPACE` | `istio-ingress` | Gateway namespace |
| `GATEWAY_CLASS_NAME` | `istio` | GatewayClass name |
| `GATEWAY_LOCAL_PORT` | `8080` | Port-forward local port |
| `BATCH_INTERNAL_GATEWAY_NAME` | `batch-internal-gateway` | Internal Gateway resource name |
| `BATCH_INTERNAL_GATEWAY_NAMESPACE` | `${GATEWAY_NAMESPACE}` | Internal Gateway namespace |
| `LLMD_VERSION` | `v0.8.1` | llm-d git ref to install |
| `LLMD_RELEASE_POSTFIX` | `llmd` | Helm release postfix |
| `MODEL_NAME` | `random` | Model name for routing |
| `CERT_MANAGER_VERSION` | `v1.20.3` | cert-manager Helm chart version |
| `KUADRANT_VERSION` | `1.3.1` | Kuadrant Helm chart version |
| `ROUTER_CHART_VERSION` | (from llm-d env.sh) | llm-d Router chart/CRD version (auto-detected from LLMD_VERSION) |
| `ISTIO_VERSION` | `1.29.2` | Istio Helm chart version |
| `ENABLE_FLOW_CONTROL` | `true` | Enable GIE priority-based flow control |
| `BATCH_FLOW_CONTROL_OBJECTIVE` | `batch-sheddable` | InferenceObjective name for batch requests (priority -1) |
| `ENABLE_DISPATCHER` | `true` | Deploy llm-d-async dispatcher and Prometheus; set `false` for explicit HTTP sync dispatch |
| `DISPATCHER_VERSION` | `v0.7.3` | llm-d-async version (image tag and chart version) |
| `UNINSTALL_ALL` | `0` | Set to `1` to remove Kuadrant, Istio, cert-manager, CRDs (ephemeral clusters only) |
