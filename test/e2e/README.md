# E2E Tests

End-to-end tests for the batch-gateway. They run against a live deployment and cover the `/v1/files` and `/v1/batches` REST APIs.

## Prerequisites

- `kubectl`, `helm`, `kind`, Docker or Podman
- Go 1.27.1 or later
- The vllm-vcr image (`ghcr.io/neuralmagic/vllm-vcr`, multi-arch: linux/amd64
  and linux/arm64). To test an unreleased vllm-vcr build it locally
  (`docker build --load -t ghcr.io/neuralmagic/vllm-vcr:dev .`) and set
  `VLLM_SIM_IMAGE=ghcr.io/neuralmagic/vllm-vcr:dev`; `make dev-deploy` loads a
  local image into kind.

## 1. Deploy the server

```bash
make dev-deploy
```

This script:
1. Creates a kind cluster if none is reachable (`KIND_CLUSTER_NAME`)
2. Builds and loads the apiserver and processor container images
3. Installs Redis via Helm
4. Installs PostgreSQL via Helm
5. Deploys [vllm-vcr](https://github.com/neuralmagic/vllm-vcr) (the real vLLM Rust frontend over a simulated engine) as the inference backend, one instance per model, each with a control API on port 8001
6. Deploys batch-gateway via Helm
7. Creates NodePort services mapping to `https://localhost:8000` (apiserver) and `http://localhost:8081` (apiserver observability)
8. Creates a processor observability service that `make test-e2e` reaches via a temporary local `kubectl port-forward`

**Environment variables**

| Variable              | Default                                    | Description                                        |
|-----------------------|--------------------------------------------|--------------------------------------------------- |
| `KIND_CLUSTER_NAME`   | `batch-gateway-dev`                        | Kind cluster name (created if needed)              |
| `HELM_RELEASE`        | `batch-gateway`                            | Helm release name                                  |
| `NAMESPACE`           | `default`                                  | Kubernetes namespace                               |
| `IMAGE_TAG`           | `0.0.1`                                    | Image tag to build and deploy                      |
| `SKIP_BUILD`          | `false`                                    | Pull images from GHCR instead of building locally  |
| `LOCAL_PORT`          | `8000`                                     | Local port for the apiserver                       |
| `LOG_VERBOSITY`       | `5`                                        | klog verbosity for apiserver and processor         |
| `POSTGRESQL_RELEASE`  | `postgresql`                               | Helm release name for PostgreSQL                   |
| `POSTGRESQL_PASSWORD` | `postgres`                                 | PostgreSQL admin password                          |
| `INFERENCE_API_KEY`   | `dummy-api-key`                            | API key written to the app secret                  |
| `S3_SECRET_ACCESS_KEY`| `minioadmin`                               | S3 secret access key written to the app secret     |
| `APP_SECRET_NAME`     | `<HELM_RELEASE>-secrets`                   | Name of the Kubernetes secret created by the script|
| `FILES_PVC_NAME`      | `<HELM_RELEASE>-files`                     | Name of the PVC created for file storage           |
| `VLLM_SIM_NAME`       | `vllm-sim`                                 | Name of the vLLM simulator deployment              |
| `VLLM_SIM_MODEL`      | `sim-model`                                | Model name served by the simulator                 |
| `VLLM_SIM_IMAGE`      | `ghcr.io/neuralmagic/vllm-vcr:0.2.2-vllm0.27` | vllm-vcr image                                  |
| `VLLM_SIM_HF_MODEL`   | `Qwen/Qwen2.5-0.5B-Instruct`               | Hugging Face id the frontend loads the tokenizer from |
| `VLLM_SIM_CONTROL_PORT` | `8001`                                   | vllm-vcr control API port (latency, failure injection, request counters) |

Example with overrides:

```bash
NAMESPACE=dev LOCAL_PORT=9000 LOG_VERBOSITY=4 make dev-deploy
```

## 2. Run the tests

```bash
make test-e2e
```

**Environment variables**

| Variable                  | Default                          | Description                                                |
|---------------------------|----------------------------------|------------------------------------------------------------|
| `TEST_APISERVER_URL`      | `https://localhost:8000`         | Base URL of the running API server (TLS)                   |
| `TEST_APISERVER_OBS_URL`  | `http://localhost:8081`          | Apiserver observability endpoint (health, metrics)         |
| `TEST_PROCESSOR_OBS_URL`  | auto-resolved by the e2e test helpers | Processor observability endpoint (health, metrics)   |
| `TEST_JAEGER_URL`         | `http://localhost:16686`         | Jaeger query endpoint for trace verification               |
| `TEST_TENANT_HEADER`      | `X-MaaS-Username`               | HTTP header used to identify the tenant                    |
| `TEST_TENANT_ID`          | `default`                        | Tenant ID sent in the tenant header                        |
| `TEST_NAMESPACE`          | `default`                        | Kubernetes namespace of the deployment                     |
| `TEST_HELM_RELEASE`       | `batch-gateway`                  | Helm release name (used for label selectors, rollouts)     |
| `TEST_POSTGRESQL_RELEASE` | `postgresql`                     | Helm release name for PostgreSQL (used by GC tests)        |
| `TEST_MODEL`              | `sim-model`                      | Primary model name for batch input                         |
| `TEST_MODEL_B`            | `sim-model-b`                    | Secondary model name for multi-model tests                 |
| `TEST_SIM_SERVICE`        | `vllm-sim`                       | K8s service name of the primary model's simulator          |
| `TEST_SIM_SERVICE_B`      | `vllm-sim-b`                     | K8s service name of the secondary model's simulator        |
| `TEST_SIM_CONTROL_PORT`   | `8001`                           | vllm-vcr control API port on the simulator services        |
| `TEST_CHART_PATH`         | `../../charts/batch-gateway`     | Path to the Helm chart (used by HelmUpgrade tests)         |

Example with overrides:

```bash
TEST_APISERVER_URL=https://localhost:9000 TEST_TENANT_ID=my-tenant make test-e2e
```

If you run `go test` directly instead of `make test-e2e`, the test helpers will auto-resolve the processor observability endpoint. You can still override it explicitly if needed:

```bash
TEST_PROCESSOR_OBS_URL=http://127.0.0.1:19090 \
go test -v ./test/e2e/...
```

### Tests that need GIE

The AIMD tests and the shed/retry tests (`FlowControl/GIE/RetryOnShed`,
`FlowControl/GIE/RetryExhaustion`) skip unless the cluster was deployed with
`ENABLE_GIE=true`. The backpressure they exercise is the EPP shedding batch
traffic under saturation (429 on an outright reject, 503 when a queued
request's TTL expires). The tests saturate a model by choking its vllm-vcr
engine through the control API (`PATCH http://vllm-sim-b:8001/config`) while
keeping non-sheddable interactive traffic aimed at the same EPP, so batch
requests are the ones shed; the engine is released the same way. Nothing in
the deployment fakes a backpressure status.

## 3. Cleanup

```bash
helm uninstall batch-gateway -n default
helm uninstall redis -n default
helm uninstall postgresql -n default
kubectl delete deployment,svc vllm-sim -n default
kubectl delete secret batch-gateway-secrets -n default
kubectl delete pvc batch-gateway-files -n default

# If using a kind cluster:
kind delete cluster --name batch-gateway-dev
```
