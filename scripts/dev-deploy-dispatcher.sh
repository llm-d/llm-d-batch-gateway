#!/bin/bash
# This script can be run standalone or sourced from dev-deploy.sh.
# When sourced, SCRIPT_DIR, REPO_ROOT, and dev-common.sh are already set.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    # Reuse the sourceable dev configuration and early fixture/tool guards.
    source "${SCRIPT_DIR}/dev-deploy.sh"
fi

# ── Configuration ────────────────────────────────────────────────────────────
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-batch-gateway-dev}"
DISPATCHER_RELEASE="${DISPATCHER_RELEASE:-dispatcher}"
DISPATCHER_VERSION="${DISPATCHER_VERSION:-v0.9.1}"
DISPATCHER_IMAGE="${DISPATCHER_IMAGE:-ghcr.io/llm-d/llm-d-async:${DISPATCHER_VERSION}@sha256:d8db64675b6a5f70486d74de9f28aa2ee88e7e2c4e3ba97ba2078d634c2fd610}"
DISPATCHER_CHART="${DISPATCHER_CHART:-oci://ghcr.io/llm-d/charts/llm-d-async}"
DISPATCHER_CHART_VERSION="${DISPATCHER_CHART_VERSION:-v0.9.1}"
DISPATCHER_REDIS_PORT="${DISPATCHER_REDIS_PORT:-6399}"
DISPATCHER_REDIS_NODE_PORT="${DISPATCHER_REDIS_NODE_PORT:-${REDIS_NODE_PORT:-30479}}"
PID_FILE="${REPO_ROOT}/.dispatcher-port-forward.pid"
# Set DISPATCHER_SOURCE to a local llm-d-async checkout to build from source
# instead of pulling a released image. The local chart is used automatically.
# Example: DISPATCHER_SOURCE=~/src/llm-d-async ENABLE_DISPATCHER=true make dev-deploy
DISPATCHER_SOURCE="${DISPATCHER_SOURCE:-}"

# ── Prerequisites (standalone only — dev-deploy.sh already checks these) ─────
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    ENABLE_DISPATCHER=true check_prerequisites

    if ! kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
        die "Kind cluster '${KIND_CLUSTER_NAME}' not found. Run 'make dev-deploy' first."
    fi
    kubectl config use-context "kind-${KIND_CLUSTER_NAME}"
fi

# ── Build or pull dispatcher image ────────────────────────────────────────────
if [[ -n "${DISPATCHER_SOURCE}" ]]; then
    if [[ ! -d "${DISPATCHER_SOURCE}" ]]; then
        die "DISPATCHER_SOURCE directory not found: ${DISPATCHER_SOURCE}"
    fi
    DISPATCHER_IMAGE="ghcr.io/llm-d/llm-d-async:dev-local"
    DISPATCHER_CHART="${DISPATCHER_SOURCE}/charts/llm-d-async"
    unset DISPATCHER_CHART_VERSION
    step "Building async-processor image from ${DISPATCHER_SOURCE}..."
    ${CONTAINER_TOOL} build -t "${DISPATCHER_IMAGE}" "${DISPATCHER_SOURCE}"
else
    step "Using registry dispatcher image ${DISPATCHER_IMAGE}"
fi

if [[ -n "${DISPATCHER_SOURCE}" ]]; then
    step "Loading dispatcher image into Kind cluster '${KIND_CLUSTER_NAME}'..."
    if docker exec "${KIND_CLUSTER_NAME}-control-plane" ctr --namespace=k8s.io images list -q 2>/dev/null | grep -q "^${DISPATCHER_IMAGE}$"; then
        log "Image already present in Kind node, skipping load"
    elif [[ "${CONTAINER_TOOL}" == "docker" ]]; then
        docker save "${DISPATCHER_IMAGE}" | docker exec -i "${KIND_CLUSTER_NAME}-control-plane" \
            ctr --namespace=k8s.io images import --snapshotter=overlayfs -
    else
        ${CONTAINER_TOOL} save "${DISPATCHER_IMAGE}" | kind load image-archive /dev/stdin --name "${KIND_CLUSTER_NAME}"
    fi
fi

# ── Deploy async-processor via Helm ──────────────────────────────────────────
HELM_VALUES="${REPO_ROOT}/test/e2e/dispatcher/helm-values.yaml"

if [[ ! -f "${HELM_VALUES}" ]]; then
    die "Helm values file not found: ${HELM_VALUES}"
fi

DISPATCHER_SCRAPE_RELEASE="${DISPATCHER_SCRAPE_RELEASE:-dispatcher-scrape}"
HELM_VALUES_SCRAPE="${REPO_ROOT}/test/e2e/dispatcher/helm-values-scrape.yaml"

# The chart renders repository:tag, so split on the tag separator in the final
# path segment and retain any @sha256:digest suffix as part of the tag value.
# This also handles registries with an explicit port.
IMAGE_WITHOUT_DIGEST="${DISPATCHER_IMAGE%%@*}"
if [[ "${IMAGE_WITHOUT_DIGEST##*/}" != *:* ]]; then
    die "DISPATCHER_IMAGE must include a tag so chart ${DISPATCHER_CHART_VERSION} can render it: ${DISPATCHER_IMAGE}"
fi
DISPATCHER_IMAGE_REPO="${IMAGE_WITHOUT_DIGEST%:*}"
DISPATCHER_IMAGE_TAG="${IMAGE_WITHOUT_DIGEST##*:}"
DISPATCHER_EXPECTED_DIGEST=""
if [[ "${DISPATCHER_IMAGE}" == *@* ]]; then
    DISPATCHER_EXPECTED_DIGEST="${DISPATCHER_IMAGE#*@}"
    DISPATCHER_IMAGE_TAG="${DISPATCHER_IMAGE_TAG}@${DISPATCHER_EXPECTED_DIGEST}"
fi

HELM_VERSION_FLAG=()
if [[ -n "${DISPATCHER_CHART_VERSION:-}" ]]; then
    HELM_VERSION_FLAG=(--version "${DISPATCHER_CHART_VERSION}")
fi

DISPATCHER_IMAGE_PULL_POLICY="Never"
if [[ -z "${DISPATCHER_SOURCE}" ]]; then
    # A tag@digest reference cannot be round-tripped through docker save with
    # the same CRI name. Let kubelet pull the immutable registry reference.
    DISPATCHER_IMAGE_PULL_POLICY="IfNotPresent"
fi

step "Deploying async-processor with redis gate (release: ${DISPATCHER_RELEASE})..."
helm upgrade --install "${DISPATCHER_RELEASE}" "${DISPATCHER_CHART}" \
    "${HELM_VERSION_FLAG[@]}" \
    --namespace "${NAMESPACE}" \
    --values "${HELM_VALUES}" \
    --set-string "ap.image.repository=${DISPATCHER_IMAGE_REPO}" \
    --set-string "ap.image.tag=${DISPATCHER_IMAGE_TAG}" \
    --set-string "ap.imagePullPolicy=${DISPATCHER_IMAGE_PULL_POLICY}" \
    --timeout=120s

step "Deploying async-processor with endpoint-scrape gate (release: ${DISPATCHER_SCRAPE_RELEASE})..."
helm upgrade --install "${DISPATCHER_SCRAPE_RELEASE}" "${DISPATCHER_CHART}" \
    "${HELM_VERSION_FLAG[@]}" \
    --namespace "${NAMESPACE}" \
    --values "${HELM_VALUES_SCRAPE}" \
    --set-string "ap.image.repository=${DISPATCHER_IMAGE_REPO}" \
    --set-string "ap.image.tag=${DISPATCHER_IMAGE_TAG}" \
    --set-string "ap.imagePullPolicy=${DISPATCHER_IMAGE_PULL_POLICY}" \
    --timeout=120s

DISPATCHER_PROM_RELEASE="${DISPATCHER_PROM_RELEASE:-dispatcher-prom}"
HELM_VALUES_PROM="${REPO_ROOT}/test/e2e/dispatcher/helm-values-prometheus.yaml"

step "Deploying async-processor with prometheus-query gate (release: ${DISPATCHER_PROM_RELEASE})..."
helm upgrade --install "${DISPATCHER_PROM_RELEASE}" "${DISPATCHER_CHART}" \
    "${HELM_VERSION_FLAG[@]}" \
    --namespace "${NAMESPACE}" \
    --values "${HELM_VALUES_PROM}" \
    --set-string "ap.image.repository=${DISPATCHER_IMAGE_REPO}" \
    --set-string "ap.image.tag=${DISPATCHER_IMAGE_TAG}" \
    --set-string "ap.imagePullPolicy=${DISPATCHER_IMAGE_PULL_POLICY}" \
    --timeout=120s

log "Dispatchers deployed."

# ── Verify dispatchers ───────────────────────────────────────────────────────
step "Waiting for dispatcher pods to be ready..."
kubectl wait --for=condition=available deployment/"${DISPATCHER_RELEASE}-llm-d-async" \
    --namespace "${NAMESPACE}" --timeout=60s
kubectl wait --for=condition=available deployment/"${DISPATCHER_SCRAPE_RELEASE}-llm-d-async" \
    --namespace "${NAMESPACE}" --timeout=60s
kubectl wait --for=condition=available deployment/"${DISPATCHER_PROM_RELEASE}-llm-d-async" \
    --namespace "${NAMESPACE}" --timeout=60s

verify_dispatcher_runtime_image() {
    local release="$1"
    local pod
    local actual_image
    local image_id

    pod="$(kubectl get pods \
        --namespace "${NAMESPACE}" \
        --selector "app.kubernetes.io/instance=${release},app.kubernetes.io/name=llm-d-async" \
        --field-selector status.phase=Running \
        -o json | jq -r '.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) | .metadata.name' | head -n1)"
    if [[ -z "${pod}" ]]; then
        die "No ready Async pod found for release ${release}"
    fi

    actual_image="$(kubectl get pod "${pod}" --namespace "${NAMESPACE}" -o jsonpath='{.spec.containers[?(@.name=="llm-d-async")].image}')"
    image_id="$(kubectl get pod "${pod}" --namespace "${NAMESPACE}" -o jsonpath='{.status.containerStatuses[?(@.name=="llm-d-async")].imageID}')"
    if [[ "${actual_image}" != "${DISPATCHER_IMAGE}" ]]; then
        die "Async pod ${pod} image is ${actual_image}, expected ${DISPATCHER_IMAGE}"
    fi
    if [[ -n "${DISPATCHER_EXPECTED_DIGEST}" && "${image_id##*@}" != "${DISPATCHER_EXPECTED_DIGEST}" ]]; then
        die "Async pod ${pod} runtime imageID is ${image_id}, expected digest ${DISPATCHER_EXPECTED_DIGEST}"
    fi
    log "Verified ${pod} uses ${actual_image} (runtime imageID: ${image_id})"
}

verify_dispatcher_runtime_image "${DISPATCHER_RELEASE}"
verify_dispatcher_runtime_image "${DISPATCHER_SCRAPE_RELEASE}"
verify_dispatcher_runtime_image "${DISPATCHER_PROM_RELEASE}"

# ── Add vllm-sim to Prometheus scrape targets ────────────────────────────────
step "Adding vllm-sim to Prometheus scrape config..."
PROM_CM="${PROMETHEUS_NAME:-prometheus}-config"
CURRENT_PROM_CONFIG=$(kubectl get configmap "${PROM_CM}" --namespace "${NAMESPACE}" -o jsonpath='{.data.prometheus\.yml}' 2>/dev/null || true)

if echo "${CURRENT_PROM_CONFIG}" | grep -q "vllm-sim"; then
    log "vllm-sim already in Prometheus scrape config, skipping"
else
    # Append vllm-sim scrape job (indentation must match existing scrape_configs entries)
    read -r -d '' VLLM_SIM_SCRAPE <<-SCRAPE || true
- job_name: 'vllm-sim'
  metrics_path: /metrics
  scrape_interval: 5s
  static_configs:
  - targets: ['${VLLM_SIM_NAME}.${NAMESPACE}.svc.cluster.local:8000']
    labels:
      component: vllm-sim
SCRAPE
    UPDATED_PROM_CONFIG="${CURRENT_PROM_CONFIG}
${VLLM_SIM_SCRAPE}"

    kubectl create configmap "${PROM_CM}" \
        --namespace "${NAMESPACE}" \
        --from-literal="prometheus.yml=${UPDATED_PROM_CONFIG}" \
        --dry-run=client -o yaml | kubectl apply -f -

    # Reload Prometheus config
    kubectl rollout restart deployment/"${PROMETHEUS_NAME:-prometheus}" --namespace "${NAMESPACE}"
    kubectl rollout status deployment/"${PROMETHEUS_NAME:-prometheus}" --namespace "${NAMESPACE}" --timeout=60s
    log "Prometheus scrape config updated with vllm-sim target"
fi

# ── Reconfigure processor for async dispatch ─────────────────────────────────
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    PROCESSOR_ASYNC_VALUES="${REPO_ROOT}/test/e2e/dispatcher/processor-async-values.yaml"

    step "Reconfiguring batch-gateway processor for async dispatch..."

    # Preserve the existing deployment settings, replacing its routing with the
    # async fixture. The composed dev deploy supplies these values on first install.
    REUSED_VALUES=$(mktemp)
    helm get values "${HELM_RELEASE}" -n "${NAMESPACE}" -o json | \
        jq 'del(.processor.config.modelGateways, .processor.config.globalInferenceGateway, .processor.config.asyncDispatch)' > "${REUSED_VALUES}"

    helm upgrade "${HELM_RELEASE}" "${REPO_ROOT}/charts/batch-gateway" \
        --namespace "${NAMESPACE}" \
        --reset-values \
        --values "${REUSED_VALUES}" \
        --values "${PROCESSOR_ASYNC_VALUES}" \
        --wait --timeout=120s
    rm -f "${REUSED_VALUES}"

    step "Restarting processor to pick up new config..."
    kubectl rollout restart statefulset/"${HELM_RELEASE}-processor" --namespace "${NAMESPACE}"
    kubectl rollout status statefulset/"${HELM_RELEASE}-processor" --namespace "${NAMESPACE}" --timeout=60s

    log "Processor reconfigured for async dispatch."
fi

# ── Expose Redis to host ──────────────────────────────────────────────────────
# dev-deploy.sh reserves this NodePort in Kind's extraPortMappings. Create the
# matching service instead of competing with Docker's host listener by trying
# to bind kubectl port-forward to the same port.
step "Exposing Redis on localhost:${DISPATCHER_REDIS_PORT}..."

# Clean up a port-forward left by an older version of this harness.
if [[ -f "${PID_FILE}" ]]; then
    old_pid=$(cat "${PID_FILE}")
    kill "${old_pid}" 2>/dev/null || true
    rm -f "${PID_FILE}"
fi

# Detect Redis service name
redis_svc="${REDIS_RELEASE}-master"
if [[ "${EXCHANGE_CLIENT_TYPE}" == "valkey" ]]; then
    redis_svc="${REDIS_RELEASE}-valkey-primary"
fi

kubectl get service "${redis_svc}" --namespace "${NAMESPACE}" -o json | \
    jq --arg name "${redis_svc}-nodeport" --argjson nodePort "${DISPATCHER_REDIS_NODE_PORT}" '{
        apiVersion: "v1",
        kind: "Service",
        metadata: {name: $name, namespace: .metadata.namespace},
        spec: {
            type: "NodePort",
            selector: .spec.selector,
            ports: [(.spec.ports[0] | {
                name: .name,
                protocol: .protocol,
                port: .port,
                targetPort: .targetPort,
                nodePort: $nodePort
            })]
        }
    }' | kubectl apply -f -

# Wait for Kind's host mapping to reach the NodePort service.
for i in $(seq 1 10); do
    if nc -z localhost "${DISPATCHER_REDIS_PORT}" 2>/dev/null; then
        break
    fi
    sleep 0.5
done

if ! nc -z localhost "${DISPATCHER_REDIS_PORT}" 2>/dev/null; then
    die "Redis NodePort failed to become reachable"
fi

log "Redis accessible at localhost:${DISPATCHER_REDIS_PORT}"

# ── Port-forward vLLM sim to host ────────────────────────────────────────────
DISPATCHER_SIM_PORT="${DISPATCHER_SIM_PORT:-8099}"
SIM_PID_FILE="${REPO_ROOT}/.dispatcher-sim-port-forward.pid"

step "Setting up vLLM sim port-forward on localhost:${DISPATCHER_SIM_PORT}..."

if [[ -f "${SIM_PID_FILE}" ]]; then
    old_pid=$(cat "${SIM_PID_FILE}")
    kill "${old_pid}" 2>/dev/null || true
    rm -f "${SIM_PID_FILE}"
fi

kubectl port-forward "svc/${VLLM_SIM_NAME}" "${DISPATCHER_SIM_PORT}:8000" \
    --namespace "${NAMESPACE}" &
SIM_PF_PID=$!
echo "${SIM_PF_PID}" > "${SIM_PID_FILE}"

for i in $(seq 1 10); do
    if nc -z localhost "${DISPATCHER_SIM_PORT}" 2>/dev/null; then
        break
    fi
    sleep 0.5
done

if ! nc -z localhost "${DISPATCHER_SIM_PORT}" 2>/dev/null; then
    die "Port-forward to vLLM sim failed to start"
fi

log "vLLM sim accessible at localhost:${DISPATCHER_SIM_PORT}"

log ""
log "Dispatcher is ready."
log ""
log "Usage:"
log "  ENABLE_DISPATCHER=true make test-e2e"
log "  ENABLE_DISPATCHER=true TEST_REDIS_URL=redis://localhost:${DISPATCHER_REDIS_PORT} go test ./test/e2e/ -run TestDispatcher -v -count=1"
log ""
log "Jaeger UI: http://localhost:${JAEGER_PORT}  (traces from both batch-gateway and llm-d-async)"
log "To stop port-forwards: make dev-clean"
log ""
if [[ -n "${DISPATCHER_SOURCE}" ]]; then
    log "Built from local source: ${DISPATCHER_SOURCE}"
    log "To rebuild after changes: DISPATCHER_SOURCE=${DISPATCHER_SOURCE} ENABLE_DISPATCHER=true make dev-deploy"
fi
