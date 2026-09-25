#!/usr/bin/env bash
set -euo pipefail

# Benchmark environment setup.
# Deploys the full stack for a single scenario.
#
# Usage:
#   KUBE_CONTEXT=my-ctx SCENARIO=2 ./benchmarks/setup.sh
#
# Required env vars:
#   KUBE_CONTEXT       — kubectl context (e.g. coreweave-waldorf)
#   SCENARIO           — scenario number (0-6)
#
# Optional:
#   MODE               — "sim" to use inference-sim instead of real vLLM (default: gpu)
#   LLM_D_REPO         — path to llm-d checkout (overrides downloading from LLM_D_TAG)
#   ROUTER_REPO        — path to llm-d-router checkout (overrides OCI chart)
#   ROUTER_CHART_VERSION — OCI chart version for llm-d-router (default: 0.9.2)
#   ROUTER_EPP_TAG     — EPP image tag (default: v0.9.0)
#   ROUTER_EPP_REGISTRY — EPP image registry (default: ghcr.io)
#   ROUTER_EPP_REPOSITORY — EPP image repository (default: llm-d/llm-d-inference-scheduler)
#   LLM_D_TAG          — git tag for llm-d guide values (default: v0.7.0)
#   NAMESPACE          — override auto-generated namespace (default: batch-bench-s${SCENARIO})
#   MODEL              — model to serve (default: Qwen/Qwen3-8B)
#   GUIDE_NAME         — inference pool name (default: optimized-baseline)
#   MODEL_REVISION     — HuggingFace model revision/commit-sha to pin (default: unset, uses latest)
#   SIM_IMAGE          — inference-sim container image (default: ghcr.io/llm-d/llm-d-inference-sim:latest)
#   SIM_TTFT           — simulated time-to-first-token (default: 50ms)
#   SIM_ITL            — simulated inter-token-latency (default: 20ms)
#   BG_IMAGE_REPO      — batch-gateway image repo override
#   BG_IMAGE_TAG       — batch-gateway image tag override
#   BENCH_DB_PASSWORD  — PostgreSQL password (default: random 24-char string)
#   PROMETHEUS_RELEASE — Prometheus Operator release label for ServiceMonitor discovery (default: llmd-kube-prometheus-stack)
#   PROMETHEUS_NAMESPACE — Namespace where Prometheus is deployed (default: llm-d-monitoring)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
MODE="${MODE:-gpu}"
if [ "${MODE}" = "sim" ]; then
    MODEL="${MODEL:-sim-model}"
else
    MODEL="${MODEL:-Qwen/Qwen3-8B}"
fi
GUIDE_NAME="${GUIDE_NAME:-optimized-baseline}"
NAMESPACE="${NAMESPACE:-batch-bench-s${SCENARIO}}"
ROUTER_CHART_VERSION="${ROUTER_CHART_VERSION:-0.9.2}"
ROUTER_EPP_TAG="${ROUTER_EPP_TAG:-v0.9.0}"
ROUTER_EPP_REGISTRY="${ROUTER_EPP_REGISTRY:-ghcr.io}"
ROUTER_EPP_REPOSITORY="${ROUTER_EPP_REPOSITORY:-llm-d/llm-d-inference-scheduler}"
LLM_D_TAG="${LLM_D_TAG:-v0.7.0}"
SIM_IMAGE="${SIM_IMAGE:-ghcr.io/llm-d/llm-d-inference-sim:latest}"
SIM_TTFT="${SIM_TTFT:-50ms}"
SIM_ITL="${SIM_ITL:-20ms}"
PROMETHEUS_RELEASE="${PROMETHEUS_RELEASE:-llmd-kube-prometheus-stack}"
PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-llm-d-monitoring}"

# Validate required vars
for var in KUBE_CONTEXT SCENARIO; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: $var is not set" >&2
        exit 1
    fi
done

if [ "${SCENARIO}" -lt 0 ] || [ "${SCENARIO}" -gt 6 ]; then
    echo "ERROR: SCENARIO must be 0-6, got: ${SCENARIO}" >&2
    exit 1
fi

if [ "${MODE}" = "sim" ] && [ "${SCENARIO}" = "3" ]; then
    echo "ERROR: MODE=sim SCENARIO=3 is unsupported; use MODE=gpu for admission control or SCENARIO=4 for the simulator EPP path" >&2
    exit 1
fi

K="kubectl --context=${KUBE_CONTEXT}"
H="helm --kube-context=${KUBE_CONTEXT}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Determine which Helm values file to use for the processor
values_file_for_scenario() {
    case "${SCENARIO}" in
        0|1) echo "" ;;  # No batch-gateway deployed
        2)   echo "${SCRIPT_DIR}/helm-values/scenario-2-ungated.yaml" ;;
        3)   echo "${SCRIPT_DIR}/helm-values/scenario-3-admission-control-aimd.yaml" ;;
        4)   echo "${SCRIPT_DIR}/helm-values/scenario-4-flow-control-aimd.yaml" ;;
        5)   echo "${SCRIPT_DIR}/helm-values/scenario-5-async.yaml" ;;
        6)   echo "${SCRIPT_DIR}/helm-values/scenario-6-low-concurrency.yaml" ;;
    esac
}

log "=== Setting up scenario ${SCENARIO} in namespace ${NAMESPACE} ==="

# Create namespace
${K} create namespace "${NAMESPACE}" 2>/dev/null || true

# Wait for RBAC to be ready (ArgoCD may take time to apply RoleBindings)
if [ "${MODE}" = "gpu" ]; then
    for i in $(seq 1 60); do
        if ${K} auth can-i create serviceaccounts -n "${NAMESPACE}" 2>/dev/null | grep -q "yes"; then
            break
        fi
        if [ "$i" -eq 60 ]; then
            echo "ERROR: RBAC not ready after 120s — cannot create ServiceAccounts in ${NAMESPACE}" >&2
            exit 1
        fi
        sleep 2
    done
fi

# --- Redis ---
log "Installing Redis"
${H} upgrade --install redis oci://registry-1.docker.io/bitnamicharts/redis \
    -n "${NAMESPACE}" \
    --set auth.enabled=false \
    --set master.persistence.size=1Gi \
    --set replica.replicaCount=0 \
    --set networkPolicy.enabled=false \
    --set pdb.create=false \
    --wait --timeout 120s >/dev/null

# --- PostgreSQL ---
log "Installing PostgreSQL"
BENCH_DB_PASSWORD="${BENCH_DB_PASSWORD:-$(head -c 32 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 24)}"
${H} upgrade --install postgresql oci://registry-1.docker.io/bitnamicharts/postgresql \
    -n "${NAMESPACE}" \
    --set auth.database=batchgateway \
    --set auth.password="${BENCH_DB_PASSWORD}" \
    --set primary.persistence.size=5Gi \
    --set networkPolicy.enabled=false \
    --wait --timeout 120s >/dev/null

# --- Secrets ---
log "Creating secrets"
${K} -n "${NAMESPACE}" create secret generic batch-gateway-secrets \
    --from-literal=redis-url="redis://redis-master.${NAMESPACE}.svc.cluster.local:6379" \
    --from-literal=postgresql-url="postgresql://postgres:${BENCH_DB_PASSWORD}@postgresql.${NAMESPACE}.svc.cluster.local:5432/batchgateway?sslmode=disable" \
    --from-literal=inference-api-key="" \
    --from-literal=s3-secret-access-key="" \
    2>/dev/null || true

# --- PVCs ---
log "Creating PVCs"
if [ "${MODE}" = "sim" ]; then
    PVC_ACCESS_MODE="ReadWriteOnce"
else
    PVC_ACCESS_MODE="ReadWriteMany"
fi
${K} -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: batch-gateway-files
spec:
  accessModes: [${PVC_ACCESS_MODE}]
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: benchmark-results
spec:
  accessModes: [${PVC_ACCESS_MODE}]
  resources:
    requests:
      storage: 10Gi
EOF

# GIE (flow control) settings for sim mode scenario 4
GIE_VERSION="${GIE_VERSION:-v1.5.0}"
GIE_REPO="${GIE_REPO:-}"
GIE_UPSTREAM_REPO="https://github.com/kubernetes-sigs/gateway-api-inference-extension.git"

# --- Inference backend ---
if [ "${MODE}" = "sim" ]; then
    # Sim mode: deploy inference-sim (no GPU, no router, no Istio)
    log "Deploying inference-sim (MODE=sim, model: ${MODEL})"
    ${K} -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: inference-sim
  labels:
    llm-d.ai/role: decode
    app: inference-sim
spec:
  replicas: 1
  selector:
    matchLabels:
      app: inference-sim
  template:
    metadata:
      labels:
        llm-d.ai/role: decode
        app: inference-sim
    spec:
      containers:
        - name: vllm-sim
          image: ${SIM_IMAGE}
          args:
            - --model
            - "${MODEL}"
            - --port
            - "8000"
            - --time-to-first-token=${SIM_TTFT}
            - --inter-token-latency=${SIM_ITL}
          ports:
            - containerPort: 8000
              name: modelserver
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /v1/models
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: inference-sim
spec:
  selector:
    app: inference-sim
  ports:
    - port: 8000
      targetPort: 8000
      name: http
EOF

    # --- Scenario 4 sim mode: deploy GIE EPP with flow control ---
    if [ "${SCENARIO}" = "4" ]; then
        log "Deploying GIE EPP with flow control (sim mode, scenario 4)"

        # Ensure GIE repo is available
        if [ -z "${GIE_REPO}" ] || [ ! -d "${GIE_REPO}" ]; then
            GIE_REPO="$(mktemp -d)/gateway-api-inference-extension"
            log "  Cloning GIE ${GIE_VERSION}..."
            git clone --depth 1 --branch "${GIE_VERSION}" "${GIE_UPSTREAM_REPO}" "${GIE_REPO}" >/dev/null 2>&1
        fi

        # Install GIE CRDs
        log "  Installing GIE CRDs"
        ${K} apply -f "${GIE_REPO}/config/crd/bases/" >/dev/null

        # Build Helm dependencies for standalone chart
        chart_dir="${GIE_REPO}/config/charts/standalone"
        (cd "${chart_dir}" && helm dependency build >/dev/null 2>&1)

        # Create flow-control values
        values_file="$(mktemp)"
        cat > "${values_file}" <<'VALUESEOF'
inferenceExtension:
  pluginsCustomConfig:
    flow-control-plugins.yaml: |
      apiVersion: inference.networking.x-k8s.io/v1alpha1
      kind: EndpointPickerConfig
      featureGates:
        - "flowControl"
      plugins:
        - type: round-robin-fairness-policy
        - type: global-strict-fairness-policy
        - type: slo-deadline-ordering-policy
        - type: concurrency-detector
          parameters:
            maxConcurrency: 1000
      flowControl:
        maxBytes: 4294967296
        defaultRequestTTL: 30s
        priorityBands:
          - priority: 100
            maxBytes: 1073741824
            fairnessPolicyRef: round-robin-fairness-policy
            orderingPolicyRef: fcfs-ordering-policy
          - priority: -1
            maxBytes: 3221225472
            fairnessPolicyRef: global-strict-fairness-policy
            orderingPolicyRef: slo-deadline-ordering-policy
        defaultPriorityBand:
          maxBytes: 536870912
          fairnessPolicyRef: global-strict-fairness-policy
          orderingPolicyRef: fcfs-ordering-policy
      saturationDetector:
        pluginRef: concurrency-detector
VALUESEOF

        # Install EPP standalone chart
        epp_release="epp-bench"
        log "  Installing EPP (release: ${epp_release})"
        ${H} upgrade --install "${epp_release}" "${chart_dir}" \
            -n "${NAMESPACE}" \
            --set "inferenceExtension.image.tag=${GIE_VERSION}" \
            --set inferenceExtension.monitoring.prometheus.auth.enabled=false \
            --set inferenceExtension.sidecar.enabled=false \
            --set inferenceExtension.endpointsServer.createInferencePool=true \
            --set "inferencePool.modelServers.matchLabels.app=inference-sim" \
            --set "inferencePool.targetPorts[0].number=8000" \
            --set inferencePool.modelServerType=vllm \
            --set inferenceExtension.pluginsConfigFile=flow-control-plugins.yaml \
            --set inferenceExtension.resources.requests.cpu=100m \
            --set inferenceExtension.resources.requests.memory=256Mi \
            --set inferenceExtension.resources.limits.memory=512Mi \
            -f "${values_file}" >/dev/null
        rm -f "${values_file}"

        # Wait for EPP deployment
        log "  Waiting for EPP to be ready..."
        ${K} -n "${NAMESPACE}" wait --for=condition=available deployment/${epp_release}-epp --timeout=120s

        # Create InferenceObjective CRDs
        log "  Creating InferenceObjective CRDs"
        ${K} -n "${NAMESPACE}" apply -f - <<EOOBJ
apiVersion: inference.networking.x-k8s.io/v1alpha2
kind: InferenceObjective
metadata:
  name: interactive-default
spec:
  priority: 100
  poolRef:
    group: inference.networking.k8s.io
    name: ${epp_release}
---
apiVersion: inference.networking.x-k8s.io/v1alpha2
kind: InferenceObjective
metadata:
  name: batch-sheddable
spec:
  priority: -1
  poolRef:
    group: inference.networking.k8s.io
    name: ${epp_release}
EOOBJ
        log "  Flow control ready: EPP at ${epp_release}-epp:8081"
    fi
else
    # GPU mode: deploy real vLLM + llm-d Router + Istio Gateway

    # --- llm-d Router (EPP) ---
    log "Installing llm-d Router (${GUIDE_NAME})"

    # Scenario 4: include router overlay for priority-based scheduling
    FLOW_CONTROL_OVERLAY=""
    if [ "${SCENARIO}" = "4" ]; then
        log "  Flow control: enabling EPP priority bands (interactive=100, batch=-1)"
    fi

    if [ -n "${ROUTER_REPO:-}" ]; then
        if [ "${SCENARIO}" = "3" ]; then
            FLOW_CONTROL_OVERLAY="-f ${SCRIPT_DIR}/helm-values/scenario-3-admission-control-overlay-router.yaml"
        elif [ "${SCENARIO}" = "4" ]; then
            FLOW_CONTROL_OVERLAY="-f ${SCRIPT_DIR}/helm-values/scenario-4-flow-control-overlay-router.yaml"
        fi
        # Local repo mode (development override)
        log "  Using local repo: ROUTER_REPO=${ROUTER_REPO}"
        chart_dir="${ROUTER_REPO}/config/charts/llm-d-router-gateway"
        rm -f "${chart_dir}/Chart.lock"
        (cd "${chart_dir}" && helm dependency build >/dev/null 2>&1)
        # Only set default pluginsConfigFile when no overlay provides one
        PLUGINS_CFG_SET=""
        if [ -z "${FLOW_CONTROL_OVERLAY}" ]; then
            PLUGINS_CFG_SET="--set router.epp.pluginsConfigFile=default-plugins.yaml"
        fi
        ${H} upgrade --install "${GUIDE_NAME}" "${chart_dir}" \
            -n "${NAMESPACE}" \
            --set router.epp.replicas=1 \
            --set router.epp.image.registry=${ROUTER_EPP_REGISTRY} \
            --set router.epp.image.repository=${ROUTER_EPP_REPOSITORY} \
            --set router.epp.image.tag=${ROUTER_EPP_TAG} \
            ${PLUGINS_CFG_SET} \
            --set router.epp.resources.requests.cpu=4 \
            --set router.epp.resources.requests.memory=8Gi \
            --set router.epp.resources.limits.memory=16Gi \
            --set router.modelServers.matchLabels.llm-d\\.ai/guide=optimized-baseline \
            --set router.inferencePool.modelServerProtocol=http \
            --set router.monitoring.prometheus.auth.enabled=false \
            ${FLOW_CONTROL_OVERLAY} \
            --set provider.name=istio \
            --set httpRoute.create=true \
            --set httpRoute.inferenceGatewayName=llm-d-inference-gateway >/dev/null
    else
        if [ "${SCENARIO}" = "3" ]; then
            FLOW_CONTROL_OVERLAY="-f ${SCRIPT_DIR}/helm-values/scenario-3-admission-control-overlay.yaml"
        elif [ "${SCENARIO}" = "4" ]; then
            FLOW_CONTROL_OVERLAY="-f ${SCRIPT_DIR}/helm-values/scenario-4-flow-control-overlay.yaml"
        fi
        # OCI mode (default — reproducible, pinned versions)
        log "  Using OCI chart: ghcr.io/llm-d/llm-d-router-gateway:${ROUTER_CHART_VERSION}"
        log "  Using llm-d guide values from tag: ${LLM_D_TAG}"

        # Download guide values from pinned llm-d tag
        LLM_D_VALUES_DIR=$(mktemp -d)
        trap "rm -rf ${LLM_D_VALUES_DIR}" EXIT
        local_base="https://raw.githubusercontent.com/llm-d/llm-d/${LLM_D_TAG}"
        curl -sL "${local_base}/guides/recipes/router/base.values.yaml" -o "${LLM_D_VALUES_DIR}/base.values.yaml"
        curl -sL "${local_base}/guides/${GUIDE_NAME}/router/${GUIDE_NAME}.values.yaml" -o "${LLM_D_VALUES_DIR}/guide.values.yaml"
        curl -sL "${local_base}/guides/recipes/router/features/monitoring.values.yaml" -o "${LLM_D_VALUES_DIR}/monitoring.values.yaml"

        ${H} upgrade --install "${GUIDE_NAME}" \
            oci://ghcr.io/llm-d/llm-d-router-gateway \
            --version "${ROUTER_CHART_VERSION}" \
            -n "${NAMESPACE}" \
            -f "${LLM_D_VALUES_DIR}/base.values.yaml" \
            -f "${LLM_D_VALUES_DIR}/guide.values.yaml" \
            -f "${LLM_D_VALUES_DIR}/monitoring.values.yaml" \
            ${FLOW_CONTROL_OVERLAY} \
            --set provider.name=istio \
            --set httpRoute.create=true \
            --set httpRoute.inferenceGatewayName=llm-d-inference-gateway >/dev/null
    fi

    # --- Istio Gateway ---
    log "Creating Istio Gateway"
    ${K} -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: llm-d-inference-gateway
  annotations:
    networking.istio.io/service-type: ClusterIP
spec:
  gatewayClassName: istio
  listeners:
  - name: default
    port: 80
    protocol: HTTP
    allowedRoutes:
      namespaces:
        from: Same
EOF

    # --- vLLM ---
    log "Deploying vLLM (${MODEL})"
    ${K} -n "${NAMESPACE}" apply -k "${SCRIPT_DIR}/manifests/vllm/"

    # Pin model revision if specified
    if [ -n "${MODEL_REVISION:-}" ]; then
        log "  Pinning model revision: ${MODEL_REVISION}"
        ${K} -n "${NAMESPACE}" patch deploy/decode --type=json \
            -p "[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--revision=${MODEL_REVISION}\"}]" >/dev/null
    fi
fi

# --- Wait for inference backend ---
if [ "${MODE}" = "sim" ]; then
    log "Waiting for inference-sim to be ready..."
    ${K} -n "${NAMESPACE}" wait pod -l llm-d.ai/role=decode \
        --for=condition=Ready --timeout=120s >/dev/null
else
    log "Waiting for vLLM to be ready..."
    ${K} -n "${NAMESPACE}" wait pod -l llm-d.ai/role=decode \
        --for=condition=Ready --timeout=1800s >/dev/null
fi

# --- Batch Gateway (scenarios 2-6 only) ---
VALUES_FILE=$(values_file_for_scenario)
if [ -n "${VALUES_FILE}" ]; then
    log "Installing batch-gateway (scenario ${SCENARIO})"
    BG_EXTRA_ARGS=()
    if [ -n "${BG_IMAGE_REPO:-}" ]; then
        BG_EXTRA_ARGS+=(
            --set "apiserver.image.repository=${BG_IMAGE_REPO}-apiserver"
            --set "processor.image.repository=${BG_IMAGE_REPO}-processor"
        )
    fi
    if [ -n "${BG_IMAGE_TAG:-}" ]; then
        BG_EXTRA_ARGS+=(
            --set-string "apiserver.image.tag=${BG_IMAGE_TAG}"
            --set-string "processor.image.tag=${BG_IMAGE_TAG}"
        )
    fi
    if [ -n "${BG_PULL_POLICY:-}" ]; then
        BG_EXTRA_ARGS+=(
            --set "apiserver.image.pullPolicy=${BG_PULL_POLICY}"
            --set "processor.image.pullPolicy=${BG_PULL_POLICY}"
        )
    fi

    # In sim mode, replace all model gateways with a single entry
    if [ "${MODE}" = "sim" ]; then
        if [ "${SCENARIO}" = "4" ]; then
            # Scenario 4: route through EPP for flow control; null out globalInferenceGateway from values file
            BG_EXTRA_ARGS+=(
                --set-json "processor.config.globalInferenceGateway=null"
                --set-json "processor.config.modelGateways={\"${MODEL}\":{\"url\":\"http://epp-bench-epp.${NAMESPACE}.svc.cluster.local:8081\",\"requestTimeout\":\"5m\",\"maxRetries\":3,\"initialBackoff\":\"2s\",\"maxBackoff\":\"30s\",\"inferenceObjective\":\"batch-sheddable\"}}"
            )
        else
            # Other scenarios: direct to inference-sim
            BG_EXTRA_ARGS+=(
                --set-json "processor.config.modelGateways={\"${MODEL}\":{\"url\":\"http://inference-sim.${NAMESPACE}.svc.cluster.local:8000\",\"requestTimeout\":\"5m\",\"maxRetries\":3,\"initialBackoff\":\"1s\",\"maxBackoff\":\"60s\"}}"
            )
        fi
    fi

    ${H} upgrade --install batch-gateway \
        "${REPO_ROOT}/charts/batch-gateway/" \
        -n "${NAMESPACE}" \
        -f "${VALUES_FILE}" \
        --set global.secretName=batch-gateway-secrets \
        --set global.fileClient.type=fs \
        --set global.fileClient.fs.pvcName=batch-gateway-files \
        --set gc.enabled=false \
        "${BG_EXTRA_ARGS[@]+"${BG_EXTRA_ARGS[@]}"}" >/dev/null

    # TMPDIR fix for large file uploads
    ${K} -n "${NAMESPACE}" set env deploy/batch-gateway-apiserver TMPDIR=/tmp/batch-gateway >/dev/null
else
    log "Skipping batch-gateway (not needed for scenario ${SCENARIO})"
fi

# --- Scenario 3/4: InferenceObjective CRDs (priority-based routing) ---
if [ "${SCENARIO}" = "3" ] || [ "${SCENARIO}" = "4" ]; then
    log "Deploying InferenceObjective CRDs for flow control"
    ${K} -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: inference.networking.x-k8s.io/v1alpha2
kind: InferenceObjective
metadata:
  name: interactive-default
spec:
  priority: 100
  poolRef:
    group: inference.networking.k8s.io
    name: ${GUIDE_NAME}
---
apiVersion: inference.networking.x-k8s.io/v1alpha2
kind: InferenceObjective
metadata:
  name: batch-sheddable
spec:
  priority: -1
  poolRef:
    group: inference.networking.k8s.io
    name: ${GUIDE_NAME}
EOF
    log "  Created InferenceObjective: interactive-default (priority 100)"
    log "  Created InferenceObjective: batch-sheddable (priority -1)"

    # Also create under llm-d.ai API group (EPP selects this when available)
    ${K} -n "${NAMESPACE}" apply -f - <<EOF2
apiVersion: llm-d.ai/v1alpha2
kind: InferenceObjective
metadata:
  name: interactive-default
spec:
  priority: 100
  poolRef:
    group: inference.networking.k8s.io
    name: ${GUIDE_NAME}
---
apiVersion: llm-d.ai/v1alpha2
kind: InferenceObjective
metadata:
  name: batch-sheddable
spec:
  priority: -1
  poolRef:
    group: inference.networking.k8s.io
    name: ${GUIDE_NAME}
EOF2
    log "  Created InferenceObjective (llm-d.ai): interactive-default, batch-sheddable"
fi

# --- Scenario 5: Async processor ---
if [ "${SCENARIO}" = "5" ]; then
    log "ERROR: Scenario 5 (async) is blocked on async-processor integration"
    exit 1
fi

if [ -n "${VALUES_FILE}" ]; then
    ${K} -n "${NAMESPACE}" rollout status deploy/batch-gateway-apiserver --timeout=60s >/dev/null
    ${K} -n "${NAMESPACE}" rollout status statefulset/batch-gateway-processor --timeout=60s >/dev/null
fi

# --- Prometheus ServiceMonitor (GPU mode, scenarios >= 3) ---
if [ "${MODE}" = "gpu" ] && [ "${SCENARIO}" -ge 3 ] && [ -n "${VALUES_FILE}" ]; then
    log "Creating Prometheus ServiceMonitor for batch-gateway-processor"
    ${K} -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: batch-gateway-processor-metrics
  labels:
    app: batch-gateway-processor
spec:
  selector:
    app.kubernetes.io/component: processor
    app.kubernetes.io/instance: batch-gateway
  ports:
    - name: metrics
      port: 9090
      targetPort: 9090
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: batch-gateway-processor
  labels:
    release: ${PROMETHEUS_RELEASE}
spec:
  namespaceSelector:
    matchNames: ["${NAMESPACE}"]
  selector:
    matchLabels:
      app: batch-gateway-processor
  endpoints:
    - port: metrics
      path: /metrics
      interval: 15s
EOF
    log "  ServiceMonitor created (discovery label: release=${PROMETHEUS_RELEASE})"
fi

log "=== Scenario ${SCENARIO} ready in namespace ${NAMESPACE} ==="
