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
#   ROUTER_CHART_VERSION — OCI chart version for llm-d-router (default: v0, built from router main branch)
#   ROUTER_EPP_TAG     — EPP image tag, used with ROUTER_REPO (default: main)
#   ROUTER_EPP_REGISTRY — EPP image registry, used with ROUTER_REPO (default: ghcr.io)
#   ROUTER_EPP_REPOSITORY — EPP image path, used with ROUTER_REPO (default: llm-d/llm-d-router-endpoint-picker)
#   LLM_D_TAG          — git tag for llm-d guide values (default: v0.10.0)
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
LLM_D_REPO="${LLM_D_REPO:-}"
# The configs here use the llm-d.ai API group (EndpointPickerConfig llm-d.ai/v1),
# which is only on llm-d-router main until the 1.0.0 release. The router publishes
# its main build as chart version "v0" and image tag "main".
# TODO: switch both defaults to v1.0.0 once that release is out.
ROUTER_CHART_VERSION="${ROUTER_CHART_VERSION:-v0}"
ROUTER_EPP_TAG="${ROUTER_EPP_TAG:-main}"
ROUTER_EPP_REPOSITORY="${ROUTER_EPP_REPOSITORY:-llm-d/llm-d-router-endpoint-picker}"
ROUTER_EPP_REGISTRY="${ROUTER_EPP_REGISTRY:-ghcr.io}"
LLM_D_TAG="${LLM_D_TAG:-v0.10.0}"
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

# Verify that the deployed EPP comes from llm-d-router, not the retired
# inference-scheduler image. The OCI chart owns its image defaults; local
# router checkouts use ROUTER_EPP_REPOSITORY above.
verify_router_deployment() {
    local epp_release="$1"
    local expected_image="ghcr.io/llm-d/llm-d-router-endpoint-picker"
    if [ -n "${ROUTER_REPO:-}" ]; then
        expected_image="${ROUTER_EPP_REGISTRY}/${ROUTER_EPP_REPOSITORY}:${ROUTER_EPP_TAG}"
    fi

    local images
    images="$(${K} -n "${NAMESPACE}" get deployment/${epp_release}-epp \
        -o jsonpath='{.spec.template.spec.containers[*].image}')"
    if [[ "${images}" != *"${expected_image}"* ]]; then
        echo "ERROR: deployment/${epp_release}-epp is not using the llm-d-router EPP image; expected ${expected_image}, got: ${images}" >&2
        exit 1
    fi
    if [[ "${images}" == *"llm-d-inference-scheduler"* ]]; then
        echo "ERROR: deployment/${epp_release}-epp still uses the retired inference scheduler image: ${images}" >&2
        exit 1
    fi
    log "  Verified deployment/${epp_release}-epp uses ${expected_image}"
}

verify_router_crds() {
    local crd
    for crd in "$@"; do
        if ! ${K} get "crd/${crd}" >/dev/null 2>&1; then
            echo "ERROR: expected router CRD crd/${crd} was not deployed" >&2
            exit 1
        fi
    done
}

# EndpointPickerConfig and InferenceObjective use llm-d.ai/v1.
verify_router_objectives() {
    local objective api_version
    for objective in interactive-default batch-sheddable; do
        api_version="$(${K} -n "${NAMESPACE}" get \
            "inferenceobjectives.v1.llm-d.ai/${objective}" -o jsonpath='{.apiVersion}')"
        if [ "${api_version}" != "llm-d.ai/v1" ]; then
            echo "ERROR: InferenceObjective ${objective} uses ${api_version}, expected llm-d.ai/v1" >&2
            exit 1
        fi
    done
    log "  Verified InferenceObjectives use llm-d.ai/v1"
}

verify_router_plugin_config() {
    local epp_release="$1"
    local config_file="$2"
    local data_key="${config_file//./\\.}"
    local plugin_config
    plugin_config="$(${K} -n "${NAMESPACE}" get "configmap/${epp_release}-epp" \
        -o "jsonpath={.data.${data_key}}")"
    if ! grep -qx 'apiVersion: llm-d.ai/v1' <<<"${plugin_config}"; then
        echo "ERROR: ${config_file} does not use llm-d.ai/v1 EndpointPickerConfig" >&2
        exit 1
    fi
    log "  Verified ${config_file} uses llm-d.ai/v1 EndpointPickerConfig"
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

# CRD sources for sim mode scenario 4. InferencePool comes from GAIE.
# InferenceObjective and InferenceModelRewrite moved to llm-d-router in GAIE 1.6.
GIE_VERSION="${GIE_VERSION:-v1.6.2}"
ROUTER_CRD_REF="${ROUTER_CHART_VERSION}"
if [ "${ROUTER_CRD_REF}" = "v0" ]; then
    ROUTER_CRD_REF="main"
fi

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

    # --- Scenario 4 sim mode: deploy router EPP with flow control ---
    if [ "${SCENARIO}" = "4" ]; then
        log "Deploying router EPP with flow control (sim mode, scenario 4)"

        # Install CRDs: InferencePool from GAIE, the rest from llm-d-router
        log "  Installing CRDs (InferencePool from GAIE ${GIE_VERSION}, the rest from llm-d-router)"
        router_crd_base="https://raw.githubusercontent.com/llm-d/llm-d-router/${ROUTER_CRD_REF}/config/crd/bases"
        if [ -n "${ROUTER_REPO:-}" ]; then
            router_crd_base="${ROUTER_REPO}/config/crd/bases"
        fi
        ${K} apply -f "https://raw.githubusercontent.com/kubernetes-sigs/gateway-api-inference-extension/${GIE_VERSION}/config/crd/bases/inference.networking.k8s.io_inferencepools.yaml" >/dev/null
        ${K} apply -f "${router_crd_base}/llm-d.ai_inferenceobjectives.yaml" >/dev/null
        ${K} apply -f "${router_crd_base}/llm-d.ai_inferencemodelrewrites.yaml" >/dev/null
        ${K} wait --for=condition=established crd/inferencepools.inference.networking.k8s.io \
            crd/inferenceobjectives.llm-d.ai crd/inferencemodelrewrites.llm-d.ai --timeout=60s >/dev/null

        # Pick the router standalone chart: local checkout or OCI
        chart_args=()
        if [ -n "${ROUTER_REPO:-}" ]; then
            chart_ref="${ROUTER_REPO}/config/charts/llm-d-router-standalone"
            rm -f "${chart_ref}/Chart.lock"
            (cd "${chart_ref}" && helm dependency build >/dev/null 2>&1)
            chart_args+=(--set "router.epp.image.registry=${ROUTER_EPP_REGISTRY}"
                --set "router.epp.image.repository=${ROUTER_EPP_REPOSITORY}"
                --set "router.epp.image.tag=${ROUTER_EPP_TAG}")
        else
            chart_ref="oci://ghcr.io/llm-d/charts/llm-d-router-standalone"
            chart_args+=(--version "${ROUTER_CHART_VERSION}")
        fi

        # Install EPP standalone chart. The flow-control plugins come from the
        # scenario 4 router overlay.
        epp_release="epp-bench"
        log "  Installing EPP (release: ${epp_release})"
        ${H} upgrade --install "${epp_release}" "${chart_ref}" \
            -n "${NAMESPACE}" \
            "${chart_args[@]}" \
            --set router.monitoring.prometheus.auth.enabled=false \
            --set "router.modelServers.matchLabels.app=inference-sim" \
            --set router.epp.resources.requests.cpu=100m \
            --set router.epp.resources.requests.memory=256Mi \
            --set router.epp.resources.limits.memory=512Mi \
            -f "${SCRIPT_DIR}/helm-values/scenario-4-flow-control-overlay-router.yaml" >/dev/null

        # Wait for EPP deployment
        log "  Waiting for EPP to be ready..."
        ${K} -n "${NAMESPACE}" wait --for=condition=available deployment/${epp_release}-epp --timeout=120s
        verify_router_deployment "${epp_release}"
        verify_router_plugin_config "${epp_release}" flow-control-plugins.yaml
        verify_router_crds inferencepools.inference.networking.k8s.io \
            inferenceobjectives.llm-d.ai inferencemodelrewrites.llm-d.ai

        # Create InferenceObjectives
        log "  Creating InferenceObjectives"
        ${K} -n "${NAMESPACE}" apply -f - <<EOOBJ
apiVersion: llm-d.ai/v1
kind: InferenceObjective
metadata:
  name: interactive-default
spec:
  priority: 100
  poolRefs:
    - group: inference.networking.k8s.io
      name: ${epp_release}
---
apiVersion: llm-d.ai/v1
kind: InferenceObjective
metadata:
  name: batch-sheddable
spec:
  priority: -1
  poolRefs:
    - group: inference.networking.k8s.io
      name: ${epp_release}
EOOBJ
        verify_router_objectives
        log "  Flow control ready: EPP at ${epp_release}-epp:8081"
    fi
else
    # GPU mode: deploy real vLLM + llm-d Router + Istio Gateway

    # The router chart does not install CRDs. Install the llm-d.ai
    # InferenceObjective and InferenceModelRewrite CRDs from llm-d-router before
    # the router starts, because the router reads them at startup.
    # Chart "v0" is built from router main.
    if [ "${SCENARIO}" = "3" ] || [ "${SCENARIO}" = "4" ]; then
        objective_crd="https://raw.githubusercontent.com/llm-d/llm-d-router/${ROUTER_CRD_REF}/config/crd/bases/llm-d.ai_inferenceobjectives.yaml"
        model_rewrite_crd="https://raw.githubusercontent.com/llm-d/llm-d-router/${ROUTER_CRD_REF}/config/crd/bases/llm-d.ai_inferencemodelrewrites.yaml"
        if [ -n "${ROUTER_REPO:-}" ]; then
            objective_crd="${ROUTER_REPO}/config/crd/bases/llm-d.ai_inferenceobjectives.yaml"
            model_rewrite_crd="${ROUTER_REPO}/config/crd/bases/llm-d.ai_inferencemodelrewrites.yaml"
        fi
        log "Installing InferenceObjective CRD from ${objective_crd}"
        ${K} apply -f "${objective_crd}" >/dev/null
        log "Installing InferenceModelRewrite CRD from ${model_rewrite_crd}"
        ${K} apply -f "${model_rewrite_crd}" >/dev/null
        ${K} wait --for=condition=established crd/inferenceobjectives.llm-d.ai \
            crd/inferencemodelrewrites.llm-d.ai --timeout=60s >/dev/null
    fi

    # --- llm-d Router (EPP) ---
    log "Installing llm-d Router (${GUIDE_NAME})"

    # Scenario 4: include router overlay for priority-based scheduling
    FLOW_CONTROL_OVERLAY=""
    if [ "${SCENARIO}" = "4" ]; then
        log "  Flow control: enabling EPP priority bands (interactive=100, batch=-1)"
    fi

    if [ "${SCENARIO}" = "3" ]; then
        FLOW_CONTROL_OVERLAY="-f ${SCRIPT_DIR}/helm-values/scenario-3-admission-control-overlay-router.yaml"
    elif [ "${SCENARIO}" = "4" ]; then
        FLOW_CONTROL_OVERLAY="-f ${SCRIPT_DIR}/helm-values/scenario-4-flow-control-overlay-router.yaml"
    fi

    if [ -n "${ROUTER_REPO:-}" ]; then
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
        # OCI mode (default)
        log "  Using OCI chart: ghcr.io/llm-d/charts/llm-d-router-gateway:${ROUTER_CHART_VERSION}"
        if [ -n "${LLM_D_REPO}" ]; then
            log "  Using llm-d guide values from local repo: ${LLM_D_REPO}"
        else
            log "  Using llm-d guide values from tag: ${LLM_D_TAG}"
        fi

        # Stage guide values from the local checkout or pinned llm-d tag.
        LLM_D_VALUES_DIR=$(mktemp -d)
        trap "rm -rf ${LLM_D_VALUES_DIR}" EXIT
        if [ -n "${LLM_D_REPO}" ]; then
            local_values=(
                "${LLM_D_REPO}/guides/recipes/router/base.values.yaml"
                "${LLM_D_REPO}/guides/${GUIDE_NAME}/router/${GUIDE_NAME}.values.yaml"
                "${LLM_D_REPO}/guides/recipes/router/features/monitoring.values.yaml"
            )
            for values_path in "${local_values[@]}"; do
                if [ ! -f "${values_path}" ]; then
                    echo "ERROR: missing llm-d values file: ${values_path}" >&2
                    exit 1
                fi
            done
            cp "${local_values[0]}" "${LLM_D_VALUES_DIR}/base.values.yaml"
            cp "${local_values[1]}" "${LLM_D_VALUES_DIR}/guide.values.yaml"
            cp "${local_values[2]}" "${LLM_D_VALUES_DIR}/monitoring.values.yaml"
        else
            local_base="https://raw.githubusercontent.com/llm-d/llm-d/${LLM_D_TAG}"
            curl --fail --silent --show-error --location \
                "${local_base}/guides/recipes/router/base.values.yaml" \
                -o "${LLM_D_VALUES_DIR}/base.values.yaml"
            curl --fail --silent --show-error --location \
                "${local_base}/guides/${GUIDE_NAME}/router/${GUIDE_NAME}.values.yaml" \
                -o "${LLM_D_VALUES_DIR}/guide.values.yaml"
            curl --fail --silent --show-error --location \
                "${local_base}/guides/recipes/router/features/monitoring.values.yaml" \
                -o "${LLM_D_VALUES_DIR}/monitoring.values.yaml"
        fi

        ${H} upgrade --install "${GUIDE_NAME}" \
            oci://ghcr.io/llm-d/charts/llm-d-router-gateway \
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

    log "  Waiting for llm-d-router EPP to be ready..."
    ${K} -n "${NAMESPACE}" wait --for=condition=available deployment/${GUIDE_NAME}-epp --timeout=120s >/dev/null
    verify_router_deployment "${GUIDE_NAME}"
    if [ "${SCENARIO}" = "3" ] || [ "${SCENARIO}" = "4" ]; then
        if [ "${SCENARIO}" = "3" ]; then
            verify_router_plugin_config "${GUIDE_NAME}" admission-control-plugins.yaml
        else
            verify_router_plugin_config "${GUIDE_NAME}" flow-control-plugins.yaml
        fi
        verify_router_crds inferenceobjectives.llm-d.ai inferencemodelrewrites.llm-d.ai
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

# --- Scenario 3/4: InferenceObjectives (priority-based routing) ---
# GPU mode only. Sim scenario 4 creates its own objectives above.
if [ "${MODE}" != "sim" ] && { [ "${SCENARIO}" = "3" ] || [ "${SCENARIO}" = "4" ]; }; then
    log "Deploying InferenceObjectives for flow control"
    ${K} -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: llm-d.ai/v1
kind: InferenceObjective
metadata:
  name: interactive-default
spec:
  priority: 100
  poolRefs:
    - group: inference.networking.k8s.io
      name: ${GUIDE_NAME}
---
apiVersion: llm-d.ai/v1
kind: InferenceObjective
metadata:
  name: batch-sheddable
spec:
  priority: -1
  poolRefs:
    - group: inference.networking.k8s.io
      name: ${GUIDE_NAME}
EOF
    verify_router_objectives
    log "  Created InferenceObjective: interactive-default (priority 100)"
    log "  Created InferenceObjective: batch-sheddable (priority -1)"
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
