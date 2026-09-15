#!/usr/bin/env bash
set -euo pipefail

# ── Deploy batch-gateway on Kubernetes / OpenShift ─────────────────────────────
#
# Works on both vanilla Kubernetes and OpenShift. All components are installed
# from open-source Helm charts (not OLM / OperatorHub), making this script
# platform-agnostic.
#
# Installs llm-d stack (Istio + GAIE InferencePool + vllm-sim) and batch-gateway.
#   - Batch Gateway exposed via istio-ingress (HTTPS:443)
#   - Kuadrant for auth (kubernetesTokenReview) and rate limiting
#   - cert-manager (Helm) + Redis/PostgreSQL + batch-gateway helm chart
#
# Single simulated model for demo/testing.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/common.sh"

# ── Configuration ─────────────────────────────────────────────────────────────
GATEWAY_CLASS_NAME="${GATEWAY_CLASS_NAME:-istio}"
GATEWAY_NAME="${GATEWAY_NAME:-istio-gateway}"
GATEWAY_NAMESPACE="${GATEWAY_NAMESPACE:-istio-ingress}"

LLM_NAMESPACE="${LLM_NAMESPACE:-llm}"

KUADRANT_NAMESPACE="${KUADRANT_NAMESPACE:-kuadrant-system}"
KUADRANT_VERSION="${KUADRANT_VERSION:-1.3.1}"
KUADRANT_RELEASE="${KUADRANT_RELEASE:-kuadrant-operator}"

CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.20.3}"

BATCH_INTERNAL_GATEWAY_NAME="${BATCH_INTERNAL_GATEWAY_NAME:-batch-internal-gateway}"
BATCH_INTERNAL_GATEWAY_NAMESPACE="${BATCH_INTERNAL_GATEWAY_NAMESPACE:-${GATEWAY_NAMESPACE}}"
GATEWAY_LOCAL_PORT="${GATEWAY_LOCAL_PORT:-8080}"

LLMD_VERSION="${LLMD_VERSION:-v0.8.1}"
LLMD_GIT_DIR="/tmp/llm-d-${LLMD_VERSION}"
LLMD_RELEASE_POSTFIX="${LLMD_RELEASE_POSTFIX:-llmd}"

ISTIO_VERSION="${ISTIO_VERSION:-1.29.2}"
# GAIE_VERSION, ROUTER_CHART_VERSION, ROUTER_GATEWAY_CHART are sourced
# from the llm-d repo's guides/env.sh (see clone_llmd_repo).

# Model name matches the simulated model default ("random")
MODEL_NAME="${MODEL_NAME:-random}"
LLMD_POOL_NAME="${LLMD_RELEASE_POSTFIX}"

MODEL_ROUTES=(
    "${MODEL_NAME}:${LLMD_POOL_NAME}"
)

# Flow control: GIE priority-based dispatch (interactive > batch).
# When enabled, EPP is configured with flow control plugins and InferenceObjective
# CRDs are created so batch requests are sheddable (priority -1) while interactive
# requests get priority 100.
ENABLE_FLOW_CONTROL="${ENABLE_FLOW_CONTROL:-true}"
INTERACTIVE_FLOW_CONTROL_OBJECTIVE="${INTERACTIVE_FLOW_CONTROL_OBJECTIVE:-interactive-default}"
BATCH_FLOW_CONTROL_OBJECTIVE="${BATCH_FLOW_CONTROL_OBJECTIVE:-batch-sheddable}"

# Async dispatcher (llm-d-async) — ENABLE_DISPATCHER is set in common.sh
DISPATCHER_RELEASE="${DISPATCHER_RELEASE:-dispatcher}"
DISPATCHER_VERSION="${DISPATCHER_VERSION:-v0.7.3}"
DISPATCHER_IMAGE="${DISPATCHER_IMAGE:-ghcr.io/llm-d-incubation/llm-d-async:${DISPATCHER_VERSION}}"
DISPATCHER_CHART="${DISPATCHER_CHART:-oci://ghcr.io/llm-d-incubation/charts/async-processor}"
DISPATCHER_CHART_VERSION="${DISPATCHER_CHART_VERSION:-0.7.3}"

# ── Infrastructure ────────────────────────────────────────────────────────────

install_cert_manager() {
    step "Installing cert-manager ${CERT_MANAGER_VERSION}..."

    if helm status cert-manager -n cert-manager &>/dev/null; then
        log "cert-manager is already installed. Skipping."
        return
    fi

    helm repo add jetstack https://charts.jetstack.io --force-update
    helm install cert-manager jetstack/cert-manager \
        --namespace cert-manager \
        --create-namespace \
        --version "${CERT_MANAGER_VERSION}" \
        --set crds.enabled=true

    for deploy in cert-manager cert-manager-webhook cert-manager-cainjector; do
        wait_for_deployment "$deploy" cert-manager 180s
    done

    log "cert-manager installed successfully."
}

create_inference_external_gateway() {
    step "Creating TLS certificate for Gateway..."
    kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${GATEWAY_NAME}-tls
  namespace: ${GATEWAY_NAMESPACE}
spec:
  secretName: ${GATEWAY_NAME}-tls
  issuerRef:
    name: ${TLS_ISSUER_NAME}
    kind: ClusterIssuer
  dnsNames:
  - "*.${GATEWAY_NAMESPACE}.svc.cluster.local"
  - localhost
EOF
    kubectl wait --for=condition=Ready --timeout=60s \
        -n "${GATEWAY_NAMESPACE}" certificate/${GATEWAY_NAME}-tls \
        || die "TLS certificate '${GATEWAY_NAME}-tls' not ready after 60s. Check cert-manager logs."
    log "Gateway TLS certificate created."

    step "Creating Istio Gateway (HTTP + HTTPS)..."
    kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: ${GATEWAY_NAME}
  namespace: ${GATEWAY_NAMESPACE}
  labels:
    kuadrant.io/gateway: "true"
spec:
  gatewayClassName: ${GATEWAY_CLASS_NAME}
  listeners:
  - name: http
    protocol: HTTP
    port: 80
    allowedRoutes:
      namespaces:
        from: Selector
        selector:
          matchLabels:
            llm-d.ai/gateway-route: "true"
  - name: https
    protocol: HTTPS
    port: 443
    tls:
      mode: Terminate
      certificateRefs:
      - name: ${GATEWAY_NAME}-tls
    allowedRoutes:
      namespaces:
        from: Selector
        selector:
          matchLabels:
            llm-d.ai/gateway-route: "true"
EOF

    wait_for_deployment "${GATEWAY_NAME}-${GATEWAY_CLASS_NAME}" "${GATEWAY_NAMESPACE}" 300s

    step "Waiting for Gateway to be programmed..."
    kubectl wait --for=condition=Programmed \
        --timeout=300s \
        -n "${GATEWAY_NAMESPACE}" \
        gateway/${GATEWAY_NAME} \
        || die "Gateway '${GATEWAY_NAME}' not programmed after 300s. Check Istio logs."

    log "Gateway created (HTTPS on port 443)."
}

install_kuadrant() {
    step "Installing Kuadrant Operator ${KUADRANT_VERSION}..."

    if helm status "${KUADRANT_RELEASE}" -n "${KUADRANT_NAMESPACE}" &>/dev/null; then
        log "Kuadrant operator '${KUADRANT_RELEASE}' is already installed. Skipping."
    else
        helm repo add kuadrant https://kuadrant.io/helm-charts/ --force-update
        helm install "${KUADRANT_RELEASE}" kuadrant/kuadrant-operator \
            --version "${KUADRANT_VERSION}" \
            --create-namespace \
            --namespace "${KUADRANT_NAMESPACE}"

        step "Waiting for Kuadrant operator deployments..."
        sleep 30
        for deploy in authorino-operator \
                      kuadrant-operator-controller-manager \
                      limitador-operator-controller-manager; do
            wait_for_deployment "$deploy" "${KUADRANT_NAMESPACE}" 180s
        done
        log "Kuadrant operator installed successfully."
    fi

    # Create Kuadrant instance
    if kubectl get kuadrant kuadrant -n "${KUADRANT_NAMESPACE}" &>/dev/null; then
        if kubectl get deployment authorino -n "${KUADRANT_NAMESPACE}" &>/dev/null \
            && kubectl get deployment limitador-limitador -n "${KUADRANT_NAMESPACE}" &>/dev/null; then
            log "Kuadrant instance already exists with authorino + limitador. Skipping."
            return
        fi
        warn "Kuadrant CR exists but authorino/limitador missing. Recreating..."
        kubectl patch kuadrant kuadrant -n "${KUADRANT_NAMESPACE}" --type=merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
        kubectl delete kuadrant kuadrant -n "${KUADRANT_NAMESPACE}" --wait=false 2>/dev/null || true
        sleep 5
    fi

    step "Creating Kuadrant instance..."
    kubectl apply -f - <<EOF
apiVersion: kuadrant.io/v1beta1
kind: Kuadrant
metadata:
  name: kuadrant
  namespace: ${KUADRANT_NAMESPACE}
spec: {}
EOF

    step "Waiting for Kuadrant instance to be ready..."
    for deploy in authorino limitador-limitador; do
        wait_for_deployment "$deploy" "${KUADRANT_NAMESPACE}" 300s
    done
    kubectl wait --timeout=300s -n "${KUADRANT_NAMESPACE}" kuadrant kuadrant --for=condition=Ready=True
    kubectl get kuadrant kuadrant -n "${KUADRANT_NAMESPACE}" -o=jsonpath='{.status.conditions[?(@.type=="Ready")].message}{"\n"}'
    log "Kuadrant instance is ready."
}

create_llm_route() {
    step "Creating llm-route..."

    # Build rules from MODEL_ROUTES array
    # Format: "model-name:inferencepool-name"
    local rules=""
    for entry in "${MODEL_ROUTES[@]}"; do
        local model_name="${entry%%:*}"
        local pool_name="${entry##*:}"
        rules="${rules}
  - matches:
    - path:
        type: PathPrefix
        value: /${LLM_NAMESPACE}/${model_name}/v1/completions
    filters:
    - type: URLRewrite
      urlRewrite:
        path:
          type: ReplacePrefixMatch
          replacePrefixMatch: /v1/completions
    backendRefs:
    - group: inference.networking.k8s.io
      kind: InferencePool
      name: ${pool_name}
  - matches:
    - path:
        type: PathPrefix
        value: /${LLM_NAMESPACE}/${model_name}/v1/chat/completions
    filters:
    - type: URLRewrite
      urlRewrite:
        path:
          type: ReplacePrefixMatch
          replacePrefixMatch: /v1/chat/completions
    backendRefs:
    - group: inference.networking.k8s.io
      kind: InferencePool
      name: ${pool_name}"
        log "  llm-route rule: /${LLM_NAMESPACE}/${model_name}/* -> InferencePool/${pool_name}"
    done

    kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: llm-route
  namespace: ${LLM_NAMESPACE}
spec:
  parentRefs:
  - name: ${GATEWAY_NAME}
    namespace: ${GATEWAY_NAMESPACE}
  rules:${rules}
EOF

    log "llm-route created."
}

create_batch_llm_route() {
    step "Creating batch-llm-route on Internal Gateway..."

    local rules=""
    for entry in "${MODEL_ROUTES[@]}"; do
        local model_name="${entry%%:*}"
        local pool_name="${entry##*:}"
        rules="${rules}
  - matches:
    - path:
        type: PathPrefix
        value: /${LLM_NAMESPACE}/${model_name}/v1/completions
    filters:
    - type: URLRewrite
      urlRewrite:
        path:
          type: ReplacePrefixMatch
          replacePrefixMatch: /v1/completions
    backendRefs:
    - group: inference.networking.k8s.io
      kind: InferencePool
      name: ${pool_name}
  - matches:
    - path:
        type: PathPrefix
        value: /${LLM_NAMESPACE}/${model_name}/v1/chat/completions
    filters:
    - type: URLRewrite
      urlRewrite:
        path:
          type: ReplacePrefixMatch
          replacePrefixMatch: /v1/chat/completions
    backendRefs:
    - group: inference.networking.k8s.io
      kind: InferencePool
      name: ${pool_name}"
        log "  batch-llm-route rule: /${LLM_NAMESPACE}/${model_name}/* -> InferencePool/${pool_name}"
    done

    kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: batch-llm-route
  namespace: ${LLM_NAMESPACE}
spec:
  parentRefs:
  - name: ${BATCH_INTERNAL_GATEWAY_NAME}
    namespace: ${BATCH_INTERNAL_GATEWAY_NAMESPACE}
  rules:${rules}
EOF

    log "batch-llm-route created (via Internal Gateway)."
}

apply_batch_llm_auth_policy() {
    step "Creating batch-llm-route AuthPolicy (authentication + model authorization)..."
    kubectl apply -f - <<EOF
apiVersion: kuadrant.io/v1
kind: AuthPolicy
metadata:
  name: batch-llm-route-auth
  namespace: ${LLM_NAMESPACE}
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: batch-llm-route
  rules:
    authentication:
      kubernetes-user:
        kubernetesTokenReview:
          audiences:
          - https://kubernetes.default.svc
    authorization:
      model-access:
        kubernetesSubjectAccessReview:
          user:
            expression: auth.identity.user.username
          authorizationGroups:
            expression: auth.identity.user.groups
          resourceAttributes:
            group:
              value: inference.networking.k8s.io
            resource:
              value: inferencepools
            namespace:
              expression: request.path.split("/")[1]
            name:
              expression: request.path.split("/")[2]
            verb:
              value: get
EOF
    log "Batch LLM AuthPolicy applied (no TokenRateLimitPolicy)."
}

init_test() {
    local test_title="$1"
    banner "Testing: ${test_title}"

    if ! kubectl get gateway "${GATEWAY_NAME}" -n "${GATEWAY_NAMESPACE}" &>/dev/null; then
        die "Gateway '${GATEWAY_NAME}' not found in namespace '${GATEWAY_NAMESPACE}'. Run '$0 install' first."
    fi

    # Resolve gateway URL
    set_gateway_url

    step "Waiting for gateway to be accessible..."
    local retries=60
    for i in $(seq 1 "${retries}"); do
        if curl -sk --connect-timeout 5 --max-time 10 -o /dev/null -w "%{http_code}" "${GATEWAY_URL}/" &>/dev/null; then
            log "Gateway is accessible."
            break
        fi
        if [ "$i" -eq "${retries}" ]; then
            die "Gateway not accessible after ${retries} attempts."
        fi
        sleep 3
    done
}

# ── llm-d stack: Istio + GAIE + vllm-sim ─────────────────────────────────────

clone_llmd_repo() {
    if [ ! -d "${LLMD_GIT_DIR}/guides" ]; then
        step "Cloning llm-d ${LLMD_VERSION}..."
        rm -rf "${LLMD_GIT_DIR}"
        git clone --depth 1 --branch "${LLMD_VERSION}" https://github.com/llm-d/llm-d.git "${LLMD_GIT_DIR}"
    fi
    source "${LLMD_GIT_DIR}/guides/env.sh"
}

install_llmd_deps() {
    step "Installing llm-d dependencies (CRDs)..."

    clone_llmd_repo

    # Gateway API + GAIE CRDs via the official install script.
    # On OpenShift, Gateway API CRDs are platform-managed; failures are skipped.
    step "Installing Gateway API + GAIE CRDs..."
    bash "${LLMD_GIT_DIR}/guides/recipes/gateway/install-gateway-crds.sh" \
        || warn "CRD install failed (may already exist on OpenShift). Continuing."

    # llm-d Router CRDs (InferenceObjective etc.)
    step "Installing llm-d Router CRDs ${ROUTER_CHART_VERSION}..."
    kubectl apply -f "https://github.com/llm-d/llm-d-router/releases/download/${ROUTER_CHART_VERSION}/manifests.yaml" \
        || warn "Router CRD install failed. Continuing."

    log "llm-d CRDs installed."
}

install_istio() {
    step "Installing Istio ${ISTIO_VERSION}..."
    helm repo add istio https://istio-release.storage.googleapis.com/charts --force-update

    if ! helm status istio-base -n istio-system &>/dev/null; then
        helm install istio-base istio/base \
            --namespace istio-system \
            --create-namespace \
            --version "${ISTIO_VERSION}"
    else
        log "istio-base already installed. Skipping."
    fi

    if ! helm status istiod -n istio-system &>/dev/null; then
        helm install istiod istio/istiod \
            --namespace istio-system \
            --version "${ISTIO_VERSION}" \
            --set pilot.env.ENABLE_GATEWAY_API_INFERENCE_EXTENSION=true \
            --wait
    else
        log "istiod already installed. Skipping."
    fi

    wait_for_deployment "istiod" "istio-system" 300s
    log "Istio installed."
}

deploy_llmd_model() {
    step "Deploying model with llm-d..."

    local sim_dir="${SCRIPT_DIR}/llmd-sim"
    local pool_name="${LLMD_POOL_NAME}"
    local epp_host="${pool_name}-epp.${LLM_NAMESPACE}.svc.cluster.local"

    # Install llm-d Router (gateway mode) — follows the official layered values
    # pattern from guides/flow-control/README.md (gateway mode section):
    #   base.values.yaml (from llm-d repo) + sim overlay + flow-control overlay
    local llmd_dir="${LLMD_GIT_DIR}"
    step "Installing llm-d Router chart ${ROUTER_CHART_VERSION}..."
    local router_helm_args=(
        --version "${ROUTER_CHART_VERSION}"
        --namespace "${LLM_NAMESPACE}"
        -f "${llmd_dir}/guides/recipes/router/base.values.yaml"
        -f "${sim_dir}/router/sim-values.yaml"
        --set provider.name=istio
        --set "provider.istio.destinationRule.host=${epp_host}"
    )
    if [ "${ENABLE_FLOW_CONTROL}" = "true" ]; then
        router_helm_args+=(-f "${sim_dir}/router/overlays/flow-control.yaml")
        log "Flow control enabled: EPP will use flow-control-plugins.yaml"
    fi
    helm upgrade --install "${pool_name}" \
        oci://ghcr.io/llm-d/charts/llm-d-router-gateway \
        "${router_helm_args[@]}"

    # Deploy model server (vllm-sim) via kustomize
    step "Deploying model server (vllm-sim)..."
    kubectl apply -n "${LLM_NAMESPACE}" -k "${sim_dir}/modelserver/"

    # Wait for llm-d deployments
    wait_for_deployment "${pool_name}-epp" "${LLM_NAMESPACE}" 300s
    wait_for_deployment "llmd-sim-decode" "${LLM_NAMESPACE}" 300s

    log "llm-d model deployed."
}

create_inference_objectives() {
    step "Creating InferenceObjective resources..."
    kubectl apply -f - <<EOF
apiVersion: llm-d.ai/v1alpha2
kind: InferenceObjective
metadata:
  name: ${INTERACTIVE_FLOW_CONTROL_OBJECTIVE}
  namespace: ${LLM_NAMESPACE}
spec:
  priority: 100
  poolRef:
    name: ${LLMD_POOL_NAME}
---
apiVersion: llm-d.ai/v1alpha2
kind: InferenceObjective
metadata:
  name: ${BATCH_FLOW_CONTROL_OBJECTIVE}
  namespace: ${LLM_NAMESPACE}
spec:
  priority: -1
  poolRef:
    name: ${LLMD_POOL_NAME}
EOF
    log "InferenceObjectives created (${INTERACTIVE_FLOW_CONTROL_OBJECTIVE}: priority 100, ${BATCH_FLOW_CONTROL_OBJECTIVE}: priority -1)."
}

verify_flow_control_config() {
    banner "Verifying Flow Control configuration"

    local pool_name="${LLMD_POOL_NAME}"
    local errors=0

    # 1. EPP plugins config
    step "Checking EPP plugins config..."
    local epp_pod
    epp_pod=$(kubectl get pod -n "${LLM_NAMESPACE}" -l "llm-d-router-gateway=${pool_name}-epp" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -z "${epp_pod}" ]; then
        warn "No EPP pod found for pool '${pool_name}'. Cannot verify plugins config."
        errors=$((errors + 1))
    else
        local pod_args
        pod_args=$(kubectl get pod -n "${LLM_NAMESPACE}" "${epp_pod}" \
            -o jsonpath='{.spec.containers[*].args}' 2>/dev/null || echo "")
        if echo "${pod_args}" | grep -q "flow-control-plugins"; then
            log "EPP is using flow-control-plugins.yaml"
        else
            warn "EPP does not appear to use flow-control-plugins.yaml"
            errors=$((errors + 1))
        fi
    fi

    # 2. InferenceObjective resources
    step "Checking InferenceObjective resources..."
    for obj in "${INTERACTIVE_FLOW_CONTROL_OBJECTIVE}" "${BATCH_FLOW_CONTROL_OBJECTIVE}"; do
        if kubectl get inferenceobjective "${obj}" -n "${LLM_NAMESPACE}" &>/dev/null; then
            log "InferenceObjective '${obj}' exists."
        else
            warn "InferenceObjective '${obj}' not found."
            errors=$((errors + 1))
        fi
    done

    # 3. Batch processor inferenceObjective config (stored in configmap, not env)
    step "Checking batch processor config..."
    if kubectl get configmap "${BATCH_INSTANCE_NAME}-processor-config" -n "${BATCH_NAMESPACE}" \
        -o jsonpath='{.data}' 2>/dev/null | grep "inference_objective" | grep -q "${BATCH_FLOW_CONTROL_OBJECTIVE}"; then
        log "Processor configured with inferenceObjective: ${BATCH_FLOW_CONTROL_OBJECTIVE}"
    else
        warn "Processor configmap does not contain inference_objective: ${BATCH_FLOW_CONTROL_OBJECTIVE}"
        errors=$((errors + 1))
    fi

    if [ "${errors}" -gt 0 ]; then
        die "Flow control verification failed with ${errors} error(s). Review output above."
    fi
    log "Flow control verification passed."
}

verify_flow_control_runtime() {
    banner "Verifying Flow Control runtime (metrics)"

    local pool_name="${LLMD_POOL_NAME}"
    local errors=0

    # Fetch metrics from EPP via the in-cluster Service (avoids port-forward
    # reliability issues). The EPP container is distroless (no curl/sh), so
    # we use a temporary pod to curl the EPP metrics endpoint.
    step "Fetching EPP flow control metrics..."
    local epp_svc="${pool_name}-epp"
    local metrics_url="http://${epp_svc}.${LLM_NAMESPACE}.svc.cluster.local:9090/metrics"

    local metrics_response
    local check_pod="epp-metrics-check-$(date +%s)"
    metrics_response=$(kubectl run "${check_pod}" --rm -i --restart=Never --quiet \
        --image=curlimages/curl -n "${LLM_NAMESPACE}" -- \
        curl -s --connect-timeout 10 --max-time 30 -w '\n%{http_code}' "${metrics_url}" \
    2>/dev/null) || true

    local metrics_body metrics_http_code
    metrics_http_code=$(echo "${metrics_response}" | tail -1)
    metrics_body=$(echo "${metrics_response}" | sed '$d')
    if [ "${metrics_http_code}" != "200" ]; then
        die "EPP metrics endpoint returned HTTP ${metrics_http_code}."
    fi
    if [ -z "${metrics_body}" ]; then
        die "EPP metrics response body is empty."
    fi

    echo "${metrics_body}" | grep 'llm_d_epp_flow_control_request_queue_duration_seconds_count' || true

    # 1. Interactive requests dispatched (priority 0)
    step "Checking flow control metrics for interactive requests (priority 0)..."
    local interactive_count
    interactive_count=$(echo "${metrics_body}" | grep 'llm_d_epp_flow_control_request_queue_duration_seconds_count' \
        | grep 'priority="0"' | grep 'outcome="Dispatched"' | grep -oE '[0-9]+$' || echo "0")
    if [ "${interactive_count}" -gt 0 ] 2>/dev/null; then
        log "Flow control dispatched ${interactive_count} interactive request(s) (priority 0)."
    else
        warn "No interactive requests (priority 0) found in flow control metrics."
        errors=$((errors + 1))
    fi

    # 2. Batch requests dispatched (priority -1)
    step "Checking flow control metrics for batch requests (priority -1)..."
    local batch_count
    batch_count=$(echo "${metrics_body}" | grep 'llm_d_epp_flow_control_request_queue_duration_seconds_count' \
        | grep 'priority="-1"' | grep 'outcome="Dispatched"' | grep -oE '[0-9]+$' || echo "0")
    if [ "${batch_count}" -gt 0 ] 2>/dev/null; then
        log "Flow control dispatched ${batch_count} batch request(s) (priority -1)."
    else
        warn "No batch requests (priority -1) found in flow control metrics."
        errors=$((errors + 1))
    fi

    # 3. Pool saturation metric exists
    step "Checking pool saturation metric..."
    local saturation_line
    saturation_line=$(echo "${metrics_body}" | grep 'llm_d_epp_flow_control_pool_saturation{' | head -1 || true)
    if [ -n "${saturation_line}" ]; then
        local saturation
        saturation=$(echo "${saturation_line}" | grep -oE '[0-9.]+$')
        log "Pool saturation: ${saturation}"
    else
        warn "Pool saturation metric not found."
        errors=$((errors + 1))
    fi

    if [ "${errors}" -gt 0 ]; then
        die "Flow control runtime verification failed with ${errors} error(s). Review output above."
    fi
    log "Flow control runtime verification passed."
}

uninstall_llmd() {
    step "Removing llm-d stack (${LLM_NAMESPACE})..."
    timeout_delete 30s httproute --all -n "${LLM_NAMESPACE}" || true
    kubectl delete -k "${SCRIPT_DIR}/llmd-sim/modelserver/" -n "${LLM_NAMESPACE}" 2>/dev/null || true
    helm uninstall "${LLMD_POOL_NAME}" -n "${LLM_NAMESPACE}" --timeout 60s 2>/dev/null || true
    timeout_delete 30s inferencepool --all -n "${LLM_NAMESPACE}" || true
}

# ── Async Dispatcher (llm-d-async) ───────────────────────────────────────────

install_prometheus_for_dispatcher() {
    local prom_name="prometheus"

    if kubectl get deployment "${prom_name}" -n "${LLM_NAMESPACE}" &>/dev/null; then
        log "Prometheus already exists in ${LLM_NAMESPACE}. Skipping."
        return
    fi

    step "Installing Prometheus"
    kubectl apply -n "${LLM_NAMESPACE}" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${prom_name}-config
data:
  prometheus.yml: |
    global:
      scrape_interval: 5s
    scrape_configs:
    - job_name: 'epp'
      metrics_path: /metrics
      static_configs:
      - targets: ['${LLMD_POOL_NAME}-epp.${LLM_NAMESPACE}.svc.cluster.local:9090']
    - job_name: 'vllm'
      metrics_path: /metrics
      kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: ['${LLM_NAMESPACE}']
      relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_llm_d_ai_model]
        action: keep
        regex: '.+'
      - source_labels: [__meta_kubernetes_pod_ip]
        target_label: __address__
        replacement: '\${1}:8000'
      metric_relabel_configs:
      - target_label: inference_pool
        replacement: '${LLMD_POOL_NAME}'
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${prom_name}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${prom_name}
  template:
    metadata:
      labels:
        app: ${prom_name}
    spec:
      serviceAccountName: ${prom_name}
      containers:
      - name: prometheus
        image: prom/prometheus:v3.5.0
        args:
        - --config.file=/etc/prometheus/prometheus.yml
        - --storage.tsdb.retention.time=1h
        - --storage.tsdb.path=/tmp/prometheus
        ports:
        - containerPort: 9090
        volumeMounts:
        - name: config
          mountPath: /etc/prometheus
      volumes:
      - name: config
        configMap:
          name: ${prom_name}-config
---
apiVersion: v1
kind: Service
metadata:
  name: ${prom_name}
spec:
  selector:
    app: ${prom_name}
  ports:
  - port: 9090
    targetPort: 9090
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${prom_name}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${prom_name}
rules:
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${prom_name}
subjects:
- kind: ServiceAccount
  name: ${prom_name}
  namespace: ${LLM_NAMESPACE}
roleRef:
  kind: ClusterRole
  name: ${prom_name}
  apiGroup: rbac.authorization.k8s.io
EOF

    wait_for_deployment "${prom_name}" "${LLM_NAMESPACE}" 120s
    log "Prometheus installed (scraping EPP + vLLM metrics)."
}

deploy_dispatcher() {
    banner "Installing Async Dispatcher (llm-d-async)"

    install_prometheus_for_dispatcher

    # Reuse batch-internal-gateway: async-processor passes through the user's
    # Authorization header (captured at batch creation), so AuthPolicy works.
    local internal_gw_svc
    internal_gw_svc=$(kubectl get svc -n "${BATCH_INTERNAL_GATEWAY_NAMESPACE}" \
        -l "gateway.networking.k8s.io/gateway-name=${BATCH_INTERNAL_GATEWAY_NAME}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    [ -z "${internal_gw_svc}" ] && die "No service found for Internal Gateway '${BATCH_INTERNAL_GATEWAY_NAME}'."

    local redis_svc="${BATCH_REDIS_RELEASE}-master"
    if [[ "${BATCH_EXCHANGE_CLIENT_TYPE}" == "valkey" ]]; then
        redis_svc="${BATCH_REDIS_RELEASE}-valkey-primary"
    fi
    local redis_host="${redis_svc}.${BATCH_NAMESPACE}.svc.cluster.local"
    local redis_url="redis://${redis_host}:6379"
    local prometheus_url="http://prometheus.${LLM_NAMESPACE}.svc.cluster.local:9090"

    local igw_base_url="http://${internal_gw_svc}.${BATCH_INTERNAL_GATEWAY_NAMESPACE}.svc.cluster.local/${LLM_NAMESPACE}/${MODEL_NAME}"
    log "Dispatcher igw_base_url (via Internal Gateway): ${igw_base_url}"
    log "Dispatcher Redis: ${redis_url}"
    log "Dispatcher Prometheus: ${prometheus_url}"

    local image_repo="${DISPATCHER_IMAGE%%:*}"
    local image_tag="${DISPATCHER_IMAGE##*:}"

    local values_file
    values_file=$(mktemp)
    cat > "${values_file}" <<YAML
ap:
  imagePullPolicy: IfNotPresent
  messageQueueImpl: "redis-sortedset"
  concurrency: 1
  prometheusURL: "${prometheus_url}"
  prometheusCacheTTL: "0s"
  redis:
    enabled: true
    url: "${redis_url}"
    pollIntervalMs: 500
    batchSize: 10
    queuesConfig:
      - queue_name: "llm-d-async:requests:${LLMD_POOL_NAME}"
        request_path_url: "/v1/chat/completions"
        igw_base_url: "${igw_base_url}"
        gate_type: "prometheus-budget"
        gate_params:
          pool: "${LLMD_POOL_NAME}"
          namespace: "${LLM_NAMESPACE}"
          max_concurrency: "100"
          baseline: "0.05"
          fallback: "1.0"
  modelServerMonitor:
    enabled: false
YAML

    step "Deploying async-processor (release: ${DISPATCHER_RELEASE})..."
    local helm_cmd=(helm)
    if helm status "${DISPATCHER_RELEASE}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
        log "Release '${DISPATCHER_RELEASE}' already exists. Upgrading..."
        helm_cmd+=(upgrade --reset-values)
    else
        helm_cmd+=(install)
    fi
    "${helm_cmd[@]}" "${DISPATCHER_RELEASE}" "${DISPATCHER_CHART}" \
        --version "${DISPATCHER_CHART_VERSION}" \
        --namespace "${BATCH_NAMESPACE}" \
        --values "${values_file}" \
        --set "ap.image.repository=${image_repo}" \
        --set "ap.image.tag=${image_tag}" \
        --wait --timeout=120s

    rm -f "${values_file}"

    wait_for_deployment "${DISPATCHER_RELEASE}-async-processor" "${BATCH_NAMESPACE}" 120s

    log "Async dispatcher deployed."
}

verify_dispatcher_config() {
    banner "Verifying Async Dispatcher configuration"

    local errors=0

    local config_yaml
    config_yaml=$(kubectl get configmap "${BATCH_INSTANCE_NAME}-processor-config" -n "${BATCH_NAMESPACE}" \
        -o jsonpath='{.data.config\.yaml}' 2>/dev/null || echo "")

    # 1. Processor dispatch mode
    step "Checking batch processor dispatch mode..."
    local dispatch_mode
    dispatch_mode=$(echo "${config_yaml}" | yq '.dispatch_mode // ""')
    if [ "${dispatch_mode}" = "async" ]; then
        log "Processor configured with dispatch_mode: async"
    else
        warn "Processor dispatch_mode is '${dispatch_mode}', expected 'async'"
        errors=$((errors + 1))
    fi

    # 2. Processor inferencePoolName
    step "Checking batch processor inferencePoolName..."
    local models_path=".async_dispatch.models"
    case "${BATCH_RELEASE_VERSION}" in
        v0.3.0|v0.4.0) models_path=".model_gateways" ;;
    esac
    local pool_name
    pool_name=$(echo "${config_yaml}" | MODEL_NAME="${MODEL_NAME}" yq "${models_path}[strenv(MODEL_NAME)].inference_pool_name // \"\"")
    if [ "${pool_name}" = "${LLMD_POOL_NAME}" ]; then
        log "Processor inference_pool_name: ${pool_name}"
    else
        warn "Processor inference_pool_name for model '${MODEL_NAME}' is '${pool_name}', expected '${LLMD_POOL_NAME}'"
        errors=$((errors + 1))
    fi

    if [ "${ENABLE_FLOW_CONTROL}" = "true" ]; then
        local objective
        objective=$(echo "${config_yaml}" | MODEL_NAME="${MODEL_NAME}" yq "${models_path}[strenv(MODEL_NAME)].inference_objective // \"\"")
        if [ "${objective}" = "${BATCH_FLOW_CONTROL_OBJECTIVE}" ]; then
            log "Processor inference_objective: ${objective}"
        else
            warn "Processor inference_objective for model '${MODEL_NAME}' is '${objective}', expected '${BATCH_FLOW_CONTROL_OBJECTIVE}'"
            errors=$((errors + 1))
        fi
    fi

    # 3. Async-processor started successfully
    local ap_deploy="${DISPATCHER_RELEASE}-async-processor"
    step "Checking async-processor startup logs..."
    local ap_logs
    ap_logs=$(kubectl logs "deployment/${ap_deploy}" -n "${BATCH_NAMESPACE}" 2>/dev/null || echo "")
    if echo "${ap_logs}" | grep -q "Async Processor starting"; then
        log "Async-processor started successfully"
    else
        warn "Async-processor startup log not found"
        errors=$((errors + 1))
    fi

    if [ "${errors}" -gt 0 ]; then
        die "Dispatcher config verification failed with ${errors} error(s)."
    fi
    log "Dispatcher configuration verified."
}

# verify_dispatcher_runtime is defined in common.sh

# ── Batch Gateway ─────────────────────────────────────────────────────────────

deploy_batch_gateway_k8s() {
    banner "Installing Batch Gateway"

    local model_key="${MODEL_NAME//./\\.}"
    local model_config="processor.config.modelGateways"
    local helm_args=(
        --set "apiserver.config.batchAPI.passThroughHeaders={Authorization}"
    )

    if [ "${ENABLE_DISPATCHER}" = "true" ]; then
        # Async dispatch: processor sends requests via Redis to async-processor
        model_config="processor.config.asyncDispatch.models"
        # These released charts predate asyncDispatch.models.
        case "${BATCH_RELEASE_VERSION}" in
            v0.3.0|v0.4.0) model_config="processor.config.modelGateways" ;;
        esac
        helm_args+=(
            --set "processor.config.dispatchMode=async"
            --set "processor.config.asyncDispatch.resultPollTimeout=30s"
            --set "${model_config}.${model_key}.inferencePoolName=${LLMD_POOL_NAME}"
        )
        log "Async dispatch enabled: processor will route through llm-d-async (pool: ${LLMD_POOL_NAME})"
    else
        # Sync dispatch: processor sends requests directly through Internal Gateway
        local internal_gw_svc
        internal_gw_svc=$(kubectl get svc -n "${BATCH_INTERNAL_GATEWAY_NAMESPACE}" \
            -l "gateway.networking.k8s.io/gateway-name=${BATCH_INTERNAL_GATEWAY_NAME}" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
        [ -z "${internal_gw_svc}" ] && die "No service found for Internal Gateway '${BATCH_INTERNAL_GATEWAY_NAME}'."
        local model_url="http://${internal_gw_svc}.${BATCH_INTERNAL_GATEWAY_NAMESPACE}.svc.cluster.local/${LLM_NAMESPACE}/${MODEL_NAME}"
        log "Model URL (via Internal Gateway): ${model_url}"

        helm_args+=(
            --set "processor.config.dispatchMode=sync"
            --set "processor.config.modelGateways.${model_key}.url=${model_url}"
            --set "processor.config.modelGateways.${model_key}.requestTimeout=${GW_REQUEST_TIMEOUT}"
            --set "processor.config.modelGateways.${model_key}.maxRetries=${GW_MAX_RETRIES}"
            --set "processor.config.modelGateways.${model_key}.initialBackoff=${GW_INITIAL_BACKOFF}"
            --set "processor.config.modelGateways.${model_key}.maxBackoff=${GW_MAX_BACKOFF}"
        )
    fi

    if [ "${ENABLE_FLOW_CONTROL}" = "true" ]; then
        helm_args+=(
            --set "${model_config}.${model_key}.inferenceObjective=${BATCH_FLOW_CONTROL_OBJECTIVE}"
        )
        log "Flow control: processor will send x-gateway-inference-objective: ${BATCH_FLOW_CONTROL_OBJECTIVE}"
    fi

    do_deploy_batch_gateway_helm "${helm_args[@]}"
}

# ── Auth & Rate Limit Policies ────────────────────────────────────────────────
apply_llm_auth_policy() {
    step "Creating llm-route AuthPolicy (authentication + model-level authorization)..."
    kubectl apply -f - <<EOF
apiVersion: kuadrant.io/v1
kind: AuthPolicy
metadata:
  name: llm-route-auth
  namespace: ${LLM_NAMESPACE}
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: llm-route
  rules:
    authentication:
      kubernetes-user:
        kubernetesTokenReview:
          audiences:
          - https://kubernetes.default.svc
    authorization:
      model-access:
        kubernetesSubjectAccessReview:
          user:
            expression: auth.identity.user.username
          authorizationGroups:
            expression: auth.identity.user.groups
          resourceAttributes:
            group:
              value: inference.networking.k8s.io
            resource:
              value: inferencepools
            namespace:
              expression: request.path.split("/")[1]
            name:
              expression: request.path.split("/")[2]
            verb:
              value: get
EOF
    log "LLM AuthPolicy applied."
}

apply_llm_token_rate_limit() {
    step "Creating TokenRateLimitPolicy for inference (500 tokens/1m per user)..."
    kubectl apply -f - <<EOF
apiVersion: kuadrant.io/v1alpha1
kind: TokenRateLimitPolicy
metadata:
  name: inference-token-limit
  namespace: ${LLM_NAMESPACE}
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: llm-route
  limits:
    per-user:
      rates:
      - limit: 500
        window: 1m
      when:
      - predicate: request.path.endsWith("/v1/chat/completions")
      counters:
      - expression: auth.identity.user.username
EOF

    step "Waiting for TokenRateLimitPolicy to be enforced..."
    kubectl wait tokenratelimitpolicy/inference-token-limit \
        --for="condition=Enforced=true" \
        -n "${LLM_NAMESPACE}" --timeout=180s 2>/dev/null \
        || die "TokenRateLimitPolicy not enforced after 180s."

    log "TokenRateLimitPolicy applied."
}

apply_batch_auth_policy() {
    step "Creating batch-route AuthPolicy (authentication only)..."
    # No authorization here; model-level authorization happens when batch processor
    # forwards requests to the Internal Gateway's batch-llm-route.
    kubectl apply -f - <<EOF
apiVersion: kuadrant.io/v1
kind: AuthPolicy
metadata:
  name: batch-route-auth
  namespace: ${BATCH_NAMESPACE}
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: batch-route
  rules:
    authentication:
      kubernetes-user:
        kubernetesTokenReview:
          audiences:
          - https://kubernetes.default.svc
EOF
    log "Batch AuthPolicy applied."
}

apply_batch_request_rate_limit() {
    step "Creating batch-route RateLimitPolicy (20 req/1m per user)..."
    kubectl apply -f - <<EOF
apiVersion: kuadrant.io/v1
kind: RateLimitPolicy
metadata:
  name: batch-ratelimit
  namespace: ${BATCH_NAMESPACE}
spec:
  targetRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: batch-route
  limits:
    per-user:
      rates:
      - limit: 20
        window: 1m
      counters:
      - expression: auth.identity.user.username
EOF
    log "RateLimitPolicy applied (20 req/min per user)."
}

# ── Prerequisite checks ─────────────────────────────────────────────────────

check_prerequisites() {
    step "Checking prerequisites..."
    local missing=()
    for cmd in kubectl helm git curl jq yq; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        die "Missing required tools: ${missing[*]}."
    fi

    if ! kubectl cluster-info --request-timeout=10s &>/dev/null; then
        die "Cannot connect to Kubernetes cluster."
    fi
    log "Connected to cluster: $(kubectl config current-context)"
}

# ── Install ──────────────────────────────────────────────────────────────────

cmd_install() {
    banner "llm-d + Batch Gateway Setup"

    case "${BATCH_RELEASE_VERSION}" in
        v0.1.0|v0.2.0)
            if [ "${ENABLE_DISPATCHER}" = "true" ]; then
                die "${BATCH_RELEASE_VERSION} does not support async dispatch. Set ENABLE_DISPATCHER=false or use a newer chart."
            fi
            ;;
    esac

    check_prerequisites

    local namespaces=("${BATCH_NAMESPACE}" "${LLM_NAMESPACE}" "${GATEWAY_NAMESPACE}")
    for ns in "${namespaces[@]}"; do
        if ! kubectl get namespace "${ns}" &>/dev/null; then
            kubectl create namespace "${ns}"
            log "Created namespace '${ns}'."
        fi
    done

    for ns in "${BATCH_NAMESPACE}" "${LLM_NAMESPACE}"; do
        kubectl label namespace "${ns}" llm-d.ai/gateway-route=true --overwrite
    done

    # cert manager
    install_cert_manager
    create_selfsigned_issuer

    # llm-d deps
    install_llmd_deps
    install_istio

    # kuadrant
    install_kuadrant

    # llm-d
    create_inference_external_gateway
    deploy_llmd_model
    if [ "${ENABLE_FLOW_CONTROL}" = "true" ]; then
        create_inference_objectives
    fi
    create_llm_route
    apply_llm_auth_policy
    apply_llm_token_rate_limit

    # batch gateway
    create_batch_internal_gateway
    create_batch_llm_route
    apply_batch_llm_auth_policy
    deploy_batch_gateway_k8s
    if [ "${ENABLE_DISPATCHER}" = "true" ]; then
        deploy_dispatcher
    fi
    apply_batch_auth_policy
    apply_batch_request_rate_limit

    # verify
    if [ "${ENABLE_DISPATCHER}" = "true" ]; then
        verify_dispatcher_config
    fi
    if [ "${ENABLE_FLOW_CONTROL}" = "true" ]; then
        verify_flow_control_config
    fi

    log "Setup complete!"
    log "  Batch Gateway: ${BATCH_INSTANCE_NAME} (${BATCH_NAMESPACE})"
    if [ -n "${BATCH_IMAGE_TAG}" ]; then
        log "  Batch Gateway image tag: ${BATCH_IMAGE_TAG}"
    fi
    if [ -n "${BATCH_RELEASE_VERSION}" ]; then
        log "  Batch Gateway version: ${BATCH_RELEASE_VERSION} (OCI chart)"
    elif [ "${BATCH_DEV_VERSION}" != "local" ]; then
        log "  Batch Gateway version: ${BATCH_DEV_VERSION} (commit chart)"
    else
        log "  Batch Gateway version: latest (local chart)"
    fi
    if [ "${ENABLE_DISPATCHER}" = "true" ]; then
        log "  Async dispatcher: ${DISPATCHER_RELEASE} (${DISPATCHER_IMAGE})"
    fi
    log "Run '$0 test' to verify."
}

# ── Test ─────────────────────────────────────────────────────────────────────

cmd_test() {
    init_test "Batch Gateway (llm-d)"

    # Auth setup: create SA + RBAC + token
    local sa_name="test-authorized-sa"
    log "Creating ServiceAccount '${sa_name}' for testing..."
    kubectl create serviceaccount "${sa_name}" -n "${LLM_NAMESPACE}" 2>/dev/null || true

    # Grant permission to access the model via SubjectAccessReview
    kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: model-access
  namespace: ${LLM_NAMESPACE}
rules:
- apiGroups: ["inference.networking.k8s.io"]
  resources: ["inferencepools"]
  resourceNames: ["${MODEL_NAME}"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: model-access-binding
  namespace: ${LLM_NAMESPACE}
subjects:
- kind: ServiceAccount
  name: ${sa_name}
  namespace: ${LLM_NAMESPACE}
roleRef:
  kind: Role
  name: model-access
  apiGroup: rbac.authorization.k8s.io
EOF
    local token
    token=$(kubectl create token "${sa_name}" -n "${LLM_NAMESPACE}" \
        --audience=https://kubernetes.default.svc --duration=10m) \
        || die "Failed to create token for SA '${sa_name}'"
    [[ "${token}" == ey* ]] || die "Token for SA '${sa_name}' doesn't look like a valid JWT"

    # Create unauthorized SA (no RBAC bindings)
    local unauth_sa="test-unauthorized-sa"
    kubectl create serviceaccount "${unauth_sa}" -n "${LLM_NAMESPACE}" 2>/dev/null || true
    local unauth_token
    unauth_token=$(kubectl create token "${unauth_sa}" -n "${LLM_NAMESPACE}" \
        --audience=https://kubernetes.default.svc --duration=10m) \
        || die "Failed to create token for SA '${unauth_sa}'"
    [[ "${unauth_token}" == ey* ]] || die "Token for SA '${unauth_sa}' doesn't look like a valid JWT"

    local llm_url="${GATEWAY_URL}/${LLM_NAMESPACE}/${MODEL_NAME}/v1/chat/completions"
    local inference_payload="{\"model\":\"${MODEL_NAME}\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}],\"max_tokens\":10}"

    local test_failures=0
    run_tests "${llm_url}" "${GATEWAY_URL}" "${MODEL_NAME}" \
        "Authorization: Bearer ${token}" \
        "Authorization: Bearer ${unauth_token}" \
        "${inference_payload}" \
        || test_failures=$?

    if [ "${ENABLE_DISPATCHER}" = "true" ]; then
        verify_dispatcher_runtime "${DISPATCHER_RELEASE}-async-processor" "${LLMD_POOL_NAME}"
    fi

    if [ "${ENABLE_FLOW_CONTROL}" = "true" ]; then
        verify_flow_control_runtime
    fi

    return "${test_failures}"
}

# ── Uninstall ────────────────────────────────────────────────────────────────

cmd_uninstall() {
    set +e
    clone_llmd_repo

    banner "Uninstalling All Components"

    step "Stopping port-forward processes..."
    pkill -f "kubectl port-forward.*${GATEWAY_NAME}" 2>/dev/null || true

    step "Removing test resources..."
    kubectl delete role model-access -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete rolebinding model-access-binding -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete serviceaccount test-authorized-sa -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete serviceaccount test-unauthorized-sa -n "${LLM_NAMESPACE}" 2>/dev/null || true

    step "Removing policies..."
    kubectl delete ratelimitpolicy batch-ratelimit -n "${BATCH_NAMESPACE}" 2>/dev/null || true
    kubectl delete authpolicy batch-route-auth -n "${BATCH_NAMESPACE}" 2>/dev/null || true
    kubectl delete authpolicy llm-route-auth -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete tokenratelimitpolicy inference-token-limit -n "${LLM_NAMESPACE}" 2>/dev/null || true

    step "Removing dispatcher resources..."
    helm uninstall "${DISPATCHER_RELEASE}" -n "${BATCH_NAMESPACE}" --timeout 60s 2>/dev/null || true
    kubectl delete deployment,svc,sa prometheus -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete configmap prometheus-config -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete clusterrole,clusterrolebinding prometheus 2>/dev/null || true

    step "Removing Internal Gateway resources..."
    kubectl delete authpolicy batch-llm-route-auth -n "${LLM_NAMESPACE}" 2>/dev/null || true
    kubectl delete httproute batch-llm-route -n "${LLM_NAMESPACE}" 2>/dev/null || true

    step "Removing batch resources (${BATCH_NAMESPACE})..."
    timeout_delete 30s httproute --all -n "${BATCH_NAMESPACE}" || true
    helm uninstall "${BATCH_INSTANCE_NAME}" -n "${BATCH_NAMESPACE}" --timeout 60s 2>/dev/null || true
    helm uninstall "${BATCH_REDIS_RELEASE}" -n "${BATCH_NAMESPACE}" --timeout 60s 2>/dev/null || true
    helm uninstall "${BATCH_POSTGRESQL_RELEASE}" -n "${BATCH_NAMESPACE}" --timeout 60s 2>/dev/null || true
    kubectl delete deployment,svc -l app="${BATCH_MINIO_RELEASE}" -n "${BATCH_NAMESPACE}" 2>/dev/null || true
    kubectl delete pvc "${BATCH_FILES_PVC_NAME}" -n "${BATCH_NAMESPACE}" 2>/dev/null || true

    step "Removing Gateways (${GATEWAY_NAMESPACE})..."
    timeout_delete 30s gateway "${BATCH_INTERNAL_GATEWAY_NAME}" -n "${BATCH_INTERNAL_GATEWAY_NAMESPACE}" || true
    timeout_delete 30s gateway "${GATEWAY_NAME}" -n "${GATEWAY_NAMESPACE}" || true
    kubectl delete destinationrule "${BATCH_INSTANCE_NAME}-backend-tls" -n "${GATEWAY_NAMESPACE}" 2>/dev/null || true

    if is_demo_uninstall_all; then
        step "Removing Kuadrant..."
        timeout_delete 30s kuadrant kuadrant -n "${KUADRANT_NAMESPACE}" || true
        helm uninstall "${KUADRANT_RELEASE}" -n "${KUADRANT_NAMESPACE}" --timeout 60s 2>/dev/null || true
        force_delete_crds 'kuadrant|authorino|limitador'
        force_delete_namespace "${KUADRANT_NAMESPACE}"

        step "Removing InferenceObjective resources..."
        kubectl delete inferenceobjective --all -n "${LLM_NAMESPACE}" 2>/dev/null || true

        step "Removing llm-d stack (${LLM_NAMESPACE})..."
        uninstall_llmd

        step "Uninstalling Istio..."
        helm uninstall istiod -n istio-system --timeout 60s 2>/dev/null || true
        helm uninstall istio-base -n istio-system --timeout 60s 2>/dev/null || true
        force_delete_crds 'istio\.io|sail'
        force_delete_namespace "istio-system"

        step "Removing CRDs..."
        clone_llmd_repo
        kubectl delete -f "https://github.com/llm-d/llm-d-router/releases/download/${ROUTER_CHART_VERSION}/manifests.yaml" 2>/dev/null || true
        bash "${LLMD_GIT_DIR}/guides/recipes/gateway/install-gateway-crds.sh" delete 2>/dev/null || true

        step "Cleaning up cache..."
        rm -rf "${LLMD_GIT_DIR}"

        step "Removing TLS resources..."
        kubectl delete clusterissuer "${TLS_ISSUER_NAME}" 2>/dev/null || true

        step "Uninstalling cert-manager..."
        helm uninstall cert-manager -n cert-manager --timeout 60s 2>/dev/null || true
        force_delete_crds 'cert-manager'
        force_delete_namespace "cert-manager"

        for ns in "${BATCH_NAMESPACE}" "${LLM_NAMESPACE}" "${GATEWAY_NAMESPACE}"; do
            [ "${ns}" != "default" ] && force_delete_namespace "${ns}"
        done
    else
        warn "Skipping Kuadrant, llm-d, Istio, cert-manager, CRD teardown, and deletes for '${LLM_NAMESPACE}' / '${GATEWAY_NAMESPACE}' (shared-cluster safety)."
        warn "For full teardown on an ephemeral demo cluster only: UNINSTALL_ALL=1 $0 uninstall"
        step "Removing batch namespace (${BATCH_NAMESPACE})..."
        force_delete_namespace "${BATCH_NAMESPACE}"
    fi

    echo ""
    log "Uninstallation complete!"

    set -e
}

# ── Usage ────────────────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 {install|test|uninstall|help}"
    echo ""
    echo "Deploy batch-gateway with llm-d on vanilla Kubernetes."
    echo ""
    echo "Commands:"
    echo "  install    Deploy llm-d stack + Kuadrant + batch-gateway"
    echo "  test       Run auth, batch lifecycle, and rate limit tests"
    echo "  uninstall  Remove demo resources (use UNINSTALL_ALL=1 for full stack teardown)"
    echo "  help       Show this help"
    echo ""
    echo "Environment Variables:"
    echo "  MODEL_NAME             Model name for routing (default: random)"
    echo "  LLMD_VERSION           llm-d branch or tag"
    echo "  LLMD_RELEASE_POSTFIX   Release name postfix (default: llmd)"
    echo "  BATCH_INSTANCE_NAME     Instance name (default: batch-gateway)"
    echo "  GATEWAY_LOCAL_PORT     Port-forward fallback port (default: 8080)"
    echo "  BATCH_DEV_VERSION      Batch gateway image tag / commit SHA (default: local)"
    echo "  BATCH_RELEASE_VERSION  Install released OCI chart (e.g. v1.0.0)"
    echo "  ENABLE_FLOW_CONTROL   Enable GIE flow control (default: true)"
    echo "  BATCH_FLOW_CONTROL_OBJECTIVE InferenceObjective name for batch (default: batch-sheddable)"
    echo "  ENABLE_DISPATCHER      Deploy llm-d-async dispatcher (default: true; false selects HTTP sync)"
    echo "  DISPATCHER_VERSION     llm-d-async version (default: v0.7.3)"
    echo "  UNINSTALL_ALL          Set to 1 to also remove Kuadrant/Istio/cert-manager and CRDs (ephemeral clusters only)"
    echo ""
    echo "Examples:"
    echo "  $0 install"
    echo "  MODEL_NAME=my-model LLMD_VERSION=v0.8.1 $0 install"
    echo "  ENABLE_DISPATCHER=false $0 install"
    exit "${1:-0}"
}

# ── Main ─────────────────────────────────────────────────────────────────────

if [ $# -eq 0 ]; then usage 0; fi

case "$1" in
    install)   shift; cmd_install "$@" ;;
    test)      shift; cmd_test "$@" ;;
    uninstall) shift; cmd_uninstall "$@" ;;
    help|-h|--help) usage 0 ;;
    *) echo "Error: Unknown command '$1'"; echo ""; usage 1 ;;
esac
