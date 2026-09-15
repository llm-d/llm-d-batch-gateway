#!/usr/bin/env bash
# Common functions

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
step() { echo -e "${BLUE}[STEP]${NC}  $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
banner() {
    local title="$1"
    local len=${#title}
    local border; border=$(printf '═%.0s' $(seq 1 $((len + 4))))
    echo ""
    echo "  ╔${border}╗"
    printf "  ║  %s  ║\n" "${title}"
    echo "  ╚${border}╝"
    echo ""
}

TLS_ISSUER_NAME="${TLS_ISSUER_NAME:-selfsigned-issuer}"

# Demo uninstall safety: default uninstall removes batch-gateway-scoped
# resources and the named Gateway only. Set UNINSTALL_ALL=1 to also tear down Kuadrant,
# Istio, cert-manager, operators, cluster CRDs, and other shared platform pieces.
UNINSTALL_ALL="${UNINSTALL_ALL:-0}"
is_demo_uninstall_all() {
    case "${UNINSTALL_ALL}" in
        1|true|yes|TRUE|YES) return 0 ;;
        *) return 1 ;;
    esac
}

# Batch Gateway configuration
BATCH_NAMESPACE="${BATCH_NAMESPACE:-batch-api}"
BATCH_INSTANCE_NAME="${BATCH_INSTANCE_NAME:-batch-gateway}"
BATCH_RELEASE_VERSION="${BATCH_RELEASE_VERSION:-}"
BATCH_DEV_VERSION="${BATCH_DEV_VERSION:-}"
if [ -n "${BATCH_RELEASE_VERSION}" ]; then
    if [[ "${BATCH_RELEASE_VERSION}" != v* ]]; then
        echo "[ERROR] BATCH_RELEASE_VERSION must start with 'v' (e.g. 'v1.0.0')" >&2
        exit 1
    fi
    if [ -n "${BATCH_DEV_VERSION}" ]; then
        echo "[ERROR] BATCH_RELEASE_VERSION and BATCH_DEV_VERSION cannot both be set" >&2
        exit 1
    fi
else
    BATCH_DEV_VERSION="${BATCH_DEV_VERSION:-local}"
    # Truncate full commit SHA (40 hex chars) to 7-char short SHA to match CI image tags
    if [[ "${BATCH_DEV_VERSION}" =~ ^[0-9a-f]{40}$ ]]; then
        BATCH_DEV_VERSION="${BATCH_DEV_VERSION:0:7}"
    fi
fi
BATCH_INFERENCE_SERVICE="${BATCH_INFERENCE_SERVICE:-${BATCH_INSTANCE_NAME}-apiserver}"
BATCH_INFERENCE_PORT="${BATCH_INFERENCE_PORT:-8000}"
BATCH_APP_SECRET_NAME="${BATCH_APP_SECRET_NAME:-${BATCH_INSTANCE_NAME}-secrets}"
BATCH_FILES_PVC_NAME="${BATCH_FILES_PVC_NAME:-${BATCH_INSTANCE_NAME}-files}"
BATCH_DB_TYPE="${BATCH_DB_TYPE:-postgresql}"
# Default HTTP settings for model gateway entries.
# Each per-model entry must be fully specified (no inheritance).
GW_REQUEST_TIMEOUT="${GW_REQUEST_TIMEOUT:-5m}"
GW_MAX_RETRIES="${GW_MAX_RETRIES:-3}"
GW_INITIAL_BACKOFF="${GW_INITIAL_BACKOFF:-1s}"
GW_MAX_BACKOFF="${GW_MAX_BACKOFF:-60s}"
BATCH_REDIS_RELEASE="${BATCH_REDIS_RELEASE:-redis}"
BATCH_EXCHANGE_CLIENT_TYPE="${BATCH_EXCHANGE_CLIENT_TYPE:-redis}"
BATCH_POSTGRESQL_RELEASE="${BATCH_POSTGRESQL_RELEASE:-postgresql}"
# WARNING: Default passwords are for demo only. For production, override via env vars or use K8s secrets.
BATCH_POSTGRESQL_PASSWORD="${BATCH_POSTGRESQL_PASSWORD:-postgres}"
BATCH_STORAGE_TYPE="${BATCH_STORAGE_TYPE:-s3}"
# When 1, disables TLS certificate verification for (a) processor -> model gateway HTTPS and
# (b) Istio Gateway -> batch apiserver (DestinationRule). Demo/lab only (CWE-295).
# Default 1 for demo scripts using self-signed certs. Set to 0 if you have trusted CA certs.
DEMO_TLS_INSECURE_SKIP_VERIFY="${DEMO_TLS_INSECURE_SKIP_VERIFY:-1}"
BATCH_MINIO_RELEASE="${BATCH_MINIO_RELEASE:-minio}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-llm-d-batch-gateway}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
# Image overrides. When set, these take precedence over defaults derived from
# BATCH_RELEASE_VERSION / BATCH_DEV_VERSION. Leave unset to use chart defaults.
# Example (upstream):
#   BATCH_IMAGE_TAG=v0.1.0 BATCH_APISERVER_REPO=ghcr.io/llm-d/batch-gateway-apiserver \
#   BATCH_PROCESSOR_REPO=ghcr.io/llm-d/batch-gateway-processor \
#   BATCH_GC_REPO=ghcr.io/llm-d/batch-gateway-gc ./deploy-k8s.sh
BATCH_IMAGE_TAG="${BATCH_IMAGE_TAG:-}"
BATCH_APISERVER_REPO="${BATCH_APISERVER_REPO:-}"
BATCH_PROCESSOR_REPO="${BATCH_PROCESSOR_REPO:-}"
BATCH_GC_REPO="${BATCH_GC_REPO:-}"

ENABLE_DISPATCHER="${ENABLE_DISPATCHER:-true}"

# Temp directory cleanup (used by do_deploy_batch_gateway_helm)
_BATCH_TMP_DIR=""
_cleanup() {
    if [ -n "${_BATCH_TMP_DIR}" ]; then
        rm -rf "${_BATCH_TMP_DIR}"
        _BATCH_TMP_DIR=""
    fi
}
trap _cleanup EXIT

# ── Helper Functions ──────────────────────────────────────────────────────────

is_openshift() {
    kubectl api-resources --api-group=route.openshift.io &>/dev/null 2>&1
}

wait_for_deployment() {
    local deploy_name="$1"
    local namespace="$2"
    local timeout="${3:-180s}"

    # Auto-detect whether the resource is a Deployment or StatefulSet.
    local kind="deploy"
    local retries=0
    local max_retries=30
    while true; do
        if kubectl get deploy "${deploy_name}" -n "${namespace}" &>/dev/null; then
            kind="deploy"
            break
        elif kubectl get statefulset "${deploy_name}" -n "${namespace}" &>/dev/null; then
            kind="statefulset"
            break
        fi
        retries=$((retries + 1))
        if [ "$retries" -ge "$max_retries" ]; then
            die "'${deploy_name}' did not become visible after $((max_retries * 2))s"
        fi
        warn "Resource not yet visible, retrying in 2s... ($retries/$max_retries)"
        sleep 2
    done

    step "Waiting for ${kind}/${deploy_name} to be ready..."
    kubectl rollout status "${kind}/${deploy_name}" -n "${namespace}" --timeout="${timeout}"
    log "${kind}/${deploy_name} is ready."
}

wait_for_subscription() {
    local namespace="$1"
    local sub_name="$2"

    step "Waiting for subscription '${sub_name}' to be ready..."

    local retries=0
    while true; do
        local csv
        csv=$(kubectl get subscription.operators.coreos.com "${sub_name}" -n "${namespace}" \
            -o jsonpath='{.status.currentCSV}' 2>/dev/null || echo "")
        if [ -n "${csv}" ]; then
            local phase
            phase=$(kubectl get csv "${csv}" -n "${namespace}" \
                -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
            if [ "${phase}" = "Succeeded" ]; then
                log "Subscription '${sub_name}' is ready (CSV: ${csv})."
                return
            fi
        fi
        retries=$((retries + 1))
        if [ "$retries" -ge 90 ]; then
            die "Subscription '${sub_name}' did not become ready after 450s"
        fi
        sleep 5
    done
}

wait_for_crd() {
    local crd="$1"
    local timeout="${2:-180}"

    step "Waiting for CRD '${crd}'..."
    local elapsed=0
    while ! kubectl get crd "${crd}" &>/dev/null; do
        if [ "$elapsed" -ge "$timeout" ]; then
            die "CRD ${crd} not available after ${timeout}s"
        fi
        elapsed=$((elapsed + 5))
        sleep 5
    done
    log "CRD '${crd}' is available."
}

timeout_delete() {
    local timeout="$1"
    shift

    if kubectl delete --timeout="${timeout}" "$@" 2>/dev/null; then
        return 0
    fi

    warn "Delete timed out, removing finalizers..."
    local resource_list
    resource_list=$(kubectl get "$@" -o jsonpath='{range .items[*]}{.kind}/{.metadata.name}{" "}{end}' 2>/dev/null) \
        || resource_list=$(kubectl get "$@" -o jsonpath='{.kind}/{.metadata.name}' 2>/dev/null) || true
    for res in $resource_list; do
        kubectl patch "$res" "${@: -2}" --type=merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
    done

    warn "Force deleting..."
    kubectl delete --wait=false --force --grace-period=0 "$@" 2>/dev/null || true
}

force_delete_crds() {
    local pattern="$1"
    local crds
    crds=$(kubectl get crds 2>/dev/null | grep -E "$pattern" | awk '{print $1}')
    if [ -z "$crds" ]; then
        log "No CRDs matching '$pattern' found."
        return 0
    fi
    for crd in $crds; do
        # Skip CRDs that still have instances (someone else might be using them)
        local count
        count=$(kubectl get "$crd" -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
        if [ "$count" -gt 0 ]; then
            warn "CRD $crd still has $count instance(s), skipping deletion."
            continue
        fi
        timeout_delete 15s crd "$crd" || warn "Could not delete CRD: $crd"
    done
}

force_delete_namespace() {
    local ns="$1"
    if ! kubectl get namespace "$ns" &>/dev/null; then
        return 0
    fi
    step "Deleting namespace '$ns'..."
    if kubectl delete namespace "$ns" --timeout=60s 2>/dev/null; then
        return 0
    fi
    warn "Namespace '$ns' stuck in Terminating. Removing finalizers..."
    kubectl get namespace "$ns" -o json \
        | jq '.spec.finalizers = []' \
        | kubectl replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null \
        || warn "Could not remove finalizers for '$ns'"
}

# ── TLS ──────────────────────────────────────────────────────────────────────

create_selfsigned_issuer() {
    step "Creating self-signed ClusterIssuer '${TLS_ISSUER_NAME}'..."
    if kubectl get clusterissuer "${TLS_ISSUER_NAME}" &>/dev/null; then
        log "ClusterIssuer '${TLS_ISSUER_NAME}' already exists. Skipping."
        return
    fi
    # cert-manager webhook may not be fully ready (TLS bootstrap race condition).
    # Retry a few times to allow the webhook certificate to propagate.
    local attempt
    for attempt in 1 2 3 4 5; do
        if kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ${TLS_ISSUER_NAME}
spec:
  selfSigned: {}
EOF
        then
            log "ClusterIssuer '${TLS_ISSUER_NAME}' created."
            return
        fi
        warn "Attempt ${attempt}/5 failed. Waiting 10s for webhook TLS to be ready..."
        sleep 10
    done
    die "Failed to create ClusterIssuer after 5 attempts."
}

# ── Gateway URL ──────────────────────────────────────────────────────────────
#
# Resolves the external gateway URL. Priority:
#
#   1. spec.listeners[0].hostname — OpenShift router uses SNI (Server Name
#      Indication) to route requests to the correct backend. The hostname
#      (e.g. llm-inference.apps.xxx) must be present in the TLS ClientHello
#      for the router to match it. Accessing via raw LB address fails because
#      the router cannot determine which route to use without the hostname.
#
#   2. status.addresses[0].value — On vanilla K8s with Istio, the gateway
#      terminates TLS directly and routes by path, not by hostname. Listeners
#      typically have no hostname configured, so the LB address works fine.
#
#   3. port-forward — Fallback for clusters without a LoadBalancer (e.g.
#      kind, minikube).

set_gateway_url() {
    # 1. Prefer spec hostname (required for SNI-based routing, e.g. OpenShift)
    local gw_hostname
    gw_hostname=$(kubectl get gateway "${GATEWAY_NAME}" -n "${GATEWAY_NAMESPACE}" \
        -o jsonpath='{.spec.listeners[0].hostname}' 2>/dev/null || echo "")
    if [[ -n "${gw_hostname}" ]]; then
        log "Gateway hostname: ${gw_hostname}"
        export GATEWAY_URL="https://${gw_hostname}"
        log "Gateway URL: ${GATEWAY_URL}"
        return
    fi

    # 2. Fall back to status address (e.g. LoadBalancer IP/hostname on vanilla K8s)
    local gw_addr
    gw_addr=$(kubectl get gateway "${GATEWAY_NAME}" -n "${GATEWAY_NAMESPACE}" \
        -o jsonpath='{.status.addresses[0].value}' 2>/dev/null || echo "")
    if [[ -n "${gw_addr}" ]]; then
        log "Gateway address: ${gw_addr}"
        export GATEWAY_URL="https://${gw_addr}"
        log "Gateway URL: ${GATEWAY_URL}"
        return
    fi

    # 3. Fall back to port-forward (no external address, e.g. kind/minikube)
    log "No external address found, falling back to port-forward."
    local gateway_svc
    gateway_svc=$(kubectl get svc -n "${GATEWAY_NAMESPACE}" \
        -l "gateway.networking.k8s.io/gateway-name=${GATEWAY_NAME}" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    [ -z "${gateway_svc}" ] && die "No service found for gateway '${GATEWAY_NAME}'."
    step "Starting port-forward: ${gateway_svc} ${GATEWAY_LOCAL_PORT}:443 -n ${GATEWAY_NAMESPACE}..."
    kubectl port-forward "svc/${gateway_svc}" "${GATEWAY_LOCAL_PORT}:443" -n "${GATEWAY_NAMESPACE}" &
    disown $!
    log "Port-forward PID: $!"
    export GATEWAY_URL="https://localhost:${GATEWAY_LOCAL_PORT}"
    log "Gateway URL: ${GATEWAY_URL}"
}

# ── Shared Route & DestinationRule Functions ──────────────────────────────────

create_batch_httproute() {
    step "Creating HTTPRoutes..."

    kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: batch-route
  namespace: ${BATCH_NAMESPACE}
spec:
  parentRefs:
  - name: ${GATEWAY_NAME}
    namespace: ${GATEWAY_NAMESPACE}
  rules:
  - matches:
    - path:
        type: PathPrefix
        value: /v1/batches
    - path:
        type: PathPrefix
        value: /v1/files
    backendRefs:
    - name: ${BATCH_INFERENCE_SERVICE}
      port: ${BATCH_INFERENCE_PORT}
EOF

    log "batch-route created (${BATCH_NAMESPACE}): /v1/batches, /v1/files -> ${BATCH_INFERENCE_SERVICE}"
}

# Tells Istio to use TLS when connecting to the apiserver backend.
# Without this, the Gateway would send plaintext to the HTTPS apiserver port.
create_batch_destinationrule() {
    step "Creating DestinationRule for backend TLS (Gateway -> apiserver)..."
    local skip="${DEMO_TLS_INSECURE_SKIP_VERIFY}"
    local istio_skip="false"
    if [[ "${skip}" == "1" ]]; then
        istio_skip="true"
        warn "DEMO_TLS_INSECURE_SKIP_VERIFY=1: Istio insecureSkipVerify enabled (demo/lab only; do not use in production)."
    fi
    kubectl apply -f - <<EOF
apiVersion: networking.istio.io/v1
kind: DestinationRule
metadata:
  name: ${BATCH_INSTANCE_NAME}-backend-tls
  namespace: ${GATEWAY_NAMESPACE}
spec:
  host: ${BATCH_INSTANCE_NAME}-apiserver.${BATCH_NAMESPACE}.svc.cluster.local
  trafficPolicy:
    portLevelSettings:
    - port:
        number: ${BATCH_INFERENCE_PORT}
      tls:
        mode: SIMPLE
        insecureSkipVerify: ${istio_skip}
EOF
    log "DestinationRule created (Gateway -> apiserver: TLS re-encrypt, insecureSkipVerify=${istio_skip})."
}

# ── Database / Storage Functions ──────────────────────────────────────────────

install_batch_exchange() {
    local chart="oci://registry-1.docker.io/bitnamicharts/${BATCH_EXCHANGE_CLIENT_TYPE}"
    step "Installing exchange backend (${chart})..."
    if helm status "${BATCH_REDIS_RELEASE}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
        local installed_chart
        installed_chart=$(helm get metadata "${BATCH_REDIS_RELEASE}" -n "${BATCH_NAMESPACE}" -o json 2>/dev/null | jq -r '.chart')
        if [[ "${installed_chart}" == "${BATCH_EXCHANGE_CLIENT_TYPE}-"* ]]; then
            log "Exchange backend (${chart}) release '${BATCH_REDIS_RELEASE}' is already installed. Skipping."
            return
        fi
        warn "Installed exchange chart '${installed_chart}' does not match requested '${BATCH_EXCHANGE_CLIENT_TYPE}'. Reinstalling..."
        helm uninstall "${BATCH_REDIS_RELEASE}" -n "${BATCH_NAMESPACE}" --wait
    fi
    helm install "${BATCH_REDIS_RELEASE}" "${chart}" \
        --namespace "${BATCH_NAMESPACE}" --create-namespace \
        --set architecture=standalone \
        --set auth.enabled=false

    local sts_name="${BATCH_REDIS_RELEASE}-master"
    if [[ "${BATCH_EXCHANGE_CLIENT_TYPE}" == "valkey" ]]; then
        sts_name="${BATCH_REDIS_RELEASE}-valkey-primary"
    fi
    kubectl rollout status statefulset/"${sts_name}" -n "${BATCH_NAMESPACE}" --timeout=180s
    log "Exchange backend (${chart}) installed (standalone, no auth)."
}

install_batch_postgresql() {
    step "Installing PostgreSQL..."
    if helm status "${BATCH_POSTGRESQL_RELEASE}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
        log "PostgreSQL release '${BATCH_POSTGRESQL_RELEASE}' is already installed. Skipping."
        return
    fi
    helm install "${BATCH_POSTGRESQL_RELEASE}" oci://registry-1.docker.io/bitnamicharts/postgresql \
        --namespace "${BATCH_NAMESPACE}" --create-namespace \
        --set auth.postgresPassword="${BATCH_POSTGRESQL_PASSWORD}" \
        --set auth.database=batch

    step "Waiting for PostgreSQL to be ready..."
    kubectl rollout status statefulset/"${BATCH_POSTGRESQL_RELEASE}" -n "${BATCH_NAMESPACE}" --timeout=180s
    log "PostgreSQL installed (database: batch)."
}

install_batch_minio() {
    step "Installing MinIO..."
    if kubectl get deployment "${BATCH_MINIO_RELEASE}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
        log "MinIO deployment '${BATCH_MINIO_RELEASE}' already exists. Skipping."
        return
    fi

    kubectl create namespace "${BATCH_NAMESPACE}" 2>/dev/null || true

    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${BATCH_MINIO_RELEASE}
  namespace: ${BATCH_NAMESPACE}
  labels:
    app: ${BATCH_MINIO_RELEASE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${BATCH_MINIO_RELEASE}
  template:
    metadata:
      labels:
        app: ${BATCH_MINIO_RELEASE}
    spec:
      containers:
      - name: minio
        image: quay.io/minio/minio:RELEASE.2024-12-18T13-15-44Z
        args:
        - server
        - /data
        - --console-address
        - ":9001"
        env:
        - name: MINIO_ROOT_USER
          value: "${MINIO_ROOT_USER}"
        - name: MINIO_ROOT_PASSWORD
          value: "${MINIO_ROOT_PASSWORD}"
        ports:
        - containerPort: 9000
          name: api
        - containerPort: 9001
          name: console
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: ${BATCH_MINIO_RELEASE}
  namespace: ${BATCH_NAMESPACE}
  labels:
    app: ${BATCH_MINIO_RELEASE}
spec:
  selector:
    app: ${BATCH_MINIO_RELEASE}
  ports:
  - name: api
    port: 9000
    targetPort: 9000
  - name: console
    port: 9001
    targetPort: 9001
  type: ClusterIP
EOF

    wait_for_deployment "${BATCH_MINIO_RELEASE}" "${BATCH_NAMESPACE}" 180s

    step "Creating MinIO bucket '${MINIO_BUCKET}'..."
    local minio_pod
    minio_pod=$(kubectl get pod -n "${BATCH_NAMESPACE}" -l app="${BATCH_MINIO_RELEASE}" -o jsonpath='{.items[0].metadata.name}')
    [[ -z "${minio_pod}" ]] && die "No MinIO pod found with label app=${BATCH_MINIO_RELEASE} in namespace ${BATCH_NAMESPACE}"
    kubectl exec -n "${BATCH_NAMESPACE}" "${minio_pod}" -- \
        mc alias set local http://localhost:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null \
        || die "Failed to configure MinIO client. Check credentials and MinIO readiness."
    kubectl exec -n "${BATCH_NAMESPACE}" "${minio_pod}" -- \
        mc mb "local/${MINIO_BUCKET}" 2>/dev/null || true
    log "MinIO installed (bucket: ${MINIO_BUCKET})."
}

create_batch_pvc() {
    if kubectl get pvc "${BATCH_FILES_PVC_NAME}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
        log "PVC '${BATCH_FILES_PVC_NAME}' already exists. Skipping."
        return
    fi
    step "Creating PVC '${BATCH_FILES_PVC_NAME}'..."
    kubectl apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${BATCH_FILES_PVC_NAME}
  namespace: ${BATCH_NAMESPACE}
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
EOF
    log "PVC '${BATCH_FILES_PVC_NAME}' created."
}

create_batch_secret() {
    step "Creating app secret '${BATCH_APP_SECRET_NAME}'..."

    local exchange_svc="${BATCH_REDIS_RELEASE}-master"
    if [[ "${BATCH_EXCHANGE_CLIENT_TYPE}" == "valkey" ]]; then
        exchange_svc="${BATCH_REDIS_RELEASE}-valkey-primary"
    fi
    local redis_url="redis://${exchange_svc}.${BATCH_NAMESPACE}.svc.cluster.local:6379/0"
    local postgresql_url="postgresql://postgres:${BATCH_POSTGRESQL_PASSWORD}@${BATCH_POSTGRESQL_RELEASE}.${BATCH_NAMESPACE}.svc.cluster.local:5432/batch?sslmode=disable"

    kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${BATCH_APP_SECRET_NAME}
  namespace: ${BATCH_NAMESPACE}
stringData:
  redis-url: "${redis_url}"
  postgresql-url: "${postgresql_url}"
  s3-secret-access-key: "${MINIO_ROOT_PASSWORD}"
EOF
    log "Secret '${BATCH_APP_SECRET_NAME}' applied."
}

get_batch_gateway_image_tag() {
    local image_tag=""
    if [ -n "${BATCH_IMAGE_TAG}" ]; then
        image_tag="${BATCH_IMAGE_TAG}"
    elif [ -n "${BATCH_RELEASE_VERSION}" ]; then
        image_tag="${BATCH_RELEASE_VERSION}"
    elif [ -n "${BATCH_DEV_VERSION}" ]; then
        if [ "${BATCH_DEV_VERSION}" != "local" ]; then
            # BATCH_DEV_VERSION is specific commit
            image_tag="${BATCH_DEV_VERSION}"
        fi
    fi
    echo "$image_tag"
}

# do_deploy_batch_gateway_helm [extra_helm_args...]
# Full batch-gateway deployment: databases, storage, helm chart and routing.
# Common helm args are built here; callers only pass script-specific overrides
# (e.g. modelGateways, passThroughHeaders).
do_deploy_batch_gateway_helm() {
    kubectl get namespace "${BATCH_NAMESPACE}" &>/dev/null || kubectl create namespace "${BATCH_NAMESPACE}"
    kubectl label namespace "${BATCH_NAMESPACE}" llm-d.ai/gateway-route=true --overwrite

    install_batch_exchange
    install_batch_postgresql
    if [ "${BATCH_STORAGE_TYPE}" = "s3" ]; then
        install_batch_minio
    else
        create_batch_pvc
    fi
    create_batch_secret

    local helm_args=(
        --namespace "${BATCH_NAMESPACE}"
        --set "global.secretName=${BATCH_APP_SECRET_NAME}"
    )

    # Image repository overrides (when set via env vars)
    [ -n "${BATCH_APISERVER_REPO}" ]  && helm_args+=(--set "apiserver.image.repository=${BATCH_APISERVER_REPO}")
    [ -n "${BATCH_PROCESSOR_REPO}" ]  && helm_args+=(--set "processor.image.repository=${BATCH_PROCESSOR_REPO}")
    [ -n "${BATCH_GC_REPO}" ]         && helm_args+=(--set "gc.image.repository=${BATCH_GC_REPO}")

    # Image tag: explicit BATCH_IMAGE_TAG takes precedence; otherwise derive from
    # BATCH_DEV_VERSION for non-release installs (release charts have tags baked in).
    local image_tag=$(get_batch_gateway_image_tag)
    if [ -n "${image_tag}" ]; then
        helm_args+=(
            --set "apiserver.image.tag=${image_tag}"
            --set "processor.image.tag=${image_tag}"
            --set "gc.image.tag=${image_tag}"
        )
    fi

    helm_args+=(
        --set "global.dbClient.type=${BATCH_DB_TYPE}"
        --set "global.fileClient.type=${BATCH_STORAGE_TYPE}"
        --set "apiserver.tls.enabled=true"
        --set "apiserver.tls.certManager.enabled=true"
        --set "apiserver.tls.certManager.issuerName=${TLS_ISSUER_NAME}"
        --set "apiserver.tls.certManager.issuerKind=ClusterIssuer"
        --set "apiserver.tls.certManager.dnsNames={${BATCH_INSTANCE_NAME}-apiserver,${BATCH_INSTANCE_NAME}-apiserver.${BATCH_NAMESPACE}.svc.cluster.local,localhost}"
    )

    if [ "${BATCH_STORAGE_TYPE}" = "s3" ]; then
        local minio_endpoint="http://${BATCH_MINIO_RELEASE}.${BATCH_NAMESPACE}.svc.cluster.local:9000"
        helm_args+=(
            --set "global.fileClient.s3.endpoint=${minio_endpoint}"
            --set "global.fileClient.s3.region=${MINIO_REGION}"
            --set "global.fileClient.s3.bucket=${MINIO_BUCKET}"
            --set "global.fileClient.s3.accessKeyId=${MINIO_ROOT_USER}"
            --set "global.fileClient.s3.prefix=${MINIO_BUCKET}"
            --set "global.fileClient.s3.usePathStyle=true"
            --set "global.fileClient.s3.autoCreateBucket=true"
        )
    else
        helm_args+=(
            --set "global.fileClient.fs.basePath=/tmp/batch-gateway"
            --set "global.fileClient.fs.pvcName=${BATCH_FILES_PVC_NAME}"
        )
    fi

    # Append caller-specific args (modelGateways, passThroughHeaders, etc.)
    helm_args+=("$@")

    step "Installing batch-gateway via Helm..."

    local chart version_args=()
    if [ -n "${BATCH_RELEASE_VERSION}" ]; then
        case "${BATCH_RELEASE_VERSION}" in
            v0.1.0|v0.2.0) chart="oci://ghcr.io/llm-d-incubation/charts/batch-gateway" ;;
            *)              chart="oci://ghcr.io/llm-d/charts/batch-gateway" ;;
        esac
        version_args=(--version "${BATCH_RELEASE_VERSION#v}")
    elif [ "${BATCH_DEV_VERSION}" = "local" ]; then
        local repo_root
        repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
        chart="${repo_root}/charts/batch-gateway"
    else
        # Download chart from GitHub at the specific commit
        _BATCH_TMP_DIR=$(mktemp -d)
        local tarball_url="https://github.com/llm-d/llm-d-batch-gateway/archive/${BATCH_DEV_VERSION}.tar.gz"
        step "Downloading chart from commit ${BATCH_DEV_VERSION}..."
        local http_code
        http_code=$(curl -sL -o "${_BATCH_TMP_DIR}/archive.tar.gz" -w '%{http_code}' "${tarball_url}")
        if [ "${http_code}" != "200" ]; then
            die "Failed to download chart (HTTP ${http_code}). Is '${BATCH_DEV_VERSION}' a valid commit?"
        fi
        tar xz -C "${_BATCH_TMP_DIR}" --strip-components=1 -f "${_BATCH_TMP_DIR}/archive.tar.gz"
        chart="${_BATCH_TMP_DIR}/charts/batch-gateway"
        if [ ! -f "${chart}/Chart.yaml" ]; then
            die "Chart not found at commit ${BATCH_DEV_VERSION}"
        fi
    fi

    if helm status "${BATCH_INSTANCE_NAME}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
        log "Release '${BATCH_INSTANCE_NAME}' already exists. Upgrading..."
        helm upgrade "${BATCH_INSTANCE_NAME}" "${chart}" --reset-values "${version_args[@]+"${version_args[@]}"}" "${helm_args[@]}"
    else
        helm install "${BATCH_INSTANCE_NAME}" "${chart}" "${version_args[@]+"${version_args[@]}"}" "${helm_args[@]}"
    fi

    wait_for_deployment "${BATCH_INSTANCE_NAME}-apiserver" "${BATCH_NAMESPACE}" 180s
    wait_for_deployment "${BATCH_INSTANCE_NAME}-processor" "${BATCH_NAMESPACE}" 180s
    wait_for_deployment "${BATCH_INSTANCE_NAME}-gc" "${BATCH_NAMESPACE}" 180s

    # Print installed versions and verify image tags
    local expected_tag=$(get_batch_gateway_image_tag)
    step "Installed batch-gateway components:"
    local mismatch=false
    for component in apiserver processor gc; do
        local actual_image
        # processor is a StatefulSet; apiserver and gc are Deployments.
        local kind="deploy"
        if kubectl get statefulset "${BATCH_INSTANCE_NAME}-${component}" -n "${BATCH_NAMESPACE}" &>/dev/null; then
            kind="statefulset"
        fi
        actual_image=$(kubectl get "${kind}" "${BATCH_INSTANCE_NAME}-${component}" -n "${BATCH_NAMESPACE}" \
            -o jsonpath='{.spec.template.spec.containers[0].image}')
        local actual_tag="${actual_image##*:}"
        log "  ${component}: ${actual_image}"
        if [ -n "${expected_tag}" ] && [ "${actual_tag}" != "${expected_tag}" ]; then
            warn "  ${component} tag '${actual_tag}' does not match expected '${expected_tag}'"
            mismatch=true
        fi
    done
    if [ "${mismatch}" = "true" ]; then
        die "Image tags do not match expected version."
    fi

    # Verify OCI chart version for release installs
    if [ -n "${BATCH_RELEASE_VERSION}" ]; then
        local installed_ver
        installed_ver=$(helm get metadata "${BATCH_INSTANCE_NAME}" -n "${BATCH_NAMESPACE}" -o json 2>/dev/null \
            | jq -r '.version // empty')
        local expected_ver="${BATCH_RELEASE_VERSION#v}"
        if [ "${installed_ver}" = "${expected_ver}" ]; then
            log "Verified: OCI chart version ${installed_ver} matches release ${BATCH_RELEASE_VERSION}"
        else
            die "Chart version '${installed_ver}' does not match expected '${expected_ver}'"
        fi
    fi

    log "batch-gateway installed."

    create_batch_httproute
    create_batch_destinationrule
}

# ── Internal Gateway ──────────────────────────────────────────────────────────
#
# Creates a ClusterIP-only Gateway for the batch processor to access LLM
# inference endpoints. Preserves AuthPolicy (model access checks) but
# bypasses TokenRateLimitPolicy applied on the external Gateway.
# Requires BATCH_INTERNAL_GATEWAY_NAME, BATCH_INTERNAL_GATEWAY_NAMESPACE, and GATEWAY_CLASS_NAME.

create_batch_internal_gateway() {
    [ -z "${GATEWAY_CLASS_NAME:-}" ] && die "GATEWAY_CLASS_NAME is required but not set."
    step "Creating Internal Gateway (ClusterIP) for batch processor..."
    if kubectl get gateway "${BATCH_INTERNAL_GATEWAY_NAME}" -n "${BATCH_INTERNAL_GATEWAY_NAMESPACE}" &>/dev/null; then
        log "Internal Gateway already exists. Skipping."
    else
        kubectl apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: ${BATCH_INTERNAL_GATEWAY_NAME}
  namespace: ${BATCH_INTERNAL_GATEWAY_NAMESPACE}
  annotations:
    networking.istio.io/service-type: ClusterIP
spec:
  gatewayClassName: ${GATEWAY_CLASS_NAME}
  listeners:
  - name: http
    port: 80
    protocol: HTTP
    allowedRoutes:
      namespaces:
        from: Selector
        selector:
          matchLabels:
            llm-d.ai/gateway-route: "true"
EOF
    fi

    wait_for_deployment "${BATCH_INTERNAL_GATEWAY_NAME}-${GATEWAY_CLASS_NAME}" "${BATCH_INTERNAL_GATEWAY_NAMESPACE}" 300s

    step "Waiting for Internal Gateway to be programmed..."
    kubectl wait --for=condition=Programmed \
        --timeout=300s \
        -n "${BATCH_INTERNAL_GATEWAY_NAMESPACE}" \
        gateway/${BATCH_INTERNAL_GATEWAY_NAME} \
        || die "Internal Gateway '${BATCH_INTERNAL_GATEWAY_NAME}' not programmed after 300s."

    log "Internal Gateway created (ClusterIP, no TLS, no rate limit)."

    check_batch_internal_gateway
}

# Verifies the internal gateway is ClusterIP-only and not exposed externally.
# Requires BATCH_INTERNAL_GATEWAY_NAME and BATCH_INTERNAL_GATEWAY_NAMESPACE.
# Aborts (die) on failure.
check_batch_internal_gateway() {
    local gw_ns="${BATCH_INTERNAL_GATEWAY_NAMESPACE}"
    local gw_name="${BATCH_INTERNAL_GATEWAY_NAME}"

    echo ""
    echo "======================================================="
    echo "  Internal Gateway Isolation"
    echo "======================================================="

    local _ig_passed=0 _ig_failed=0

    local svc_type
    svc_type=$(kubectl get svc -n "${gw_ns}" \
        -l "gateway.networking.k8s.io/gateway-name=${gw_name}" \
        -o jsonpath='{.items[0].spec.type}' 2>/dev/null || echo "")

    echo ""
    echo "── Check: Internal Gateway Service type is ClusterIP ──"
    if [ -z "${svc_type}" ]; then
        warn "FAILED: No service found for Internal Gateway '${gw_name}' in namespace '${gw_ns}'"
        _ig_failed=$((_ig_failed + 1))
    elif [ "${svc_type}" = "ClusterIP" ]; then
        echo "  Service type: ${svc_type}"
        log "PASSED: Internal Gateway Service is ClusterIP (no external IP)"
        _ig_passed=$((_ig_passed + 1))
    else
        warn "FAILED: Expected ClusterIP, got '${svc_type}'"
        _ig_failed=$((_ig_failed + 1))
    fi

    local gw_label="gateway.networking.k8s.io/gateway-name=${gw_name}"

    echo ""
    echo "── Check: No Route exposes Internal Gateway ──"
    if is_openshift; then
        if [ -n "$(kubectl get route -A -l "${gw_label}" -o name 2>/dev/null)" ]; then
            warn "FAILED: Found Route exposing ${gw_name}"
            _ig_failed=$((_ig_failed + 1))
        else
            echo "  No Route found for ${gw_name}"
            log "PASSED: No Route exposes Internal Gateway"
            _ig_passed=$((_ig_passed + 1))
        fi
    else
        echo "  Skipping Route check (not OpenShift)."
    fi

    echo ""
    echo "── Check: No Ingress exposes Internal Gateway ──"
    if [ -n "$(kubectl get ingress -A -l "${gw_label}" -o name 2>/dev/null)" ]; then
        warn "FAILED: Found Ingress exposing ${gw_name}"
        _ig_failed=$((_ig_failed + 1))
    else
        echo "  No Ingress found for ${gw_name}"
        log "PASSED: No Ingress exposes Internal Gateway"
        _ig_passed=$((_ig_passed + 1))
    fi

    if [ "${_ig_failed}" -gt 0 ]; then
        die "Internal Gateway isolation check failed (${_ig_failed} failures). Aborting tests."
    fi
    log "Internal Gateway isolation verified (${_ig_passed}/${_ig_passed} checks passed)."
}

# ── Test Framework ────────────────────────────────────────────────────────────
#
# Usage:
#   init_test_framework                          # reset counters
#   test_group_header "LLM Authentication"       # print group header
#   assert_http 401 "No auth -> 401" "${url}"    # simple HTTP code assertion
#   assert_http 200 "Auth -> 200" -H "Authorization: Bearer ${t}" "${url}"
#   run_batch_tests batch_url model authorized_header unauthorized_header [extra_headers...]
#   finish_tests                                 # print summary + wait

# run_tests llm_url batch_url model_name authorized_header unauthorized_header \
#           inference_payload [extra_headers...]
#
# Runs ALL test groups:
#   1. LLM Authentication     (unauthenticated -> 401, authenticated -> 200)
#   2. LLM Authorization      (unauthorized -> 403, authorized -> 200)
#   3. LLM Token Rate Limit   (send requests until 429)
#   4. sleep 60
#   5. Batch Authentication   (unauthenticated -> 401, authenticated -> 200)
#   6. Batch Authorization    (unauthorized batch -> LLM route rejects)
#   7. Batch Lifecycle        (upload + create + poll -> completed + download output)
#   8. Batch Request Rate Limit (rapid requests -> 429)
#
# llm_url:            inference endpoint (e.g. .../v1/chat/completions)
# batch_url:          batch base URL (e.g. https://host)
# authorized_header:        authorized user header (e.g. "Authorization: Bearer xxx")
# unauthorized_header:      unauthorized user header
# inference_payload:  JSON body for inference requests
# extra_headers:      optional headers for token rate limit and batch creation
#                     (e.g. "X-MaaS-Subscription: xxx")
run_tests() {
    local llm_url="$1"
    local batch_url="$2"
    local model_name="$3"
    local authorized_header="$4"
    local unauthorized_header="$5"
    local inference_payload="$6"
    shift 6
    local extra_headers=("$@")

    # ── Test framework ───────────────────────────────────────────
    local _T=0 _TEST_TOTAL=0 _TEST_PASSED=0 _TEST_FAILED=0 _TEST_FAILED_LIST=""

    next_test() { _T=$((_T + 1)); echo ""; echo "── Test ${_T}: $* ──"; }
    pass_test() { _TEST_TOTAL=$((_TEST_TOTAL + 1)); _TEST_PASSED=$((_TEST_PASSED + 1)); log "PASSED: Test ${_T}: $*"; }
    fail_test() { _TEST_TOTAL=$((_TEST_TOTAL + 1)); _TEST_FAILED=$((_TEST_FAILED + 1)); _TEST_FAILED_LIST="${_TEST_FAILED_LIST}\n  - Test ${_T}: $*"; warn "FAILED: Test ${_T}: $*"; }

    test_group_header() {
        echo ""
        echo "======================================================="
        echo "  $1"
        echo "======================================================="
    }

    # assert_http expected_code description curl_args...
    assert_http() {
        local expected="$1" desc="$2"
        shift 2
        next_test "${desc}"
        local http_code
        http_code=$(curl -sk -o /dev/null -w '%{http_code}' "$@")
        echo "  HTTP ${http_code}"
        if [ "$http_code" = "$expected" ]; then
            pass_test "${desc}"
        else
            fail_test "Expected ${expected}, got HTTP ${http_code}"
        fi
    }

    local response http_code body

    # ── Internal helpers ─────────────────────────────────────────

    # Extract JSON field (jq with grep fallback)
    _jval() {
        local field="$1" data="$2" val
        val=$(echo "$data" | jq -r ".${field} // empty" 2>/dev/null)
        [ -z "$val" ] && val=$(echo "$data" | grep -o "\"${field}\":\"[^\"]*\"" | head -1 | cut -d'"' -f4)
        echo "$val"
    }

    # Upload file + create batch; sets _BATCH_ID on success
    _create_batch() {
        local url="$1" header="$2" model="$3"
        shift 3
        local extra_h=("$@")

        local input_file="/tmp/batch-$$-${RANDOM}.jsonl"
        cat > "${input_file}" <<JSONL
{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"${model}","messages":[{"role":"user","content":"Hello"}],"max_tokens":10}}
{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"${model}","messages":[{"role":"user","content":"Tell me a joke"}],"max_tokens":50}}
JSONL
        response=$(curl -sk -w "\n%{http_code}" -X POST "${url}/v1/files" \
            -H "${header}" -F "purpose=batch" -F "file=@${input_file}")
        http_code=$(echo "$response" | sed -n '$p')
        body=$(echo "$response" | sed '$d')
        rm -f "${input_file}"

        _BATCH_FILE_ID=""
        if [ "$http_code" = "200" ]; then
            _BATCH_FILE_ID=$(_jval id "$body")
            echo "  File uploaded: ${_BATCH_FILE_ID}"
        else
            echo "  File upload failed (HTTP ${http_code}): ${body}"
            return 1
        fi

        local create_args=(-H "${header}" -H 'Content-Type: application/json')
        for h in ${extra_h[@]+"${extra_h[@]}"}; do create_args+=(-H "$h"); done
        response=$(curl -sk -w "\n%{http_code}" -X POST "${url}/v1/batches" \
            "${create_args[@]}" \
            -d "{\"input_file_id\":\"${_BATCH_FILE_ID}\",\"endpoint\":\"/v1/chat/completions\",\"completion_window\":\"24h\"}")
        http_code=$(echo "$response" | sed -n '$p')
        body=$(echo "$response" | sed '$d')

        _BATCH_ID=""
        if [ "$http_code" = "200" ]; then
            _BATCH_ID=$(_jval id "$body")
            echo "  Batch created: ${_BATCH_ID}"
        else
            echo "  Batch creation failed (HTTP ${http_code}): ${body}"
            return 1
        fi
    }

    # Poll batch until terminal state; sets _BATCH_STATUS, _BATCH_FAILED
    _poll_batch() {
        local url="$1" batch_id="$2" header="$3"
        _BATCH_STATUS="unknown"
        _BATCH_FAILED=0
        local poll=0
        while [ "$poll" -lt 60 ]; do
            response=$(curl -sk "${url}/v1/batches/${batch_id}" -H "${header}")
            _BATCH_STATUS=$(_jval status "$response")
            _BATCH_FAILED=$(echo "$response" | jq -r '.request_counts.failed // 0' 2>/dev/null)
            [ "$_BATCH_FAILED" = "null" ] || [ -z "$_BATCH_FAILED" ] && \
                _BATCH_FAILED=$(echo "$response" | grep -o '"failed":[0-9]*' | head -1 | cut -d':' -f2)
            echo "  Poll $((poll+1)): status=${_BATCH_STATUS}, failed=${_BATCH_FAILED:-0}"
            case "$_BATCH_STATUS" in completed|failed|expired|cancelled) break ;; esac
            poll=$((poll + 1))
            sleep 5
        done
    }

    # ── 1. LLM Authn ───────────────────────────────────
    test_group_header "LLM Authn"
    assert_http 401 "Unauthenticated inference -> 401" \
        -X POST "${llm_url}" -H "Content-Type: application/json" -d "${inference_payload}"
    assert_http 200 "Authenticated inference -> 200" \
        -X POST "${llm_url}" -H "${authorized_header}" -H "Content-Type: application/json" -d "${inference_payload}"

    # ── 2. LLM Authz ────────────────────────────────────
    test_group_header "LLM Authz"
    assert_http 403 "Unauthorized inference -> 403" \
        -X POST "${llm_url}" -H "${unauthorized_header}" -H "Content-Type: application/json" -d "${inference_payload}"
    assert_http 200 "Authorized inference -> 200" \
        -X POST "${llm_url}" -H "${authorized_header}" -H "Content-Type: application/json" -d "${inference_payload}"

    # ── 3. LLM Token Rate Limit ─────────────────────────────────
    test_group_header "LLM Token Rate Limit"
    next_test "Token rate limiting (inference)"
    echo "  Goal: Send inference requests until 429"
    local trl_success=0 trl_limited=false
    local trl_args=(-H "${authorized_header}" -H "Content-Type: application/json")
    for h in ${extra_headers[@]+"${extra_headers[@]}"}; do trl_args+=(-H "$h"); done
    for i in $(seq 1 100); do
        http_code=$(curl -sk -o /dev/null -w '%{http_code}' \
            "${trl_args[@]}" \
            -d "{\"model\":\"${model_name}\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}],\"max_tokens\":100}" \
            "${llm_url}")
        if [ "$http_code" = "429" ]; then
            echo "  Request $i: 429 Token Rate Limited"
            trl_limited=true
            break
        else
            trl_success=$((trl_success + 1))
            [ "$i" -le 3 ] && echo "  Request $i: $http_code"
        fi
        sleep 0.2
    done
    if [ "$trl_limited" = "true" ]; then
        pass_test "Token rate limiting triggered after $trl_success requests"
    else
        fail_test "Token rate limit not triggered after 100 requests"
    fi
    echo ""
    echo "  Waiting 60s for rate limit counters to reset..."
    sleep 60

    # ── 4. Batch Authn ──────────────────────────────────
    test_group_header "Batch Authn"
    assert_http 401 "No credentials batch request -> 401" "${batch_url}/v1/batches"
    assert_http 200 "Authenticated batch request -> 200" -H "${authorized_header}" "${batch_url}/v1/batches"
    assert_http 200 "Unauthorized but authenticated batch request -> 200 (no authz on batch-route)" -H "${unauthorized_header}" "${batch_url}/v1/batches"

    # ── 5. Batch Authz (LLM route enforces) ──────────────
    test_group_header "Batch Authz"
    next_test "Unauthorized user batch -> batch-llm-route rejects with 403"
    echo "  Goal: Batch processor forwards unauthorized credentials to batch-llm-route, gets 403"

    if _create_batch "${batch_url}" "${unauthorized_header}" "${model_name}"; then
        _poll_batch "${batch_url}" "${_BATCH_ID}" "${unauthorized_header}"
        if [ "${_BATCH_FAILED:-0}" -gt 0 ] || [ "$_BATCH_STATUS" = "failed" ]; then
            local output_file_id
            output_file_id=$(curl -sk "${batch_url}/v1/batches/${_BATCH_ID}" \
                -H "${unauthorized_header}" | jq -r '.output_file_id // .error_file_id // empty')
            local output_content=""
            [ -n "$output_file_id" ] && \
                output_content=$(curl -sk "${batch_url}/v1/files/${output_file_id}/content" \
                    -H "${unauthorized_header}")
            if echo "$output_content" | grep -qE '"status_code":403|HTTP 403'; then
                pass_test "LLM route rejected with 403 (status=${_BATCH_STATUS})"
            else
                fail_test "Requests failed but not with 403: ${output_content:0:200}"
            fi
        else
            fail_test "Expected failed requests, got status=${_BATCH_STATUS}, failed=${_BATCH_FAILED:-0}"
        fi
    else
        fail_test "Batch creation failed (HTTP $http_code)"
    fi

    # ── 6. Batch Lifecycle ───────────────────────────────────────
    test_group_header "Batch Lifecycle"
    next_test "Upload file + create batch"
    if _create_batch "${batch_url}" "${authorized_header}" "${model_name}" ${extra_headers[@]+"${extra_headers[@]}"}; then
        pass_test "File uploaded + batch created (batch: ${_BATCH_ID})"

        next_test "Batch completion"
        echo "  Goal: Verify processor forwards credentials through gateway to model"
        _poll_batch "${batch_url}" "${_BATCH_ID}" "${authorized_header}"
        if [ "$_BATCH_STATUS" = "completed" ]; then
            pass_test "Batch completed successfully"

            next_test "Download output file"
            local output_file_id
            output_file_id=$(curl -sk "${batch_url}/v1/batches/${_BATCH_ID}" \
                -H "${authorized_header}" | jq -r '.output_file_id // empty')
            if [ -n "$output_file_id" ]; then
                local output_content
                output_content=$(curl -sk "${batch_url}/v1/files/${output_file_id}/content" \
                    -H "${authorized_header}")
                if [ -n "$output_content" ]; then
                    pass_test "Output file downloaded (file: ${output_file_id})"
                    echo "  --- output content ---"
                    echo "$output_content"
                    echo "  --- end ---"
                else
                    fail_test "Output file is empty (file: ${output_file_id})"
                fi
            else
                fail_test "No output_file_id in completed batch"
            fi
        else
            fail_test "Batch ended with status=${_BATCH_STATUS} (expected completed)"
        fi
    else
        fail_test "Batch creation failed (HTTP $http_code)"
    fi

    # ── 7. Batch Request Rate Limit ──────────────────────────────
    test_group_header "Batch Request Rate Limit"
    next_test "Batch API rate limiting"
    local rl_success=0 rl_limited=0
    for i in $(seq 1 25); do
        http_code=$(curl -sk -o /dev/null -w '%{http_code}' -H "${authorized_header}" "${batch_url}/v1/batches")
        if [ "$http_code" = "429" ]; then
            rl_limited=$((rl_limited + 1))
            echo "  Request $i: 429 Rate Limited"
        else
            rl_success=$((rl_success + 1))
            [ "$i" -le 3 ] && echo "  Request $i: $http_code"
        fi
    done
    echo "  Result: $rl_success passed, $rl_limited rate-limited"
    if [ "$rl_limited" -ge 1 ]; then
        pass_test "Rate limiting is working"
    else
        fail_test "No 429 received after 25 requests"
    fi
    echo ""
    echo "  Waiting 60s for rate limit counters to reset..."
    sleep 60

    # ── Summary ─────────────────────────────────────────────────
    test_group_header "Summary"
    if [ "$_TEST_FAILED" -eq 0 ]; then
        log "All ${_TEST_TOTAL} tests passed!"
    else
        warn "${_TEST_FAILED}/${_TEST_TOTAL} tests failed:"
        echo -e "${_TEST_FAILED_LIST}"
        echo ""
    fi
    echo "  Passed: ${_TEST_PASSED}  Failed: ${_TEST_FAILED}  Total: ${_TEST_TOTAL}"

    return "${_TEST_FAILED}"
}

# ── Async dispatcher runtime verification ────────────────────────────────────
# Usage: verify_dispatcher_runtime <async-processor-deploy-name> <inference-pool-name>
verify_dispatcher_runtime() {
    local ap_deploy="$1"
    local pool_name="$2"

    banner "Verifying Async Dispatch (post-test)"

    step "Setting up port-forward to async-processor metrics..."
    local ap_metrics_port=19090
    kubectl port-forward -n "${BATCH_NAMESPACE}" \
        "deployment/${ap_deploy}" "${ap_metrics_port}:9090" &>/dev/null &
    local pf_pid=$!

    local attempt
    for attempt in $(seq 1 15); do
        if curl -sf "http://localhost:${ap_metrics_port}/metrics" &>/dev/null; then
            break
        fi
        sleep 1
    done
    if ! curl -sf "http://localhost:${ap_metrics_port}/metrics" &>/dev/null; then
        kill "${pf_pid}" 2>/dev/null || true
        die "Cannot reach async-processor metrics on port ${ap_metrics_port}."
    fi

    local metrics_body
    metrics_body=$(curl -sf "http://localhost:${ap_metrics_port}/metrics")
    local errors=0

    step "Checking async-processor successful requests..."
    local success_count
    success_count=$(echo "${metrics_body}" | { grep "^llm_d_async_async_successful_requests_total" || true; } \
        | awk '{sum+=$2} END {printf "%d", sum}')
    if [ "${success_count}" -gt 0 ]; then
        log "Async-processor completed ${success_count} successful request(s)."
    else
        warn "No successful requests via async-processor."
        errors=$((errors + 1))
    fi

    step "Checking async-processor worker pool..."
    local pool_limit
    pool_limit=$(echo "${metrics_body}" | { grep "^llm_d_async_async_pool_worker_limit" || true; } \
        | awk '{sum+=$2} END {printf "%d", sum}')
    if [ "${pool_limit}" -gt 0 ]; then
        log "Worker pool active: pool_worker_limit=${pool_limit}"
    else
        warn "Worker pool not active (pool_worker_limit=0)."
        errors=$((errors + 1))
    fi

    step "Checking dispatch gate budget..."
    local budget_line
    budget_line=$(echo "${metrics_body}" | { grep "^llm_d_async_async_dispatch_budget" || true; } | head -1)
    if [ -n "${budget_line}" ]; then
        local budget_val
        budget_val=$(echo "${budget_line}" | awk '{print $2}')
        log "Gate budget: ${budget_val} (${budget_line})"
    else
        warn "Gate budget metric not found — dispatch gate is not active."
        errors=$((errors + 1))
    fi

    kill "${pf_pid}" 2>/dev/null || true

    step "Checking Prometheus metrics for async dispatch..."
    local prom_url="http://prometheus.${LLM_NAMESPACE}.svc.cluster.local:9090"

    local prom_queries=(
        "inference_pool_ready_pods{name=\"${pool_name}\"}"
        "vllm:num_requests_running{inference_pool=\"${pool_name}\"}"
    )
    local prom_labels=(
        "inference_pool_ready_pods (ready pods count)"
        "vllm:num_requests_running with inference_pool label"
    )

    for i in "${!prom_queries[@]}"; do
        local query="${prom_queries[$i]}"
        local label="${prom_labels[$i]}"
        local prom_query_pod="prom-verify-${i}-$(date +%s)"
        local result
        result=$(kubectl run "${prom_query_pod}" --rm -i --restart=Never --quiet \
            --image=curlimages/curl -n "${LLM_NAMESPACE}" -- \
            curl -sf -G "${prom_url}/api/v1/query" --data-urlencode "query=${query}" 2>/dev/null) || true
        local data_count
        data_count=$(echo "${result}" | jq '.data.result | length' 2>/dev/null || echo "0")
        if [ "${data_count}" -gt 0 ]; then
            log "${label}: ${data_count} series found"
        else
            warn "${label}: no data in Prometheus"
            errors=$((errors + 1))
        fi
    done

    if [ "${errors}" -gt 0 ]; then
        die "Async dispatch runtime verification failed with ${errors} error(s)."
    fi
    log "Async dispatch runtime verified."
}
