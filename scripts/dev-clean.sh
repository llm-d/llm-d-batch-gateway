#!/bin/bash
set -euo pipefail

# Source common functions and configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/dev-common.sh"

# ── Cleanup ───────────────────────────────────────────────────────────────────

cleanup_kubernetes_resources() {
    step "Cleaning up Kubernetes resources in namespace '${NAMESPACE}'..."

    # Clean up GIE EPP releases and CRDs (if any were deployed with ENABLE_GIE=true).
    # Uses managed-by label to scope deletion to resources created by this script.
    local epp_prefix="${GIE_EPP_RELEASE:-epp}"
    for release in $(helm list -n "${NAMESPACE}" -q --filter "^${epp_prefix}-" 2>/dev/null); do
        log "Uninstalling GIE EPP release '${release}'..."
        helm uninstall "${release}" -n "${NAMESPACE}" || warn "Failed to uninstall EPP release '${release}'"
    done
    kubectl delete inferenceobjective -l app.kubernetes.io/managed-by=batch-gateway-dev -n "${NAMESPACE}" --ignore-not-found=true \
        || warn "Failed to delete InferenceObjective CRDs"

    log "Uninstalling helm releases..."
    # Clean up dispatcher port-forward
    for pid_file in "${SCRIPT_DIR}/../.dispatcher-port-forward.pid" "${SCRIPT_DIR}/../.dispatcher-sim-port-forward.pid"; do
        if [[ -f "${pid_file}" ]]; then
            local pf_pid
            pf_pid=$(cat "${pid_file}")
            kill "${pf_pid}" 2>/dev/null || true
            rm -f "${pid_file}"
            log "Stopped port-forward (PID: ${pf_pid})"
        fi
    done

    helm uninstall "${DISPATCHER_RELEASE:-dispatcher}" -n "${NAMESPACE}" 2>/dev/null || warn "Failed to uninstall dispatcher (may not exist)"
    helm uninstall "${DISPATCHER_SCRAPE_RELEASE:-dispatcher-scrape}" -n "${NAMESPACE}" 2>/dev/null || warn "Failed to uninstall dispatcher-scrape (may not exist)"
    helm uninstall "${DISPATCHER_PROM_RELEASE:-dispatcher-prom}" -n "${NAMESPACE}" 2>/dev/null || warn "Failed to uninstall dispatcher-prom (may not exist)"
    helm uninstall "${HELM_RELEASE}" -n "${NAMESPACE}" 2>/dev/null || warn "Failed to uninstall ${HELM_RELEASE} (may not exist)"
    helm uninstall "${REDIS_RELEASE}" -n "${NAMESPACE}" 2>/dev/null || warn "Failed to uninstall ${REDIS_RELEASE} (may not exist)"
    helm uninstall "${POSTGRESQL_RELEASE}" -n "${NAMESPACE}" 2>/dev/null || warn "Failed to uninstall ${POSTGRESQL_RELEASE} (may not exist)"

    # Delete developer-created access services (created outside of Helm)
    log "Deleting developer-created services..."
    kubectl delete svc "${HELM_RELEASE}-apiserver-nodeport" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete svc "${HELM_RELEASE}-processor-nodeport" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete svc "${PROMETHEUS_NAME}-nodeport" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete svc "${GRAFANA_NAME}-nodeport" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete svc "${JAEGER_NAME}-nodeport" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete svc "${SEAWEEDFS_NAME}-nodeport" -n "${NAMESPACE}" --ignore-not-found=true

    # Delete deployments and services
    log "Deleting deployments and services..."
    kubectl delete deployment,svc "${JAEGER_NAME}" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete deployment,svc "${GRAFANA_NAME}" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete configmap "${GRAFANA_NAME}-provisioning-datasources" "${GRAFANA_NAME}-provisioning-dashboards" "${GRAFANA_NAME}-dashboards" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete deployment,svc,configmap,sa "${PROMETHEUS_NAME}" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete configmap "${PROMETHEUS_NAME}-config" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete clusterrole,clusterrolebinding "${PROMETHEUS_NAME}" --ignore-not-found=true
    kubectl delete deployment,svc "${VLLM_SIM_NAME}" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete deployment,svc "${VLLM_SIM_B_NAME}" -n "${NAMESPACE}" --ignore-not-found=true
    kubectl delete deployment,svc "${SEAWEEDFS_NAME}" -n "${NAMESPACE}" --ignore-not-found=true

    # Delete e2e helper pods (created by test runs, safe to ignore if absent)
    kubectl delete pod "batch-gateway-e2e-curl" -n "${NAMESPACE}" --ignore-not-found=true
    # Legacy pod name from before curl pod consolidation; remove once no
    # clusters are likely to have it lingering (safe to drop after v0.10+).
    kubectl delete pod "batch-gateway-epp-metrics-curl" -n "${NAMESPACE}" --ignore-not-found=true

    # Delete secrets
    log "Deleting secrets..."
    kubectl delete secret "${APP_SECRET_NAME}" "${TLS_SECRET_NAME}" -n "${NAMESPACE}" --ignore-not-found=true

    # Delete PVC
    log "Deleting PVC..."
    kubectl delete pvc "${FILES_PVC_NAME}" -n "${NAMESPACE}" --ignore-not-found=true

    log "Kubernetes resources cleaned up."
}

# ── Main ──────────────────────────────────────────────────────────────────────

main() {
    echo ""
    echo "  ╔══════════════════════════════════════╗"
    echo "  ║   Batch Gateway Cleanup Script       ║"
    echo "  ╚══════════════════════════════════════╝"
    echo ""

    # Check if required tools are available
    if ! command -v kubectl &>/dev/null; then
        die "kubectl not found. Please install it first."
    fi

    if ! command -v helm &>/dev/null; then
        die "helm not found. Please install it first."
    fi

    step "Cleaning all batch-gateway resources from namespace '${NAMESPACE}'..."

    cleanup_kubernetes_resources

    log ""
    log "Cleanup complete!"
    log ""
}

main "$@"
