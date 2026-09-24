#!/bin/bash
# Common functions and configuration for dev scripts
# Source this file from other dev-*.sh scripts

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ── Logging Functions ─────────────────────────────────────────────────────────
log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
step() { echo -e "${BLUE}[STEP]${NC}  $*"; }
die()  { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Common Configuration (override via env vars) ──────────────────────────────
NAMESPACE="${NAMESPACE:-default}"
HELM_RELEASE="${HELM_RELEASE:-batch-gateway}"

# Port configuration
LOCAL_PORT="${LOCAL_PORT:-8000}"
LOCAL_OBS_PORT="${LOCAL_OBS_PORT:-8081}"
LOCAL_PROCESSOR_PORT="${LOCAL_PROCESSOR_PORT:-9090}"
JAEGER_PORT="${JAEGER_PORT:-16686}"

# Service names
REDIS_RELEASE="${REDIS_RELEASE:-redis}"
EXCHANGE_CLIENT_TYPE="${EXCHANGE_CLIENT_TYPE:-redis}"
POSTGRESQL_RELEASE="${POSTGRESQL_RELEASE:-postgresql}"
JAEGER_NAME="${JAEGER_NAME:-jaeger}"
PROMETHEUS_NAME="${PROMETHEUS_NAME:-prometheus}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9091}"
GRAFANA_NAME="${GRAFANA_NAME:-grafana}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
S3_STORAGE_HOST_PORT="${S3_STORAGE_HOST_PORT:-9002}"
VLLM_SIM_NAME="${VLLM_SIM_NAME:-vllm-sim}"
VLLM_SIM_B_NAME="${VLLM_SIM_B_NAME:-vllm-sim-b}"
SEAWEEDFS_NAME="${SEAWEEDFS_NAME:-seaweedfs}"
SEAWEEDFS_S3_PORT="${SEAWEEDFS_S3_PORT:-8333}"

# Secret and PVC names
TLS_SECRET_NAME="${TLS_SECRET_NAME:-${HELM_RELEASE}-tls}"
APP_SECRET_NAME="${APP_SECRET_NAME:-${HELM_RELEASE}-secrets}"
FILES_PVC_NAME="${FILES_PVC_NAME:-${HELM_RELEASE}-files}"
