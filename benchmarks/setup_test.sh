#!/usr/bin/env bash
# Regression tests for the benchmark setup script. These tests do not need a
# Kubernetes cluster; they validate the Helm values passed for scenario 5.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/setup.sh"

transport_config="$(sed -n 's/.*--set-json "ap\.transportConfig=\(.*\)"/\1/p' "${SETUP_SCRIPT}")"
if [[ -z "${transport_config}" ]]; then
    echo "FAIL - could not find scenario-5 transportConfig Helm value" >&2
    exit 1
fi

# Substitute the shell variables that are expanded when the setup script runs,
# so the resulting Helm JSON can be checked with jq.
transport_config="$(printf '%s' "${transport_config}" | sed \
    -e 's/${NAMESPACE}/test-namespace/g' \
    -e 's/${ASYNC_POOL_NAME}/test-pool/g' \
    -e 's|${ASYNC_IGW_URL}|http://gateway.test|g' \
    -e 's|${ASYNC_METRICS_URL}|http://metrics.test|g' \
    -e 's/\\"/"/g' \
    -e 's/ \\$//')"

if ! printf '%s' "${transport_config}" | jq -e \
    '.result_queue_name == "result-list" and (.queues | all(has("result_queue_name") | not))' \
    >/dev/null; then
    echo "FAIL - scenario-5 queues must preserve producer-provided result queues" >&2
    exit 1
fi

echo "ok   - scenario-5 transportConfig preserves producer result queues"
