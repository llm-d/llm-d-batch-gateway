#!/bin/bash
# Offline orchestration tests. Infrastructure functions and external tools are
# stubbed; the real main, gateway Helm argument builder, and dispatcher run.
set -euo pipefail

TEST_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "${1:-}" == "--case" ]]; then
    source "${TEST_SCRIPT_DIR}/dev-deploy.sh"
    FIXTURE_ROOT="$(mktemp -d)"
    trap 'rm -rf "${FIXTURE_ROOT}"' EXIT
    mkdir "${FIXTURE_ROOT}/scripts"
    ln -s "${REPO_ROOT}/test" "${FIXTURE_ROOT}/test"
    ln -s "${REPO_ROOT}/charts" "${FIXTURE_ROOT}/charts"
    for script in dev-deploy.sh dev-deploy-dispatcher.sh dev-common.sh; do
        ln -s "${TEST_SCRIPT_DIR}/${script}" "${FIXTURE_ROOT}/scripts/${script}"
    done
    REPO_ROOT="${FIXTURE_ROOT}"

    record() { printf '%s\n' "$*" >> "${TEST_LOG}"; }
    command() {
        if [[ "${1:-}" == "-v" && "${2:-}" == "${MISSING_TOOL:-}" ]]; then
            return 1
        fi
        builtin command "$@"
    }
    docker() {
        # Prerequisite queries are not mutations.
        if [[ "$1" == info ]]; then
            [[ "${DOCKER_DOWN:-false}" != true ]]
            return
        fi
        record "docker $*"
    }
    podman() { record "podman $*"; }
    kind() {
        record "kind $*"
        printf '%s\n' "${KIND_CLUSTER_NAME:-batch-gateway-dev}"
    }
    kubectl() {
        record "kubectl $*"
        case "$*" in
            'get pods '*)
                local arg release
                for arg in "$@"; do
                    if [[ "${arg}" == app.kubernetes.io/instance=* ]]; then
                        release="${arg#*=}"
                        release="${release%%,*}"
                    fi
                done
                printf '{"items":[{"metadata":{"name":"%s-pod"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}\n' "${release}"
                ;;
            'get pod '*'.imageID}')
                printf '%s\n' "${RUNTIME_IMAGE_ID:-${DISPATCHER_IMAGE}}"
                ;;
            'get pod '*'.image}')
                printf '%s\n' "${DISPATCHER_IMAGE}"
                ;;
            'get configmap '*)
                printf '%s\n' 'vllm-sim' # Scrape job already exists.
                ;;
            'get service '*)
                printf '%s\n' '{"metadata":{"namespace":"default"},"spec":{"selector":{"app":"redis"},"ports":[{"name":"redis","protocol":"TCP","port":6379,"targetPort":6379}]}}'
                ;;
            'apply -f -')
                builtin command jq -e '.kind == "Service" and .metadata.name == "redis-master-nodeport" and .metadata.namespace == "default" and .spec.type == "NodePort" and .spec.selector == {"app":"redis"} and .spec.ports == [{"name":"redis","protocol":"TCP","port":6379,"targetPort":6379,"nodePort":30479}]' >/dev/null
                ;;
        esac
    }
    helm() {
        record "helm $*"
        if [[ "$1" == status ]]; then
            [[ "${EXISTING_RELEASE:-false}" == true ]]
            return
        fi
        if [[ "$1 $2" == 'get values' ]]; then
            printf '%s\n' '{"processor":{"config":{"modelGateways":{"old":{}},"globalInferenceGateway":{},"asyncDispatch":{"models":{"old":{}}}}}}'
        fi
    }
    jq() {
        record "jq $*"
        builtin command jq "$@"
    }
    nc() { record "nc $*"; }
    make() { record "make $*"; }
    export -f record command docker podman kind kubectl helm jq nc make

    # No builds, certificates, real infrastructure, or HTTP calls. Keep main's
    # orchestration and both scripts' actual Helm commands under test.
    for fn in build_images pull_images ensure_cluster install_exchange install_postgresql \
        create_secret create_tls_secret install_minio create_pvc load_images \
        install_jaeger install_prometheus install_grafana install_vllm_sim \
        ensure_gie_repo install_gie_crds install_gie_epp create_inference_objectives \
        verify_deployment create_nodeport_services print_usage; do
        eval "${fn}() { record '${fn}' \"\$@\"; }"
    done

    if [[ "${STANDALONE:-false}" == true ]]; then
        bash "${REPO_ROOT}/scripts/dev-deploy-dispatcher.sh"
    else
        main
        [[ "${IMAGE_TAG}" == gateway-test ]]
    fi
    exit
fi

TEST_TMP="$(mktemp -d)"
trap 'rm -rf "${TEST_TMP}"' EXIT
FAILURES=0

run_case() {
    local name="$1" expected="$2"
    shift 2
    local status=0
    TEST_LOG="${TEST_TMP}/calls"
    : > "${TEST_LOG}"
    env -i PATH="${PATH}" HOME="${HOME}" TEST_LOG="${TEST_LOG}" \
        IMAGE_TAG=gateway-test DISPATCHER_IMAGE=localhost:5000/async:dispatcher-test \
        "$@" bash "${BASH_SOURCE[0]}" --case > "${TEST_TMP}/output" 2>&1 || status=$?
    if [[ "${expected}" == ok && "${status}" == 0 ]]; then
        printf 'ok   - %s\n' "${name}"
    elif [[ "${expected}" == runtime-digest-error && "${status}" != 0 ]] && \
        grep -Fq 'expected digest sha256:' "${TEST_TMP}/output"; then
        printf 'ok   - %s (runtime verification rejected image)\n' "${name}"
    elif [[ "${expected}" != ok && "${status}" != 0 ]] && \
        grep -Fq "${expected}" "${TEST_TMP}/output" && [[ ! -s "${TEST_LOG}" ]]; then
        printf 'ok   - %s (before any actions)\n' "${name}"
    else
        printf 'FAIL - %s (status %s)\n' "${name}" "${status}"
        cat "${TEST_TMP}/output" "${TEST_LOG}"
        FAILURES=$((FAILURES + 1))
    fi
}

assert_log() {
    if ! grep -Fq -- "$1" "${TEST_LOG}"; then
        printf 'FAIL - missing call: %s\n' "$1"
        FAILURES=$((FAILURES + 1))
    fi
}

assert_no_log() {
    if grep -Fq -- "$1" "${TEST_LOG}"; then
        printf 'FAIL - unexpected call: %s\n' "$1"
        FAILURES=$((FAILURES + 1))
    fi
}

for existing in false true; do
    run_case "default async (existing release: ${existing})" ok EXISTING_RELEASE="${existing}"
    assert_log 'helm upgrade --install dispatcher '
    assert_log 'helm upgrade --install dispatcher-scrape '
    assert_log 'helm upgrade --install dispatcher-prom '
    assert_log 'oci://ghcr.io/llm-d/charts/llm-d-async --version v0.9.1'
    assert_log '--set-string ap.image.repository=localhost:5000/async --set-string ap.image.tag=dispatcher-test --set-string ap.imagePullPolicy=IfNotPresent'
    for release in dispatcher dispatcher-scrape dispatcher-prom; do
        assert_log "kubectl get pods --namespace default --selector app.kubernetes.io/instance=${release},app.kubernetes.io/name=llm-d-async --field-selector status.phase=Running -o json"
        assert_log "kubectl get pod ${release}-pod --namespace default -o jsonpath={.status.containerStatuses[?(@.name==\"llm-d-async\")].imageID}"
    done
    assert_log 'kubectl get service redis-master --namespace default -o json'
    assert_log 'jq --arg name redis-master-nodeport --argjson nodePort 30479'
    assert_log 'kubectl apply -f -'
    assert_log 'nc -z localhost 6399'
    assert_no_log 'kubectl port-forward svc/redis'
    assert_log '/test/e2e/dispatcher/processor-async-values.yaml'
    assert_log 'apiserver.image.tag=gateway-test'
    assert_log 'processor.image.tag=gateway-test'
    assert_log 'gc.image.tag=gateway-test'
    assert_no_log 'processor.config.dispatchMode=sync'
    assert_no_log 'processor.config.modelGateways.'
    assert_no_log 'helm get values'
    # Exactly one gateway install/upgrade, after all dispatcher Helm calls.
    if ! awk '/^helm (install|upgrade) batch-gateway / { gateway++; if (dispatchers != 3) exit 1 }
        /^helm upgrade --install dispatcher/ { dispatchers++ }
        END { if (gateway != 1) exit 1 }' "${TEST_LOG}"; then
        printf 'FAIL - dispatcher/gateway Helm order or count\n'
        FAILURES=$((FAILURES + 1))
    fi
    if [[ "${existing}" == true ]]; then
        assert_log 'helm upgrade batch-gateway ./charts/batch-gateway --reset-values'
    else
        assert_log 'helm install batch-gateway ./charts/batch-gateway'
    fi

    run_case "explicit sync (existing release: ${existing})" ok ENABLE_DISPATCHER=false EXISTING_RELEASE="${existing}"
    assert_log 'processor.config.dispatchMode=sync'
    assert_log 'processor.config.asyncDispatch=null'
    assert_log 'processor.config.modelGateways.sim-model.url=http://vllm-sim.default.svc.cluster.local:8000'
    assert_log 'processor.config.modelGateways.sim-model-b.url=http://vllm-sim-b.default.svc.cluster.local:8000'
    assert_no_log 'dispatcher'
done

DIGEST=sha256:d8db64675b6a5f70486d74de9f28aa2ee88e7e2c4e3ba97ba2078d634c2fd610
run_case 'digest parsing preserves gateway image tag' ok \
    DISPATCHER_IMAGE="localhost:5000/async:v0.9.1@${DIGEST}" \
    RUNTIME_IMAGE_ID="docker-pullable://localhost:5000/async@${DIGEST}"
assert_log "--set-string ap.image.repository=localhost:5000/async --set-string ap.image.tag=v0.9.1@${DIGEST}"
assert_log 'apiserver.image.tag=gateway-test'
assert_log 'processor.image.tag=gateway-test'
assert_log 'gc.image.tag=gateway-test'

run_case 'runtime digest mismatch rejected' runtime-digest-error \
    DISPATCHER_IMAGE="localhost:5000/async:v0.9.1@${DIGEST}" \
    RUNTIME_IMAGE_ID='docker-pullable://localhost:5000/async@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
assert_no_log 'helm install batch-gateway '
assert_no_log 'helm upgrade batch-gateway '

run_case 'sync GIE' ok ENABLE_DISPATCHER=false ENABLE_GIE=true
assert_log 'processor.config.dispatchMode=sync'
assert_log 'processor.config.modelGateways.sim-model.url=http://epp-sim-model-epp.default.svc.cluster.local:8081'
assert_log 'processor.config.modelGateways.sim-model-b.url=http://epp-sim-model-b-epp.default.svc.cluster.local:8081'
assert_log 'processor.config.modelGateways.sim-model.inferenceObjective=batch-sheddable-sim-model'
assert_no_log 'dispatcher'

run_case 'standalone still reconfigures gateway' ok STANDALONE=true ENABLE_DISPATCHER=false
assert_log 'helm get values batch-gateway'
assert_log 'jq del(.processor.config.modelGateways, .processor.config.globalInferenceGateway, .processor.config.asyncDispatch)'
assert_log '--reset-values --values'
assert_log '/test/e2e/dispatcher/processor-async-values.yaml --wait'
assert_log 'kubectl config use-context kind-batch-gateway-dev'
assert_log 'kubectl rollout restart statefulset/batch-gateway-processor --namespace default'
assert_log 'kubectl rollout status statefulset/batch-gateway-processor --namespace default --timeout=60s'
assert_no_log 'deployment/batch-gateway-processor'

run_case 'combined GIE/async rejected' 'make dev-deploy-gie' ENABLE_GIE=true
run_case 'invalid dispatcher flag rejected' 'must be true or false' ENABLE_DISPATCHER=typo
for setting in USE_KIND=false NAMESPACE=custom EXCHANGE_CLIENT_TYPE=valkey REDIS_RELEASE=custom \
    VLLM_SIM_NAME=custom VLLM_SIM_B_NAME=custom VLLM_SIM_MODEL=custom VLLM_SIM_B_MODEL=custom \
    JAEGER_NAME=custom PROMETHEUS_NAME=custom KIND_EXPERIMENTAL_PROVIDER=podman \
    DISPATCHER_SOURCE=/nonexistent; do
    run_case "async rejects ${setting}" 'ENABLE_DISPATCHER=false' "${setting}"
    run_case "sync accepts ${setting}" ok ENABLE_DISPATCHER=false "${setting}"
done
for tool in jq nc docker; do
    run_case "missing ${tool}" 'Missing required tools' MISSING_TOOL="${tool}"
    run_case "sync preserves prerequisite check for ${tool}" ok ENABLE_DISPATCHER=false MISSING_TOOL="${tool}"
done
run_case 'Docker daemon unavailable' 'requires running Docker' DOCKER_DOWN=true
run_case 'standalone rejects custom namespace early' 'ENABLE_DISPATCHER=false' STANDALONE=true NAMESPACE=custom
run_case 'standalone rejects GIE early' 'make dev-deploy-gie' STANDALONE=true ENABLE_GIE=true

if [[ "${FAILURES}" != 0 ]]; then
    printf '%s dev-deploy test(s) failed\n' "${FAILURES}"
    exit 1
fi
printf 'All dev-deploy tests passed. No cluster or network actions performed.\n'
