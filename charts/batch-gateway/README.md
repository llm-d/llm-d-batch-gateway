# Batch Gateway Helm Chart

This Helm chart deploys the Batch Gateway on a Kubernetes cluster, which includes:
- **API Server**: REST API server for file and batch management
- **Processor**: Background processor for batch job execution
- **Garbage Collector**: Periodic cleanup of expired jobs and files

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Components

### API Server (batch-gateway-apiserver)
The API server provides a REST API for managing files and batch jobs.

### Processor (batch-gateway-processor)
The processor is a background worker component that polls for and processes batch jobs.

For TLS to HTTPS inference backends (custom CA, mTLS, mounting certificate Secrets), see the [Processor inference TLS guide](../../docs/guides/processor-inference-tls.md).

### Garbage Collector (batch-gateway-gc)
The garbage collector periodically cleans up expired jobs and files.

## Installing the Chart

### From OCI registry (recommended for releases)

The chart is published to GitHub Container Registry for each release. Use `--version` with the **chart semver** (same as `Chart.yaml` `version`: the git tag **without** `v`, e.g. `v1.0.0` → `1.0.0`):

```bash
helm install batch-gateway oci://ghcr.io/llm-d/charts/batch-gateway --version 1.0.0
```

Replace `1.0.0` with the version you need. Image tags in the published chart are pinned to the release version.

### From source

To install the chart with the release name `my-release`:

```bash
helm install my-release ./charts/batch-gateway
```

This will deploy the API server, processor, and garbage collector by default.

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
helm uninstall my-release
```

## Upgrading the Chart

To upgrade an existing release with new values:

```bash
# Upgrade with a values file
helm upgrade my-release ./charts/batch-gateway -f my-values.yaml

# Or use --set
helm upgrade my-release ./charts/batch-gateway \
  --set apiserver.replicaCount=5

# View what would change before upgrading
helm upgrade my-release ./charts/batch-gateway \
  -f my-values.yaml \
  --dry-run --debug
```

## Configuration

For a complete list of parameters, see [values.yaml](./values.yaml).

### Additional inference endpoints

Batch Gateway accepts the standard OpenAI Batch API endpoints by default. To use an endpoint supplied by an OpenAI-compatible backend, add it explicitly to the shared allowlist:

```yaml
global:
  extraEndpoints:
    - /v1/classify
    - /v1/pooling
```

The chart renders this list into both the API server and processor configurations. Each value must be an absolute path beginning with exactly one slash and cannot contain a host, query string, fragment, or escaped path. Invalid values cause startup validation to fail.

For a rolling upgrade, enable new endpoints on processors before API servers can accept batches that use them:

1. Set `processor.config.extraEndpoints` and wait for the processor rollout to complete.
2. Set `global.extraEndpoints`, which enables the endpoints for the API server as well.
3. Reset `processor.config.extraEndpoints` to `null` so it inherits the global value.

Both `processor.config.extraEndpoints` and `apiserver.config.batchAPI.extraEndpoints` default to `null`, meaning they inherit `global.extraEndpoints`. Setting either override to a list replaces the global value for that component; an explicit `[]` disables configured extensions for that component.

The endpoint remains batch-scoped: every request URL in the input JSONL must exactly match the endpoint declared when creating the batch. The configured path is forwarded unchanged in synchronous and asynchronous dispatch modes. Omitting `global.extraEndpoints` preserves the default OpenAI-only behavior.

## Usage

### Method 1: Using a Custom Values File

```bash
cat > my-values.yaml <<EOF
apiserver:
  replicaCount: 3
  resources:
    requests:
      memory: 256Mi
      cpu: 200m
EOF

helm install batch-gateway ./charts/batch-gateway -f my-values.yaml
```

### Method 2: Using --set Flags

```bash
helm install batch-gateway ./charts/batch-gateway \
  --set apiserver.replicaCount=3 \
  --set apiserver.resources.requests.memory=256Mi
```

### Install on OpenShift

OpenShift uses Security Context Constraints (SCC) that assign UIDs dynamically. Override the podSecurityContext to use SCC-assigned UIDs:

```bash
cat > openshift-values.yaml <<EOF
apiserver:
  podSecurityContext: {}  # Let OpenShift SCC assign UIDs
  securityContext:
    allowPrivilegeEscalation: false
    capabilities:
      drop:
      - ALL
    readOnlyRootFilesystem: true

processor:
  podSecurityContext: {}  # Let OpenShift SCC assign UIDs
  securityContext:
    allowPrivilegeEscalation: false
    capabilities:
      drop:
      - ALL
    readOnlyRootFilesystem: true

gc:
  podSecurityContext: {}  # Let OpenShift SCC assign UIDs
  securityContext:
    allowPrivilegeEscalation: false
    capabilities:
      drop:
      - ALL
    readOnlyRootFilesystem: true
EOF

helm install batch-gateway ./charts/batch-gateway -f openshift-values.yaml
```

The chart will work with OpenShift's `restricted` SCC by default when podSecurityContext is empty.

## Accessing the Services

### API Server

For ClusterIP service type (default):

```bash
export POD_NAME=$(kubectl get pods -l "app.kubernetes.io/component=apiserver" -o jsonpath="{.items[0].metadata.name}")
kubectl port-forward $POD_NAME 8080:8000 8081:8081
curl http://localhost:8080/v1/batches  # API port
curl http://localhost:8081/health       # health check port
```

### Processor

The processor is a background worker and does not expose a service. To view logs:

```bash
kubectl logs -l "app.kubernetes.io/component=processor" -f
```

## Exposing the API Server

The chart creates a ClusterIP Service by default. To expose it externally, create your own Ingress or HTTPRoute.

### Using Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: batch-gateway-ingress
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  ingressClassName: nginx
  rules:
  - host: api.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: <release-name>-batch-gateway-apiserver
            port:
              number: 8000
  tls:
  - hosts:
    - api.example.com
    secretName: batch-gateway-tls
```

### Using Gateway API HTTPRoute

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: batch-gateway-route
spec:
  parentRefs:
  - name: my-gateway
    namespace: gateway-system
  hostnames:
  - api.example.com
  rules:
  - backendRefs:
    - name: <release-name>-batch-gateway-apiserver
      port: 8000
```

## Health Checks

### API Server
- **Liveness Probe**: `GET /health` on port 8081
- **Readiness Probe**: `GET /ready` on port 8081

### Processor
- **Liveness Probe**: `GET /health` on port 9090
- **Readiness Probe**: `GET /ready` on port 9090

## Monitoring & Dashboards

### Prometheus Metrics

Enable ServiceMonitor/PodMonitor for automatic Prometheus scraping:

```yaml
apiserver:
  serviceMonitor:
    enabled: true

processor:
  podMonitor:
    enabled: true
```

Requires the [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator) (included in OpenShift Monitoring).

### Prometheus Alerts

Enable built-in alert rules:

```yaml
prometheusRule:
  enabled: true
```

### Grafana Dashboards

The chart includes pre-built dashboards in `charts/batch-gateway/dashboards/`:
- **apiserver.json** — request rate, error rate, latency, in-flight requests
- **processor.json** — job throughput, queue wait, worker utilization, per-model metrics

**Option 1: Grafana sidecar (Kubernetes/OpenShift)**

Enable the dashboard ConfigMap and configure Grafana's sidecar to auto-load it:

```yaml
grafana:
  dashboards:
    enabled: true
```

The ConfigMap is labeled `grafana_dashboard: "1"` for sidecar discovery.

**Option 2: Manual import (any Grafana instance)**

1. Copy the JSON file from `charts/batch-gateway/dashboards/`
2. In Grafana UI: Dashboards → Import → paste JSON
3. Select your Prometheus datasource

**Option 3: OpenShift with Grafana Operator**

If the [Grafana Operator](https://github.com/grafana/grafana-operator) is installed, create a `GrafanaDashboard` CR referencing the JSON. This is not automated by the chart yet — see [#176](https://github.com/llm-d/llm-d-batch-gateway/issues/176) for updates.

## Security

The chart follows security best practices:
- Runs as non-root user (UID 65532 from Dockerfile by default)
- Uses read-only root filesystem
- Drops all Linux capabilities
- Prevents privilege escalation
- Uses seccomp RuntimeDefault profile

### OpenShift Compatibility

The chart is compatible with OpenShift's Security Context Constraints (SCC):
- Set `podSecurityContext: {}` to allow OpenShift to assign UIDs from the namespace range
- The minimal `securityContext` (drop all capabilities, no privilege escalation, read-only filesystem) works with the `restricted` SCC
- No additional SCC configuration is required

## License

Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0.
