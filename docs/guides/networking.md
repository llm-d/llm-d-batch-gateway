# Networking

## 1. API Server

The API Server runs two listeners on separate ports. The API port serves REST endpoints with optional TLS, while the observability port serves health, readiness, and metrics over plain HTTP.

### 1.1 TLS Configuration

TLS is disabled by default. Choose **one** of the following options to enable it.

#### Option A: Pre-existing Kubernetes Secret (`secretName`)

Use this when you already have a `kubernetes.io/tls` Secret in the cluster, for example one managed by HashiCorp Vault, an enterprise PKI, or shared across multiple services.

```yaml
apiserver:
  tls:
    enabled: true
    secretName: "my-existing-tls-secret"
```

The Secret must contain `tls.crt` and `tls.key` entries:

```bash
kubectl create secret tls my-existing-tls-secret \
  --cert=server.crt \
  --key=server.key
```

#### Option B: cert-manager (`certManager`)

Use this for automated certificate issuance and renewal via [cert-manager](https://cert-manager.io/). cert-manager must be installed in the cluster.

```yaml
apiserver:
  tls:
    enabled: true
    certManager:
      enabled: true
      issuerName: "letsencrypt-prod"
      issuerKind: ClusterIssuer        # or Issuer
      dnsNames:
        - "batch-api.example.com"
```

The chart creates a `Certificate` resource; cert-manager handles issuance, storage, and rotation of the TLS Secret automatically.

### 1.2 Endpoints

#### API port (default 8000)

Base URL: `http(s)://<host>:8000` — scheme depends on TLS configuration.

| Endpoint | Description |
|---|---|
| `POST /v1/batches` | Create a batch |
| `GET /v1/batches/{id}` | Get batch status |
| `POST /v1/batches/{id}/cancel` | Cancel a batch |
| `POST /v1/files` | Upload a file |
| `GET /v1/files/{id}` | Get file metadata |
| `GET /v1/files/{id}/content` | Download file content |

#### Observability port (default 8081)

Base URL: `http://<host>:8081` — always plain HTTP.

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `GET /ready` | Readiness check |
| `GET /metrics` | Prometheus metrics |

### 1.3 NetworkPolicy

The API server trusts the tenant header that the auth proxy injects, so only the proxy should reach the API port. The chart can create a `NetworkPolicy` for the API server pods that allows ingress to each port only from the peers you list. It is off by default, because the chart cannot know where the gateway runs.

```yaml
apiserver:
  networkPolicy:
    enabled: true
    api:
      # Who may reach the API port (default 8000): the gateway or auth proxy.
      from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: gateway-system
    observability:
      # Who may reach the observability port (default 8081): Prometheus.
      from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: monitoring
```

Each `from` is a list of [NetworkPolicyPeer](https://kubernetes.io/docs/reference/kubernetes-api/policy-resources/network-policy-v1/#NetworkPolicyPeer) entries, so `namespaceSelector`, `podSelector` and `ipBlock` all work.

- `api.from` is required. The chart fails to render when the policy is enabled without it.
- An empty `observability.from` allows no peer on the observability port. Liveness and readiness probes come from the kubelet on the pod's node, which network plugins allow regardless of the policy.
- Traffic that enters through a `NodePort` or `LoadBalancer` Service is not from a pod, so namespace and pod selectors do not match it. Allow it with an `ipBlock` peer if you expose the API server that way.
- The policy covers ingress only. Egress from the API server is not restricted.
- The cluster's network plugin has to enforce `NetworkPolicy`. Without one that does, the object is created and has no effect.
- Other pods in the release namespace are not allowed unless a peer selects them. That includes the processor, which does not call the API server.

## 2. Processor

The Processor is a background worker and only serves observability endpoints. It does not support TLS.

### 2.1 Endpoints

#### Observability port (default 9090) — always HTTP

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `GET /ready` | Readiness check |
| `GET /metrics` | Prometheus metrics |
