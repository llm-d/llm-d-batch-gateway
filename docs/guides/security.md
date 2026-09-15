# Security

This guide provides an overview of the security features in batch-gateway. It covers what is built in, what requires configuration, and links to detailed guides for each area.

## 1. Authentication

Batch-gateway delegates authentication to an external system (e.g. Envoy `ext_authz`, Kuadrant/Authorino). The API server itself does not validate credentials — it trusts that authenticated identity attributes (tenant, tier, username) are injected as HTTP headers by the upstream gateway. The auth proxy is therefore a **hard security prerequisite**; see [Multi-Tenancy Isolation](#2-multi-tenancy-isolation) for implications.

The tenant header name is configurable (`X-MaaS-Username` by default). The middleware takes the **last** entry when multiple values are present, preventing callers from injecting a spoofed identity ahead of the gateway-injected one.

Three authentication options are documented in detail:

| Option | Mechanism | Cluster Requirement |
|--------|-----------|---------------------|
| API Key | Kuadrant API Key Secrets + OPA Rego | Any Kubernetes |
| ServiceAccount Token | Kubernetes TokenReview + SubjectAccessReview | Any Kubernetes |
| User Token | OpenShift user tokens + SubjectAccessReview | OpenShift only |

See [Kuadrant Integration](kuadrant-integration.md) for full setup instructions.

## 2. Multi-Tenancy Isolation

> **Security prerequisite**: The API server has no built-in authentication. Tenant identity is derived entirely from headers injected by the upstream auth proxy (e.g. Envoy, Kuadrant/Authorino). Direct access to the API server bypasses all tenant isolation. Deployments **must** ensure the API server is not reachable without passing through the auth proxy.

All data access is scoped to the authenticated tenant:

- Every database query filters by tenant ID.
- File storage paths use SHA256-based folder names derived from the tenant ID, preventing path traversal between tenants.
- Cross-tenant access attempts return **404** (not 403), avoiding tenant enumeration.
- Batch and file records are tagged with the tenant ID at creation time.

## 3. TLS

### 3.1 API Server (Inbound)

TLS is disabled by default. When enabled, the API server enforces TLS 1.2 as the minimum version. Two provisioning methods are supported:

- **Pre-existing Kubernetes Secret** — mount a `kubernetes.io/tls` Secret.
- **cert-manager** — automated certificate issuance and renewal.

See [Networking](networking.md) for configuration details.

### 3.2 Processor (Outbound to llm-d Router)

The processor supports per-gateway TLS configuration:

- Custom CA certificates for private PKI.
- mTLS with client certificate and key.
- cert-manager-managed Secrets for automated rotation.
- Configurable via `globalInferenceGateway` (shared) or `modelGateways` (per-model).

See [Processor Inference TLS](processor-inference-tls.md) for scenario-by-scenario setup.

### 3.3 Database Connections

- **Redis**: optional TLS via `global.dbClient.redis.enableTLS` in Helm values.
- **PostgreSQL**: TLS is configured through the connection URL (e.g. `?sslmode=require`).

## 4. Input Validation

The API server validates all inbound data before processing:

| Check | Default |
|-------|---------|
| Max file content size | 200 MB |
| Max upload request body, including multipart framing and fields | File size limit + 1 MiB (also enforced for chunked uploads) |
| Max lines per input file | 50,000 |
| JSON decoding | Strict (`DisallowUnknownFields`) |
| File purpose | Must be a known enum value |
| `expires_after` | `anchor` and `seconds` required together |

## 5. HTTP Server Hardening

The API server configures defensive timeouts and limits:

| Setting | Default | Purpose |
|---------|---------|---------|
| `MaxHeaderBytes` | 1 MB | Limits header size |
| `ReadHeaderTimeout` | 10s | Mitigates Slowloris attacks |
| `ReadTimeout` | Configurable | Bounds total request read time |
| `WriteTimeout` | Configurable | Bounds response write time |
| `IdleTimeout` | Configurable | Closes idle keep-alive connections |

## 6. Security Headers

The security headers middleware sets the following on every response:

- `X-Content-Type-Options: nosniff` — prevents MIME-type sniffing.
- `X-Frame-Options: DENY` — prevents clickjacking.
- `X-XSS-Protection: 1; mode=block` — enables browser XSS filtering.

CORS preflight (`OPTIONS`) requests receive a `204 No Content` response.

## 7. Secret Management

Sensitive values (database URLs, API keys) are stored in a Kubernetes Secret and mounted read-only at `/etc/.secrets/` in each container. The application reads secrets using `os.OpenInRoot()`, which prevents path traversal outside the mount directory.

Expected secret keys:

| Key | Purpose |
|-----|---------|
| `redis-url` | Redis connection URL |
| `postgresql-url` | PostgreSQL connection URL |
| `inference-api-key` | Global llm-d Router API key |
| `s3-secret-access-key` | S3 secret access key |

Per-model API keys can also be loaded from arbitrary file paths via `api_key_file` in the gateway configuration.

## 8. Pod Security

The Helm chart defaults enforce a restricted container security posture:

```yaml
# Pod-level
podSecurityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault

# Container-level
securityContext:
  allowPrivilegeEscalation: false
  capabilities:
    drop:
    - ALL
  readOnlyRootFilesystem: true
```

These defaults are compatible with OpenShift's `restricted-v2` SCC and Kubernetes Pod Security Standards (`restricted` profile).

## 9. Rate Limiting

Rate limiting is delegated to Kuadrant/Limitador at the gateway layer:

- **Batch route**: request-count based (`RateLimitPolicy`).
- **LLM route**: token-based (`TokenRateLimitPolicy`), counting LLM tokens consumed in inference responses.

Both support per-tier limits using identity attributes extracted by AuthPolicy. See [Kuadrant Integration](kuadrant-integration.md) for configuration.

## 10. Observability and Audit

- **Request IDs**: a UUID is generated for each request and propagated via the `x-request-id` header.
- **Structured logging**: every log entry includes tenant ID, request ID, batch ID, and file ID where applicable.
- **Distributed tracing**: OpenTelemetry integration with configurable OTLP endpoint. Redis and PostgreSQL operations are traced when enabled.
- **Metrics**: Prometheus metrics exposed on the observability port. See [Metrics](metrics.md).

The observability port is always plain HTTP and should not be exposed externally. See [Networking](networking.md) for the port layout.

## 11. Vulnerability Reporting

Security vulnerabilities should be reported following the process in [SECURITY.md](../../SECURITY.md).
