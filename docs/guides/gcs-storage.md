# Google Cloud Storage (GCS) file backend

The Batch Gateway stores input and output files (uploaded via `POST /v1/files` and produced by the processor) in a pluggable object store. Three backends are supported:

| `fileClient.type` | Backend | Notes |
|-------------------|---------|-------|
| `fs` | PVC / local filesystem | Default; uses `os.Root` semantics. |
| `s3` | Any S3-compatible service | AWS S3, MinIO, **GCS via S3-interop (HMAC keys)**. |
| `gcs` | Google Cloud Storage (native) | **This guide.** Uses the `cloud.google.com/go/storage` SDK and **Application Default Credentials**. |

This guide covers the `gcs` backend. For the cookbook on the S3-interop path (reusing the `s3` backend against GCS's HMAC endpoint), see *Alternatives* at the end.

## Why native GCS?

The `gcs` backend authenticates with **Application Default Credentials (ADC)** — on GKE, that means **Workload Identity** — so there are **no long-lived HMAC access keys to rotate** and no shared S3-interop endpoints. It speaks the GCS JSON API directly, so it gets first-class GCS error semantics (`storage.ErrObjectNotExist`, `storage.ErrBucketNotExist`) and the SDK's built-in idempotent retry.

It implements the same `BatchFilesClient` interface as the `s3` and `fs` backends and is wired into the same tracing + retry wrappers, so it is a drop-in replacement for the file-storage role.

## Prerequisites

1. A GCP **project** with the Cloud Storage API enabled.
2. A **GCS bucket** — create it out-of-band (Terraform, `gcloud storage buckets create`) before deploying; `autoCreateBucket` is off by default and the gateway does not provision buckets by default. Bucket names are globally unique; pick a single bucket to share across tenants (the batch gateway keys objects as `<prefix>/<tenant-folder>/<file>`).
3. A **GCP service account** that the batch-gateway pods will impersonate via Workload Identity. Grant it `roles/storage.objectUser` on the bucket (object create/read/update/delete — the full `BatchFilesClient` surface). See *Auto-creating the bucket* below before granting anything broader.
4. A **GKE cluster** with [Workload Identity enabled](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).

## 1. Create the GCP service account and IAM binding

```bash
GCP_PROJECT=my-project
GSA_NAME=batch-gateway-gcs
BUCKET=llm-d-batch-gateway

# Service account
gcloud iam service-accounts create ${GSA_NAME} \
  --project=${GCP_PROJECT}
GSA_EMAIL="${GSA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"

# Object access on the bucket (Store / Retrieve / Delete) — least privilege
# (roles/storage.objectUser: create, read, update, delete objects)
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
  --member="serviceAccount:${GSA_EMAIL}" \
  --role="roles/storage.objectUser" \
  --project=${GCP_PROJECT}

# Startup bucket check (storage.buckets.get) — needed because
# verifyBucketOnStart defaults to true in the chart. Skip this binding and set
# verifyBucketOnStart: false if the SA should hold no bucket-level permissions.
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
  --member="serviceAccount:${GSA_EMAIL}" \
  --role="roles/storage.bucketViewer" \
  --project=${GCP_PROJECT}
```

### Auto-creating the bucket (opt-in)

`autoCreateBucket` defaults to **false**: the gateway expects the bucket to
already exist. It is **not required** for the feature — it exists as opt-in
convenience for dev/demo setups.

When enabled, the service account gets `roles/storage.admin` (project-scoped;
bucket creation is a project-level `storage.buckets.create` call) — and that
grant persists for the lifetime of the deployment, including after the bucket
exists: the gateway only ever *adds* IAM bindings when auto-creating (never strips
or re-evaluates them), so the elevated permission remains until an operator
revokes it. Treat `autoCreateBucket: true` accordingly:

- Prefer it only where `roles/storage.admin` is acceptable (dev, demo, throwaway
  clusters).
- In shared or production projects, create the bucket out-of-band and keep the
  SA on `objectUser` (+ `bucketViewer` for the startup check).
- If you used auto-create to bootstrap and want to drop back to least privilege,
  remove the `storage.admin` binding once the bucket exists.

```bash
# Required only for the opt-in auto-create path (roles/storage.admin
# includes storage.buckets.create at the project level)
gcloud projects add-iam-policy-binding ${GCP_PROJECT} \
  --member="serviceAccount:${GSA_EMAIL}" \
  --role="roles/storage.admin"
```

## 2. Bind the GCP SA to the Kubernetes ServiceAccount (Workload Identity)

The Helm chart already creates per-component Kubernetes ServiceAccounts (`apiserver.serviceAccount`, `processor.serviceAccount`, `gc.serviceAccount`). Each one needs its `iam.gke.io/gcp-service-account` annotation pointing at the GCP SA, plus a GCP-side policy binding between the KSA and GSA.

```bash
KSA_NAMESPACE=batch-gateway
GSA_NAME=batch-gateway-gcs
GSA_EMAIL="${GSA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"

# Allow the Kubernetes SA in this namespace to impersonate the GCP SA
gcloud iam service-accounts add-iam-policy-binding ${GSA_EMAIL} \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${GCP_PROJECT}.svc.id.goog[${KSA_NAMESPACE}/batch-gateway-apiserver]" \
  --project=${GCP_PROJECT}
```

Repeat the `add-iam-policy-binding` for `batch-gateway-processor` and `batch-gateway-gc`. Then in Helm:

```yaml
apiserver:
  serviceAccount:
    annotations:
      iam.gke.io/gcp-service-account: "batch-gateway-gcs@my-project.iam.gserviceaccount.com"
processor:
  serviceAccount:
    annotations:
      iam.gke.io/gcp-service-account: "batch-gateway-gcs@my-project.iam.gserviceaccount.com"
gc:
  serviceAccount:
    annotations:
      iam.gke.io/gcp-service-account: "batch-gateway-gcs@my-project.iam.gserviceaccount.com"
```

> No `s3-secret-access-key` (or any GCS secret key) is required in the app Secret. The pod gets its GCP identity from the Kubernetes ServiceAccount via Workload Identity automatically. To override for local development, set `GOOGLE_APPLICATION_CREDENTIALS` in the container environment to a SA JSON file path.

## 3. Set the file client type and GCS config

In Helm (`global.fileClient`):

```yaml
global:
  fileClient:
    type: "gcs"
    gcs:
      bucket: "llm-d-batch-gateway"
      prefix: "batch-gateway"   # optional key prefix
      projectId: ""             # only required when autoCreateBucket is true
      autoCreateBucket: false   # bucket must pre-exist; see Auto-creating the bucket (opt-in)
      verifyBucketOnStart: true # HeadBucket at startup; requires roles/storage.bucketViewer,
                               # or set false when the SA holds object-level roles only
    retry:
      maxRetries: 3
      initialBackoff: "1s"
      maxBackoff: "10s"
```

Or in a raw YAML config (`cmd/*/config.yaml`):

```yaml
file_client:
  type: "gcs"
  gcs:
    bucket: "llm-d-batch-gateway"
    prefix: "batch-gateway"
    project_id: ""              # only required when auto_create_bucket is true
    auto_create_bucket: false
```

## Behavior notes

- **Single bucket, key prefix for tenant isolation.** Keys are `<prefix>/<tenant-folder>/<file>`, where `<tenant-folder>` is a deterministic SHA256 hash of the tenant ID (`com.GetFolderNameByTenantID`). This matches the `s3` backend.
- **Store uses atomic conditional writes.** Instead of the S3 backend's HeadObject-then-Put pattern, `Store` issues a single GCS PUT with an `IfDoesNotExist` precondition: GCS rejects the write with `412 Precondition Failed` if the object already exists, which the client maps to `ErrFileExists`. This eliminates the HeadObject/Put race window a two-step approach has — concurrent writers cannot both pass an existence check and overwrite each other; the loser of the race receives `ErrFileExists` without touching the winner's object. (The `s3` and `fs` backends retain their two-step patterns; GCS uniquely offers a conditional put whose failure surfaces as a distinguishable error.)
- **Size/line limit rejections are all-or-nothing.** When an upload trips the size or line limit mid-stream the client cancels the writer's context, aborting the SDK's resumable session — no partial object is committed, and a retry under the same filename starts from a clean not-exists (the `s3` backend's uploader similarly fails without leaving an object behind).
- **Delete of a missing object** returns `os.ErrNotExist`; the apiserver cleanup path logs-and-continues and the GC reconciler explicitly tolerates this (`errors.Is(err, os.ErrNotExist)`).
- **Auto-create** calls `BucketHandle.Create` if `Attrs` fails with `storage.ErrBucketNotExist`. Bucket creation needs `projectId` and is project-scoped. Auto-create only runs from the startup HeadBucket path, so it is gated on `verify_bucket_on_start: true`. A config with `auto_create_bucket: true` but `verify_bucket_on_start: false` is rejected by `Validate()` (the flag would otherwise be silently dropped — the verify=false path's whole purpose is to skip all bucket-level calls, which is incompatible with creating a bucket).

## Verifying

After a `helm install` / `make dev-deploy`:

1. The apiserver log line `GCS file client created bucket=...` should appear on startup.
2. `POST /v1/files` to upload a small JSONL, then `POST /v1/batches` referencing the returned file id.
3. Download the output via `GET /v1/files/<id>/content` once the job reaches `completed`.
4. Confirm the object exists in the bucket under the configured prefix:
   ```bash
   gcloud storage ls gs://llm-d-batch-gateway/batch-gateway/
   ```

## Testing

The GCS backend has a standalone integration test suite (`test/integration/gcs_client_test.go`) that exercises `Store`/`Retrieve`/`Delete`, the atomic conditional write (`ErrFileExists` on overwrite), the size/line limits, prefixed keys, and the `VerifyBucketOnStart=true` fail-fast path.

- **No GCP credentials needed (recommended):** `make test-integration-gcs` auto-provisions a [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) container via Docker/Podman, points the SDK at it via `STORAGE_EMULATOR_HOST`, and runs every `TestGCS*` test. The emulator image is pinned by digest for reproducibility. The suite skips gracefully if no container runtime is available, so `make test-integration` stays green on machines without Docker.
- **Against real GCS:** set `GCS_TEST_PROJECT` and `GCS_TEST_BUCKET` and run `make test-integration` (the SDK uses Application Default Credentials — typically `gcloud auth application-default login` locally, or Workload Identity in GKE).
- **Against a manually-started emulator:** `docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http -port 4443 -public-host localhost:4443`, then `STORAGE_EMULATOR_HOST=http://localhost:4443 GCS_TEST_BUCKET=my-bucket make test-integration`.

The `gcs-integration-tests` CI job runs `make test-integration-gcs` on every push to `main`/`release-*` and nightly, so emulator regressions (e.g. a fake-gcs-server upgrade that stops honoring the `IfDoesNotExist` precondition) are caught before they reach a release.

## Alternatives

- **GCS via S3-interop + HMAC** — keep `fileClient.type: "s3"` and point it at `https://storage.googleapis.com` with HMAC access-key credentials. Zero application code change, fine for batch upload/download, but requires HMAC-key lifecycle management and uses the S3 wire protocol rather than GCS-native auth.
- **gcsfuse / Cloud Storage FUSE → `fs` backend** — mount the bucket as a POSIX directory and use `fileClient.type: "fs"`. No code change, but the `fs` backend relies on `O_EXCL` + atomic `rename` semantics that gcsfuse emulates imperfectly; safe with a single processor replica, riskier with concurrent writers.
