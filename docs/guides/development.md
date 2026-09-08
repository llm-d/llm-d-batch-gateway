# Development

This guide walks through deploying both the **batch-gateway-apiserver** and **batch-gateway-processor** in a Kind(Kubernetes in Docker) cluster for local development and testing.

## Prerequisites

- [Make]
- [Golang]
- [Docker] (or [Podman])
- [Kind]
- [Helm]
- [kubectl]

[Make]:https://www.gnu.org/software/make/
[Golang]:https://go.dev/
[Docker]:https://www.docker.com/
[Podman]:https://podman.io/
[Kind]:https://github.com/kubernetes-sigs/kind
[Helm]:https://helm.sh
[kubectl]:https://kubernetes.io/docs/tasks/tools/

## 1. Create a Kind Cluster

```bash
$ kind create cluster --name batch-gateway-dev
```

Switch context to the Kind cluster and verify the cluster is up:

```bash
$ kubectl cluster-info --context kind-batch-gateway-dev

$ kubectl get nodes
```

## 2. Build Container Images

From the repository root, build both images. The Makefile auto-detects Docker or Podman.

```bash
$ make image-build
```

To force Podman:

```bash
$ CONTAINER_TOOL=podman make image-build
```

This produces:

- `ghcr.io/llm-d/batch-gateway-apiserver:0.0.1`
- `ghcr.io/llm-d/batch-gateway-processor:0.0.1`

To use a different tag:

```bash
IMAGE_TAG=dev make image-build
```

## 3. Load Images to Kind

Kind runs in Docker/Podman, so load the local images into the cluster.

### Docker

```bash
# load apiserver image
$ kind load docker-image ghcr.io/llm-d/batch-gateway-apiserver:0.0.1 --name batch-gateway-dev

# load processor image
$ kind load docker-image ghcr.io/llm-d/batch-gateway-processor:0.0.1 --name batch-gateway-dev
```

### Podman

If `kind load docker-image` does not work with your Podman setup (e.g. no Docker API socket), save images to a tar and load the archive:

```bash
# save images to tar
$ podman save -o /tmp/apiserver.tar ghcr.io/llm-d/batch-gateway-apiserver:0.0.1
$ podman save -o /tmp/processor.tar ghcr.io/llm-d/batch-gateway-processor:0.0.1

# load into Kind
$ kind load image-archive /tmp/apiserver.tar --name batch-gateway-dev
$ kind load image-archive /tmp/processor.tar --name batch-gateway-dev
```

If you built with a custom tag (e.g. `dev`), use that tag in both `make image-build` and the load commands above.

## 4. Deploy with Helm

The chart defaults to **apiserver only**. To run both apiserver and processor using the images loaded in #3, override the values.

```bash
$ helm install batch-gateway ./charts/batch-gateway \
    --set apiserver.image.pullPolicy=IfNotPresent \
    --set apiserver.image.tag=0.0.1 \
    --set processor.enabled=true \
    --set processor.image.pullPolicy=IfNotPresent \
    --set processor.image.tag=0.0.1 \
    --namespace default
```

## 5. Verify Deployment

The PostgreSQL schema is created by a `migrate` initContainer on each pod; see
[Database Schema Migrations](database-migrations.md) for how it works and how
to add a migration.


```bash
# Pods
$ kubectl get pods -l app.kubernetes.io/name=batch-gateway-apiserver
$ kubectl get pods -l app.kubernetes.io/name=batch-gateway-processor

# Services
$ kubectl get svc -l app.kubernetes.io/instance=batch-gateway
```

Check apiserver health:

```bash
# forward traffic from cluster port to your machine port
$ kubectl port-forward svc/batch-gateway-batch-gateway-apiserver 8000:8000

# In another terminal:
$ curl -s http://localhost:8000/health
```

## 6. Rebuild and Reloading images

After code changes:

**Docker:**

```bash
# rebuild images
$ make image-build

# load images with docker
$ kind load docker-image ghcr.io/llm-d/batch-gateway-apiserver:0.0.1 --name batch-gateway-dev
$ kind load docker-image ghcr.io/llm-d/batch-gateway-processor:0.0.1 --name batch-gateway-dev

# load images with podman
$ podman save -o /tmp/apiserver.tar ghcr.io/llm-d/batch-gateway-apiserver:0.0.1
$ podman save -o /tmp/processor.tar ghcr.io/llm-d/batch-gateway-processor:0.0.1
$ kind load image-archive /tmp/apiserver.tar --name batch-gateway-dev
$ kind load image-archive /tmp/processor.tar --name batch-gateway-dev

# reload deployment
$ kubectl rollout restart deployment -l app.kubernetes.io/instance=batch-gateway
```

## 7. Cleanup

```bash
# removes all resources
$ helm uninstall batch-gateway

# deletes the Kind cluster
$ kind delete cluster --name batch-gateway-dev
```
<!-- TODO: Update the file to add any additional setup if required for batch processor -->
