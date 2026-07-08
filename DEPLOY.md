# Deploy Guide

This is the production deploy checklist for the Helm/Jenkins path. The raw
`deploy/*.yaml` manifests are for local/manual validation only.

## What Creates What

Not every value is created in Helm.

- Kubernetes Secret data is normally pre-created in the target namespace. The
  current QA6 release uses `secrets.create=false`, so Helm expects the Secret to
  already exist.
- GitLab CI variables are pipeline inputs. The deploy job converts them into
  Helm `--set` overrides.
- Helm values decide which Kubernetes objects are rendered: Deployments,
  StatefulSets, Services, PVCs, Ingress, RBAC, and optionally the internal
  template registry.

Helm can create the app Secret only if you set `secrets.create=true` and provide
all secret values in Helm values. That is not the current QA6 production mode.

## Required Kubernetes Secret

For QA6, create this Secret in `spr-apps` before deploying:

```sh
kubectl -n spr-apps create secret generic sndbx-qa6-tier1-secret \
  --from-literal=API_KEY='<api-key>' \
  --from-literal=INTERNAL_API_KEY='<internal-api-key>' \
  --from-literal=PORTAL_SESSION_SECRET='<session-secret>' \
  --from-literal=DATABASE_TYPE='postgres' \
  --from-literal=DATABASE_URL='<host>:5432/<db>?sslmode=require&connect_timeout=10' \
  --from-literal=DATABASE_USERNAME='<postgres-user>' \
  --from-literal=DATABASE_PASSWORD='<postgres-password>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

For MongoDB, set `DATABASE_TYPE=mongo`. `DATABASE_USERNAME` can be empty only
when your MongoDB deployment does not require username/password auth, or when
the username is already present in `DATABASE_URL` such as
`mongodb+srv://<mongodb-user>@<cluster-host>/<db>`. A password-only MongoDB URI
is not valid; if `DATABASE_PASSWORD` is set, the app must have a username from
either `DATABASE_USERNAME` or the URL userinfo. The URI must include a database
name, or the pod must set `MONGODB_DATABASE`.

```sh
kubectl -n spr-apps create secret generic sndbx-qa6-tier1-secret \
  --from-literal=API_KEY='<api-key>' \
  --from-literal=INTERNAL_API_KEY='<internal-api-key>' \
  --from-literal=PORTAL_SESSION_SECRET='<session-secret>' \
  --from-literal=DATABASE_TYPE='mongo' \
  --from-literal=DATABASE_URL='mongodb+srv://<mongodb-user>@<cluster-host>/<db>?retryWrites=true&w=majority' \
  --from-literal=DATABASE_USERNAME='' \
  --from-literal=DATABASE_PASSWORD='<mongodb-password>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

Required app Secret keys are now only:

- `API_KEY`
- `INTERNAL_API_KEY`
- `PORTAL_SESSION_SECRET`
- `DATABASE_TYPE`
- `DATABASE_URL`
- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`

`DATABASE_TYPE` must be `postgres` or `mongo`. `DATABASE_URL` can be a full DSN
or a host/path endpoint. The app injects `DATABASE_USERNAME` and
`DATABASE_PASSWORD` into the URL when credentials are not already present. For
MongoDB with a separate password and empty username, the URL itself must contain
the username.
SQLite is intentionally not supported.

Template registry credentials are not required in the production default. Built
template images are pushed to the chart-managed in-cluster registry pod.

`api-service` must not be given per-shard `DOCKER_HOST` values. In the Helm
deployment it talks to runtime-gateway over HTTP only. The dockerd sidecar binds
to `127.0.0.1:2375` inside each runtime-gateway pod and is not exposed through a
Kubernetes Service.

## AWS Aurora Postgres

AWS Aurora PostgreSQL works with the current app path. Use standard Postgres
username/password authentication through `DATABASE_USERNAME` and
`DATABASE_PASSWORD`.

Recommended `DATABASE_URL` shape:

```text
<aurora-writer-endpoint>:5432/<db>?sslmode=require&connect_timeout=10
```

Use the Aurora writer endpoint for normal operation. A reader endpoint is not
valid because api-service writes sandbox, warm-pool, template, client, and lease
state.

Authentication handling:

- The app builds a PostgreSQL DSN from `DATABASE_TYPE`, `DATABASE_URL`,
  `DATABASE_USERNAME`, and `DATABASE_PASSWORD`, then passes it to `psycopg`.
- Username/password auth is handled by `psycopg` and Aurora.
- TLS is handled by `psycopg` when `sslmode=require` is present in the URL.
- Special characters in `DATABASE_PASSWORD` are URL encoded by the app.
- IAM database authentication is not implemented in the app because IAM tokens
  expire quickly and need token refresh logic. Use normal DB password auth, or
  put an RDS Proxy in front if your platform requires credential rotation.

Before deploying with Aurora, confirm:

- The EKS/GKE/VPC network route can reach the Aurora endpoint on port `5432`.
- The Aurora security group allows inbound `5432` from the cluster/node/pod CIDR
  or the worker-node security group.
- The database user can create/alter tables and indexes in the target database.
- Automated backups and storage monitoring are enabled on Aurora.
- `DATABASE_URL`, `DATABASE_USERNAME`, and `DATABASE_PASSWORD` are stored only
  in the Kubernetes Secret, not in ConfigMaps.

## MongoDB

MongoDB works by setting `DATABASE_TYPE=mongo`.

Recommended URL shapes:

```text
mongodb+srv://<cluster-host>/<db>?retryWrites=true&w=majority
mongodb+srv://<mongodb-user>@<cluster-host>/<db>?retryWrites=true&w=majority
mongodb://<host>:27017/<db>?authSource=admin
```

Operational behavior:

- The app creates MongoDB collections and indexes on startup.
- Sandbox, template, client, API key, command history, warm-pool, lease, and
  snapshot state use MongoDB collections with the same method behavior as the
  PostgreSQL path.
- Warm-pool leadership uses an atomic MongoDB lock document with a refreshable
  expiry instead of PostgreSQL advisory locks.
- The MongoDB user needs read/write plus index creation privileges on the target
  database.
- `mongodb+srv://` requires the `pymongo[srv]` dependency, which is installed in
  the api-service image.

Before deploying with MongoDB, confirm:

- The cluster network can reach the MongoDB hosts or Atlas private endpoint.
- The database name is present in the URI path, or `MONGODB_DATABASE` is set.
- If `DATABASE_PASSWORD` is set, MongoDB credentials include a username either
  in `DATABASE_USERNAME` or in `DATABASE_URL` userinfo.
- TLS and replica-set options required by your provider are present in the URI.
- `DATABASE_URL`, `DATABASE_USERNAME`, and `DATABASE_PASSWORD` are stored only
  in Kubernetes Secrets, not ConfigMaps.

## GitLab CI Variables

These are GitLab pipeline variables, not Kubernetes Secret keys:

- `DEPLOY_SNDBX=true`
- `DOCKER_REPO=asia-south1-docker.pkg.dev/gc-qa6/gc-qa6`
- `DATABASE_SECRET_NAME=sndbx-qa6-tier1-secret`
- `DATABASE_SECRET_KEY=DATABASE_URL`
- `TEMPLATE_REGISTRY_INTERNAL_ENABLED=auto`
- `TEMPLATE_REGISTRY_REPO_PREFIX=`
- `TEMPLATE_REGISTRY_SERVER=`
- `TEMPLATE_REGISTRY_AUTH_REQUIRED=false`
- `TEMPLATE_REGISTRY_LAYOUT=auto`
- `TEMPLATE_REGISTRY_IMAGE_NAME=template-registry`

The GitLab job builds four images through Jenkins and then calls the custom
Helm deploy job with these values.

Keep `DATABASE_SECRET_NAME` and `DATABASE_SECRET_KEY` pointing at the same
Kubernetes Secret and `DATABASE_URL` key. The chart reads `DATABASE_TYPE`,
`DATABASE_USERNAME`, and `DATABASE_PASSWORD` from that same Secret by default.

## Registry Mode A: Internal Registry

Use this when you do not want to provide external registry credentials for
template images.

Set GitLab variables:

```sh
TEMPLATE_REGISTRY_INTERNAL_ENABLED=auto
TEMPLATE_REGISTRY_REPO_PREFIX=
TEMPLATE_REGISTRY_SERVER=
TEMPLATE_REGISTRY_AUTH_REQUIRED=false
```

With `templateRegistry.pushEnabled=true`, Helm creates:

- `sndbx-qa6-tier1-template-registry` Deployment
- `sndbx-qa6-tier1-template-registry` Service
- `sndbx-qa6-tier1-template-registry-data` PVC

The registry pod image is built by the Jenkins pipeline from
`template-registry/Dockerfile`, which wraps the standard Docker distribution
registry image. The deploy job passes that built image to Helm as
`templateRegistry.internal.image`.

For manual deploys where no CI-built registry image is passed, the chart falls
back to the configured production image repo, rendered as:

```text
<images.apiService.repo>/registry:3
```

Set `templateRegistry.internal.image` directly if you want a different
pre-existing image ref.

Runtime-gateway then pushes template images to:

```text
sndbx-qa6-tier1-template-registry.spr-apps.svc.cluster.local:5000/templates
```

The Docker daemon sidecar is rendered with:

```text
--insecure-registry=sndbx-qa6-tier1-template-registry.spr-apps.svc.cluster.local:5000
```

This registry is internal ClusterIP only. It is not exposed through ingress.

Optional pull-through cache mode can front an upstream registry:

```sh
templateRegistry.internal.proxy.enabled=true
templateRegistry.internal.proxy.remoteUrl=https://registry.example.com
templateRegistry.internal.proxy.remoteServer=registry.example.com
```

In this mode runtime-gateway can pull a fully-qualified upstream image through
the internal registry cache, tag it back to the original ref locally, and then
create the sandbox using the original image ref. This cache is shared across
runtime-gateway shards; Docker's normal layer cache is still per-shard/PVC.

## External Registry Mode

External template registries are intentionally not part of the current
production path. Leave `templateRegistry.repoPrefix` empty and
`templateRegistry.authRequired=false` so built templates stay in the internal
registry pod and no ECR/template-registry credentials are required.

## Ingress

Helm creates one Ingress when `ingress.enabled=true`.

For QA6 it renders:

- Ingress name: `sndbx-qa6-tier1-ingress`
- Namespace: `spr-apps`
- Ingress class: `ingress-nginx-office`
- TLS secret: `sndbx-sprinklr-com-tls`

The ingress routes are:

- `https://api.sndbx.sprinklr.com/` -> `sndbx-qa6-tier1-api-service:8000`
- `https://*.sndbx.sprinklr.com/` -> `sndbx-qa6-tier1-runtime-gateway:8080`

Sandbox data-plane requests use hostnames like:

```text
https://<guest-port>-<sandbox-id>.sndbx.sprinklr.com/
```

Runtime-gateway parses the host, asks api-service for the sandbox route, and
then proxies traffic to the correct running sandbox.

Before deploy, confirm:

- `api.sndbx.sprinklr.com` DNS points to the ingress controller/load balancer.
- `*.sndbx.sprinklr.com` DNS points to the same ingress controller/load balancer.
- `sndbx-sprinklr-com-tls` exists in `spr-apps` and covers both the API host and
  wildcard sandbox host.
- The ingress controller named `ingress-nginx-office` exists in the cluster.

## Direct Helm Validation

Render QA6 with test image tags:

```sh
helm lint sndbx -f sndbx/releases/qa6-tier1/values.yaml \
  --set images.apiService.tag=test \
  --set images.runtimeGateway.tag=test \
  --set images.dockerDind.tag=test
```

Render internal-registry mode:

```sh
helm template internal-reg sndbx \
  --set secrets.name=sndbx-qa6-tier1-secret \
  --set images.apiService.tag=test \
  --set images.runtimeGateway.tag=test \
  --set images.dockerDind.tag=test
```

Render MongoDB mode with an existing Secret:

```sh
helm template mongo sndbx \
  --set secrets.name=sndbx-qa6-tier1-secret \
  --set images.apiService.tag=test \
  --set images.runtimeGateway.tag=test \
  --set images.dockerDind.tag=test
```

Expected intentional failures:

- Missing `DATABASE_URL` source: no `secrets.name`, no Helm-created Secret, and
  no `database.url`
- Unsupported `database.type`
- `templateRegistry.pushEnabled=true`, empty repo prefix, and
  `templateRegistry.internal.enabled=false`

## Deploy Flow

1. Push application code to the GitHub repo configured in `.gitlab-ci.yml`.
2. Push the `sndbx` chart to the Helm internal tools repo branch used by Jenkins.
3. Create/update `sndbx-qa6-tier1-secret` in `spr-apps`.
4. Set GitLab CI variables for the internal registry path.
5. Run the GitLab pipeline with `DEPLOY_SNDBX=true`.
6. Jenkins builds `api-service`, `runtime-gateway`, `dockerd-gvisor`, and `template-registry`.
7. Jenkins deploys Helm release `sndbx-qa6-tier1` into `spr-apps`.
8. Verify pods, services, ingress, and registry mode:

```sh
kubectl -n spr-apps get pods,svc,ingress,pvc | grep sndbx-qa6-tier1
kubectl -n spr-apps get secret sndbx-qa6-tier1-secret
kubectl -n spr-apps describe ingress sndbx-qa6-tier1-ingress
```
