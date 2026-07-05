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
  --from-literal=DATABASE_URL='postgresql://<user>:<password>@<host>:5432/<db>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

If you use an external authenticated template registry, add these keys to the
same Secret or to the Secret referenced by `templateRegistry.existingSecretName`:

```sh
kubectl -n spr-apps create secret generic sndbx-qa6-tier1-secret \
  --from-literal=API_KEY='<api-key>' \
  --from-literal=INTERNAL_API_KEY='<internal-api-key>' \
  --from-literal=PORTAL_SESSION_SECRET='<session-secret>' \
  --from-literal=DATABASE_URL='postgresql://<user>:<password>@<host>:5432/<db>' \
  --from-literal=TEMPLATE_REGISTRY_USERNAME='AWS' \
  --from-literal=TEMPLATE_REGISTRY_PASSWORD='<ecr-login-password>' \
  --dry-run=client -o yaml | kubectl apply -f -
```

`DATABASE_URL` must use `postgres://` or `postgresql://`. SQLite is intentionally
not supported.

Template registry credentials are consumed by `runtime-gateway`, not
`api-service`. The API chooses the shard and asks that shard over the internal
runtime-gateway API to pull a missing template image; the runtime-gateway pod
performs the authenticated registry pull against its own Docker daemon.

## AWS Aurora Postgres

AWS Aurora PostgreSQL works with the current app path. Use standard Postgres
username/password authentication in `DATABASE_URL`.

Recommended URL shape:

```text
postgresql://<user>:<url-encoded-password>@<aurora-writer-endpoint>:5432/<db>?sslmode=require&connect_timeout=10
```

Use the Aurora writer endpoint for normal operation. A reader endpoint is not
valid because api-service writes sandbox, warm-pool, template, client, and lease
state.

Authentication handling:

- The app passes `DATABASE_URL` directly to `psycopg`.
- Username/password auth is handled by `psycopg` and Aurora.
- TLS is handled by `psycopg` when `sslmode=require` is present in the URL.
- Special characters in the password must be URL encoded.
- IAM database authentication is not implemented in the app because IAM tokens
  expire quickly and need token refresh logic. Use normal DB password auth, or
  put an RDS Proxy in front if your platform requires credential rotation.

Before deploying with Aurora, confirm:

- The EKS/GKE/VPC network route can reach the Aurora endpoint on port `5432`.
- The Aurora security group allows inbound `5432` from the cluster/node/pod CIDR
  or the worker-node security group.
- The database user can create/alter tables and indexes in the target database.
- Automated backups and storage monitoring are enabled on Aurora.
- `DATABASE_URL` is stored only in the Kubernetes Secret, not in ConfigMaps.

## GitLab CI Variables

These are GitLab pipeline variables, not Kubernetes Secret keys:

- `DEPLOY_SNDBX=true`
- `DOCKER_REPO=asia-south1-docker.pkg.dev/gc-qa6/gc-qa6`
- `DATABASE_SECRET_NAME=sndbx-qa6-tier1-secret`
- `DATABASE_SECRET_KEY=DATABASE_URL`
- `TEMPLATE_REGISTRY_INTERNAL_ENABLED=auto`
- `TEMPLATE_REGISTRY_REPO_PREFIX`
- `TEMPLATE_REGISTRY_SERVER`
- `TEMPLATE_REGISTRY_SECRET_NAME=sndbx-qa6-tier1-secret`
- `TEMPLATE_REGISTRY_AUTH_REQUIRED`
- `TEMPLATE_REGISTRY_LAYOUT=auto`

The GitLab job builds three images through Jenkins and then calls the custom
Helm deploy job with these values.

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

Runtime-gateway then pushes template images to:

```text
sndbx-qa6-tier1-template-registry.spr-apps.svc.cluster.local:5000/templates
```

The Docker daemon sidecar is rendered with:

```text
--insecure-registry=sndbx-qa6-tier1-template-registry.spr-apps.svc.cluster.local:5000
```

This registry is internal ClusterIP only. It is not exposed through ingress.

## Registry Mode B: AWS ECR

Use this when template images must be stored in AWS ECR.

Set GitLab variables:

```sh
TEMPLATE_REGISTRY_INTERNAL_ENABLED=false
TEMPLATE_REGISTRY_REPO_PREFIX=<your-ecr-repository-uri>
TEMPLATE_REGISTRY_SERVER=<registry-host>
TEMPLATE_REGISTRY_AUTH_REQUIRED=true
TEMPLATE_REGISTRY_LAYOUT=auto
```

For public ECR:

```sh
aws ecr-public get-login-password --region us-east-1
```

For private ECR:

```sh
aws ecr get-login-password --region <region>
```

Store that password in the Kubernetes Secret as `TEMPLATE_REGISTRY_PASSWORD` and
store `AWS` as `TEMPLATE_REGISTRY_USERNAME`.

For ECR, `layout=auto` stores all templates in one repository using tags like:

```text
<repoPrefix>:<template-id>-<tag>
```

Helm intentionally fails if an ECR repo is configured with
`templateRegistry.authRequired=false`.

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
  --set database.url=postgresql://user:pass@postgres.example.com:5432/sndbx \
  --set images.apiService.tag=test \
  --set images.runtimeGateway.tag=test \
  --set images.dockerDind.tag=test \
  --set templateRegistry.pushEnabled=true \
  --set templateRegistry.repoPrefix=
```

Expected intentional failures:

- Missing `database.url` and missing `database.existingSecretName`
- Non-Postgres `database.url`
- ECR repo prefix with `templateRegistry.authRequired=false`
- `templateRegistry.pushEnabled=true`, empty repo prefix, and
  `templateRegistry.internal.enabled=false`

## Deploy Flow

1. Push application code to the GitHub repo configured in `.gitlab-ci.yml`.
2. Push the `sndbx` chart to the Helm internal tools repo branch used by Jenkins.
3. Create/update `sndbx-qa6-tier1-secret` in `spr-apps`.
4. Set GitLab CI variables for either internal registry or external ECR.
5. Run the GitLab pipeline with `DEPLOY_SNDBX=true`.
6. Jenkins builds `api-service`, `runtime-gateway`, and `dockerd-gvisor`.
7. Jenkins deploys Helm release `sndbx-qa6-tier1` into `spr-apps`.
8. Verify pods, services, ingress, and registry mode:

```sh
kubectl -n spr-apps get pods,svc,ingress,pvc | grep sndbx-qa6-tier1
kubectl -n spr-apps get secret sndbx-qa6-tier1-secret
kubectl -n spr-apps describe ingress sndbx-qa6-tier1-ingress
```
