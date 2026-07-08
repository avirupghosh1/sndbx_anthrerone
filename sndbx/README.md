# sndbx Helm Chart

This chart is shaped to fit the same Jenkins custom Helm deployment flow used by Custodian's Prism deployment.

Expected layout:

- `sndbx/Chart.yaml`
- `sndbx/values.yaml`
- `sndbx/templates/*`
- `sndbx/releases/qa6-tier1/values.yaml`

Expected Jenkins deploy inputs:

- `GIT_REPO_URL` points at the Helm repo containing this `sndbx` folder
- `CHART_NAME=sndbx`
- `CHART_RELEASE_NAME=qa6-tier1`
- `CHART_NAMESPACE=spr-apps`
- `CHART_REPO_BRANCH=origin/main`

Expected image override keys from GitLab/Jenkins:

- `images.apiService.repo`
- `images.apiService.name`
- `images.apiService.tag`
- `images.runtimeGateway.repo`
- `images.runtimeGateway.name`
- `images.runtimeGateway.tag`
- `images.dockerDind.repo`
- `images.dockerDind.name`
- `images.dockerDind.tag`
- `templateRegistry.pushEnabled`
- `templateRegistry.authRequired`
- `templateRegistry.repoPrefix`
- `templateRegistry.layout`
- `templateRegistry.server`
- `templateRegistry.existingSecretName`
- `templateRegistry.internal.enabled`
- `images.templateRegistry`

The chart deploys:

- `api-service` Deployment + Service
- `runtime-gateway` StatefulSet + headless/cluster Services + per-shard PVCs
- optional `template-registry` Deployment + Service + PVC when template push is enabled without an external registry prefix
- one ingress that routes:
  - `api.<domain>` to `api-service`
  - `*.domain` to `runtime-gateway`
- service account / role / role binding
- pod disruption budgets

Pre-create the Kubernetes Secret named by `secrets.name`.

Required secret keys:

- `API_KEY`
- `INTERNAL_API_KEY`
- `PORTAL_SESSION_SECRET`
- `DATABASE_TYPE`
- `DATABASE_URL`
- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`

Important production notes:

- `api-service` requires PostgreSQL or MongoDB via `DATABASE_TYPE=postgres|mongo`; there is no local database fallback.
- `DATABASE_URL` may be a full DSN or a host/path endpoint. The app injects `DATABASE_USERNAME` and `DATABASE_PASSWORD` when credentials are not already present. For MongoDB, include the database name in the URI path or set `MONGODB_DATABASE`; if `DATABASE_USERNAME` is empty and `DATABASE_PASSWORD` is set, the username must already be present in `DATABASE_URL`.
- The Jenkins/image pipeline must publish four images for a full release: `api-service`, `runtime-gateway`, `dockerd-gvisor`, and `template-registry`.
- The internal registry pod uses the CI-built `template-registry` image through `images.templateRegistry.*`. For manual deploys, it can pull `<images.templateRegistry.repo>/registry:3`, or you can set `templateRegistry.internal.image` as a full-image override.
- Production template builds push to the chart-managed internal registry pod by default. No external template-registry credentials are required.
- With `templateRegistry.pushEnabled=true` and an empty `templateRegistry.repoPrefix`, `templateRegistry.internal.enabled=auto` creates an in-cluster registry Deployment, Service, and PVC. Runtime-gateway pushes template images to `<release>-template-registry.<namespace>.svc.cluster.local:5000/templates`, and dockerd is configured with that internal registry as insecure HTTP.
- For a clean registry-pull test, set `runtimeGateway.docker.persistence.enabled=false`; production keeps it `true` by default.
- When `secrets.create=false`, the secret named by `secrets.name` must already exist in the target namespace before Helm deploy runs.

The chart intentionally fails fast when:

- image tags are still `CHANGE_ME`
- `secrets.create=false` and `secrets.name` is missing
- `secrets.create=true` but one of the required secret values is empty
- no database URL source is configured
- `database.type` uses an unsupported value
- `templateRegistry.pushEnabled=true` has no external repo prefix and internal registry is disabled
- ingress host lists are empty
