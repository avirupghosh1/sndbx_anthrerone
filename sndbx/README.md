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
- `database.existingSecretName`
- `database.existingSecretKey`
- `templateRegistry.pushEnabled`
- `templateRegistry.authRequired`
- `templateRegistry.repoPrefix`
- `templateRegistry.layout`
- `templateRegistry.server`
- `templateRegistry.existingSecretName`
- `templateRegistry.internal.enabled`

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
- `DATABASE_URL`
- `TEMPLATE_REGISTRY_USERNAME` and `TEMPLATE_REGISTRY_PASSWORD` when external `templateRegistry.authRequired=true`

Important production notes:

- `api-service` requires PostgreSQL via `database.url` or `database.existingSecretName`; there is no local database fallback.
- The Jenkins/image pipeline must publish three images for a full release: `api-service`, `runtime-gateway`, and `dockerd-gvisor`.
- Production template builds should push to `templateRegistry.repoPrefix` in the same company registry namespace as the release images when an external registry is configured; every runtime-gateway shard logs its local Docker daemon into `templateRegistry.server` before becoming ready when external `templateRegistry.authRequired=true`.
- `templateRegistry.layout=auto` preserves the existing Artifact Registry/GCR-style `<repoPrefix>/<template-id>:<tag>` layout, but uses `<repoPrefix>:<template-id>-<tag>` for AWS ECR public/private hosts so one ECR repository can hold all templates. Set `templateRegistry.layout=single_repository` explicitly for ECR if you want no auto-detection.
- If `templateRegistry.pushEnabled=true` and `templateRegistry.repoPrefix` is empty, `templateRegistry.internal.enabled=auto` creates an in-cluster registry Deployment, Service, and PVC. Runtime-gateway pushes template images to `<release>-template-registry.<namespace>.svc.cluster.local:5000/templates`, and dockerd is configured with that internal registry as insecure HTTP.
- The GitLab deploy job follows the same rule: with `TEMPLATE_REGISTRY_INTERNAL_ENABLED=auto` and an empty `TEMPLATE_REGISTRY_REPO_PREFIX`, Jenkins deploys the internal registry and disables external registry auth. Set `TEMPLATE_REGISTRY_REPO_PREFIX` explicitly to use Artifact Registry, ECR, or another external registry.
- AWS ECR pushes require `templateRegistry.authRequired=true` plus `TEMPLATE_REGISTRY_USERNAME=AWS` and `TEMPLATE_REGISTRY_PASSWORD`. Omit `templateRegistry.repoPrefix` or set `templateRegistry.internal.enabled=true` when you want to run without ECR credentials.
- For a clean registry-pull test, set `runtimeGateway.docker.persistence.enabled=false`; production keeps it `true` by default.
- When `secrets.create=false`, the secret named by `secrets.name` must already exist in the target namespace before Helm deploy runs.

The chart intentionally fails fast when:

- image tags are still `CHANGE_ME`
- `secrets.create=false` and `secrets.name` is missing
- `secrets.create=true` but one of the required secret values is empty
- neither `database.url` nor `database.existingSecretName` is set
- an AWS ECR repo prefix is configured for template pushes without registry auth enabled
- `templateRegistry.pushEnabled=true` has no external repo prefix and internal registry is disabled
- ingress host lists are empty
