# agent-sandbox Helm Chart

This chart is shaped to fit the same Jenkins custom Helm deployment flow used by Custodian's Prism deployment.

Expected layout:

- `agent-sandbox/Chart.yaml`
- `agent-sandbox/values.yaml`
- `agent-sandbox/templates/*`
- `agent-sandbox/releases/qa6-tier1/values.yaml`

Expected Jenkins deploy inputs:

- `GIT_REPO_URL` points at the Helm repo containing this `agent-sandbox` folder
- `CHART_NAME=agent-sandbox`
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
- `imageBuilding.authRequired`
- `imageBuilding.existingSecretName`
- `imageBuilding.s3.prefix`
- `imageBuilding.s3.endpointUrl`
- `images.templateRegistry`

The chart deploys:

- `api-service` Deployment + Service
- `runtime-gateway` StatefulSet + headless/cluster Services with ephemeral `emptyDir` Docker graph storage
- optional `template-registry` Deployment + Service with ephemeral `emptyDir` storage when template push is enabled without an external registry prefix
- one ingress that routes:
  - `api.<domain>` to `api-service`
  - `*.domain` to `runtime-gateway`
- service account / role / role binding
- pod disruption budgets

Pre-create the Kubernetes Secret named by `secrets.name`.

Required secret keys:

- `API_KEY`
- `INTERNAL_API_KEY`
- `ADMIN_API_KEY`
- `PORTAL_SESSION_SECRET`
- `DATABASE_TYPE`
- `DATABASE_URL`
- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`

Required only when `imageBuilding.authRequired=true`:

- `IMAGE_BUILDING_S3_BUCKET`
- `IMAGE_BUILDING_S3_REGION`
- `IMAGE_BUILDING_S3_ACCESS_KEY_ID`
- `IMAGE_BUILDING_S3_SECRET_ACCESS_KEY`
- `IMAGE_BUILDING_S3_SESSION_TOKEN` (optional)

Important production notes:

- `api-service` requires PostgreSQL or MongoDB via `DATABASE_TYPE=postgres|mongo`; there is no local database fallback.
- `DATABASE_URL` may be a full DSN or a host/path endpoint. The app injects `DATABASE_USERNAME` and `DATABASE_PASSWORD` when credentials are not already present. For MongoDB, include the database name in the URI path or set `MONGODB_DATABASE`; if `DATABASE_USERNAME` is empty and `DATABASE_PASSWORD` is set, the username must already be present in `DATABASE_URL`.
- The Jenkins/image pipeline must publish four images for a full release: `api-service`, `runtime-gateway`, `dockerd-gvisor`, and `template-registry`.
- The internal registry pod uses the CI-built `template-registry` image through `images.templateRegistry.*`. For manual deploys, it can pull `<images.templateRegistry.repo>/registry:3`, or you can set `templateRegistry.internal.image` as a full-image override.
- Production template builds push to the chart-managed internal registry pod by default. No external template-registry credentials are required.
- E2B/Daytona build-context uploads use API-local S3-compatible URLs. By default they are stored in the configured database; set `imageBuilding.authRequired=true` to store those upload objects in S3 instead.
- With `templateRegistry.pushEnabled=true` and an empty `templateRegistry.repoPrefix`, `templateRegistry.internal.enabled=auto` creates an in-cluster registry Deployment and Service using `emptyDir` storage. Runtime-gateway pushes template images to `<release>-template-registry.<namespace>.svc.cluster.local:5000/templates`, and dockerd is configured with that internal registry as insecure HTTP.
- The chart does not create PersistentVolumeClaims; restarting runtime-gateway or template-registry pods clears their local image data.
- When `secrets.create=false`, the secret named by `secrets.name` must already exist in the target namespace before Helm deploy runs.

The chart intentionally fails fast when:

- image tags are still `CHANGE_ME`
- `secrets.create=false` and `secrets.name` is missing
- `secrets.create=true` but one of the required secret values is empty
- no database URL source is configured
- `database.type` uses an unsupported value
- `templateRegistry.pushEnabled=true` has no external repo prefix and internal registry is disabled
- `imageBuilding.authRequired=true` is enabled without an existing Secret or Helm-created S3 secret values
- ingress host lists are empty
