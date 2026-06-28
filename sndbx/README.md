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

The chart deploys:

- `api-service` Deployment + Service + PVC
- `runtime-gateway` Deployment + Service + PVC
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

The chart intentionally fails fast when:

- image tags are still `CHANGE_ME`
- `secrets.create=false` and `secrets.name` is missing
- `secrets.create=true` but one of the required secret values is empty
- ingress host lists are empty
