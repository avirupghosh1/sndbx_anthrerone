# Sandbox Platform

This repo deploys two services:

- `api-service`: control plane, metadata, auth, templates, warm pool, TTL reaper
- `runtime-gateway`: data plane, Docker runtime host, ingress bridge, template builds

Only two Kubernetes manifests are kept in this repo:

- [deploy/api-service.yaml](/Users/avirup.ghosh/Desktop/cont_in_vm/deploy/api-service.yaml)
- [deploy/runtime-gateway.yaml](/Users/avirup.ghosh/Desktop/cont_in_vm/deploy/runtime-gateway.yaml)

## Deployment

For the Helm/Jenkins production path, use [DEPLOY.md](/Users/avirup.ghosh/Desktop/cont_in_vm/DEPLOY.md).

Build and load images however your cluster expects, then apply:

```sh
kubectl apply -f deploy/api-service.yaml
kubectl apply -f deploy/runtime-gateway.yaml
```

Both manifests expect a `sandbox-secrets` secret in namespace `sandboxes` with:

- `API_KEY`
- `INTERNAL_API_KEY`
- `PORTAL_SESSION_SECRET`
- `DATABASE_TYPE`
- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`

## Access Modes

Production:

- `api.<domain>` -> ingress -> `api-service`
- `*-<sandbox_id>.<domain>` -> ingress -> `runtime-gateway`
- `runtime-gateway` asks `api-service` for sandbox routing and authorization state
- guest envd keeps using `X-Access-Token`; do not reuse that header for ingress auth

Local:

```sh
kubectl -n sandboxes port-forward svc/api-service 8001:8000
kubectl -n sandboxes port-forward svc/runtime-gateway 18080:8080
```

In local mode:

- control-plane requests go to `http://127.0.0.1:8001`
- data-plane requests go to `http://127.0.0.1:18080?...`
- SDK/local clients keep using the same API semantics; only the base URLs differ

## Persistence

- API metadata persists via `DATABASE_TYPE` plus `DATABASE_URL`, using PostgreSQL or MongoDB
- Docker image/container graph persists in per-shard runtime-gateway PVCs at `/var/lib/docker`
- For clean registry-pull validation, deploy the Helm chart with `runtimeGateway.docker.persistence.enabled=false` so every restarted shard starts with an empty Docker graph.

## Template Registry

- In Helm and the GitLab deploy job, `templateRegistry.pushEnabled=true` with no template registry repo prefix uses the chart-managed internal registry by default. That path persists template images in the registry PVC and avoids requiring external registry credentials.
- Jenkins builds a `template-registry` image from `template-registry/Dockerfile` and passes that image to Helm for the registry pod. Manual deploys can still set `templateRegistry.internal.image`, or fall back to `<images.apiService.repo>/registry:2`.

## Current Supported Flow

- sandbox create / kill / timeout refresh
- template register / dockerfile build / warm snapshot
- warm pool provisioning
- pause / resume
- filesystem snapshots
- envd guest file and process APIs
- agentlib / websocket guest servers through runtime-gateway

## Notes

- `api-service` restart keeps DB-backed state
- `runtime-gateway` restart does not restore old live containers; stale sandboxes are surfaced as lost and later purged
- ingress mode and local port-forward mode are both intentionally supported without code changes
