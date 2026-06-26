# Sandbox Platform

This repo deploys two services:

- `api-service`: control plane, metadata, auth, templates, warm pool, TTL reaper
- `runtime-gateway`: data plane, Docker runtime host, ingress bridge, template builds

Only two Kubernetes manifests are kept in this repo:

- [deploy/api-service.yaml](/Users/avirup.ghosh/Desktop/cont_in_vm/deploy/api-service.yaml)
- [deploy/runtime-gateway.yaml](/Users/avirup.ghosh/Desktop/cont_in_vm/deploy/runtime-gateway.yaml)

## Deployment

Build and load images however your cluster expects, then apply:

```sh
kubectl apply -f deploy/api-service.yaml
kubectl apply -f deploy/runtime-gateway.yaml
```

Both manifests expect a `sandbox-secrets` secret in namespace `sandboxes` with:

- `API_KEY`
- `INTERNAL_API_KEY`
- `PORTAL_SESSION_SECRET`

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

- API database persists in PVC `api-service-data` at `/var/lib/api/sandboxes.db`
- Docker image/container graph persists in PVC `runtime-gateway-docker-graph` at `/var/lib/docker`

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
