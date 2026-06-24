# Office Cloud Deployment

This directory is the production deployment shape for the runtime-gateway architecture:

- `api-service`: control plane for sandbox lifecycle, template builds, warm pool, snapshots, TTL reaping
- `runtime-gateway`: one privileged pod containing:
  - `dockerd` for sandbox container execution
  - the data-plane gateway for HTTP/WebSocket traffic to guest ports

It is separate from the Mac/minikube manifests so local behavior stays unchanged.

## Traffic model

1. Clients call `https://api.<your-domain>` for control-plane APIs such as `POST /sandboxes`.
2. `api-service` creates a sandbox container through the runtime gateway Docker daemon, records metadata, mints access tokens, and stores guest routing.
3. Clients talk to guest services through `https://{port}-{sandbox_id}.<your-domain>` or `wss://{port}-{sandbox_id}.<your-domain>`.
4. DNS wildcard `*.<your-domain>` points at the ingress/controller in front of `runtime-gateway`.
5. The gateway parses `{port}-{sandbox_id}` from the host, asks `api-service` for the authoritative route, validates traffic tokens, and proxies to the sandbox guest container.

In production there is no port-forward. The ingress is the only public entry point.

## Required DNS

- `api.<your-domain>` -> ingress public IP / LB
- `*.<your-domain>` -> same ingress public IP / LB

## Required secrets

Create `sandbox-secrets` with at least:

- `API_KEY`
- `CONTROL_PLANE_API_KEY`
- `INGRESS_ACCESS_TOKEN`

`API_KEY` and `CONTROL_PLANE_API_KEY` may be the same value.

## Important defaults

- `UPSTREAM_RESOLVE_MODE=control_plane`
  This keeps runtime-gateway routing authoritative and avoids reconstructing upstreams inside the data plane.
- `SANDBOX_LEASE_REAPER_INTERVAL_SEC=5`
  The API enforces TTL by killing expired sandboxes.
- `TEMPLATE_DOCKERFILE_BUILD_MODE=parsed`
  Template builds use the same remote Docker execution plane as sandbox creation so warm snapshots land in the runtime daemon image store.

## Architectural limit

The current control plane uses SQLite on a single PVC, so `api-service` should stay at one replica unless you replace the database layer with a multi-writer store.
