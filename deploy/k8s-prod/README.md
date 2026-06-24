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

## Ingress requirements

Any ingress/controller is fine if it preserves the request `Host` header, supports wildcard host routing,
passes WebSocket upgrades through unchanged, and forwards traffic to:

- `api.<your-domain>` -> `api-service`
- `{port}-{sandbox_id}.<your-domain>` -> `runtime-gateway`

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
- `TEMPLATE_DOCKERFILE_BUILD_MODE=docker_cli`
  Template builds run as real Docker Engine builds against the same runtime-gateway daemon used for sandbox execution, so large Dockerfiles use normal layer caching instead of parsed step replay.
- `TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY=true`
  `api-service` keeps the public template API and SQLite metadata, but the actual template build and warm snapshot execution happen inside `runtime-gateway`.

## Template Build Visibility

For interactive builds, use `POST /templates/from-dockerfile/stream` on `api-service`.
It streams Docker build logs from `runtime-gateway` as SSE and ends with the registered template payload.

## Architectural limit

The current control plane uses SQLite on a single PVC, so `api-service` should stay at one replica unless you replace the database layer with a multi-writer store.
