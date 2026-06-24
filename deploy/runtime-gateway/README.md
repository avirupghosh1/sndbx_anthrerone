# Runtime Gateway Deployment

This deployment replaces the separate `proxy-service` and the K8s-per-sandbox runtime with:

- `api-service` as the control plane
- one privileged `runtime-gateway` pod containing:
  - `dockerd` for sandbox container lifecycle
  - the existing proxy application as the data-plane gateway

## Request flow

1. Client calls `api-service` for sandbox lifecycle.
2. `api-service` talks to the runtime gateway Docker daemon via `DOCKER_HOST`.
3. Sandboxes are created as Docker containers inside the runtime gateway pod.
4. Client calls `https://{port}-{sandbox_id}.<domain>`.
5. Ingress sends that traffic to the gateway container in the runtime gateway pod.
6. The gateway asks `api-service` for the authoritative route and proxies to the sandbox container IP inside the Docker bridge network.
7. Template builds and one-time warm snapshot builds also execute inside `runtime-gateway`, so built images do not need to be copied out of the gateway Docker graph.

## Why this reduces cold start

Per-sandbox pod scheduling, CNI setup, and Service creation are removed from the hot path.
The hot path becomes Docker container create/start plus guest bootstrap.

## Operational notes

- `dockerd` is exposed on port `2375` only as an internal cluster service for `api-service`.
- The gateway uses `UPSTREAM_RESOLVE_MODE=control_plane` so it always proxies to the route returned by `api-service`.
- The runtime gateway should have persistent Docker storage in production if you want warm pool, snapshots, and built images to survive pod restart.
- `POST /templates/from-dockerfile` stays on `api-service`, but the actual build runs inside `runtime-gateway`.
- `POST /templates/from-dockerfile/stream` streams Docker build logs back as SSE while that gateway build is running.

## Local Mac distinction

- Runtime-gateway architecture:
  - control plane: `kubectl port-forward -n sandboxes svc/api-service 8001:8000`
  - data plane: `kubectl port-forward -n sandboxes svc/runtime-gateway 18080:8080`
  - client env:
    - `SANDBOX_API_URL=http://127.0.0.1:8001`
    - `SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080`
- Legacy K8s-per-sandbox architecture:
  - control plane still uses `api-service`
  - data plane must forward `svc/proxy-service`, not `svc/runtime-gateway`

- Do not reuse a `18080` port-forward from the legacy deployment when testing runtime-gateway.
  The old `proxy-service` cannot route Docker-backed runtime-gateway sandboxes.

## Local ingress mode

You can also test the production structure on a Mac without direct service port-forwards:

- expose `ingress-nginx-controller` as `LoadBalancer` and run `minikube tunnel`
- set a wildcard-resolving dev domain such as `127-0-0-1.sslip.io`
- run `deploy/mac-minikube/enable-ingress-domain.sh`

That gives the same request shape as production:

- `http://api.<domain>` -> `api-service`
- `http://{port}-{sandbox_id}.<domain>` -> `runtime-gateway`

In this mode clients should set:

- `SANDBOX_API_URL=http://api.<domain>`
- `SANDBOX_DATA_PLANE_URL` unset

This does not replace the direct localhost workflow. You can still use:

- `SANDBOX_API_URL=http://127.0.0.1:8001`
- `SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080`

as long as `18080` is forwarded to `svc/runtime-gateway`, not `svc/proxy-service`.

The SDK then keeps using `api.<domain>` for control-plane calls and uses sandbox hosts such as
`8765-sb-...<domain>` or `49983-sb-...<domain>` for guest traffic through ingress.

## Image Build Note

The `runtime-gateway` image now expects the repository root as build context so it can include
`api_server copy/envd_guest` for template-time envd bake support:

- `docker build -t runtime-gateway:latest -f proxy_service/Dockerfile .`
