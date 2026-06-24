# `cont_in_vm`

Current deployment shape:

- `api-service`: control plane for sandbox create/kill/resume, template registration, warm pool, snapshots, and TTL reaping
- `runtime-gateway`: one privileged pod containing:
  - `dockerd` for sandbox container lifecycle and image storage
  - `gateway` for HTTP/WebSocket data-plane proxying
  - internal template-build endpoints so images are built in the same Docker graph used for sandbox startup

Ingress contract:

- `api.sndbx.com` -> `api-service`
- `{port}-{sandbox_id}.sndbx.com` -> `runtime-gateway`

The gateway parses `{port}-{sandbox_id}` from the host, asks `api-service` for the authoritative route and traffic token, and proxies to the guest container port.

Local modes:

- Direct mode:
  - `kubectl port-forward -n sandboxes svc/api-service 8001:8000`
  - `kubectl port-forward -n sandboxes svc/runtime-gateway 18080:8080`
  - `SANDBOX_API_URL=http://127.0.0.1:8001`
  - `SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080`
- Production-style local ingress mode:
  - run `deploy/mac-minikube/enable-ingress-domain.sh`
  - run `minikube tunnel`
  - `SANDBOX_API_URL=http://api.127-0-0-1.sslip.io`
  - unset `SANDBOX_DATA_PLANE_URL`

Primary references:

- `deploy/k8s-prod/README.md`
- `deploy/runtime-gateway/README.md`
- `deploy/mac-minikube/up.sh`
- `deploy/mac-minikube/enable-ingress-domain.sh`
