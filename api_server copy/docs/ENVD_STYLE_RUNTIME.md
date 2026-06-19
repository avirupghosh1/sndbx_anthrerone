# Envd-style sandbox data plane (E2B-inspired)

## What E2B’s `envd` does (reference)

| Layer | Role |
|-------|------|
| **Control plane** (E2B API) | Create / delete sandboxes, auth, quotas. |
| **Data plane** (`envd` in guest, port **49983**) | High-frequency runtime: filesystem metadata, process start/stream, PTY. |
| **gRPC / Connect** | `FilesystemService`, `ProcessService` — small messages, streaming events. |
| **HTTP** | Bulk file read (`GET /files`) and write (`POST /files` multipart) — avoids stuffing large blobs through gRPC. |

## What this repo does today

| Operation | Path |
|-----------|------|
| Lifecycle | `POST/GET/DELETE /sandboxes` (control plane). |
| Commands | `POST /sandboxes/{id}/commands/run` (+ optional SSE stream). |
| Files | `POST …/files/write`, read/delete/list routes. |

All of that goes **API → execution plane** (Docker exec, Firecracker SSH, etc.) — no long-lived guest daemon.

## Target architecture (phased)

### Phase 1 — **implemented here**

- **`api_server/envd_guest/`**: small **HTTP** service (Starlette) in the guest on port **49983**.
- **Template-time embed (Docker, default on):** when ``ENVD_EMBED_AT_TEMPLATE_BUILD=true`` (default), the API adds ``/opt/envd_guest`` + ``pip install`` during **template** builds: ``POST /templates`` warm snapshot, ``POST /templates/from-dockerfile`` (parsed + ``docker_cli``), and the **first** ``POST /sandboxes`` for ``template_id == DEFAULT_TEMPLATE`` (e.g. ``python:3.11``) auto-registers that logical template and commits a snapshot. For ``docker_cli`` / in-container bake, if ``python3`` is missing the injected layer runs ``apt-get install python3 python3-pip`` (Debian/Ubuntu) or ``apk add python3 py3-pip`` (Alpine); images that already have Python skip that. A marker file records the bake so **runtime** work is only starting **uvicorn** (no per-sandbox tarball/pip). Set ``ENVD_EMBED_AT_TEMPLATE_BUILD=false`` to disable embed entirely (e.g. RHEL without apt/apk — add Python in your Dockerfile first). The injected Dockerfile fragment uses ``USER root`` for ``COPY``/``RUN`` (so ``touch /opt/envd_guest/.mysandbox_envd_baked`` works when your Dockerfile ends with a non-root ``USER``). With ``ENVD_DOCKERFILE_RESTORE_USER=auto`` (default), the **docker_cli** append step infers a trailing ``USER ubuntu`` when the submitted Dockerfile clearly creates or selects that account; otherwise it leaves the image on **root** after the envd layer (safe for slim bases). Set ``ENVD_DOCKERFILE_RESTORE_USER`` to a concrete login or ``none`` only to override that inference.
- **Runtime fallback:** if a sandbox image predates the bake (no marker), ``ENVD_AUTO_START=true`` still performs the legacy **upload + pip + uvicorn** path when ``ENVD_PUBLISH_PORT=true``.
- **Split transport** (same *idea* as E2B, not wire-compatible with their protobufs yet):
  - JSON **`/v1/fs/*`** for stat, list_dir, mkdir, move, remove.
  - **`GET/POST /files`** for raw body bulk read/write.
  - **`POST /v1/process/start`** returns **NDJSON** stream (`stdout` / `stderr` / `exit`).
- **Auth**: `X-Access-Token` must match `ENVD_ACCESS_TOKEN` in the container env.
- **Docker publish** (optional): `ENVD_PUBLISH_PORT=true` maps container `:49983` → random host port; API stores host port in sandbox metadata and mints a random token (stripped from public `GET /sandboxes` responses).
- **`GET /sandboxes/{id}/envd-connection`**: returns `http_base_url`, `access_token`, `envd_port` for SDKs.
- **E2E script:** from the repo root, with the API up and `ENVD_PUBLISH_PORT=true`, run `python examples/test_envd_sandbox_e2e.py` (needs `httpx`; see the script docstring for env vars).

### Phase 2 — **Connect / gRPC parity**

- Vendor or copy `E2B/spec/envd/**/*.proto`, run **buf** / `grpcio-tools` code generation.
- Add **Connect** (`grpc-go` / `connect-python`) or **grpcio** servers **alongside** HTTP (or behind a mux on `:49983`).
- Map Connect error codes to SDK exceptions like E2B’s `envd/rpc.py`.

### Phase 3 — **Filesystem watch + PTY**

- **WatchDir**: inotify (Linux) → server-streaming RPC or SSE.
- **PTY**: attach to `ProcessService` with raw mode + resize messages (needs TTY allocation in guest).

### Phase 4 — **SDK**

- Thin Python/JS client: same split (gRPC for control, `httpx`/`fetch` for `/files`).
- Optional: align URL + header names with E2B SDK for drop-in ergonomics.

## Files in this repo

| Path | Purpose |
|------|---------|
| `docs/ENVD_STYLE_RUNTIME.md` | This document. |
| `envd_guest/server.py` | Guest daemon (run with `uvicorn envd_guest.server:app`). |
| `envd_guest/requirements.txt` | `starlette`, `uvicorn` (guest install). |
| `handlers/sandbox_envd.py` | `GET …/envd-connection`. |
| `config.py` | `ENVD_*` toggles. |
| `orchestrator/container_manager.py` | Publish `:49983` when enabled. |
| `orchestrator/sandbox_manager.py` | Inject token + host port metadata; template-time **embed**; **auto-start** (uvicorn only when baked, else upload + pip + uvicorn). |

**Warm pool:** with ``ENVD_EMBED_AT_TEMPLATE_BUILD`` (default), warm-pool provision runs **pip** during the template snapshot build, not on every idle slot (runtime only starts uvicorn). Legacy images without a bake marker still pay per-sandbox pip if you rely on runtime bootstrap.

## Wire format (Phase 1 HTTP)

- **Health**: `GET /health` → `{"ok":true,"service":"envd-guest"}`.
- **Stat**: `POST /v1/fs/stat` body `{"path":"/tmp"}`.
- **List**: `POST /v1/fs/list_dir` body `{"path":"/","depth":1}`.
- **Mkdir / remove / move**: `POST /v1/fs/mkdir`, `/v1/fs/remove`, `/v1/fs/move`.
- **Files**: `GET /files?path=/x` (raw bytes); `POST /files` form `path` + raw body or `multipart/form-data` field `file`.
- **Process**: `POST /v1/process/start` JSON `{"argv":["/bin/sh","-c","echo hi"],"cwd":"/","env":{}}` → `text/x-ndjson` lines `{"type":"start","pid":...}` then `stdout`/`stderr` chunks, then `{"type":"exit","code":n}`.

This is **not** protobuf on the wire; it is a stepping stone toward the same separation of concerns (control vs bulk vs streaming).
