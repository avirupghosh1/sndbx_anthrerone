# Single sandbox + agent loop vs E2B

## How one sandbox stays the same across an agent loop

1. **Your app creates one `Sandbox` once** (e.g. `Sandbox.create(...)` in `examples/deepagents_my_sandbox.py`).
2. That construct holds a **stable `sandbox_id`** returned by `POST /sandboxes` (or taken from the **warm pool**, same mechanism).
3. You pass **`SandboxDeepAgentBackend(sandbox=sb)`** into `create_deep_agent`. Deep Agents’ filesystem and `execute` tools call into that backend on **every** graph step.
4. Each tool call hits the REST API as **`/sandboxes/{sandbox_id}/...`** — always the **same** id — so the **same Docker container** is mutated in place (files written, commands run). Nothing “rotates” unless you create another sandbox or kill this one.

The LangGraph loop only **re-invokes the model and tools**; it does not recreate the sandbox unless your code does.

## When a warm pool helps

**Yes, for cold-start latency** after `Sandbox.create`:

- First-time **image pull** and **container create** can take minutes.
- A **warm pool** keeps **N** already-running sandboxes that match a fixed profile (`SANDBOX_WARM_POOL_*` env vars). A matching `POST /sandboxes` can **reuse** one of those ids immediately, then the pool **refills** in the background.

It does **not** remove LLM latency or fix flaky free models. **Warm pool** is optional (`SANDBOX_WARM_POOL_*`).

See `GET /health` → `warm_pool` for `target_size` / `ready`.

**Compose / env gotcha:** the API reads **`SANDBOX_WARM_POOL_SIZE`** (and `SANDBOX_WARM_POOL_*` for profile). Names like `WARM_POOL_SIZE` are ignored. With Docker Compose, variables must appear under `services.api.environment` (or `env_file`); exporting on the host alone does nothing unless Compose passes them through (this repo’s `docker-compose.yml` uses `${SANDBOX_WARM_POOL_SIZE:-0}` so `export SANDBOX_WARM_POOL_SIZE=4` before `docker compose up` works). After changing pool size, **recreate** the API container (`docker compose up -d --force-recreate`).

**How it helps `my_test`:** a background thread keeps **N** idle sandboxes (real Docker containers, names like `sandbox-…`) matching the pool profile. The first `POST /sandboxes` whose `template_id` / cpu / memory / **timeout** exactly match that profile **reuses** one id immediately; the pool then refills. If your request uses different limits than the pool (e.g. custom `timeout` in JSON), `try_acquire` returns nothing and you get a fresh create — pool still holds warm containers for *matching* clients.

## Pause vs warm pool vs snapshot (Docker)

These are **not** the same mechanism:

- **Pause / resume** — The **same** container id. The kernel **freezes** processes in place (`docker pause` / cgroup freezer). RAM and open FDs stay in the container; nothing is written to a new image. Resuming continues **exactly** that process tree (until the container is removed).
- **Warm pool** — **Different** containers kept idle so `POST /sandboxes` can hand out an **existing** id quickly. They are **not** clones of your workload’s disk; they are blank sandboxes from a fixed image profile.
- **Snapshot (this repo)** — **`docker commit`** on a running sandbox: the **writable layer** is baked into a **new local image** tag (see `SANDBOX_SNAPSHOT_REPO`). **Restore** means starting a **new** sandbox with `from_snapshot_image` set to that tag. You get **filesystem** state from commit time; you do **not** get frozen RAM or running processes (that would be checkpoint/CRIU territory, not implemented here).

## E2B-style features: what you have vs gaps

| Area | E2B (typical) | This repo |
|------|----------------|-----------|
| Create / kill / pause / resume | Yes | Yes |
| Exec + cwd + env | Yes | Yes |
| Files read/write/list | Yes | Yes (Docker: writes use Engine **`put_archive`** / `docker cp` semantics, not exec argv) |
| Metrics | Yes | Yes (`/metrics`) |
| **Streaming command stdout** | Yes | **Added** (`POST /sandboxes/{id}/commands/run/stream`, SSE; Docker ``exec_start`` stream) |
| **Public ingress / dev URLs** (expose port) | Yes | **Missing** |
| **PTY / interactive shell** | Yes | **Missing** (non-TTY `sh -c`) |
| **Custom templates (image + env + start_cmd)** | Yes (build service) | **Added** (Docker: SQLite registry + one-time ``docker commit`` warm snapshot; see ``docs/CUSTOM_TEMPLATES.md`` — not E2B’s CI/build grid) |
| **Secrets manager** scoped to sandbox | Yes | **Missing** |
| **Connect by id** (`Sandbox.connect`) | Yes | **Added** (`Sandbox.connect` / `AsyncSandbox.connect`) |
| **Cheap liveness** | Various | **Added** (`GET /sandboxes/{id}/status`, SDK `lifecycle()`) |
| **Warm pool** | Yes (managed service) | **Added** (`SANDBOX_WARM_POOL_SIZE`, Docker only) |
| **Snapshots / restore** | Beta / product-specific | **Added** (Docker: `docker commit` → new image; restore = new sandbox with `from_snapshot_image`; **not** RAM/process state) |
| **Global edge network** | Yes | **Missing** (self-hosted API + Docker) |
| **Built-in SaaS dashboard** | Yes | **Missing** |

Implementing **port exposure**, **PTY**, **Dockerfile CI pipeline**, and **secrets** would each be a larger follow-up (new API routes, Docker networking or sidecars, secret store integration). Command **streaming** over SSE is implemented (see table above).

## File writes into the sandbox (Docker)

**Not** the same limit as “HTTP max body size” alone: shipping file bytes through **`exec_run`** as **base64 on `sys.argv`** hits the kernel’s **maximum argument / environment size** (`ARG_MAX` / `execve` limits), so multi‑hundred‑KB or MB payloads can fail even when the REST request is accepted.

This repo’s Docker path uses the Engine **`put_archive`** API (what **`docker cp`** uses under the hood): a tiny tar containing one file member is streamed to the daemon and extracted under the target directory, so **large writes are not passed on the exec argv**. The API process still decodes the request body and builds the tar in memory; **multi‑GB** payloads would need a different design (e.g. chunked upload + streaming tar, or bind mounts).

## SDK quick reference

```python
from my_sdk import Sandbox, SandboxLifecycle

sb = Sandbox.create(api_url="http://127.0.0.1:8000", api_key="...")
assert sb.lifecycle().running

# Reattach later (default ``with_e2b=True``: ``GET …/status`` + ``GET …/e2b-connection``)
sb2 = Sandbox.connect(sb.sandbox_id, api_url="http://127.0.0.1:8000", api_key="...")
```
