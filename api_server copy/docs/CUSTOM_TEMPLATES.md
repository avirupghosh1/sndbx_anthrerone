# Custom templates (Docker) and warm snapshots

This is **not** a full clone of E2B’s managed template builder (global cache, edge distribution, hosted build grid). It is a **self-hosted** pattern that maps to parts of [E2B’s Python template SDK](https://github.com/e2b-dev/E2B/tree/main/packages/python-sdk/e2b/template) as follows:

| E2B SDK concept | This repo |
|-----------------|-----------|
| `fromImage` / base image | `POST /templates` field `base_image`, or image produced by `POST /templates/from-dockerfile` |
| `startCmd` | `start_cmd` (shell once in the build container) |
| `readyCmd` | `ready_cmd` (optional; polled until exit 0 or `TEMPLATE_READY_TIMEOUT_SEC`) |
| Dockerfile + context → image | `POST /templates/from-dockerfile` (`dockerfile` + optional `context_tar_gzip_base64`) |
| `COPY` / `RUN` as build steps | Handled by **`docker build`** for `from-dockerfile`; for `POST /templates` only runtime `start_cmd` + settle applies |
| Build logs / build IDs / remote status API | **Not implemented** — build runs inline in the API process; use server logs and HTTP errors |

## Registration flows

### A) `POST /templates` (existing)

1. You register a **logical** `template_id` via `POST /templates` (the HTTP API rejects some characters such as `/` in `template_id`; use `base_image` for full refs in that flow).
2. The API stores **base image**, **container env**, **`start_cmd`**, **`settle_seconds`**, and optional **`ready_cmd`**.
3. On the **first** `POST /sandboxes` with that `template_id` (Docker only), the server:
   - starts a **throwaway** build container from `base_image` with `env`;
   - runs `start_cmd` (shell, via `sh -c`);
   - sleeps `settle_seconds` (default **20**, max 600);
   - if **`ready_cmd`** is non-empty: runs it in a loop every ~2s until exit code **0** or **`TEMPLATE_READY_TIMEOUT_SEC`** (config, default **600**);
   - runs **`docker commit`** → stores `warm_snapshot_image` in SQLite;
   - kills the build container (no long-lived build sandbox in the DB).
4. Later sandboxes with the same `template_id` start **from that snapshot image** (plus the same `env` at create time). The expensive build runs **once per template_id** until you re-register the template (which clears the snapshot row).

### B) `POST /templates/from-dockerfile` (new)

Runs **`docker build`** on the **API host** (requires Docker CLI + Engine). Body: `template_id`, `dockerfile` (full text), optional `image_tag`, `build_args`, **`context_tar_gzip_base64`** (gzip-compressed tar of build context for `COPY`/`ADD`), plus the same `env` / `start_cmd` / `ready_cmd` / `settle_seconds` fields used after the image is registered.

**Not supported** when `SANDBOX_ISOLATION=lima` (no Docker build path). **Firecracker** (`SANDBOX_ENGINE=firecracker`) **is supported**: the API builds with Docker on the host, exports the image to a host ``*.ext4``, and stores that path as ``warm_snapshot_image`` (see ``docs/FIRECRACKER.md``).

## Warm pool interaction

If `SANDBOX_WARM_POOL_SIZE > 0`, after a warm snapshot exists the API starts a **separate warm-pool segment** keyed by

`(template_id, cpu_limit, memory_limit, timeout)` — the same tuple your client sends on `POST /sandboxes`.

That segment provisions **N** idle sandboxes using `from_snapshot_image=<warm_snapshot_image>`. The default pool segment (from `SANDBOX_WARM_POOL_TEMPLATE_ID`, etc.) still provisions from the **base** image when no logical template row exists.

### Auto-registration when `SANDBOX_WARM_POOL_SIZE > 0`

If `POST /sandboxes` uses a `template_id` that is **not** already in SQLite **and** is **not** exactly `SANDBOX_WARM_POOL_TEMPLATE_ID` (after env defaulting), the API **inserts** a minimal logical template: `base_image` = the usual image resolution for that id (e.g. `node:18` → `node:18`), empty `env` / `start_cmd`, `settle_seconds` 20. The next steps match registered templates: one-time warm snapshot build, then `ensure_pool_for` for `(template_id, cpu, memory, timeout)` so idle sandboxes are provisioned. This lets `my_test.py`-style clients pass a Docker image as `template_id` without calling `POST /templates` first.

## E2B comparison (precise)

- **E2B (public product):** templates are typically **built artifacts** (OCI images from Dockerfiles or their build service), versioned and distributed; warm capacity is **prepared template instances**, not arbitrary `docker commit` after an ad-hoc `start_cmd` in your API process. Their [Python `e2b/template`](https://github.com/e2b-dev/E2B/tree/main/packages/python-sdk/e2b/template) package parses Dockerfiles (`dockerfile_parser.py`) and sends **declarative steps** to **their** cloud builder (`main.py`), including `readyCmd` semantics.
- **This repo:** templates are **local SQLite + local Docker images**. You can either (A) point at a pulled image + `start_cmd` + optional `ready_cmd`, or (B) **`docker build` a Dockerfile** via `POST /templates/from-dockerfile`, then still optionally run `start_cmd` / `ready_cmd` before **`docker commit`** warm snapshot. Still **no** SaaS build grid, **no** multi-tenant global cache, **no** build-status polling API like E2B’s hosted service.

## API

- `POST /templates` — register or update (update clears `warm_snapshot_image` so the next sandbox triggers a rebuild).
- `POST /templates/from-dockerfile` — build image with Docker, then register (clears warm snapshot).
- `GET /templates`, `GET /templates/{template_id}`.

## Limits

- **`docker commit`** captures filesystem + image metadata from the container at commit time, **not** live RAM or all dynamic process state.
- **`start_cmd` non-zero exit:** the server still settles and commits (you may want idempotent `start_cmd`); check logs.
- **Build resources:** `TEMPLATE_BUILD_CPU` / `TEMPLATE_BUILD_MEMORY` (see `config.py`) apply only to the throwaway build container.

## Example

```bash
curl -s -X POST "http://127.0.0.1:8000/templates" \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{
    "template_id": "datascience",
    "base_image": "python:3.11-slim",
    "env": {"PIP_DISABLE_PIP_VERSION_CHECK": "1"},
    "start_cmd": "pip install --no-cache-dir pandas && echo ok > /tmp/warm_ready",
    "settle_seconds": 25
  }'

# First create blocks for pull + start_cmd + settle + commit; later creates are fast.
curl -s -X POST "http://127.0.0.1:8000/sandboxes" \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"template_id": "datascience"}'
```

### From Dockerfile (build on API host)

```bash
# Self-contained Dockerfile (no COPY) — API runs: docker build -t … && registers template_id → that tag
curl -s -X POST "http://127.0.0.1:8000/templates/from-dockerfile" \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d @- <<'JSON'
{
  "template_id": "myapp",
  "dockerfile": "FROM python:3.11-slim\nRUN pip install --no-cache-dir requests\nWORKDIR /app\n",
  "settle_seconds": 5,
  "ready_cmd": "python3 -c \"import requests; print('ok')\""
}
JSON
```

With a build context (Dockerfile uses `COPY`), gzip+base64 the project directory and set `context_tar_gzip_base64` (see request schema in OpenAPI `/docs`).
