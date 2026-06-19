# Guest `envd`-style daemon (Phase 1 — HTTP)

## Automatic start (Docker / gVisor, recommended)

On the API host set **`ENVD_PUBLISH_PORT=true`**.

**Default (E2B-style):** with **`ENVD_EMBED_AT_TEMPLATE_BUILD=true`** (default), the API **bakes** `envd_guest` into **template images** during `POST /templates` / `from-dockerfile` builds and during the **first** sandbox create for **`template_id == DEFAULT_TEMPLATE`** (e.g. `python:3.11`): `pip install` runs at **image build** time, and a marker is written under `/opt/envd_guest/`. On each **`POST /sandboxes`**, **`ENVD_AUTO_START=true`** (default) then only **starts uvicorn** (no per-sandbox tarball/pip).

**Legacy path:** images without that bake marker still get steps 2–4 on each create (copy `api_server/envd_guest/`, `pip install`, start uvicorn) so older templates keep working.

1. Inject **`ENVD_ACCESS_TOKEN`** into the container env and publish **TCP 49983** (or **`ENVD_PORT`**).
2. If the image is not pre-baked: copy **`api_server/envd_guest/`** to **`/opt/envd_guest`** and run **`pip install -r …/requirements.txt`** (timeout **`ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC`**).
3. Start **`uvicorn envd_guest.server:app`** in the background until **49983** accepts connections.

Then call **`GET /sandboxes/{id}/envd-connection`** for `http_base_url` + `access_token`.

Set **`ENVD_EMBED_AT_TEMPLATE_BUILD=false`** if your Dockerfile final stage has no Python 3 / pip. Set **`ENVD_AUTO_START=false`** if you start the daemon yourself.

## Manual run (debugging / non-Docker backends)

```bash
export ENVD_ACCESS_TOKEN="$(openssl rand -hex 24)"
uvicorn envd_guest.server:app --host 0.0.0.0 --port 49983
```

Example client (after `envd-connection`):

```bash
curl -sS -H "X-Access-Token: $TOKEN" "$BASE/v1/fs/stat" -d '{"path":"/"}' -H content-type:application/json
```

Install deps yourself when not using auto-start: `pip install -r envd_guest/requirements.txt` (paths relative to `api_server/`).
