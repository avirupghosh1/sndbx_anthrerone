# Testing the E2B drop-in (agentlib / Custodian path)

## 1. Run the API with drop-in secrets

From `api_server/` (use a venv with `pip install -r requirements.txt`):

```bash
export API_KEY=test-key-12345
export E2B_DROPIN_WS_SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
# Optional if behind TLS terminator:
# export E2B_DROPIN_PUBLIC_WS_BASE=wss://api.example.com
uvicorn main:app --host 0.0.0.0 --port 8000
```

Docker must be reachable from this process (same host or `DOCKER_HOST`).

### You do not need **Docker Desktop**

This project talks to the **Docker Engine HTTP API** (via `docker-py`). Any setup that provides that API is enough:

| Setup | Notes |
|--------|--------|
| **[Colima](https://github.com/abiosoft/colima)** | Common on macOS without Docker Desktop: `brew install colima docker`, then `colima start`, then `docker info`. |
| **[OrbStack](https://orbstack.dev/)** | Lightweight Docker/Linux VMs on Apple Silicon / Intel Mac. |
| **Rancher Desktop** | Bundles `docker` / `nerdctl` and a socket; point `DOCKER_HOST` if needed. |
| **Linux (native)** | Install `docker.io` or Docker CE; add your user to the `docker` group or use `sudo`. |
| **Remote Linux VM** | Run Docker on the VM and set `DOCKER_HOST` (often `ssh://…`); see `docs/REMOTE_SANDBOX_VM.md`. |
| **Lima VM sandboxes** | With `SANDBOX_ISOLATION=lima` the API uses `limactl` instead of local Docker for **sandboxes** (different mode); still read `docs/LIMA_SANDBOX.md` — the API process is usually run **on the host**. |

### Colima on macOS: `FileNotFoundError` / missing socket

If logs show **`No such file or directory`** when fetching the Docker API version, the SDK is looking for a **Unix socket that does not exist** (often **`/var/run/docker.sock`** on Mac).

1. Start Colima: `colima start`
2. Point the API at Colima’s socket (same terminal session **before** `uvicorn`):

   ```bash
   export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
   ```

   Or: `docker context use colima` (if your Docker CLI is configured that way) and ensure **`DOCKER_HOST`** is set in the environment of the API process.

3. Restart **uvicorn**.

You can put `DOCKER_HOST=unix://...` in `api_server/.env` (loaded by `main.py`) so it is always set.

## 2. Install the client shim (replaces PyPI `e2b` for imports)

From repo root:

```bash
pip uninstall -y e2b 2>/dev/null || true
pip install -e ./e2b_shim
pip install httpx websockets
```

## 3. Local WebSocket URL scheme

`check_Code.E2BSandboxManager` builds the sandbox WebSocket URL from `sandbox.get_host()`.
For a **plain HTTP** API the proxy speaks **`ws://`**, not **`wss://`**. This repo sets:

```bash
export AGENTLIB_SANDBOX_WS_SCHEME=ws
```

before running Custodian-style tests (see `check_Code.py` `_build_ws_url`). Omit or use `wss` when the API is behind TLS.

## 4. Smoke test (curl only)

```bash
export API_BASE=http://127.0.0.1:8000
export API_KEY=test-key-12345
bash api_server/scripts/test_dropin_smoke.sh
```

### Smoke test: `POST /sandboxes` returns **503**

The API returns **503** when `create_sandbox` could not start a workload (same message as in the JSON `detail` field). The smoke script now prints that body instead of a `JSONDecodeError`.

**Check:**

1. **`docker info`** on the machine where **uvicorn** runs — not only where you run `curl`.
2. **`docker pull python:3.11`** — rule out registry / auth / disk issues.
3. If the API runs **inside a container**, the Docker socket must be mounted (e.g. `/var/run/docker.sock`) and `DOCKER_HOST` unset unless you use a remote engine.
4. **API logs** — look for `Failed to create workload`, `Failed to connect to Docker`, or template warm-build errors.
5. **Same Python as uvicorn:** `cd api_server && python scripts/diagnose_docker_env.py` (use the venv you run the API with). If this fails, fix that environment before restarting uvicorn.

### Log: `Docker client not available`

That line means **`docker.from_env()` failed** when the API started (or the client is `None`), so the SDK never got a working connection to the daemon. After a recent change, **`GET /health`** includes **`docker_engine_ok`** and **`docker_engine_detail`**, and **`POST /sandboxes`** **503** JSON `detail` includes the same hint.

**Fix (pick what matches your setup):**

- **macOS / Windows without Docker Desktop:** Install a **Docker Engine** provider (e.g. **[Colima](https://github.com/abiosoft/colima)** + Docker CLI: `brew install colima docker`, `colima start`, then `docker info`). Alternatives: **OrbStack**, **Rancher Desktop**. Then restart uvicorn.
- **macOS / Windows with Docker Desktop:** Start Docker Desktop and wait until it is idle; then restart uvicorn.
- **Linux:** Ensure the daemon is running (`sudo systemctl start docker`) and your user can access the socket (`sudo usermod -aG docker "$USER"` then log out/in), or use `sudo` to run uvicorn only if appropriate for your environment.
- **Wrong `DOCKER_HOST`:** Unset it or point it at a reachable engine (`echo $DOCKER_HOST`).
- **API inside a container without socket:** Mount `/var/run/docker.sock` (see `docker-compose.yml` in this repo).

## 5. Integration test (REST + shim + optional WS probe)

```bash
export API_BASE=http://127.0.0.1:8000
export API_KEY=test-key-12345
python api_server/scripts/test_dropin_integration.py
# optional: also open agent WS (fails fast if nothing listens on :8765 in the guest)
python api_server/scripts/test_dropin_integration.py --ws-probe
```

## 5b. Generic “Claude protocol” WebSocket E2E (**no** internal `agentlib`)

Same in-guest mock as §6 and the same **`prompt` → `result`** message shape Custodian expects from the agent WebSocket, but implemented with **only** `httpx` + `websockets` (no `check_Code` / `agentlib`).

```bash
export API_BASE=http://127.0.0.1:8000
export API_KEY=test-key-12345
export AGENTLIB_SANDBOX_WS_SCHEME=ws
pip install httpx websockets
pip install -e ./e2b_shim   # optional
python scripts/test_dropin_generic_claude_ws_e2e.py
python scripts/test_dropin_generic_claude_ws_e2e.py --no-shim   # skip AsyncSandbox.set_timeout
```

## 5c. Dockerfile template + **Anthropic SDK** in guest + drop-in E2E

Registers a template via ``POST /templates/from-dockerfile`` using a stock Dockerfile (``api_server/scripts/dockerfiles/claude_dropin_agent.Dockerfile``) that ``pip install``\ s ``anthropic``, ``httpx``, and ``websockets``. Then creates a sandbox, optionally runs a **live** Messages API call inside the guest (``ANTHROPIC_API_KEY`` from your host), starts the mock WS agent, and completes the same ``prompt`` → ``result`` probe as §5b.

```bash
export API_BASE=http://127.0.0.1:8000
export API_KEY=test-key-12345
export AGENTLIB_SANDBOX_WS_SCHEME=ws
export ANTHROPIC_API_KEY=sk-ant-api03-...   # optional
pip install httpx websockets
python scripts/test_dropin_dockerfile_claude_sdk_e2e.py
python scripts/test_dropin_dockerfile_claude_sdk_e2e.py --dockerfile /path/to/Dockerfile
python scripts/test_dropin_dockerfile_claude_sdk_e2e.py --reuse-template-id ClaudeDropin_ab12cd34ef56
```

Template builds can take several minutes (``pip install`` in the parsed Dockerfile path). Use ``--reuse-template-id`` after a successful run to iterate only on sandbox + WS steps.

## 5d. Sandbox WebSocket lab (multi-message VM drill)

End-to-end through **your API proxy** (``GET …/e2b-connection`` + ``WS …/agent-ws``): create a Docker sandbox, start a small in-guest WebSocket server on **8765**, then run a **checklist** on one connection (ping, echo, VM ``uname``/hostname, batch, second ping).

```bash
export SANDBOX_API_URL=http://127.0.0.1:8000
export API_KEY=test-key-12345
pip install httpx 'websockets>=12,<15'
python examples/sandbox_ws_lab_e2e.py
# iterate without recreating the sandbox:
python examples/sandbox_ws_lab_e2e.py --skip-create --reuse-sandbox <sandbox_id> --keep
```

Guest protocol lives in ``examples/guest_ws_lab_server.py``. For Custodian-shaped ``prompt``/``result`` only, use §5b instead.

## 6. Full agentlib `execute_turn` (mock in-guest server)

> **Important:** `check_Code.py` needs the **internal Custodian `agentlib`** (`agentlib.claude`, etc.), **not** [`pip install agentlib`](https://pypi.org/project/agentlib/) (that is a different package). See **`docs/AGENTLIB_AND_CHECK_CODE.md`**.

Uses `check_Code.E2BSandboxManager`: writes `mock_agentlib_e2b_ws_server.py` into the sandbox,
installs `websockets`, starts the mock on port **8765**, then runs **`execute_turn`** (expects a
`result` message with `status: ok`).

Requires the **real** `agentlib` package on `PYTHONPATH` or `pip install -e "/path/to/agentlib[e2b]"` (and anything `check_Code` imports) in the same venv.

```bash
export API_BASE=http://127.0.0.1:8000
export API_KEY=test-key-12345
export AGENTLIB_SANDBOX_WS_SCHEME=ws
export PYTHONPATH="$(pwd):${PYTHONPATH}"
python scripts/test_dropin_agentlib_e2e.py
```

## 7. Optional Docker image with mock preinstalled

Build context must be `api_server/`:

```bash
docker build -f api_server/scripts/Dockerfile.e2b_dropin_test_agent -t e2b-dropin-test-agent:latest api_server
```

Register a logical template pointing at that image via `POST /templates`, then use that
`template_id` for sandboxes so you can skip the `pip install` step inside the guest.

## 8. Networking (API → container IP)

If the API runs **inside Docker** and cannot reach the bridge IP of sibling containers, join the
same user-defined network or use host networking. See `docs/E2B_DROP_IN_IMPLEMENTATION.md` (B7).

By default the API **publishes** the in-container agent port (**8765**) to a random **host** TCP
port and connects the WebSocket proxy via ``ws://<E2B_DROPIN_UPSTREAM_WS_HOST>:<published>/``
(usually ``127.0.0.1``). That avoids ``172.17.0.x`` paths that fail on some macOS/Colima setups.
Set ``E2B_DROPIN_PUBLISH_AGENT_PORT=0`` to disable publishing and use bridge IP only.

When the API process runs **inside a container**, ``127.0.0.1`` is the container itself — set
``E2B_DROPIN_UPSTREAM_WS_HOST=host.docker.internal`` (Docker Desktop) or your Linux host gateway IP.

Logs like **`timed out during opening handshake`** mean the API still could not complete the
upstream WebSocket (guest not listening yet, wrong host, or firewall). Tune:

- ``E2B_DROPIN_UPSTREAM_OPEN_TIMEOUT_SEC`` (default **60**)
- ``E2B_DROPIN_UPSTREAM_CONNECT_RETRIES`` (default **3**)
- ``E2B_DROPIN_UPSTREAM_RETRY_DELAY_SEC`` (default **1**)
