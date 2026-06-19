# E2B drop-in replacement — full build list and implementation status

This document lists **everything** required so a host app (e.g. unmodified `check_Code.py` / Custodian) **cannot tell** it is not talking to E2B, while the backend uses **your API + Docker** (or other engines later).

Legend: **Done** = implemented in this repo (initial slice). **Todo** = still required for full parity.

---

## A. User-visible surface (must match E2B)

| # | Item | Purpose | Status |
|---|------|---------|--------|
| A1 | **`from my_sdk import AsyncSandbox`** (or legacy ``from e2b import AsyncSandbox`` via shim) | Same code import | **Done** — canonical: ``pip install -e ./my_sandbox_sdk``; legacy: ``pip install -e ./e2b_shim`` (uninstall PyPI ``e2b`` first) |
| A2 | **`AsyncSandbox.create(...)`** / **`beta_create`** | Spawn sandbox from template alias | **Done** — ``my_sdk`` / shim → ``POST /sandboxes``; optional ``E2B_TEMPLATE_MAP`` JSON for aliases (shim) |
| A3 | **`AsyncSandbox.connect(sandbox_id)`** | Reattach to existing VM | **Done** — ``my_sdk`` / shim: ``GET /sandboxes/{id}/status`` + ``GET …/e2b-connection`` |
| A4 | **`sandbox.sandbox_id`** | Stable id string | **Done** — your DB already has `sandbox_id` |
| A5 | **`sandbox.get_host(port)`** | Hostname for WSS URL | **Done** — shim uses `e2b_style_host` from `GET …/e2b-connection` |
| A6 | **`sandbox.traffic_access_token`** | Edge auth header | **Done** — `GET /sandboxes/{id}/e2b-connection` returns minted token |
| A7 | **`sandbox.commands.run(cmd, envs=..., timeout=...)`** | In-guest shell | **Done** — shim → `POST /sandboxes/{id}/commands/run` → `CommandResult` |
| A8 | **`sandbox.set_timeout(seconds)`** | Lease refresh | **Done** — `POST /sandboxes/{id}/timeout` + shim; updates SQLite lease (container wall-clock unchanged) |
| A9 | **WebSocket URL + headers** | `wss://…` + `e2b-traffic-access-token` + optional `Authorization` | **Done** — `WS /sandboxes/{id}/agent-ws` + token verify |
| A10 | **WS payload semantics** | `prompt`, `tool_call`, `claude_sdk_event`, `result`, … | **Done** (transparent proxy) — assumes in-container `agentlib-e2b-server` on configured port |

---

## B. API server (this repo)

| # | Item | Purpose | Status |
|---|------|---------|--------|
| B1 | **HMAC traffic token** | Stateless auth for WS without `X-API-Key` on every frame | **Done** — `e2b_dropin/tokens.py` |
| B2 | **Docker internal IP + port** | Upstream ``ws://<container_ip>:<port>/`` or **published** ``ws://<host>:<mapped>/`` | **Done** — ``get_container_internal_ipv4`` + optional host publish (``E2B_DROPIN_PUBLISH_AGENT_PORT``) |
| B3 | **`GET /sandboxes/{id}/e2b-connection`** | Returns `ws_url`, `traffic_access_token`, `agent_port` | **Done** |
| B4 | **`WS /sandboxes/{id}/agent-ws`** | Bidirectional proxy to upstream WS | **Done** — Docker engine only; FC/Lima return 501 |
| B5 | **Config** `E2B_DROPIN_WS_SECRET`, `E2B_DROPIN_AGENT_PORT`, `E2B_DROPIN_TOKEN_TTL_SEC` | Token signing + default agent port (8765) | **Done** in `config.py` |
| B6 | **Public WS base override** `E2B_DROPIN_PUBLIC_WS_BASE` | When API is behind reverse proxy | **Done** — `_public_ws_base` prefers `E2B_DROPIN_PUBLIC_WS_BASE` |
| B7 | **API-in-Docker → container IP** | Same-host Docker cannot always reach bridge IP | **Todo** — document `network_mode: host` or join same compose network; **default host port publish** avoids bridge for many setups |
| B8 | **`network={"allow_public_traffic": False}`** parity | Enforce no public ingress except tokened WS | **Todo** — security policy / bind addresses |
| B9 | **``POST /sandboxes/{id}/timeout``** | E2B ``set_timeout`` / Custodian heartbeat | **Done** |

---

| # | Item | Purpose | Status |
|---|------|---------|--------|
| C1 | **Template alias → `template_id`** | `AsyncSandbox.create(template="custodian-…")` | **Partial** — env `E2B_TEMPLATE_MAP` JSON; full DB alias table still optional |
| C2 | **E2B `Template.build` from Dockerfile** | One-time image build | **Partial** — you have `POST /templates/from-dockerfile`; shim should call it with same Dockerfile/context |

---

## D. Shim package (`e2b_shim/`)

| # | Item | Status |
|---|------|--------|
| D1 | **`AsyncSandbox` stub** with `create` / `connect` | **Done** — `e2b_shim/src/e2b/async_sandbox.py` |
| D2 | **`Sandbox` handle** with `get_host`, `traffic_access_token`, `commands`, `set_timeout` | **Done** (same class; sync `Sandbox` not required for Custodian path) |
| D3 | **Env `E2B_API_KEY` → your API key** | **Done** |

---

## E. In-container image

| # | Item | Status |
|---|------|--------|
| E1 | **`agentlib-e2b-server` listening on `0.0.0.0:8765`** | **Your** Dockerfile / template responsibility |
| E2 | **Same JSON protocol** as `check_Code.py` | No server change if binary-compatible |

---

## Implementation order (recommended)

1. **Image + port** — Confirm `agentlib-e2b-server` reachable from API host to container IP:port (**E1**, **B7**).
2. **Token + `GET …/e2b-connection`** — Host can mint URL + token (**B1**, **B3**, **A6**).
3. **WS proxy** — Unlocks `execute_turn` without shim (**B2**, **B4**, **A9–A10**).
4. **`AsyncSandbox` shim** — Swap imports (**A1–A3**, **A5**, **D**).
5. **`commands.run` + `set_timeout`** — (**A7–A8**).
6. **Template alias registry** — (**C1–C2**).
7. **Hardening** — tokens, TLS, `allow_public_traffic` (**B8**).

Steps **1–4** in this PR are the **minimum vertical slice** for a demo: WS Claude turn works if the app uses connection info + raw websockets; full drop-in still needs **D** + **A7–A8** + **C**.
