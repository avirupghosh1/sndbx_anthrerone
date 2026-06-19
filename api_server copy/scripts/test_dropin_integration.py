#!/usr/bin/env python3
"""
Integration tests: sandbox REST + e2b-connection + **my_sdk** ``AsyncSandbox`` + optional WebSocket open.

Requires: API running, Docker, ``E2B_DROPIN_WS_SECRET`` on server, ``httpx``, ``websockets`` (optional).

  export API_BASE=http://127.0.0.1:8000
  export API_KEY=test-key-12345
  pip install httpx websockets
  python api_server/scripts/test_dropin_integration.py

With ``--ws-probe``, opens the agent WS (upstream may close quickly if nothing listens on :8765).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def _env(name: str, default: str) -> str:
    return (os.environ.get(name) or default).strip()


async def _main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--ws-probe", action="store_true", help="Try agent WebSocket after create")
    args = p.parse_args()

    try:
        import httpx
    except ImportError:
        print("Install httpx: pip install httpx", file=sys.stderr)
        return 2

    base = _env("API_BASE", "http://127.0.0.1:8000").rstrip("/")
    key = _env("API_KEY", "test-key-12345")

    async with httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(180.0, connect=15.0)) as c:
        h = {"X-API-Key": key}
        r = await c.get("/health", headers=h)
        r.raise_for_status()
        print("health:", r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text[:200])

        cr = await c.post(
            "/sandboxes",
            headers=h,
            json={"template_id": "python:3.11", "timeout": 900},
        )
        cr.raise_for_status()
        sid = cr.json()["sandbox_id"]
        print("created:", sid)

        st = await c.get(f"/sandboxes/{sid}/status", headers=h)
        st.raise_for_status()
        body = st.json()
        print("status:", body)
        assert body.get("running") is True, "sandbox should be running"
        assert "timeout_seconds" in body

        tr = await c.post(
            f"/sandboxes/{sid}/timeout",
            headers=h,
            json={"timeout_seconds": 1200},
        )
        tr.raise_for_status()
        print("timeout refresh:", tr.json())
        assert tr.json().get("refreshed") is True

        er = await c.get(f"/sandboxes/{sid}/e2b-connection", headers=h)
        conn: dict | None = None
        if er.status_code == 503:
            print("SKIP e2b-connection: server missing E2B_DROPIN_WS_SECRET")
        else:
            er.raise_for_status()
            conn = er.json()
            print("e2b-connection keys:", sorted(conn.keys()))
            assert conn.get("traffic_access_token")
            assert conn.get("e2b_style_host")
            assert conn.get("ws_url")

        rr = await c.post(
            f"/sandboxes/{sid}/commands/run",
            headers=h,
            json={"command": "echo integration-ok", "cwd": "/", "timeout": 30},
        )
        rr.raise_for_status()
        out = rr.json()
        print("run:", out)
        assert out.get("exit_code") == 0

        # my_sdk AsyncSandbox set_timeout + commands.run (``pip install -e ../my_sandbox_sdk``)
        try:
            from my_sdk import AsyncSandbox
        except ImportError:
            print("SKIP my_sdk: pip install -e ./my_sandbox_sdk (from repo root)")
        else:
            sb = await AsyncSandbox.connect(sid, api_url=base, api_key=key)
            await sb.set_timeout(1800)
            r2 = await sb.commands.run("echo shim-ok")
            assert r2.exit_code == 0
            print("shim commands.run:", r2.stdout.strip())

        if args.ws_probe and conn is not None:
            try:
                import websockets
            except ImportError:
                print("SKIP ws-probe: pip install websockets")
            else:
                ws_url = conn["ws_url"]
                token = conn["traffic_access_token"]
                headers = {"e2b-traffic-access-token": token}
                try:
                    async with websockets.connect(ws_url, additional_headers=headers) as ws:
                        print("ws opened:", ws_url[:80])
                        await asyncio.wait_for(ws.recv(), timeout=2.0)
                except Exception as ex:  # noqa: BLE001
                    print("ws probe (expected if no agent on 8765):", type(ex).__name__, ex)

        kr = await c.post(f"/sandboxes/{sid}/kill", headers=h)
        kr.raise_for_status()
        print("killed:", sid)

    print("ALL OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
