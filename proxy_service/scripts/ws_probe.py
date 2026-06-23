#!/usr/bin/env python3
"""Probe WebSocket paths through proxy-service (run inside cluster pod)."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.request

import websockets


async def try_conn(label: str, uri: str, headers: dict[str, str]) -> None:
    try:
        async with websockets.connect(uri, additional_headers=headers, open_timeout=8):
            print(f"{label}: OK")
    except Exception as exc:  # noqa: BLE001
        print(f"{label}: FAIL {exc}")


async def main() -> None:
    api = os.environ.get("API_URL", "http://api-service:8000").rstrip("/")
    sid = os.environ.get("SANDBOX_ID", "").strip()
    jwt = os.environ.get("SESSION_JWT", "").strip()
    if not sid:
        print("SANDBOX_ID required", file=sys.stderr)
        sys.exit(1)

    req = urllib.request.Request(
        f"{api}/sandboxes/{sid}/e2b-connection?port=8765",
        headers={"X-API-Key": os.environ.get("API_KEY", "test-key-12345")},
    )
    conn = json.loads(urllib.request.urlopen(req, timeout=10).read())
    traffic = conn["traffic_access_token"]
    host = conn["e2b_style_host"]

    base = {"e2b-traffic-access-token": traffic}
    auth = {"Authorization": f"Bearer {jwt}"} if jwt else {}

    await try_conn("direct", f"ws://sandbox-{sid}.sandboxes.svc.cluster.local:8765/", auth)
    await try_conn("proxy-no-host", "ws://127.0.0.1:8080/", base)
    await try_conn("proxy-host", "ws://127.0.0.1:8080/", {**base, "Host": host})
    await try_conn(
        "proxy-debug",
        "ws://127.0.0.1:8080/",
        {**base, "X-Sandbox-Id": sid, "X-Guest-Port": "8765", **auth},
    )


if __name__ == "__main__":
    asyncio.run(main())
