#!/usr/bin/env python3
"""Minimal in-container WebSocket server for E2B drop-in integration tests.

Responds to ``{"type":"prompt",...}`` with ``{"type":"result","data":{...}}`` so
``check_Code.E2BSandboxManager.execute_turn`` can complete without real Claude.

Bind: ``0.0.0.0:8765`` (override with ``AGENTLIB_MOCK_WS_PORT``).

``websockets`` 14+ uses ``websockets.asyncio.server.serve`` + ``serve_forever()``;
older releases used ``websockets.serve`` + ``asyncio.Future()``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

logger = logging.getLogger(__name__)


async def _handler(websocket):
    path = getattr(websocket, "path", "") or ""
    logger.info("client connected path=%s", path)
    try:
        async for raw in websocket:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            mtype = msg.get("type")
            if mtype == "prompt":
                await websocket.send(
                    json.dumps(
                        {
                            "type": "result",
                            "data": {
                                "status": "ok",
                                "source": "mock_agentlib_e2b_ws_server",
                                "message": "drop-in test stub",
                            },
                        }
                    )
                )
                return
            if mtype == "heartbeat":
                await websocket.send(json.dumps({"type": "heartbeat", "data": {}}))
    except Exception as ex:  # noqa: BLE001
        logger.warning("handler error: %s", ex)


async def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    port = int(os.environ.get("AGENTLIB_MOCK_WS_PORT", "8765"))
    host = "0.0.0.0"

    # websockets >= 14
    try:
        from websockets.asyncio.server import serve as asyncio_serve
    except ImportError:
        asyncio_serve = None

    if asyncio_serve is not None:
        async with asyncio_serve(_handler, host, port) as server:
            logger.info("mock agentlib e2b ws listening on %s:%s (asyncio.server)", host, port)
            await server.serve_forever()
        return

    # websockets 12–13 legacy
    import websockets

    async with websockets.serve(_handler, host, port):
        logger.info("mock agentlib e2b ws listening on %s:%s (legacy serve)", host, port)
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except Exception:
        logging.basicConfig(level=logging.DEBUG)
        logger.exception("mock server fatal")
        sys.exit(1)
