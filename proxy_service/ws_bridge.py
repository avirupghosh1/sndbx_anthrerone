"""Bidirectional WebSocket bridge (client ↔ sandbox guest)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Tuple

import websockets
from starlette.websockets import WebSocket, WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


async def connect_upstream_with_retries(
    upstream_uri: str,
    *,
    open_timeout: float,
    connect_retries: int,
    retry_delay: float,
    ping_interval: float | None = None,
    ping_timeout: float | None = None,
) -> Tuple[Any, Any]:
    last_exc: BaseException | None = None
    for attempt in range(1, connect_retries + 1):
        cm = websockets.connect(
            upstream_uri,
            open_timeout=open_timeout,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            close_timeout=10,
            max_size=None,
        )
        try:
            ws = await cm.__aenter__()
            return cm, ws
        except BaseException as ex:
            last_exc = ex
            try:
                await cm.__aexit__(type(ex), ex, ex.__traceback__)
            except BaseException:  # noqa: BLE001
                pass
            if attempt >= connect_retries:
                break
            logger.warning(
                "upstream WS connect attempt %s/%s to %s failed: %s",
                attempt,
                connect_retries,
                upstream_uri,
                ex,
            )
            if retry_delay > 0:
                await asyncio.sleep(retry_delay)
    assert last_exc is not None
    raise last_exc


async def run_starlette_upstream_pumps(websocket: WebSocket, upstream: Any) -> None:
    async def pump_client_to_upstream() -> None:
        try:
            while True:
                msg = await websocket.receive()
                mtype = msg.get("type")
                if mtype == "websocket.disconnect":
                    break
                if mtype != "websocket.receive":
                    continue
                if "text" in msg and msg["text"] is not None:
                    await upstream.send(msg["text"])
                elif "bytes" in msg and msg["bytes"] is not None:
                    await upstream.send(msg["bytes"])
        except WebSocketDisconnect:
            pass

    async def pump_upstream_to_client() -> None:
        try:
            async for raw in upstream:
                if isinstance(raw, str):
                    await websocket.send_text(raw)
                else:
                    await websocket.send_bytes(raw)
        except ConnectionClosed:
            pass

    t1 = asyncio.create_task(pump_client_to_upstream())
    t2 = asyncio.create_task(pump_upstream_to_client())
    _done, _ = await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    if t1 in _done and not t2.done():
        try:
            await asyncio.wait_for(t2, timeout=30.0)
        except asyncio.TimeoutError:
            t2.cancel()
    elif t2 in _done:
        await asyncio.sleep(0)
        if not t1.done():
            t1.cancel()
    for p in (t1, t2):
        if not p.done():
            p.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
