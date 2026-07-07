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
    additional_headers: dict[str, str] | None = None,
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
            additional_headers=additional_headers or None,
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


def _is_downstream_already_closed(exc: BaseException) -> bool:
    message = str(exc)
    return (
        "Unexpected ASGI message" in message
        and ("websocket.close" in message or "websocket.send" in message)
    )


async def _close_upstream(upstream: Any, code: int = 1000) -> None:
    try:
        await upstream.close(code=code)
    except Exception:  # noqa: BLE001
        pass


async def _close_downstream(websocket: WebSocket, code: int = 1000) -> None:
    try:
        await websocket.close(code=code)
    except RuntimeError as exc:
        if not _is_downstream_already_closed(exc):
            raise
    except Exception:  # noqa: BLE001
        pass


async def run_starlette_upstream_pumps(websocket: WebSocket, upstream: Any) -> None:
    downstream_closed = asyncio.Event()
    upstream_closed = asyncio.Event()

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
        except RuntimeError as exc:
            if not _is_downstream_already_closed(exc):
                raise
        finally:
            downstream_closed.set()
            if not upstream_closed.is_set():
                await _close_upstream(upstream)

    async def pump_upstream_to_client() -> None:
        try:
            async for raw in upstream:
                if downstream_closed.is_set():
                    break
                if isinstance(raw, str):
                    try:
                        await websocket.send_text(raw)
                    except RuntimeError as exc:
                        if not _is_downstream_already_closed(exc):
                            raise
                        break
                else:
                    try:
                        await websocket.send_bytes(raw)
                    except RuntimeError as exc:
                        if not _is_downstream_already_closed(exc):
                            raise
                        break
        except ConnectionClosed:
            pass
        except WebSocketDisconnect:
            pass
        finally:
            upstream_closed.set()
            if not downstream_closed.is_set():
                await _close_downstream(websocket)

    t1 = asyncio.create_task(pump_client_to_upstream())
    t2 = asyncio.create_task(pump_upstream_to_client())
    await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for p in (t1, t2):
        if not p.done():
            p.cancel()
    results = await asyncio.gather(t1, t2, return_exceptions=True)
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            continue
        if isinstance(result, BaseException):
            raise result
