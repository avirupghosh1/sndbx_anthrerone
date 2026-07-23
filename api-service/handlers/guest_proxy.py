"""API-domain WebSocket proxy for sandbox guest ports."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple

import httpx
import websockets
from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from starlette.responses import StreamingResponse
from websockets.exceptions import ConnectionClosed

from async_runner import run_io
from config import get_config
from middleware import (
    ApiKeyPrincipal,
    ClientAuthError,
    authenticate_client_credential,
    ensure_sandbox_access,
)
from handlers.guest_proxy_utils import (
    forward_headers as build_forward_headers,
    gateway_http_url,
    gateway_ws_url,
)
from orchestrator import SandboxManager
from orchestrator.guest_ports import ports_from_metadata
from orchestrator.sandbox_connections import (
    allow_public_traffic_for_row,
    traffic_access_token_for_row,
    verify_traffic_access_token,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["guest-proxy"])


def _api_credential_from_headers(headers: Any) -> Tuple[str, bool, bool]:
    api_key = (headers.get("x-api-key") or "").strip()
    if api_key:
        return api_key, False, True
    auth = (headers.get("authorization") or "").strip()
    scheme, _, token = auth.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        return token.strip(), True, False
    return "", False, False


def _close_code_for_status(status_code: int) -> int:
    if status_code == 401:
        return 4401
    if status_code == 403:
        return 4403
    if status_code == 404:
        return 4404
    if status_code == 400:
        return 1008
    return 1011


async def _close_ws(websocket: WebSocket, code: int, reason: str = "") -> None:
    try:
        await websocket.close(code=code, reason=(reason or "")[:120])
    except RuntimeError:
        pass
    except Exception:  # noqa: BLE001
        pass


async def _authenticate_ws(websocket: WebSocket) -> Tuple[Optional[ApiKeyPrincipal], bool, Optional[HTTPException]]:
    credential, auth_used_authorization, explicit_api_key = _api_credential_from_headers(websocket.headers)
    if not credential:
        return None, False, None
    cfg = get_config()
    try:
        if auth_used_authorization:
            principal = authenticate_client_credential(
                credential,
                allow_jwt=True,
                allow_api_key=bool(getattr(cfg, "AUTH_BEARER_API_KEYS_ENABLED", True)),
            )
        else:
            principal = authenticate_client_credential(credential, allow_jwt=False, allow_api_key=True)
    except ClientAuthError as exc:
        error = HTTPException(status_code=exc.status_code, detail=exc.detail)
        if explicit_api_key:
            raise error from exc
        return None, False, error
    return principal, auth_used_authorization, None


async def _authenticate_http(
    request: Request,
) -> Tuple[Optional[ApiKeyPrincipal], bool, Optional[HTTPException]]:
    credential, auth_used_authorization, explicit_api_key = _api_credential_from_headers(request.headers)
    if not credential:
        return None, False, None
    cfg = get_config()
    try:
        if auth_used_authorization:
            principal = authenticate_client_credential(
                credential,
                allow_jwt=True,
                allow_api_key=bool(getattr(cfg, "AUTH_BEARER_API_KEYS_ENABLED", True)),
            )
        else:
            principal = authenticate_client_credential(credential, allow_jwt=False, allow_api_key=True)
    except ClientAuthError as exc:
        error = HTTPException(status_code=exc.status_code, detail=exc.detail)
        if explicit_api_key:
            raise error from exc
        return None, False, error
    return principal, auth_used_authorization, None


def _provided_traffic_token(connection: Any) -> Optional[str]:
    token = (
        (connection.headers.get("e2b-traffic-access-token") or "").strip()
        or (connection.query_params.get("traffic_token") or "").strip()
        or (connection.query_params.get("traffic_access_token") or "").strip()
        or (connection.query_params.get("token") or "").strip()
    )
    return token or None


def _is_envd_port(guest_port: int) -> bool:
    envd_port = max(1, min(65535, int(getattr(get_config(), "ENVD_PORT", 49983))))
    return int(guest_port) == envd_port


def _has_envd_access_token(request: Request, guest_port: int) -> bool:
    if not _is_envd_port(guest_port):
        return False
    return bool(
        (request.headers.get("x-access-token") or "").strip()
        or (request.query_params.get("signature") or "").strip()
    )


async def _traffic_token_for_proxy(
    connection: Any,
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    row: dict,
    *,
    allow_mint: bool,
) -> Optional[str]:
    token = _provided_traffic_token(connection)
    if token:
        return token
    if not allow_mint:
        return None
    token = (traffic_access_token_for_row(row) or "").strip()
    if token:
        return token
    try:
        return await run_io(sandbox_manager.get_traffic_access_token, sandbox_id)
    except Exception:  # noqa: BLE001
        return None


def _build_http_forward_headers(
    inbound_headers: Any,
    *,
    sandbox_id: str,
    guest_port: int,
    traffic_access_token: Optional[str],
    api_auth_used_authorization: bool,
) -> Dict[str, str]:
    hop_by_hop = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
    }
    out = {k: v for k, v in inbound_headers.items() if k.lower() not in hop_by_hop}
    out["x-runtime-gateway-forwarded"] = "1"
    out["x-sandbox-id"] = sandbox_id
    out["x-guest-port"] = str(int(guest_port))
    token = (traffic_access_token or "").strip()
    if token:
        out["e2b-traffic-access-token"] = token
    if api_auth_used_authorization:
        guest_authorization = (
            (inbound_headers.get("x-guest-authorization") or "").strip()
            or (inbound_headers.get("x-sandbox-authorization") or "").strip()
        )
        if guest_authorization:
            out["authorization"] = guest_authorization
        else:
            out.pop("authorization", None)
    return out


async def _connect_upstream_with_retries(
    upstream_uri: str,
    *,
    open_timeout: float,
    connect_retries: int,
    retry_delay: float,
    additional_headers: Optional[Dict[str, str]] = None,
) -> Tuple[Any, Any]:
    last_exc: BaseException | None = None
    for attempt in range(1, connect_retries + 1):
        cm = websockets.connect(
            upstream_uri,
            open_timeout=open_timeout,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=10,
            max_size=None,
            additional_headers=additional_headers or None,
        )
        try:
            ws = await cm.__aenter__()
            return cm, ws
        except BaseException as exc:
            last_exc = exc
            try:
                await cm.__aexit__(type(exc), exc, exc.__traceback__)
            except BaseException:  # noqa: BLE001
                pass
            if attempt >= connect_retries:
                break
            logger.warning(
                "guest proxy upstream connect attempt %s/%s to %s failed: %s",
                attempt,
                connect_retries,
                upstream_uri,
                exc,
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


async def _run_websocket_bridge(websocket: WebSocket, upstream: Any) -> None:
    downstream_closed = asyncio.Event()
    upstream_closed = asyncio.Event()

    async def pump_client_to_upstream() -> None:
        try:
            while True:
                msg = await websocket.receive()
                msg_type = msg.get("type")
                if msg_type == "websocket.disconnect":
                    break
                if msg_type != "websocket.receive":
                    continue
                if msg.get("text") is not None:
                    await upstream.send(msg["text"])
                elif msg.get("bytes") is not None:
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
                try:
                    if isinstance(raw, str):
                        await websocket.send_text(raw)
                    else:
                        await websocket.send_bytes(raw)
                except RuntimeError as exc:
                    if not _is_downstream_already_closed(exc):
                        raise
                    break
        except (ConnectionClosed, WebSocketDisconnect):
            pass
        finally:
            upstream_closed.set()
            if not downstream_closed.is_set():
                await _close_ws(websocket, 1000)

    t1 = asyncio.create_task(pump_client_to_upstream())
    t2 = asyncio.create_task(pump_upstream_to_client())
    await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for task in (t1, t2):
        if not task.done():
            task.cancel()
    results = await asyncio.gather(t1, t2, return_exceptions=True)
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            continue
        if isinstance(result, BaseException):
            raise result


_HTTP_PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@router.api_route(
    "/sandboxes/{sandbox_id}/guest/{guest_port}",
    methods=_HTTP_PROXY_METHODS,
)
@router.api_route(
    "/sandboxes/{sandbox_id}/guest/{guest_port}/{guest_path:path}",
    methods=_HTTP_PROXY_METHODS,
)
async def http_guest_proxy(
    request: Request,
    sandbox_id: str,
    guest_port: int,
    guest_path: str = "",
):
    sid = (sandbox_id or "").strip()
    p = int(guest_port)
    if not sid or not (1 <= p <= 65535):
        raise HTTPException(status_code=400, detail="sandbox_id and valid guest_port are required")

    sandbox_manager = SandboxManager.__dict__.get("instance")
    if sandbox_manager is None:
        raise HTTPException(status_code=503, detail="sandbox manager unavailable")

    principal: Optional[ApiKeyPrincipal] = None
    auth_used_authorization = False
    auth_error: Optional[HTTPException] = None
    try:
        principal, auth_used_authorization, auth_error = await _authenticate_http(request)
        row = await run_io(sandbox_manager.get_sandbox, sid)
        if not row:
            raise HTTPException(status_code=404, detail=f"Sandbox not found: {sid}")
        if principal is not None:
            ensure_sandbox_access(principal, row, sid)
        reason = await run_io(sandbox_manager.get_sandbox_runtime_failure, sid)
        if reason:
            raise HTTPException(status_code=503, detail=str(reason))
        declared = ports_from_metadata(row.get("metadata") or {})
        if declared and p not in declared and not _is_envd_port(p):
            raise HTTPException(status_code=400, detail=f"port {p} not declared for sandbox")
        if principal is None:
            provided_token = _provided_traffic_token(request)
            has_valid_traffic_token = verify_traffic_access_token(row, provided_token or "")
            if (
                not allow_public_traffic_for_row(row, get_config())
                and not has_valid_traffic_token
                and not _has_envd_access_token(request, p)
            ):
                detail = (
                    str(auth_error.detail)
                    if auth_error
                    else (
                        "X-API-Key, Authorization Bearer token, valid "
                        "e2b-traffic-access-token, or envd access token required"
                    )
                )
                raise HTTPException(status_code=401, detail=detail)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "guest proxy HTTP preflight failed sandbox_id=%s guest_port=%s: %s",
            sid,
            p,
            exc,
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    cfg = get_config()
    traffic_token = await _traffic_token_for_proxy(
        request,
        sandbox_manager,
        sid,
        row,
        allow_mint=principal is not None,
    )
    if (
        not traffic_token
        and not allow_public_traffic_for_row(row, cfg)
        and not _has_envd_access_token(request, p)
    ):
        raise HTTPException(status_code=503, detail="traffic_access_token missing on sandbox")

    try:
        gateway_base = gateway_http_url(getattr(cfg, "RUNTIME_GATEWAY_URL", ""))
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    clean_guest_path = "/" + (guest_path or "").lstrip("/")
    upstream_url = f"{gateway_base.rstrip('/')}{clean_guest_path}"
    if request.url.query:
        upstream_url = f"{upstream_url}?{request.url.query}"

    gateway_headers = _build_http_forward_headers(
        request.headers,
        sandbox_id=sid,
        guest_port=p,
        traffic_access_token=traffic_token,
        api_auth_used_authorization=auth_used_authorization,
    )

    hop_by_hop = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
    body = await request.body()
    timeout = httpx.Timeout(
        600.0,
        connect=float(getattr(cfg, "E2B_DROPIN_UPSTREAM_OPEN_TIMEOUT_SEC", 60)),
    )
    client = httpx.AsyncClient(timeout=timeout, follow_redirects=False)
    try:
        upstream_req = client.build_request(
            request.method,
            upstream_url,
            headers=gateway_headers,
            content=body if body else None,
        )
        upstream_resp = await client.send(upstream_req, stream=True)
    except httpx.RequestError as exc:
        await client.aclose()
        logger.warning(
            "guest proxy HTTP failed sandbox_id=%s guest_port=%s url=%s: %s",
            sid,
            p,
            upstream_url,
            exc,
        )
        raise HTTPException(status_code=502, detail=f"guest unreachable: {exc}") from exc

    response_headers = {
        k: v for k, v in upstream_resp.headers.items() if k.lower() not in hop_by_hop
    }

    async def body_iter():
        try:
            async for chunk in upstream_resp.aiter_raw():
                yield chunk
        finally:
            await upstream_resp.aclose()
            await client.aclose()

    return StreamingResponse(
        body_iter(),
        status_code=upstream_resp.status_code,
        headers=response_headers,
    )


@router.websocket("/ws/guest")
async def websocket_guest_proxy(
    websocket: WebSocket,
    sandbox_id: str = Query(..., description="Sandbox id, e.g. sb_xxx"),
    guest_port: Optional[int] = Query(None, ge=1, le=65535, description="Guest TCP port"),
    port: Optional[int] = Query(None, ge=1, le=65535, description="Alias for guest_port"),
):
    sid = (sandbox_id or "").strip()
    p = guest_port if guest_port is not None else port
    if not sid or p is None:
        await _close_ws(websocket, 1008, "sandbox_id and guest_port are required")
        return
    p = int(p)

    sandbox_manager = SandboxManager.__dict__.get("instance")
    if sandbox_manager is None:
        await _close_ws(websocket, 1011, "sandbox manager unavailable")
        return

    try:
        principal, auth_used_authorization, auth_error = await _authenticate_ws(websocket)
        row = await run_io(sandbox_manager.get_sandbox, sid)
        if not row:
            await _close_ws(websocket, 4404, f"Sandbox not found: {sid}")
            return
        if principal is not None:
            ensure_sandbox_access(principal, row, sid)
        reason = await run_io(sandbox_manager.get_sandbox_runtime_failure, sid)
        if reason:
            await _close_ws(websocket, 1011, str(reason))
            return
        declared = ports_from_metadata(row.get("metadata") or {})
        if declared and p not in declared:
            await _close_ws(websocket, 1008, f"port {p} not declared for sandbox")
            return
        if principal is None:
            provided_token = _provided_traffic_token(websocket)
            if not allow_public_traffic_for_row(row, get_config()) and not verify_traffic_access_token(
                row,
                provided_token or "",
            ):
                detail = (
                    str(auth_error.detail)
                    if auth_error
                    else "X-API-Key, Authorization Bearer token, or valid e2b-traffic-access-token required"
                )
                await _close_ws(websocket, 4401, detail)
                return
    except HTTPException as exc:
        await _close_ws(websocket, _close_code_for_status(exc.status_code), str(exc.detail))
        return
    except Exception as exc:  # noqa: BLE001
        logger.warning("guest proxy preflight failed sandbox_id=%s guest_port=%s: %s", sid, p, exc)
        await _close_ws(websocket, 1011, str(exc))
        return

    cfg = get_config()
    traffic_token = await _traffic_token_for_proxy(
        websocket,
        sandbox_manager,
        sid,
        row,
        allow_mint=principal is not None,
    )
    if not traffic_token and not allow_public_traffic_for_row(row, cfg):
        await _close_ws(websocket, 1011, "traffic_access_token missing on sandbox")
        return

    try:
        upstream_uri = gateway_ws_url(getattr(cfg, "RUNTIME_GATEWAY_URL", ""))
    except ValueError as exc:
        await _close_ws(websocket, 1011, str(exc))
        return

    gateway_headers = build_forward_headers(
        websocket.headers,
        sandbox_id=sid,
        guest_port=p,
        traffic_access_token=traffic_token,
        api_auth_used_authorization=auth_used_authorization,
    )

    connect_cm = None
    accepted = False
    try:
        logger.info(
            "guest proxy WS connecting sandbox_id=%s guest_port=%s gateway=%s",
            sid,
            p,
            upstream_uri,
        )
        connect_cm, upstream = await _connect_upstream_with_retries(
            upstream_uri,
            open_timeout=float(getattr(cfg, "E2B_DROPIN_UPSTREAM_OPEN_TIMEOUT_SEC", 60)),
            connect_retries=int(getattr(cfg, "E2B_DROPIN_UPSTREAM_CONNECT_RETRIES", 3)),
            retry_delay=float(getattr(cfg, "E2B_DROPIN_UPSTREAM_RETRY_DELAY_SEC", 1.0)),
            additional_headers=gateway_headers,
        )
        await websocket.accept()
        accepted = True
        try:
            await _run_websocket_bridge(websocket, upstream)
        finally:
            try:
                await connect_cm.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass
    except WebSocketDisconnect:
        return
    except Exception as exc:  # noqa: BLE001
        logger.warning("guest proxy WS failed sandbox_id=%s guest_port=%s: %s", sid, p, exc)
        if accepted:
            await _close_ws(websocket, 1011, str(exc))
        else:
            await _close_ws(websocket, 1011, f"gateway websocket connect failed: {exc}")
