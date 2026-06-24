"""ASGI data-plane proxy: ``{port}-{sandbox_id}.{domain}`` → sandbox pod (via K8s DNS)."""

from __future__ import annotations

import hmac
import logging
from typing import Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import httpx
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocket, WebSocketDisconnect

from config import get_config
from control_plane import SandboxRoute, get_control_plane_client
from host_parse import parse_ingress_host
from upstream import resolve_upstream_http
from ws_bridge import connect_upstream_with_retries, run_starlette_upstream_pumps

logger = logging.getLogger(__name__)


class SandboxDataPlaneMiddleware:
    """Reverse-proxy E2B-style sandbox hostnames to guest pods in the same namespace."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        cfg = get_config()
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        host = headers.get("host", "")
        sandbox_id_hdr = headers.get("x-sandbox-id")
        guest_port_hdr = headers.get("x-guest-port")
        parsed = parse_ingress_host(
            host,
            sandbox_domain=getattr(cfg, "SANDBOX_DOMAIN", "sndbx.com"),
            debug=bool(getattr(cfg, "SANDBOX_INGRESS_DEBUG", False)),
            sandbox_id_header=sandbox_id_hdr,
            guest_port_header=guest_port_hdr,
        )
        if not parsed:
            parsed = _parse_local_query_route(scope, host)
        if not parsed:
            await self.app(scope, receive, send)
            return

        guest_port, sandbox_id = parsed

        layer2 = _layer2_deny(cfg, headers)
        if layer2:
            await _send_error(scope, receive, send, layer2[0], layer2[1])
            return

        route = await get_control_plane_client().fetch_route(sandbox_id, guest_port)
        if route is None:
            await _send_error(scope, receive, send, 404, "sandbox not found or not running")
            return

        deny = _layer3_deny(route, headers)
        if deny:
            await _send_error(scope, receive, send, deny[0], deny[1])
            return

        upstream_base = resolve_upstream_http(
            cfg,
            sandbox_id=sandbox_id,
            guest_port=guest_port,
            route_upstream=route.upstream_http or None,
        )
        if not upstream_base:
            await _send_error(scope, receive, send, 502, "guest upstream unavailable")
            return

        if scope["type"] == "websocket":
            await self._proxy_websocket(scope, receive, send, upstream_base, headers)
            return

        request = Request(scope, receive)
        response = await self._proxy_http(request, upstream_base, headers)
        await response(scope, receive, send)

    async def _proxy_http(
        self,
        request: Request,
        upstream_base: str,
        inbound_headers: Dict[str, str],
    ) -> Response:
        path = request.url.path or "/"
        query = request.url.query
        url = f"{upstream_base.rstrip('/')}{path}"
        if query:
            url = f"{url}?{query}"

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
        out_headers = {k: v for k, v in inbound_headers.items() if k.lower() not in hop_by_hop}

        body = await request.body()
        cfg = get_config()
        timeout = httpx.Timeout(600.0, connect=float(cfg.UPSTREAM_CONNECT_TIMEOUT_SEC))
        client = httpx.AsyncClient(timeout=timeout, follow_redirects=False)
        try:
            upstream_req = client.build_request(
                request.method,
                url,
                headers=out_headers,
                content=body if body else None,
            )
            upstream_resp = await client.send(upstream_req, stream=True)
        except httpx.RequestError as exc:
            await client.aclose()
            logger.warning("data-plane HTTP proxy failed %s → %s: %s", request.url, url, exc)
            return Response(f"guest unreachable: {exc}", status_code=502)

        resp_headers = {
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
            headers=resp_headers,
        )

    async def _proxy_websocket(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        upstream_base: str,
        inbound_headers: Dict[str, str],
    ) -> None:
        ws = WebSocket(scope, receive=receive, send=send)
        parsed = urlparse(upstream_base)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 80
        path = scope.get("path") or "/"
        query = scope.get("query_string", b"").decode("latin-1")
        upstream_uri = f"ws://{host}:{port}{path}"
        if query:
            upstream_uri = f"{upstream_uri}?{query}"

        upstream_headers: Dict[str, str] = {}
        for key in ("authorization", "origin", "sec-websocket-protocol"):
            value = (inbound_headers.get(key) or "").strip()
            if value:
                upstream_headers[key.title() if key == "authorization" else key] = value

        cfg = get_config()
        accepted = False
        try:
            connect_cm, upstream = await connect_upstream_with_retries(
                upstream_uri,
                open_timeout=float(cfg.UPSTREAM_WS_OPEN_TIMEOUT_SEC),
                connect_retries=int(cfg.UPSTREAM_WS_CONNECT_RETRIES),
                retry_delay=float(cfg.UPSTREAM_WS_RETRY_DELAY_SEC),
                ping_interval=None,
                ping_timeout=None,
                additional_headers=upstream_headers or None,
            )
            await ws.accept()
            accepted = True
            try:
                await run_starlette_upstream_pumps(ws, upstream)
            finally:
                try:
                    await connect_cm.__aexit__(None, None, None)
                except Exception:  # noqa: BLE001
                    pass
        except WebSocketDisconnect:
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("data-plane WS proxy failed %s: %s", upstream_uri, exc)
            if accepted:
                try:
                    await ws.close(code=1011)
                except Exception:  # noqa: BLE001
                    pass


def _layer2_deny(cfg, headers: Dict[str, str]) -> Optional[Tuple[int, str]]:
    """Optional shared secret from nginx ingress (``X-Access-Token``)."""
    expected = (getattr(cfg, "INGRESS_ACCESS_TOKEN", None) or "").strip()
    if not expected:
        return None
    provided = (headers.get("x-access-token") or "").strip()
    if not provided:
        return 401, "missing X-Access-Token"
    if not hmac.compare_digest(provided, expected):
        return 401, "invalid X-Access-Token"
    return None


def _parse_local_query_route(scope: Scope, host_header: str) -> Optional[Tuple[int, str]]:
    """Route localhost port-forward traffic that encodes sandbox identity in query params."""
    host = (host_header or "").split(",")[0].strip().lower()
    if ":" in host and not host.startswith("["):
        host = host.rpartition(":")[0]
    if host not in ("127.0.0.1", "localhost", "::1", "[::1]"):
        return None
    raw_qs = (scope.get("query_string") or b"").decode("latin-1")
    if not raw_qs:
        return None
    params = parse_qs(raw_qs, keep_blank_values=False)
    sid = ((params.get("sandbox_id") or [""])[0]).strip()
    port_s = ((params.get("guest_port") or params.get("port") or [""])[0]).strip()
    if not sid or not port_s.isdigit():
        return None
    guest_port = int(port_s)
    if not (1 <= guest_port <= 65535):
        return None
    return guest_port, sid


def _layer3_deny(route: SandboxRoute, headers: Dict[str, str]) -> Optional[Tuple[int, str]]:
    """``e2b-traffic-access-token`` when sandbox is not public."""
    if route.allow_public_traffic:
        return None
    expected = (route.traffic_access_token or "").strip()
    if not expected:
        return 503, "traffic_access_token missing on sandbox (recreate sandbox)"
    traffic = (headers.get("e2b-traffic-access-token") or "").strip()
    if not traffic:
        return 401, "missing e2b-traffic-access-token"
    if not hmac.compare_digest(traffic, expected):
        return 401, "invalid e2b-traffic-access-token"
    return None


async def _send_error(scope: Scope, receive: Receive, send: Send, status: int, detail: str) -> None:
    if scope["type"] == "websocket":
        ws = WebSocket(scope, receive=receive, send=send)
        await ws.close(code=4401 if status == 401 else 1011, reason=detail[:120])
        return
    body = detail.encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
