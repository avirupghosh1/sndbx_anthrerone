"""Pure helpers for API-domain guest WebSocket proxying."""

from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse


def gateway_ws_url(runtime_gateway_url: str) -> str:
    raw = (runtime_gateway_url or "").strip().rstrip("/")
    if not raw:
        raise ValueError("RUNTIME_GATEWAY_URL is not configured")
    parsed = urlparse(raw if "://" in raw else f"http://{raw}")
    if not parsed.netloc:
        raise ValueError(f"Invalid RUNTIME_GATEWAY_URL: {runtime_gateway_url!r}")
    scheme = "wss" if (parsed.scheme or "http").lower() in ("https", "wss") else "ws"
    path = (parsed.path or "").rstrip("/") + "/"
    return urlunparse((scheme, parsed.netloc, path, "", "", ""))


def gateway_http_url(runtime_gateway_url: str) -> str:
    raw = (runtime_gateway_url or "").strip().rstrip("/")
    if not raw:
        raise ValueError("RUNTIME_GATEWAY_URL is not configured")
    parsed = urlparse(raw if "://" in raw else f"http://{raw}")
    if not parsed.netloc:
        raise ValueError(f"Invalid RUNTIME_GATEWAY_URL: {runtime_gateway_url!r}")
    scheme = "https" if (parsed.scheme or "http").lower() in ("https", "wss") else "http"
    path = (parsed.path or "").rstrip("/")
    return urlunparse((scheme, parsed.netloc, path, "", "", ""))


def forward_headers(
    inbound_headers: Any,
    *,
    sandbox_id: str,
    guest_port: int,
    traffic_access_token: Optional[str],
    api_auth_used_authorization: bool = False,
) -> Dict[str, str]:
    out: Dict[str, str] = {
        "x-runtime-gateway-forwarded": "1",
        "x-sandbox-id": sandbox_id,
        "x-guest-port": str(int(guest_port)),
    }

    guest_authorization = (
        (inbound_headers.get("x-guest-authorization") or "").strip()
        or (inbound_headers.get("x-sandbox-authorization") or "").strip()
    )
    if guest_authorization:
        out["authorization"] = guest_authorization
    elif not api_auth_used_authorization:
        authorization = (inbound_headers.get("authorization") or "").strip()
        if authorization:
            out["authorization"] = authorization

    for key in ("origin", "sec-websocket-protocol"):
        value = (inbound_headers.get(key) or "").strip()
        if value:
            out[key] = value

    token = (traffic_access_token or "").strip()
    if token:
        out["e2b-traffic-access-token"] = token
    return out
