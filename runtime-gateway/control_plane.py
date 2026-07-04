"""HTTP client to api-service (control plane) for route auth and optional upstream."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx

from config import Config, get_config

logger = logging.getLogger(__name__)


@dataclass
class SandboxRoute:
    sandbox_id: str
    guest_port: int
    upstream_http: str
    upstream_ws: str
    allow_public_traffic: bool
    traffic_access_token: Optional[str] = None
    guest_routing: Optional[dict[str, Any]] = None
    gateway_instance_id: Optional[str] = None
    gateway_route_base: Optional[str] = None
    gateway_api_base: Optional[str] = None


@dataclass
class SandboxRouteFailure:
    status_code: int
    detail: str


class ControlPlaneClient:
    def __init__(self, config: Optional[Config] = None) -> None:
        self._cfg = config or get_config()

    def _headers(self) -> dict[str, str]:
        key = (self._cfg.CONTROL_PLANE_API_KEY or "").strip()
        if not key:
            return {}
        return {"X-API-Key": key}

    async def fetch_route(self, sandbox_id: str, guest_port: int) -> Optional[SandboxRoute | SandboxRouteFailure]:
        base = (self._cfg.CONTROL_PLANE_URL or "").strip().rstrip("/")
        if not base:
            logger.error("CONTROL_PLANE_URL is not set")
            return None
        sid = (sandbox_id or "").strip()
        url = f"{base}/internal/sandboxes/{sid}/route"
        timeout = httpx.Timeout(self._cfg.CONTROL_PLANE_TIMEOUT_SEC)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.get(
                    url,
                    params={"port": int(guest_port)},
                    headers=self._headers(),
                )
        except httpx.RequestError as exc:
            logger.warning("control plane route lookup failed sandbox=%s port=%s: %s", sid, guest_port, exc)
            return SandboxRouteFailure(status_code=502, detail=f"control plane unreachable: {exc}")

        if resp.status_code in (404, 409, 410):
            try:
                data = resp.json()
                detail = str(data.get("message") or data.get("detail") or resp.text[:500] or "control plane rejected route lookup")
            except Exception:
                detail = resp.text[:500] or "control plane rejected route lookup"
            return SandboxRouteFailure(status_code=resp.status_code, detail=detail)
        if resp.status_code >= 400:
            logger.warning(
                "control plane route lookup HTTP %s sandbox=%s port=%s body=%s",
                resp.status_code,
                sid,
                guest_port,
                resp.text[:500],
            )
            return SandboxRouteFailure(status_code=502, detail=resp.text[:500] or "control plane route lookup failed")

        data = resp.json()
        return SandboxRoute(
            sandbox_id=str(data.get("sandbox_id") or sid),
            guest_port=int(data.get("guest_port") or guest_port),
            upstream_http=str(data.get("upstream_http") or ""),
            upstream_ws=str(data.get("upstream_ws") or ""),
            allow_public_traffic=bool(data.get("allow_public_traffic")),
            traffic_access_token=(str(data.get("traffic_access_token")).strip() or None)
            if data.get("traffic_access_token")
            else None,
            guest_routing=data.get("guest_routing") if isinstance(data.get("guest_routing"), dict) else None,
            gateway_instance_id=(str(data.get("gateway_instance_id")).strip() or None)
            if data.get("gateway_instance_id")
            else None,
            gateway_route_base=(str(data.get("gateway_route_base")).strip() or None)
            if data.get("gateway_route_base")
            else None,
            gateway_api_base=(str(data.get("gateway_api_base")).strip() or None)
            if data.get("gateway_api_base")
            else None,
        )


_client: Optional[ControlPlaneClient] = None


def get_control_plane_client() -> ControlPlaneClient:
    global _client
    if _client is None:
        _client = ControlPlaneClient()
    return _client
