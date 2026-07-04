"""Internal routing registry for runtime-gateway (control plane -> guest upstream)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from async_runner import run_io
from middleware import validate_internal_api_key, SandboxNotFoundException, SandboxRuntimeLostException
from orchestrator import SandboxManager
from orchestrator.sandbox_connections import (
    allow_public_traffic_for_row,
    resolve_guest_upstream_http,
    traffic_access_token_for_row,
)

router = APIRouter(prefix="/internal", tags=["internal"])


@router.get("/sandboxes/{sandbox_id}/route")
async def get_sandbox_route(
    sandbox_id: str,
    port: int = Query(..., ge=1, le=65535, description="Guest TCP port"),
    api_key: str = Depends(validate_internal_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Return dialable upstream for runtime-gateway. Not for SDK clients; use data-plane hostnames."""
    sid = sandbox_id.strip()
    row = await run_io(sandbox_manager.get_sandbox, sid)
    if not row:
        raise SandboxNotFoundException(sid)
    reason = await run_io(sandbox_manager.get_sandbox_runtime_failure, sid)
    if reason:
        raise SandboxRuntimeLostException(sid, reason)

    ready = await run_io(
        sandbox_manager.ensure_guest_port_ready,
        sid,
        port,
        timeout_seconds=float(getattr(sandbox_manager._config, "SANDBOX_ROUTE_READY_WAIT_SEC", 12.0) or 0.0),
    )
    if not ready:
        raise HTTPException(status_code=502, detail="guest upstream not ready")

    upstream = await run_io(resolve_guest_upstream_http, sandbox_manager, sid, port)
    if not upstream:
        raise HTTPException(status_code=502, detail="guest upstream unavailable")

    cfg = sandbox_manager._config
    upstream_ws = upstream.replace("https://", "wss://").replace("http://", "ws://").rstrip("/") + "/"
    out: dict = {
        "sandbox_id": sid,
        "guest_port": port,
        "upstream_http": upstream,
        "upstream_ws": upstream_ws,
        "allow_public_traffic": allow_public_traffic_for_row(row, cfg),
        "gateway_instance_id": str(row.get("gateway_instance_id") or ""),
        "gateway_route_base": str(row.get("gateway_route_base") or ""),
        "gateway_api_base": str(row.get("gateway_api_base") or ""),
    }
    guest_routing = (row.get("metadata") or {}).get("guest_routing")
    if isinstance(guest_routing, dict):
        out["guest_routing"] = guest_routing
    if not out["allow_public_traffic"]:
        tok = traffic_access_token_for_row(row)
        if tok:
            out["traffic_access_token"] = tok
    return out
