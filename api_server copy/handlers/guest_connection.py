"""Generic data-plane connection metadata for any guest port."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from async_runner import run_io
from config import get_config
from middleware import validate_api_key, SandboxNotFoundException
from models import SandboxE2bConnectionResponse, SandboxGuestConnectionResponse
from orchestrator import SandboxManager
from orchestrator.guest_ports import ports_from_metadata
from orchestrator.sandbox_connections import (
    data_plane_base_url,
    data_plane_enabled_for_config,
    get_host_for_sandbox,
)

router = APIRouter(prefix="/sandboxes", tags=["data-plane"])


async def _connection_for_port(
    *,
    sandbox_id: str,
    port: int,
    scheme: Literal["ws", "http"],
    sandbox_manager: SandboxManager,
) -> SandboxGuestConnectionResponse:
    cfg = get_config()
    sid = sandbox_id.strip()
    p = int(port)
    if not (1 <= p <= 65535):
        raise HTTPException(status_code=400, detail="port must be between 1 and 65535")

    row = await run_io(sandbox_manager.get_sandbox, sid)
    if not row:
        raise SandboxNotFoundException(sid)
    if not await run_io(sandbox_manager.is_running, sid):
        raise HTTPException(status_code=409, detail="Sandbox is not running")

    declared = ports_from_metadata(row.get("metadata") or {})
    if declared and p not in declared:
        raise HTTPException(
            status_code=400,
            detail=f"port {p} not declared for sandbox (metadata.guest_ports={declared})",
        )

    if not data_plane_enabled_for_config(cfg):
        raise HTTPException(status_code=503, detail="SANDBOX_DATA_PLANE_ENABLED is false on api-service")

    token = await run_io(sandbox_manager.get_traffic_access_token, sid)
    if not token:
        raise HTTPException(
            status_code=503,
            detail="traffic_access_token missing on sandbox (recreate with SANDBOX_DATA_PLANE_ENABLED)",
        )

    url = data_plane_base_url(cfg, sandbox_id=sid, port=p, scheme=scheme)
    if scheme == "ws":
        url = url.rstrip("/") + "/"

    return SandboxGuestConnectionResponse(
        sandbox_id=sid,
        guest_port=p,
        scheme=scheme,
        url=url,
        data_plane_host=get_host_for_sandbox(cfg, sandbox_id=sid, port=p),
        traffic_access_token=token,
    )


@router.get("/{sandbox_id}/connection", response_model=SandboxGuestConnectionResponse)
async def get_guest_connection(
    sandbox_id: str,
    port: int = Query(..., ge=1, le=65535, description="Guest TCP port your server listens on"),
    scheme: Literal["ws", "http"] = Query("ws", description="Client protocol (WebSocket or HTTP)"),
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Return data-plane URL + ``traffic_access_token`` for any guest port (via proxy-service)."""
    return await _connection_for_port(
        sandbox_id=sandbox_id,
        port=port,
        scheme=scheme,
        sandbox_manager=sandbox_manager,
    )


@router.get("/{sandbox_id}/e2b-connection", response_model=SandboxE2bConnectionResponse)
async def get_e2b_connection_compat(
    sandbox_id: str,
    port: int = Query(..., ge=1, le=65535, description="Guest WebSocket port (required; no default)"),
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Legacy alias for SDK compat — same as ``/connection?scheme=ws``; port is never assumed."""
    info = await _connection_for_port(
        sandbox_id=sandbox_id,
        port=port,
        scheme="ws",
        sandbox_manager=sandbox_manager,
    )
    return SandboxE2bConnectionResponse(
        sandbox_id=info.sandbox_id,
        agent_port=info.guest_port,
        ws_url=info.url,
        traffic_access_token=info.traffic_access_token,
        e2b_style_host=info.data_plane_host,
    )
