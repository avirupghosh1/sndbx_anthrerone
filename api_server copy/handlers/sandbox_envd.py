"""Envd-style in-guest HTTP daemon connection metadata (Phase 1)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from async_runner import run_io
from middleware import validate_api_key, SandboxNotFoundException
from models import SandboxEnvdConnectionResponse
from orchestrator import SandboxManager

router = APIRouter(prefix="/sandboxes", tags=["envd"])


@router.get("/{sandbox_id}/envd-connection", response_model=SandboxEnvdConnectionResponse)
async def get_envd_connection(
    sandbox_id: str,
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Return host URL + token to talk **directly** to the guest envd HTTP server (port publish).

    Requires ``ENVD_PUBLISH_PORT=true`` on the API, Docker/gVisor runtime, and the guest process
    listening on ``ENVD_PORT`` (default 49983) with ``ENVD_ACCESS_TOKEN`` set (injected at create).
    """
    sid = sandbox_id.strip()
    row = await run_io(sandbox_manager.get_sandbox, sid)
    if not row:
        raise SandboxNotFoundException(sid)
    if not await run_io(sandbox_manager.is_running, sid):
        raise HTTPException(status_code=409, detail="Sandbox is not running")

    info, deny = await run_io(sandbox_manager.get_envd_connection_ex, sid)
    if not info:
        raise HTTPException(
            status_code=503,
            detail=(
                "envd connection unavailable. "
                + (deny or "unknown reason")
                + " General fix: Docker/gVisor with ENVD_PUBLISH_PORT=true, guest on ENVD_PORT with "
                "ENVD_ACCESS_TOKEN (set at sandbox create); restart API after changing env, then create a new sandbox."
            ),
        )
    return SandboxEnvdConnectionResponse(**info)
