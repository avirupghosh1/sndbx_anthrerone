"""Sandbox endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, List

from async_runner import run_io
from models import (
    CreateSandboxRequest,
    CreateSnapshotRequest,
    RefreshSandboxTimeoutRequest,
    SandboxResponse,
    SandboxLifecycleResponse,
    SandboxTimeoutRefreshResponse,
    SnapshotRecordResponse,
)
from middleware import (
    ApiKeyPrincipal,
    SandboxNotFoundException,
    SandboxRuntimeLostException,
    ensure_sandbox_access,
    validate_api_key,
)
from orchestrator import SandboxManager
from orchestrator.sandbox_connections import enrich_sandbox_response

router = APIRouter(prefix="/sandboxes", tags=["sandboxes"])


def _resolve_template_id_for_principal(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, template_id: str | None) -> str:
    requested = (template_id or "").strip() or "python:3.11"
    owned = sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, requested)
    if owned:
        return str(owned["template_id"])
    if principal.client_id == "bootstrap-local-client":
        # Local compatibility: the static API_KEY is a bootstrap/dev key, while
        # templates may have been built through a real portal client key.
        materialized = sandbox_manager.db.get_best_sandbox_template_by_alias(
            requested,
            exclude_template_id=requested,
        )
        if materialized:
            return str(materialized["template_id"])
    return requested


def _owned_sandbox_or_404(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, sandbox_id: str) -> dict:
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)
    return ensure_sandbox_access(principal, sandbox, sandbox_id)


def _ensure_live_sandbox(sandbox_manager: SandboxManager, sandbox_id: str) -> None:
    reason = sandbox_manager.get_sandbox_runtime_failure(sandbox_id)
    if reason:
        raise SandboxRuntimeLostException(sandbox_id, reason)


@router.post("", response_model=SandboxResponse)
async def create_sandbox(
    request: CreateSandboxRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Create new sandbox."""
    resolved_template_id = _resolve_template_id_for_principal(sandbox_manager, principal, request.template_id)
    sandbox_id = await run_io(
        sandbox_manager.create_sandbox,
        resolved_template_id,
        request.metadata,
        request.cpu_limit,
        request.memory_limit,
        request.timeout,
        request.from_snapshot_image,
        principal.client_id,
        principal.key_id,
    )

    if not sandbox_id:
        hint = sandbox_manager.describe_docker_workload_blocker()
        detail = (
            "Failed to create sandbox: Docker could not start a workload. "
            "Check Docker socket, image pull, and template_id."
        )
        if hint:
            detail = f"{detail} {hint}"
        raise HTTPException(status_code=503, detail=detail)

    sandbox = sandbox_manager.get_sandbox_for_create_response(sandbox_id)

    return SandboxResponse(**enrich_sandbox_response(sandbox, sandbox_manager._config, include_secrets=True))


@router.post("/{sandbox_id}/snapshot", response_model=SnapshotRecordResponse)
async def create_sandbox_snapshot(
    sandbox_id: str,
    request: CreateSnapshotRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Docker: ``docker commit`` (default OCI or ``runsc``) or Firecracker: full VM snapshot (``fc-bundle:`` ref)."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    out = await run_io(sandbox_manager.create_filesystem_snapshot, sandbox_id, request.label)
    if not out:
        if not sandbox_manager.get_sandbox(sandbox_id):
            raise SandboxNotFoundException(sandbox_id)
        raise HTTPException(
            status_code=501,
            detail=(
                "Filesystem snapshot unavailable: requires Docker Engine + successful `docker commit`, "
                "or Firecracker with snapshot support (`fc-bundle:`); see docs/FIRECRACKER.md and "
                "docs/E2B_COMPARISON.md."
            ),
        )
    return SnapshotRecordResponse(**out)


@router.get("/{sandbox_id}/snapshots", response_model=List[SnapshotRecordResponse])
async def list_sandbox_snapshots(
    sandbox_id: str,
    limit: int = 50,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List filesystem snapshots recorded for this sandbox."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    rows = await run_io(sandbox_manager.list_filesystem_snapshots, sandbox_id, limit)
    return [SnapshotRecordResponse(**r) for r in rows]


@router.get("/{sandbox_id}/status", response_model=SandboxLifecycleResponse)
async def get_sandbox_status(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Return DB state and whether the workload is still running (cheap poll vs full ``GET``)."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    data = await run_io(sandbox_manager.get_sandbox_lifecycle, sandbox_id)
    if not data:
        raise SandboxNotFoundException(sandbox_id)
    return SandboxLifecycleResponse(**data)


@router.post("/{sandbox_id}/timeout", response_model=SandboxTimeoutRefreshResponse)
async def refresh_sandbox_timeout(
    sandbox_id: str,
    request: RefreshSandboxTimeoutRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Refresh stored sandbox lease (E2B ``set_timeout`` / Custodian heartbeat)."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    ok = await run_io(
        sandbox_manager.refresh_sandbox_timeout,
        sandbox_id,
        request.timeout_seconds,
    )
    return SandboxTimeoutRefreshResponse(
        sandbox_id=sandbox_id.strip(),
        timeout_seconds=int(request.timeout_seconds),
        refreshed=bool(ok),
    )


@router.get("/{sandbox_id}", response_model=SandboxResponse)
async def get_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Get sandbox info."""
    sandbox = _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    return SandboxResponse(**enrich_sandbox_response(sandbox, sandbox_manager._config, include_secrets=False))


@router.get("", response_model=list)
async def list_sandboxes(
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List all sandboxes."""
    sandboxes = sandbox_manager.db.list_sandboxes(limit=limit, offset=offset, owner_client_id=principal.client_id)

    return [
        SandboxResponse(**enrich_sandbox_response(s, sandbox_manager._config, include_secrets=False))
        for s in sandboxes
    ]


@router.post("/{sandbox_id}/kill")
async def kill_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Kill sandbox."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    success = await run_io(sandbox_manager.kill_sandbox, sandbox_id)

    if not success:
        raise SandboxNotFoundException(sandbox_id)

    return {"success": True, "sandbox_id": sandbox_id}


@router.post("/{sandbox_id}/pause")
async def pause_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Pause sandbox."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    success = await run_io(sandbox_manager.pause_sandbox, sandbox_id)

    if not success:
        raise SandboxNotFoundException(sandbox_id)

    return {"success": True, "sandbox_id": sandbox_id}


@router.post("/{sandbox_id}/resume")
async def resume_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Resume sandbox."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    success = await run_io(sandbox_manager.resume_sandbox, sandbox_id)

    if not success:
        raise SandboxNotFoundException(sandbox_id)

    return {"success": True, "sandbox_id": sandbox_id}


@router.get("/{sandbox_id}/metrics")
async def get_sandbox_metrics(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Get sandbox metrics."""
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)
    metrics = await run_io(sandbox_manager.get_metrics, sandbox_id)

    if not metrics:
        raise SandboxNotFoundException(sandbox_id)

    return metrics
