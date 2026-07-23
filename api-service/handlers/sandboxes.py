"""Generic sandbox endpoints used by the local SDK."""

import asyncio
import logging
import threading
import time
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from async_runner import run_io
from middleware import (
    ApiKeyPrincipal,
    SandboxNotFoundException,
    SandboxRuntimeLostException,
    ensure_sandbox_access,
    validate_api_key,
)
from models import (
    CreateSandboxRequest,
    CreateSnapshotRequest,
    RefreshSandboxTimeoutRequest,
    ResizeWarmPoolRequest,
    SandboxLifecycleResponse,
    SandboxResponse,
    SandboxTimeoutRefreshResponse,
    SnapshotRecordResponse,
    WarmPoolResizeResponse,
)
from orchestrator import SandboxManager
from orchestrator.sandbox_connections import enrich_sandbox_response

router = APIRouter(prefix="/sandboxes", tags=["sandboxes"])
logger = logging.getLogger(__name__)


def strip_secret_metadata(metadata: Any) -> dict:
    md = dict(metadata or {}) if isinstance(metadata, dict) else {}
    md.pop("envd_access_token", None)
    md.pop("traffic_access_token", None)
    return md


def sandbox_response_payload(
    sandbox: Optional[dict],
    sandbox_manager: SandboxManager,
    *,
    include_secrets: bool,
) -> dict:
    raw = dict(
        enrich_sandbox_response(
            sandbox or {},
            sandbox_manager._config,
            include_secrets=include_secrets,
        )
    )
    raw["metadata"] = strip_secret_metadata(raw.get("metadata"))
    return raw


def resolve_snapshot_image(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    snapshot_id: str,
) -> Optional[str]:
    sid = (snapshot_id or "").strip()
    if not sid:
        return None
    get_snapshot = getattr(sandbox_manager.db, "get_sandbox_snapshot", None)
    if not callable(get_snapshot):
        return None
    row = get_snapshot(sid, owner_client_id=principal.client_id)
    if not row and principal.client_id == "bootstrap-local-client":
        row = get_snapshot(sid)
    image_ref = str((row or {}).get("image_ref") or "").strip()
    return image_ref or None


def resolve_template_id_for_principal(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    template_id: str | None,
) -> str:
    requested = (template_id or "").strip() or "python:3.11"
    owned = sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, requested)
    if owned:
        return str(owned["template_id"])
    materialized = sandbox_manager.db.get_best_sandbox_template_by_alias(
        requested,
        owner_client_id=principal.client_id,
        exclude_template_id=requested,
    )
    if materialized:
        return str(materialized["template_id"])
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


def owned_sandbox_or_404(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    sandbox_id: str,
) -> dict:
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)
    return ensure_sandbox_access(principal, sandbox, sandbox_id)


def ensure_live_sandbox(sandbox_manager: SandboxManager, sandbox_id: str) -> None:
    reason = sandbox_manager.get_sandbox_runtime_failure(sandbox_id)
    if reason:
        raise SandboxRuntimeLostException(sandbox_id, reason)


def warm_pool_shape_for_sandbox(
    sandbox_manager: SandboxManager,
    sandbox: dict,
) -> tuple[str, str, str, str, int]:
    """Derive the canonical warm-pool segment key from a sandbox row."""
    cfg = sandbox_manager._config
    metadata = sandbox.get("metadata") if isinstance(sandbox.get("metadata"), dict) else {}
    stored_key = str(
        metadata.get("sandbox_allocation_pool_key") or sandbox.get("warm_pool_key") or ""
    ).strip()
    key_parts = stored_key.split("|") if stored_key else []

    template_id = str(sandbox.get("template_id") or "").strip()
    cpu_limit = str(sandbox.get("cpu_limit") or getattr(cfg, "DEFAULT_CPU_LIMIT", "1") or "1")
    memory_limit = str(
        sandbox.get("memory_limit") or getattr(cfg, "DEFAULT_MEMORY_LIMIT", "512m") or "512m"
    )

    if key_parts:
        # A sandbox handed out from a warm pool has already lost its DB
        # warm_pool_key, so the allocation metadata is the authoritative
        # segment identity. Prefer it fully; otherwise resize can update a
        # different key and the reconciler will refill the original segment.
        template_id = key_parts[0].strip() or template_id
        if len(key_parts) >= 2:
            cpu_limit = key_parts[1].strip() or cpu_limit
        if len(key_parts) >= 3:
            memory_limit = key_parts[2].strip() or memory_limit

    try:
        timeout = int(sandbox.get("timeout") or getattr(cfg, "DEFAULT_TIMEOUT", 3600) or 3600)
    except (TypeError, ValueError):
        timeout = int(getattr(cfg, "DEFAULT_TIMEOUT", 3600) or 3600)

    if not template_id:
        raise HTTPException(status_code=409, detail="Sandbox row does not contain a template_id")

    warm_pool_key = sandbox_manager.warm_pool_key(template_id, cpu_limit, memory_limit, int(timeout))
    return warm_pool_key, template_id, cpu_limit, memory_limit, int(timeout)


def current_template_image_ref_for_warm_pool(
    sandbox_manager: SandboxManager,
    template_id: str,
) -> Optional[str]:
    tid = (template_id or "").strip()
    if not tid:
        return None
    row = sandbox_manager.db.get_sandbox_template(tid)
    if not row:
        return None
    ensure_template_image = getattr(sandbox_manager, "_ensure_template_runtime_image", None)
    if callable(ensure_template_image):
        row = ensure_template_image(tid, row)
    image_ref = str(row.get("warm_snapshot_image") or row.get("registry_image_ref") or "").strip()
    return image_ref or None


async def create_sandbox_row(
    request: CreateSandboxRequest,
    principal: ApiKeyPrincipal,
    sandbox_manager: SandboxManager,
    *,
    allow_public_traffic: Optional[bool] = None,
) -> dict:
    metadata = dict(request.metadata or {})
    if allow_public_traffic is not None:
        metadata["allow_public_traffic"] = bool(allow_public_traffic)
    if isinstance(request.env_vars, dict) and request.env_vars:
        shim_env = metadata.get("env")
        merged_env = dict(shim_env) if isinstance(shim_env, dict) else {}
        merged_env.update({str(k): str(v) for k, v in request.env_vars.items() if v is not None})
        metadata["env"] = merged_env

    def _resolve_create_inputs() -> tuple[Optional[str], str]:
        return (
            request.from_snapshot_image or resolve_snapshot_image(
                sandbox_manager,
                principal,
                request.template_id or "",
            ),
            resolve_template_id_for_principal(
                sandbox_manager,
                principal,
                request.template_id,
            ),
        )

    from_snapshot_image, resolved_template_id = await run_io(_resolve_create_inputs)
    queue_timeout = float(
        getattr(sandbox_manager._config, "SANDBOX_CREATE_QUEUE_TIMEOUT_SEC", 2.0) or 2.0
    )
    request_timeout = float(
        getattr(sandbox_manager._config, "SANDBOX_CREATE_REQUEST_TIMEOUT_SEC", 120.0) or 0.0
    )
    create_started = threading.Event()
    create_started_at = time.monotonic()

    def _create() -> Optional[str]:
        create_started.set()
        return sandbox_manager.create_sandbox(
            resolved_template_id,
            metadata,
            request.cpu_limit,
            request.memory_limit,
            request.timeout,
            from_snapshot_image,
            principal.client_id,
            principal.key_id,
            request.warmpool_size,
        )

    logger.info(
        "Sandbox create requested template_id=%r resolved_template_id=%r owner_client_id=%r warm_pool_size=%r timeout=%s",
        request.template_id,
        resolved_template_id,
        principal.client_id,
        request.warmpool_size,
        request.timeout,
    )
    create_task = asyncio.create_task(run_io(_create))
    try:
        if queue_timeout > 0:
            sandbox_id = await asyncio.wait_for(asyncio.shield(create_task), timeout=queue_timeout)
        else:
            sandbox_id = await asyncio.shield(create_task)
    except asyncio.TimeoutError:
        if not create_started.is_set():
            create_task.cancel()
            logger.error(
                "Sandbox create rejected because worker queue did not start within %.3fs template_id=%r owner_client_id=%r",
                queue_timeout,
                resolved_template_id,
                principal.client_id,
            )
            raise HTTPException(
                status_code=503,
                detail="Sandbox create capacity is temporarily unavailable; retry shortly.",
            ) from None

        if request_timeout <= 0:
            sandbox_id = await create_task
        else:
            remaining = max(0.1, request_timeout - queue_timeout)
            try:
                sandbox_id = await asyncio.wait_for(asyncio.shield(create_task), timeout=remaining)
            except asyncio.TimeoutError:
                elapsed = time.monotonic() - create_started_at

                def _log_late_completion(task: asyncio.Task) -> None:
                    try:
                        late_sid = task.result()
                    except Exception as ex:  # noqa: BLE001
                        logger.error("Late sandbox create failed after HTTP timeout: %s", ex)
                        return
                    logger.warning(
                        "Late sandbox create completed after HTTP timeout sandbox_id=%s template_id=%r elapsed=%.3fs",
                        late_sid or "-",
                        resolved_template_id,
                        time.monotonic() - create_started_at,
                    )

                create_task.add_done_callback(_log_late_completion)
                logger.error(
                    "Sandbox create timed out after %.3fs template_id=%r owner_client_id=%r",
                    elapsed,
                    resolved_template_id,
                    principal.client_id,
                )
                raise HTTPException(
                    status_code=503,
                    detail="Sandbox create did not complete before the API timeout; retry shortly.",
                ) from None

    if not sandbox_id:
        hint = await run_io(sandbox_manager.describe_docker_workload_blocker)
        detail = (
            "Failed to create sandbox: Docker could not start a workload. "
            "Check Docker socket, image pull, and template_id."
        )
        if hint:
            detail = f"{detail} {hint}"
        raise HTTPException(status_code=503, detail=detail)

    sandbox = await run_io(sandbox_manager.get_sandbox_for_create_response, sandbox_id)
    if not sandbox:
        raise HTTPException(status_code=500, detail="Sandbox was created but no DB row was returned")
    return sandbox


async def create_filesystem_snapshot_row(
    sandbox_id: str,
    label: Optional[str],
    principal: ApiKeyPrincipal,
    sandbox_manager: SandboxManager,
) -> dict:
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
    out = await run_io(sandbox_manager.create_filesystem_snapshot, sandbox_id, label)
    if not out:
        if not sandbox_manager.get_sandbox(sandbox_id):
            raise SandboxNotFoundException(sandbox_id)
        raise HTTPException(
            status_code=501,
            detail="Filesystem snapshot unavailable: requires Docker Engine and successful `docker commit`.",
        )
    return out


@router.post("", response_model=SandboxResponse)
async def create_sandbox(
    request: CreateSandboxRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Create a sandbox using the generic local SDK response shape."""
    sandbox = await create_sandbox_row(request, principal, sandbox_manager)
    return JSONResponse(
        status_code=201,
        content=sandbox_response_payload(sandbox, sandbox_manager, include_secrets=True),
    )


@router.post("/{sandbox_id}/snapshot", response_model=SnapshotRecordResponse)
async def create_sandbox_snapshot(
    sandbox_id: str,
    request: CreateSnapshotRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Persist the sandbox writable layer with Docker commit."""
    out = await create_filesystem_snapshot_row(sandbox_id, request.label, principal, sandbox_manager)
    return SnapshotRecordResponse(**out)


@router.get("/{sandbox_id}/snapshots", response_model=List[SnapshotRecordResponse])
async def list_sandbox_snapshots(
    sandbox_id: str,
    limit: int = 50,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List filesystem snapshots recorded for this sandbox."""
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
    rows = await run_io(sandbox_manager.list_filesystem_snapshots, sandbox_id, limit)
    return [SnapshotRecordResponse(**r) for r in rows]


@router.get("/{sandbox_id}/status", response_model=SandboxLifecycleResponse)
async def get_sandbox_status(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Return DB state and whether the workload is still running."""
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
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
    """Refresh the stored sandbox lease."""
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
    ok = await run_io(
        sandbox_manager.refresh_sandbox_timeout,
        sandbox_id,
        request.timeout_seconds,
    )
    return SandboxTimeoutRefreshResponse(
        sandbox_id=sandbox_id.strip(),
        timeout_seconds=int(request.timeout_seconds or 0),
        refreshed=bool(ok),
    )


@router.post("/{sandbox_id}/warm-pool/size", response_model=WarmPoolResizeResponse)
async def resize_sandbox_warm_pool(
    sandbox_id: str,
    request: ResizeWarmPoolRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Update desired warm-pool size for the template/cpu/memory segment matching this sandbox."""
    sandbox = owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    desired_size = max(0, int(request.warmpool_size))

    def _resize() -> dict:
        warm_pool_key, template_id, cpu_limit, memory_limit, timeout = warm_pool_shape_for_sandbox(
            sandbox_manager,
            sandbox,
        )
        before = sandbox_manager.db.get_warm_pool_segment(warm_pool_key) or {}
        previous_desired = int(before.get("desired_size") or 0)
        preferred_gateway = str(
            before.get("preferred_gateway_instance_id") or sandbox.get("gateway_instance_id") or ""
        ).strip() or None
        image_ref = current_template_image_ref_for_warm_pool(sandbox_manager, template_id)
        pool = getattr(sandbox_manager, "warm_pool", None)
        apply_size = getattr(sandbox_manager, "_apply_requested_warm_pool_size", None)
        if pool is not None and callable(apply_size):
            apply_size(
                pool,
                template_id=template_id,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                from_snapshot_image=image_ref,
                desired_size=desired_size,
                preferred_gateway_instance_id=preferred_gateway,
            )
        else:
            sandbox_manager.note_warm_pool_segment(
                template_id=template_id,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                desired_size=desired_size,
                preferred_gateway_instance_id=preferred_gateway,
            )
        sandbox_manager.trim_warm_pool_to_size(warm_pool_key, desired_size)
        return {
            "sandbox_id": sandbox_id.strip(),
            "warm_pool_key": warm_pool_key,
            "template_id": template_id,
            "cpu_limit": str(cpu_limit),
            "memory_limit": str(memory_limit),
            "timeout": int(timeout),
            "previous_desired_size": previous_desired,
            "desired_size": desired_size,
            "ready_count": int(sandbox_manager.warm_pool_ready_count(warm_pool_key)),
            "updated": True,
        }

    return WarmPoolResizeResponse(**await run_io(_resize))


@router.get("/{sandbox_id}", response_model=SandboxResponse)
async def get_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Get sandbox info."""
    sandbox = owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
    return sandbox_response_payload(sandbox, sandbox_manager, include_secrets=False)


@router.get("", response_model=list)
async def list_sandboxes(
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List sandboxes for the authenticated client."""
    rows = sandbox_manager.db.list_sandboxes(
        limit=limit,
        offset=offset,
        owner_client_id=principal.client_id,
    )
    return [
        sandbox_response_payload(row, sandbox_manager, include_secrets=False)
        for row in rows
    ]


@router.post("/{sandbox_id}/kill")
async def kill_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Kill a sandbox."""
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
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
    """Pause a sandbox."""
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
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
    """Resume a sandbox."""
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
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
    owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    ensure_live_sandbox(sandbox_manager, sandbox_id)
    metrics = await run_io(sandbox_manager.get_metrics, sandbox_id)
    if not metrics:
        raise SandboxNotFoundException(sandbox_id)
    return metrics
