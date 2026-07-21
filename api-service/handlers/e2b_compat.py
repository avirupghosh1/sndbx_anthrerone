"""E2B SDK compatibility wrapper over the generic local API handlers."""

import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from async_runner import run_io
from handlers import sandboxes as sandbox_handlers
from handlers import templates as template_handlers
from handlers.build_context import (
    E2B_UPLOAD_NAMESPACE,
    e2b_upload_key,
    merged_context_from_uploads,
    put_template_build_upload,
    template_build_upload_exists,
)
from middleware import ApiKeyPrincipal, SandboxNotFoundException, validate_api_key
from models import (
    CreateSandboxRequest,
    RefreshSandboxTimeoutRequest,
)
from orchestrator import SandboxManager
from handlers.e2b_compat_support import (
    _error_response,
    _is_e2b_request,
    _json_body,
    _api_base_url,
    _looks_like_e2b_create,
    _e2b_allow_public_traffic,
    _coerce_iso,
    _parse_cpu_count,
    _parse_bytes_or_mb,
    _e2b_sandbox_payload,
    _e2b_snapshot_payload,
    _metadata_filter_matches,
    _e2b_build_name_parts,
    _upload_token,
    _verify_upload_token,
    _dockerfile_from_e2b_template_payload,
    _e2b_template_response,
    _e2b_build_status_payload,
    _uuid_for_e2b,
    _split_e2b_template_ref,
    _tagged_alias,
    _latest_build_for_template,
    _run_e2b_template_build_background,
)

router = APIRouter(tags=["e2b-compat"])
logger = logging.getLogger(__name__)

@router.post("/sandboxes")
async def create_sandbox(
    raw_request: Request,
    request: CreateSandboxRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(raw_request)
    if not (_is_e2b_request(raw_request) or _looks_like_e2b_create(body)):
        return await sandbox_handlers.create_sandbox(request, principal, sandbox_manager)
    sandbox = await sandbox_handlers.create_sandbox_row(
        request,
        principal,
        sandbox_manager,
        allow_public_traffic=_e2b_allow_public_traffic(body),
    )
    return JSONResponse(
        status_code=201,
        content=_e2b_sandbox_payload(
            sandbox,
            sandbox_manager,
            include_secrets=True,
            default_client_id=principal.client_id,
        ),
    )

@router.get("/sandboxes/{sandbox_id}")
async def get_sandbox(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    if not _is_e2b_request(request):
        return await sandbox_handlers.get_sandbox(sandbox_id, principal, sandbox_manager)
    sandbox = sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    sandbox_handlers.ensure_live_sandbox(sandbox_manager, sandbox_id)
    return JSONResponse(
        content=_e2b_sandbox_payload(
            sandbox,
            sandbox_manager,
            include_secrets=False,
            default_client_id=principal.client_id,
        )
    )

@router.delete("/sandboxes/{sandbox_id}")
async def delete_sandbox(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    success = await run_io(sandbox_manager.kill_sandbox, sandbox_id)
    if not success:
        raise SandboxNotFoundException(sandbox_id)
    return Response(status_code=204)

@router.post("/sandboxes/{sandbox_id}/snapshots")
async def create_sandbox_snapshot(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    label = str(body.get("name") or body.get("label") or "").strip() or None
    try:
        out = await sandbox_handlers.create_filesystem_snapshot_row(
            sandbox_id,
            label,
            principal,
            sandbox_manager,
        )
    except HTTPException as ex:
        if ex.status_code == 501:
            return _error_response(
                501,
                "Filesystem snapshot unavailable: requires Docker Engine and successful docker commit.",
            )
        raise
    return JSONResponse(status_code=201, content=_e2b_snapshot_payload(out))

@router.post("/sandboxes/{sandbox_id}/timeout")
async def refresh_sandbox_timeout(
    sandbox_id: str,
    raw_request: Request,
    request: RefreshSandboxTimeoutRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(raw_request)
    response = await sandbox_handlers.refresh_sandbox_timeout(
        sandbox_id,
        request,
        principal,
        sandbox_manager,
    )
    if _is_e2b_request(raw_request) or ("timeout" in body and "timeout_seconds" not in body):
        return Response(status_code=204)
    return response

@router.post("/sandboxes/{sandbox_id}/pause")
async def pause_sandbox(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    response = await sandbox_handlers.pause_sandbox(sandbox_id, principal, sandbox_manager)
    if _is_e2b_request(request) or "memory" in body:
        return Response(status_code=204)
    return response

@router.post("/sandboxes/{sandbox_id}/resume")
async def resume_sandbox(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    is_e2b = _is_e2b_request(request) or "timeout" in body or "autoPause" in body
    if not is_e2b:
        return await sandbox_handlers.resume_sandbox(sandbox_id, principal, sandbox_manager)
    sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    sandbox_handlers.ensure_live_sandbox(sandbox_manager, sandbox_id)
    success = await run_io(sandbox_manager.resume_sandbox, sandbox_id)
    if not success:
        raise SandboxNotFoundException(sandbox_id)
    timeout = body.get("timeout")
    if timeout is not None:
        await run_io(sandbox_manager.refresh_sandbox_timeout, sandbox_id, int(timeout))
    sandbox = sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    return JSONResponse(
        status_code=201,
        content=_e2b_sandbox_payload(
            sandbox,
            sandbox_manager,
            include_secrets=True,
            default_client_id=principal.client_id,
        ),
    )

@router.post("/sandboxes/{sandbox_id}/connect")
async def connect_sandbox(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    sandbox = sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    timeout = int(body.get("timeout") or sandbox.get("timeout") or 3600)
    if str(sandbox.get("state") or "").lower() == "paused":
        await run_io(sandbox_manager.resume_sandbox, sandbox_id)
    await run_io(sandbox_manager.refresh_sandbox_timeout, sandbox_id, timeout)
    sandbox = sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    sandbox_handlers.ensure_live_sandbox(sandbox_manager, sandbox_id)
    return JSONResponse(
        content=_e2b_sandbox_payload(
            sandbox,
            sandbox_manager,
            include_secrets=True,
            default_client_id=principal.client_id,
        )
    )

@router.put("/sandboxes/{sandbox_id}/network")
async def update_sandbox_network(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    return _error_response(501, "Sandbox network updates are not implemented yet.")

@router.get("/sandboxes/{sandbox_id}/metrics")
async def get_sandbox_metrics(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    if not (_is_e2b_request(request) or "start" in request.query_params or "end" in request.query_params):
        return await sandbox_handlers.get_sandbox_metrics(sandbox_id, principal, sandbox_manager)
    sandbox = sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    sandbox_handlers.ensure_live_sandbox(sandbox_manager, sandbox_id)
    metrics = await run_io(sandbox_manager.get_metrics, sandbox_id)
    if not metrics:
        raise SandboxNotFoundException(sandbox_id)
    now = datetime.now(timezone.utc)
    return [
        {
            "cpuCount": _parse_cpu_count(sandbox.get("cpu_limit")),
            "cpuUsedPct": float(metrics.get("cpu_percent") or 0.0),
            "diskTotal": _parse_bytes_or_mb(sandbox.get("disk_limit"), 0) * 1024 * 1024,
            "diskUsed": 0,
            "memCache": 0,
            "memTotal": int(metrics.get("memory_limit") or 0),
            "memUsed": int(metrics.get("memory_usage") or 0),
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "timestampUnix": int(now.timestamp()),
        }
    ]

@router.get("/v2/sandboxes")
async def list_sandboxes_v2(
    metadata: Optional[str] = None,
    state: Optional[str] = None,
    next_token: Optional[str] = Query(default=None, alias="nextToken"),
    limit: Optional[int] = 100,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ = next_token
    rows = sandbox_manager.db.list_sandboxes(
        limit=max(1, int(limit or 100)),
        offset=0,
        owner_client_id=principal.client_id,
    )
    allowed_states = {s.strip() for s in (state or "").split(",") if s.strip()}
    out = []
    for row in rows:
        if allowed_states and str(row.get("state") or "") not in allowed_states:
            continue
        if not _metadata_filter_matches(row.get("metadata"), metadata):
            continue
        out.append(
            _e2b_sandbox_payload(
                row,
                sandbox_manager,
                include_secrets=False,
                default_client_id=principal.client_id,
            )
        )
    return JSONResponse(content=out)

@router.get("/snapshots")
async def list_snapshots(
    sandbox_id: Optional[str] = Query(default=None, alias="sandboxID"),
    next_token: Optional[str] = Query(default=None, alias="nextToken"),
    limit: Optional[int] = 100,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ = next_token
    max_rows = max(1, int(limit or 100))
    if sandbox_id:
        sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
        rows = sandbox_manager.db.list_sandbox_snapshots(
            sandbox_id,
            max_rows,
            owner_client_id=principal.client_id,
        )
    else:
        list_all = getattr(sandbox_manager.db, "list_all_sandbox_snapshots", None)
        rows = list_all(max_rows, owner_client_id=principal.client_id) if callable(list_all) else []
    return JSONResponse(content=[_e2b_snapshot_payload(row) for row in rows])

@router.post("/v3/templates")
async def request_template_build(
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    alias, tags, names = _e2b_build_name_parts(str(body.get("name") or ""), body.get("tags") or [])
    existing = await run_io(sandbox_manager.db.get_sandbox_template_by_alias, principal.client_id, alias)
    tid = str(existing["template_id"]) if existing else template_handlers._storage_template_id(principal, alias)
    build_id = await template_handlers._create_build_record(
        sandbox_manager,
        template_id=tid,
        template_alias=alias,
        principal=principal,
        requested_mode="e2b_sdk",
        effective_mode="waiting",
        status="waiting",
    )
    return JSONResponse(
        status_code=202,
        content=_e2b_template_response(tid, build_id, alias, names, tags),
    )

@router.post("/v2/templates/{template_id}/builds/{build_id}")
async def start_template_build(
    template_id: str,
    build_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    build = await run_io(sandbox_manager.db.get_template_build, build_id)
    if not build or str(build.get("template_id") or "") != template_id:
        raise HTTPException(status_code=404, detail=f"Unknown build_id: {build_id}")
    if str(build.get("owner_client_id") or "") != principal.client_id:
        raise HTTPException(status_code=404, detail=f"Unknown build_id: {build_id}")

    body = await _json_body(request)
    try:
        dockerfile, env, start_cmd, ready_cmd, context_keys = _dockerfile_from_e2b_template_payload(template_id, body)
        try:
            context_tar_gzip_base64 = await run_io(
                merged_context_from_uploads,
                sandbox_manager.db,
                owner_client_id=principal.client_id,
                namespace=E2B_UPLOAD_NAMESPACE,
                object_keys=context_keys,
            )
        except KeyError as ex:
            raise HTTPException(status_code=400, detail=f"Missing E2B template upload(s): {ex}") from ex
        alias = str(build.get("template_alias") or template_id)
        await template_handlers._finish_build_record(
            sandbox_manager,
            build_id,
            status="running",
            effective_mode="e2b_sdk",
            build_log="Queued E2B SDK template build",
        )
        asyncio.create_task(
            _run_e2b_template_build_background(
                sandbox_manager=sandbox_manager,
                principal=principal,
                template_id=template_id,
                build_id=build_id,
                alias=alias,
                dockerfile=dockerfile,
                env=env,
                start_cmd=start_cmd,
                ready_cmd=ready_cmd,
                context_tar_gzip_base64=context_tar_gzip_base64,
            )
        )
    except HTTPException as ex:
        await template_handlers._finish_build_record(
            sandbox_manager,
            build_id,
            status="failed",
            effective_mode="e2b_sdk",
            build_log="",
            error_text=str(ex.detail),
        )
        raise
    except Exception as ex:  # noqa: BLE001
        await template_handlers._finish_build_record(
            sandbox_manager,
            build_id,
            status="failed",
            effective_mode="e2b_sdk",
            build_log="",
            error_text=str(ex),
        )
        raise HTTPException(status_code=500, detail=str(ex)) from ex
    return Response(status_code=202)

@router.get("/templates/{template_id}/builds/{build_id}/status")
async def get_template_build_status(
    template_id: str,
    build_id: str,
    logs_offset: int = Query(default=0, alias="logsOffset"),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    build = await run_io(sandbox_manager.db.get_template_build, build_id)
    if not build or str(build.get("template_id") or "") != template_id:
        raise HTTPException(status_code=404, detail=f"Unknown build_id: {build_id}")
    if str(build.get("owner_client_id") or "") != principal.client_id:
        raise HTTPException(status_code=404, detail=f"Unknown build_id: {build_id}")
    return JSONResponse(content=_e2b_build_status_payload(build, logs_offset=logs_offset))

@router.get("/templates/aliases/{alias}")
async def get_template_alias(
    alias: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = await run_io(template_handlers._resolve_template_row_for_principal, sandbox_manager, principal, alias)
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown template alias: {alias}")
    return JSONResponse(content={"public": False, "templateID": str(row.get("template_id") or "")})

@router.post("/templates/tags")
async def assign_template_tags(
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    target = str(body.get("target") or "").strip()
    tags = [str(t).strip() for t in (body.get("tags") or []) if str(t).strip()]
    if not target or not tags:
        raise HTTPException(status_code=400, detail="target and tags are required")
    source = await run_io(template_handlers._resolve_template_row_for_principal, sandbox_manager, principal, target)
    if not source:
        raise HTTPException(status_code=404, detail=f"Unknown template: {target}")
    target_base, _ = _split_e2b_template_ref(target)
    base_alias = target_base or str(source.get("template_alias") or source.get("template_id") or "")
    build = await _latest_build_for_template(sandbox_manager, principal, str(source.get("template_id") or ""))
    build_id = str((build or {}).get("build_id") or source.get("template_id") or base_alias)
    image_ref = str(source.get("warm_snapshot_image") or source.get("registry_image_ref") or "")
    for tag in tags:
        alias = _tagged_alias(base_alias, tag)
        tagged_template_id = template_handlers._storage_template_id(principal, alias)
        await run_io(
            sandbox_manager.db.upsert_sandbox_template,
            tagged_template_id,
            str(source.get("base_image") or image_ref or "python:3.11"),
            source.get("env") if isinstance(source.get("env"), dict) else {},
            str(source.get("start_cmd") or ""),
            int(source.get("settle_seconds") or 20),
            str(source.get("ready_cmd") or ""),
            owner_client_id=principal.client_id,
            owner_api_key_id=principal.key_id,
            template_alias=alias,
        )
        if image_ref:
            await run_io(
                sandbox_manager.db.set_template_warm_snapshot,
                tagged_template_id,
                image_ref,
                registry_image_ref=str(source.get("registry_image_ref") or "") or None,
                materialized_gateway_instance_id=str(source.get("materialized_gateway_instance_id") or "") or None,
            )
    return JSONResponse(status_code=201, content={"buildID": _uuid_for_e2b(build_id), "tags": tags})

@router.delete("/templates/tags")
async def remove_template_tags(
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    name = str(body.get("name") or "").strip()
    tags = [str(t).strip() for t in (body.get("tags") or []) if str(t).strip()]
    if not name or not tags:
        raise HTTPException(status_code=400, detail="name and tags are required")
    base_alias, _ = _split_e2b_template_ref(name)
    for tag in tags:
        alias = _tagged_alias(base_alias, tag)
        await template_handlers.delete_template_for_principal(sandbox_manager, principal, alias)
    return Response(status_code=204)

@router.get("/templates/{template_id}/tags")
async def get_template_tags(
    template_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    source = await run_io(template_handlers._resolve_template_row_for_principal, sandbox_manager, principal, template_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Unknown template: {template_id}")
    base_alias, _ = _split_e2b_template_ref(str(source.get("template_alias") or template_id))
    rows = await run_io(sandbox_manager.db.list_sandbox_templates, principal.client_id)
    out: list[dict[str, Any]] = []
    for row in rows:
        alias = str(row.get("template_alias") or "")
        prefix = f"{base_alias}:"
        if not alias.startswith(prefix):
            continue
        tag = alias[len(prefix) :]
        if not tag:
            continue
        build = await _latest_build_for_template(sandbox_manager, principal, str(row.get("template_id") or ""))
        build_id = str((build or {}).get("build_id") or row.get("template_id") or alias)
        out.append(
            {
                "buildID": _uuid_for_e2b(build_id),
                "createdAt": _coerce_iso(row.get("created_at")),
                "tag": tag,
            }
        )
    return JSONResponse(content=out)

@router.get("/templates/{template_id}/files/{hash_}")
async def get_template_file_upload_link(
    template_id: str,
    hash_: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    object_key = e2b_upload_key(template_id, hash_)
    present = await run_io(
        template_build_upload_exists,
        sandbox_manager.db,
        principal.client_id,
        E2B_UPLOAD_NAMESPACE,
        object_key,
    )
    token = _upload_token(principal.client_id, E2B_UPLOAD_NAMESPACE, object_key)
    base = _api_base_url(request)
    url = (
        f"{base}/templates/{quote(template_id, safe='')}/files/{quote(hash_, safe='')}/upload"
        f"?owner={quote(principal.client_id, safe='')}&token={quote(token, safe='')}"
    )
    return JSONResponse(status_code=201, content={"present": bool(present), "url": url})

@router.put("/templates/{template_id}/files/{hash_}/upload")
async def upload_template_file_archive(
    template_id: str,
    hash_: str,
    request: Request,
    owner: str = Query(default=""),
    token: str = Query(default=""),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    object_key = e2b_upload_key(template_id, hash_)
    owner_id = str(owner or "").strip()
    if not owner_id or not _verify_upload_token(owner_id, E2B_UPLOAD_NAMESPACE, object_key, token):
        return _error_response(403, "Invalid E2B template upload token.")
    payload = await request.body()
    if not payload:
        return _error_response(400, "Empty E2B template upload archive.")
    await run_io(
        put_template_build_upload,
        sandbox_manager.db,
        owner_id,
        E2B_UPLOAD_NAMESPACE,
        object_key,
        payload,
        content_type=request.headers.get("content-type") or "application/x-tar",
        metadata={"template_id": template_id, "files_hash": hash_},
    )
    return Response(status_code=200)

@router.delete("/templates/{template_id}")
async def delete_template_or_snapshot(
    template_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    if await template_handlers.delete_template_for_principal(sandbox_manager, principal, template_id):
        return Response(status_code=204)
    row = await run_io(
        sandbox_manager.db.get_sandbox_snapshot,
        template_id,
        principal.client_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown snapshot: {template_id}")
    ok = await run_io(
        sandbox_manager.db.delete_sandbox_snapshot,
        template_id,
        principal.client_id,
    )
    if not ok:
        raise HTTPException(status_code=404, detail=f"Unknown snapshot: {template_id}")
    return Response(status_code=204)
