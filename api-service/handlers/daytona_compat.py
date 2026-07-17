"""Daytona SDK compatibility wrapper over the generic local API handlers."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import secrets
import shlex
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import quote as url_quote, urlsplit, urlunsplit

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse

from async_runner import run_io
from config import get_config
from envd_guest.proto import process_pb2
from handlers import sandboxes as sandbox_handlers
from handlers import templates as template_handlers
from handlers.build_context import (
    DAYTONA_CONTEXT_BUCKET,
    DAYTONA_UPLOAD_NAMESPACE,
    daytona_context_key,
    put_template_build_upload,
    template_build_upload_exists,
)
from handlers import daytona_ssh_gateway
from middleware import ApiKeyPrincipal, SandboxNotFoundException, validate_api_key
from models import CreateSandboxRequest, RegisterTemplateFromDockerfileRequest
from orchestrator import SandboxManager
from orchestrator.guest_ports import ports_from_metadata
from orchestrator.sandbox_connections import data_plane_base_url, traffic_access_token_for_row
from handlers.daytona_compat_support import (
    _CODE_TOOLBOX_LANGUAGE_LABEL,
    _DAYTONA_ENTRYPOINT_SESSION_ID,
    _DEFAULT_SNAPSHOT,
    _json_body,
    _now_iso,
    _api_base_url,
    _storage_token,
    _storage_token_ok,
    _not_implemented,
    _metadata,
    _daytona_meta,
    _str_dict,
    _websocket_principal,
    _memory_limit_from_daytona,
    _sandbox_dto,
    _snapshot_dto,
    _snapshot_row_or_404,
    _latest_template_build_for_snapshot,
    _resolve_daytona_snapshot_reference,
    _safe_template_alias,
    _daytona_context_tar_gzip_base64,
    _ensure_daytona_build_template,
    _update_daytona_metadata,
    _daytona_process_sessions,
    _save_daytona_process_sessions,
    _daytona_pty_sessions,
    _save_daytona_pty_sessions,
    _save_daytona_pty_session,
    _pty_session_or_404,
    _pty_session_dto,
    _session_command_dto,
    _session_dto,
    _session_or_404,
    _session_command_or_404,
    _owned_row,
    _ensure_live,
    _run_command,
    _envd_connection_or_503,
    _envd_headers,
    _connect_envelope,
    _parse_connect_messages,
    _process_selector,
    _session_has_process,
    _envd_send_pty_input,
    _envd_send_stdin_input,
    _envd_resize_pty,
    _envd_kill_process,
    _start_daytona_shell_session,
    _collect_shell_command_result,
    _write_bytes,
    _stat_file_info,
    _list_files_recursive,
    _multipart_part,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["daytona-compat"])
toolbox_router = APIRouter(prefix="/{sandbox_id}", tags=["daytona-toolbox-compat"])
deprecated_toolbox_router = APIRouter(prefix="/toolbox/{sandbox_id}/toolbox", tags=["daytona-toolbox-compat"])
_TOOLBOX_ROUTERS = (toolbox_router, deprecated_toolbox_router)

_ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]
































































































































def _add_toolbox_route(
    path: str,
    endpoint: Callable[..., Awaitable[Any]],
    methods: list[str],
    *,
    status_code: Optional[int] = None,
) -> None:
    for r in _TOOLBOX_ROUTERS:
        r.add_api_route(path, endpoint, methods=methods, status_code=status_code)


def _add_toolbox_ws_route(path: str, endpoint: Callable[..., Awaitable[Any]]) -> None:
    for r in _TOOLBOX_ROUTERS:
        r.add_api_websocket_route(path, endpoint)


def _unsupported_toolbox_endpoint(feature: str, *, with_path: bool = False) -> Callable[..., Awaitable[Any]]:
    if with_path:
        async def endpoint(
            sandbox_id: str,
            path: str,
            principal: ApiKeyPrincipal = Depends(validate_api_key),
            sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
        ):
            _ = path
            _ensure_live(sandbox_manager, principal, sandbox_id)
            return _not_implemented(feature)
    else:
        async def endpoint(
            sandbox_id: str,
            principal: ApiKeyPrincipal = Depends(validate_api_key),
            sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
        ):
            _ensure_live(sandbox_manager, principal, sandbox_id)
            return _not_implemented(feature)

    suffix = "path" if with_path else "root"
    endpoint.__name__ = f"daytona_unsupported_{re.sub(r'[^a-zA-Z0-9_]+', '_', feature)}_{suffix}"
    return endpoint


def _add_unsupported_api_family(prefix: str, feature: str) -> None:
    async def root_endpoint(
        principal: ApiKeyPrincipal = Depends(validate_api_key),
    ):
        _ = principal
        return _not_implemented(feature)

    async def nested_endpoint(
        path: str,
        principal: ApiKeyPrincipal = Depends(validate_api_key),
    ):
        _ = (path, principal)
        return _not_implemented(feature)

    root_endpoint.__name__ = f"daytona_unsupported_{prefix.strip('/').replace('-', '_')}_root"
    nested_endpoint.__name__ = f"daytona_unsupported_{prefix.strip('/').replace('-', '_')}_path"
    router.add_api_route(prefix, root_endpoint, methods=_ALL_METHODS)
    router.add_api_route(f"{prefix}/{{path:path}}", nested_endpoint, methods=_ALL_METHODS)


@router.get("/health/ready")
async def daytona_ready() -> dict[str, str]:
    return {"status": "ready"}


@router.get("/config")
async def daytona_config() -> dict[str, Any]:
    return {
        "defaultTarget": "local",
        "targets": [{"id": "local", "name": "local"}],
        "defaultSnapshot": _DEFAULT_SNAPSHOT,
    }


@router.post("/sandbox")
async def create_sandbox(
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    body = await _json_body(request)
    built_template = await _ensure_daytona_build_template(body, principal, sandbox_manager)
    requested_snapshot = str(body.get("snapshot") or "").strip()
    template_id = built_template or _resolve_daytona_snapshot_reference(
        sandbox_manager,
        principal,
        requested_snapshot or _DEFAULT_SNAPSHOT,
    )
    labels = _str_dict(body.get("labels"))
    labels.setdefault(_CODE_TOOLBOX_LANGUAGE_LABEL, "python")
    env = _str_dict(body.get("env"))
    metadata = {
        "name": str(body.get("name") or ""),
        "labels": labels,
        "env": env,
        "daytona": {
            "name": body.get("name"),
            "snapshot": requested_snapshot or template_id,
            "user": body.get("user") or "root",
            "target": body.get("target") or "local",
            "labels": labels,
            "autoStopInterval": body.get("autoStopInterval"),
            "autoArchiveInterval": body.get("autoArchiveInterval"),
            "autoDeleteInterval": body.get("autoDeleteInterval"),
            "networkBlockAll": bool(body.get("networkBlockAll", False)),
            "networkAllowList": body.get("networkAllowList"),
            "domainAllowList": body.get("domainAllowList"),
            "volumes": body.get("volumes") or [],
            "buildInfo": body.get("buildInfo"),
            "buildLog": "Daytona declarative image build completed\n" if built_template else "",
        },
    }
    timeout = 3600
    if body.get("autoStopInterval") is not None:
        try:
            minutes = int(body.get("autoStopInterval") or 0)
            timeout = minutes * 60 if minutes > 0 else 3600
        except Exception:
            timeout = 3600
    req = CreateSandboxRequest(
        template_id=template_id,
        metadata=metadata,
        env_vars=env,
        cpu_limit=str(body.get("cpu") or "1"),
        memory_limit=_memory_limit_from_daytona(body.get("memory")),
        timeout=timeout,
    )
    allow_public = bool(body.get("public")) if body.get("public") is not None else None
    row = await sandbox_handlers.create_sandbox_row(
        req,
        principal,
        sandbox_manager,
        allow_public_traffic=allow_public,
    )
    dto = _sandbox_dto(row, sandbox_manager, request)
    if built_template:
        dto["state"] = "pending_build"
        dto["desiredState"] = "started"
    return dto


@router.get("/sandbox")
async def list_sandboxes(
    request: Request,
    cursor: Optional[str] = None,
    limit: Optional[int] = 100,
    id: Optional[str] = None,  # noqa: A002
    name: Optional[str] = None,
    labels: Optional[str] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    try:
        offset = int(cursor or 0)
    except Exception:
        offset = 0
    rows = sandbox_manager.db.list_sandboxes(
        limit=max(1, min(200, int(limit or 100))),
        offset=offset,
        owner_client_id=principal.client_id,
    )
    label_filter = json.loads(labels) if labels else {}
    out = []
    for row in rows:
        dto = _sandbox_dto(row, sandbox_manager, request, include_full_fields=False)
        if id and not dto["id"].lower().startswith(id.lower()):
            continue
        if name and not dto["name"].lower().startswith(name.lower()):
            continue
        if isinstance(label_filter, dict) and any(dto["labels"].get(str(k)) != str(v) for k, v in label_filter.items()):
            continue
        out.append(dto)
    next_cursor = str(offset + len(rows)) if len(rows) >= int(limit or 100) else None
    return {"items": out, "nextCursor": next_cursor}


@router.get("/sandbox/paginated")
async def list_sandboxes_paginated_deprecated(
    request: Request,
    page: Optional[int] = 1,
    limit: Optional[int] = 100,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    page_num = max(1, int(page or 1))
    lim = max(1, min(200, int(limit or 100)))
    rows = sandbox_manager.db.list_sandboxes(limit=lim, offset=(page_num - 1) * lim, owner_client_id=principal.client_id)
    return {
        "items": [_sandbox_dto(row, sandbox_manager, request) for row in rows],
        "total": len(rows),
        "page": page_num,
        "totalPages": 1,
    }


@router.get("/sandbox/{sandbox_id_or_name}")
async def get_sandbox(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    if sandbox_id_or_name == "for-runner":
        return _not_implemented("runner sandbox listing")
    row = _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.delete("/sandbox/{sandbox_id_or_name}")
async def delete_sandbox(
    sandbox_id_or_name: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    ok = await run_io(sandbox_manager.kill_sandbox, sandbox_id_or_name)
    if not ok:
        raise SandboxNotFoundException(sandbox_id_or_name)
    return Response(status_code=204)


@router.post("/sandbox/{sandbox_id_or_name}/start")
async def start_sandbox(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    await run_io(sandbox_manager.resume_sandbox, sandbox_id_or_name)
    _update_daytona_metadata(sandbox_manager, sandbox_id_or_name, {"_daytona_state": ""})
    row = _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.post("/sandbox/{sandbox_id_or_name}/stop")
async def stop_sandbox(
    sandbox_id_or_name: str,
    request: Request,
    force: Optional[bool] = False,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ = force
    _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    ok = await run_io(sandbox_manager.pause_sandbox, sandbox_id_or_name)
    if not ok:
        raise SandboxNotFoundException(sandbox_id_or_name)
    _update_daytona_metadata(sandbox_manager, sandbox_id_or_name, {"_daytona_state": "stopped"})
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.post("/sandbox/{sandbox_id_or_name}/pause")
async def pause_sandbox(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    ok = await run_io(sandbox_manager.pause_sandbox, sandbox_id_or_name)
    if not ok:
        raise SandboxNotFoundException(sandbox_id_or_name)
    _update_daytona_metadata(sandbox_manager, sandbox_id_or_name, {"_daytona_state": "paused"})
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.put("/sandbox/{sandbox_id_or_name}/labels")
async def replace_labels(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    body = await _json_body(request)
    labels = _str_dict(body.get("labels") if "labels" in body else body)
    labels.setdefault(_CODE_TOOLBOX_LANGUAGE_LABEL, "python")
    row = sandbox_manager.get_sandbox(sandbox_id_or_name)
    md = _metadata(row)
    md["labels"] = labels
    dm = dict(md.get("daytona") or {})
    dm["labels"] = labels
    md["daytona"] = dm
    sandbox_manager.db.merge_sandbox_metadata(sandbox_id_or_name, md)
    return {"labels": labels}


@router.post("/sandbox/{sandbox_id_or_name}/autostop/{interval}")
async def set_autostop_interval(
    sandbox_id_or_name: str,
    interval: int,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    _update_daytona_metadata(sandbox_manager, sandbox_id_or_name, {"autoStopInterval": interval})
    if int(interval) > 0:
        await run_io(sandbox_manager.refresh_sandbox_timeout, sandbox_id_or_name, int(interval) * 60)
    return Response(status_code=204)


@router.post("/sandbox/{sandbox_id_or_name}/autoarchive/{interval}")
async def set_autoarchive_interval(
    sandbox_id_or_name: str,
    interval: int,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    _update_daytona_metadata(sandbox_manager, sandbox_id_or_name, {"autoArchiveInterval": interval})
    return Response(status_code=204)


@router.post("/sandbox/{sandbox_id_or_name}/autodelete/{interval}")
async def set_autodelete_interval(
    sandbox_id_or_name: str,
    interval: int,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    _update_daytona_metadata(sandbox_manager, sandbox_id_or_name, {"autoDeleteInterval": interval})
    return Response(status_code=204)


@router.post("/sandbox/{sandbox_id_or_name}/network-settings")
async def update_network_settings(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    body = await _json_body(request)
    _update_daytona_metadata(
        sandbox_manager,
        sandbox_id_or_name,
        {
            "networkBlockAll": body.get("networkBlockAll"),
            "networkAllowList": body.get("networkAllowList"),
            "domainAllowList": body.get("domainAllowList"),
        },
    )
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.post("/sandbox/{sandbox_id_or_name}/public/{is_public}")
async def set_public_access(
    sandbox_id_or_name: str,
    is_public: bool,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    md = _metadata(row)
    md["allow_public_traffic"] = bool(is_public)
    sandbox_manager.db.merge_sandbox_metadata(sandbox_id_or_name, md)
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.get("/sandbox/{sandbox_id_or_name}/ports/{port}/preview-url")
async def get_port_preview_url(
    sandbox_id_or_name: str,
    port: int,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    token = traffic_access_token_for_row(row) or ""
    return {
        "sandboxId": sandbox_id_or_name,
        "url": data_plane_base_url(sandbox_manager._config, sandbox_id=sandbox_id_or_name, port=port, scheme="http"),
        "token": token,
    }


@router.get("/sandbox/{sandbox_id_or_name}/ports/{port}/signed-preview-url")
async def get_signed_port_preview_url(
    sandbox_id_or_name: str,
    port: int,
    expires_in_seconds: Optional[int] = Query(default=None, alias="expiresInSeconds"),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    _ = expires_in_seconds
    token = traffic_access_token_for_row(row) or secrets.token_urlsafe(24)
    url = data_plane_base_url(sandbox_manager._config, sandbox_id=sandbox_id_or_name, port=port, scheme="http")
    return {"sandboxId": sandbox_id_or_name, "port": port, "token": token, "url": f"{url}?token={token}"}


@router.post("/sandbox/{sandbox_id_or_name}/ports/{port}/signed-preview-url/{token}/expire")
async def expire_signed_port_preview_url(sandbox_id_or_name: str, port: int, token: str):
    _ = (sandbox_id_or_name, port, token)
    return Response(status_code=204)


@router.post("/sandbox/{sandbox_id_or_name}/ssh-access")
async def create_ssh_access(
    sandbox_id_or_name: str,
    request: Request,
    expires_in_minutes: Optional[float] = Query(default=None, alias="expiresInMinutes"),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    return daytona_ssh_gateway.create_ssh_access_record(
        sandbox_manager,
        principal,
        row,
        request,
        expires_in_minutes=expires_in_minutes,
    )


@router.delete("/sandbox/{sandbox_id_or_name}/ssh-access")
async def revoke_ssh_access(
    sandbox_id_or_name: str,
    request: Request,
    token: Optional[str] = Query(default=None),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    daytona_ssh_gateway.revoke_ssh_access_record(sandbox_manager, row, token=token)
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.get("/sandbox/ssh-access/validate")
async def validate_ssh_access(
    token: str = Query(...),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    return daytona_ssh_gateway.validate_ssh_access_token(
        sandbox_manager,
        token,
        owner_client_id=principal.client_id,
    )


@router.post("/sandbox/{sandbox_id_or_name}/snapshot")
async def create_sandbox_snapshot(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    body = await _json_body(request)
    label = str(body.get("name") or "").strip() or None
    await sandbox_handlers.create_filesystem_snapshot_row(sandbox_id_or_name, label, principal, sandbox_manager)
    row = _ensure_live(sandbox_manager, principal, sandbox_id_or_name)
    return _sandbox_dto(row, sandbox_manager, request)


@router.get("/sandbox/{sandbox_id_or_name}/build-logs-url")
async def get_sandbox_build_logs_url(
    sandbox_id_or_name: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    return {"url": f"{_api_base_url(request)}/sandbox/{sandbox_id_or_name}/build-logs"}


@router.get("/sandbox/{sandbox_id_or_name}/build-logs")
async def get_sandbox_build_logs(
    sandbox_id_or_name: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = _owned_row(sandbox_manager, principal, sandbox_id_or_name)
    log = str(_daytona_meta(row).get("buildLog") or "Daytona image build completed\n")
    return StreamingResponse(iter([log]), media_type="text/plain")


@router.post("/sandbox/{sandbox_id}/last-activity")
async def update_last_activity(sandbox_id: str):
    _ = sandbox_id
    return Response(status_code=204)


@router.get("/sandbox/{sandbox_id}/toolbox-proxy-url")
async def get_toolbox_proxy_url(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    _owned_row(sandbox_manager, principal, sandbox_id)
    return {"url": _api_base_url(request)}


@router.get("/sandbox/{sandbox_id}/telemetry/metrics")
async def get_sandbox_telemetry_metrics(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    metrics = await run_io(sandbox_manager.get_metrics, sandbox_id)
    now = _now_iso()
    return {
        "series": [
            {"name": "cpuUsedPct", "points": [{"timestamp": now, "value": float((metrics or {}).get("cpu_percent") or 0.0)}]},
            {"name": "memUsed", "points": [{"timestamp": now, "value": int((metrics or {}).get("memory_usage") or 0)}]},
        ]
    }


@router.post("/snapshots")
async def create_snapshot(
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    body = await _json_body(request)
    build = body.get("buildInfo") or body.get("build_info") or {}
    name = _safe_template_alias(str(body.get("name") or f"daytona-{secrets.token_hex(6)}"))
    dockerfile = ""
    context_tar_gzip_base64 = None
    if isinstance(build, dict):
        dockerfile = str(build.get("dockerfileContent") or build.get("dockerfile_content") or "").strip()
        context_tar_gzip_base64 = await _daytona_context_tar_gzip_base64(build, principal, sandbox_manager)
    if not dockerfile:
        image = str(body.get("imageName") or body.get("image_name") or _DEFAULT_SNAPSHOT).strip()
        dockerfile = f"FROM {image}\n"
    req = RegisterTemplateFromDockerfileRequest(
        template_id=name,
        dockerfile=dockerfile,
        env={},
        context_tar_gzip_base64=context_tar_gzip_base64,
        settle_seconds=20,
    )
    await template_handlers.register_template_from_dockerfile(req, principal, sandbox_manager)
    row = sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, name) or {}
    return _snapshot_dto(row, owner_client_id=principal.client_id)


@router.get("/snapshots")
async def list_snapshots(
    page: Optional[int] = 1,
    limit: Optional[int] = 100,
    name: Optional[str] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    rows = sandbox_manager.db.list_sandbox_templates(principal.client_id)
    items = [_snapshot_dto(r, owner_client_id=principal.client_id) for r in rows]
    if name:
        items = [item for item in items if name.lower() in item["name"].lower()]
    page_num = max(1, int(page or 1))
    lim = max(1, min(200, int(limit or 100)))
    start = (page_num - 1) * lim
    page_items = items[start : start + lim]
    return {"items": page_items, "total": len(items), "page": page_num, "totalPages": max(1, (len(items) + lim - 1) // lim)}


@router.get("/snapshots/{snapshot_id}")
async def get_snapshot(
    snapshot_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _snapshot_row_or_404(sandbox_manager, principal, snapshot_id)
    return _snapshot_dto(row, owner_client_id=principal.client_id)


@router.get("/snapshots/{snapshot_id}/build-logs-url")
async def get_snapshot_build_logs_url(
    snapshot_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    _snapshot_row_or_404(sandbox_manager, principal, snapshot_id)
    return {"url": f"{_api_base_url(request)}/snapshots/{snapshot_id}/build-logs"}


@router.get("/snapshots/{snapshot_id}/build-logs")
async def get_snapshot_build_logs(
    snapshot_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = _snapshot_row_or_404(sandbox_manager, principal, snapshot_id)
    build = _latest_template_build_for_snapshot(sandbox_manager, principal, row)
    log = str((build or {}).get("build_log") or "")
    if not log:
        if row.get("build_error"):
            log = str(row.get("build_error"))
        else:
            log = "Daytona snapshot build completed\n"
    return StreamingResponse(iter([log]), media_type="text/plain")


@router.delete("/snapshots/{snapshot_id}")
async def delete_snapshot(snapshot_id: str):
    _ = snapshot_id
    return Response(status_code=204)


@router.get("/secret")
async def list_secrets() -> list[Any]:
    return []


@router.get("/secret/paginated")
async def list_secrets_paginated() -> dict[str, Any]:
    return {"items": [], "total": 0, "nextCursor": None}


@router.get("/volumes")
async def list_volumes() -> list[Any]:
    return []


@router.post("/secret")
async def unsupported_secret_collection():
    return _not_implemented("secrets")


@router.get("/secret/{secret_id}")
@router.patch("/secret/{secret_id}")
@router.delete("/secret/{secret_id}")
async def unsupported_secret_item(secret_id: str):
    _ = secret_id
    return _not_implemented("secrets")


@router.post("/volumes")
async def unsupported_volume_collection():
    return _not_implemented("volumes")


@router.get("/volumes/by-name/{name}")
async def unsupported_volume_by_name(name: str):
    _ = name
    return _not_implemented("volumes")


@router.get("/volumes/{volume_id}")
@router.delete("/volumes/{volume_id}")
async def unsupported_volume_item(volume_id: str):
    _ = volume_id
    return _not_implemented("volumes")


@router.post("/sandbox/{sandbox_id_or_name}/archive")
@router.post("/sandbox/{sandbox_id_or_name}/backup")
@router.post("/sandbox/{sandbox_id_or_name}/fork")
@router.post("/sandbox/{sandbox_id_or_name}/recover")
@router.post("/sandbox/{sandbox_id_or_name}/resize")
@router.get("/sandbox/{sandbox_id_or_name}/ancestors")
@router.get("/sandbox/{sandbox_id_or_name}/forks")
@router.get("/sandbox/{sandbox_id_or_name}/parent")
async def unsupported_sandbox_named_control_plane(sandbox_id_or_name: str):
    _ = sandbox_id_or_name
    return _not_implemented("control-plane operation")


@router.get("/sandbox/{sandbox_id}/organization")
@router.get("/sandbox/{sandbox_id}/region-quota")
@router.get("/sandbox/{sandbox_id}/secrets")
@router.get("/sandbox/{sandbox_id}/telemetry/logs")
@router.get("/sandbox/{sandbox_id}/telemetry/traces")
@router.put("/sandbox/{sandbox_id}/state")
async def unsupported_sandbox_id_control_plane(sandbox_id: str):
    _ = sandbox_id
    return _not_implemented("control-plane operation")


@router.get("/sandbox/{sandbox_id}/telemetry/traces/{trace_id}")
async def unsupported_sandbox_trace(sandbox_id: str, trace_id: str):
    _ = (sandbox_id, trace_id)
    return _not_implemented("control-plane operation")


@router.get("/sandbox/for-runner")
async def unsupported_static_sandbox_control_plane():
    return _not_implemented("control-plane operation")


@router.get("/object-storage/push-access")
async def object_storage_push_access(
    request: Request,
    x_daytona_organization_id: Optional[str] = Header(default=None, alias="X-Daytona-Organization-ID"),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
) -> dict[str, str]:
    _ = x_daytona_organization_id
    organization_id = str(principal.client_id)
    storage_url = str(getattr(get_config(), "DAYTONA_OBJECT_STORAGE_URL", "") or "").strip().rstrip("/")
    return {
        "accessKey": "sndbx",
        "secret": "sndbx",
        "sessionToken": _storage_token(organization_id),
        "storageUrl": storage_url or _api_base_url(request),
        "organizationId": organization_id,
        "bucket": DAYTONA_CONTEXT_BUCKET,
    }


@router.api_route("/{bucket}/{organization_id}/{context_hash}/context.tar", methods=["HEAD", "PUT"])
async def daytona_object_storage_context(
    bucket: str,
    organization_id: str,
    context_hash: str,
    request: Request,
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> Response:
    if bucket != DAYTONA_CONTEXT_BUCKET:
        return Response(status_code=404)
    if not _storage_token_ok(request, organization_id):
        return Response(status_code=403)
    object_key = daytona_context_key(organization_id, context_hash)
    if request.method.upper() == "HEAD":
        exists = await run_io(
            template_build_upload_exists,
            sandbox_manager.db,
            organization_id,
            DAYTONA_UPLOAD_NAMESPACE,
            object_key,
        )
        return Response(status_code=200 if exists else 404)
    payload = await request.body()
    if not payload:
        return Response(status_code=400)
    await run_io(
        put_template_build_upload,
        sandbox_manager.db,
        organization_id,
        DAYTONA_UPLOAD_NAMESPACE,
        object_key,
        payload,
        content_type=request.headers.get("content-type") or "application/x-tar",
        metadata={"organization_id": organization_id, "context_hash": context_hash},
    )
    return Response(status_code=200)


@router.post("/snapshots/{snapshot_id}/activate")
@router.post("/snapshots/{snapshot_id}/deactivate")
async def unsupported_snapshot_control_plane(snapshot_id: str):
    _ = snapshot_id
    return _not_implemented("control-plane operation")


for _api_prefix, _api_feature in (
    ("/admin", "admin API"),
    ("/audit", "audit API"),
    ("/docker-registry", "Docker registry API"),
    ("/jobs", "jobs API"),
    ("/object-storage", "object storage API"),
    ("/organizations", "organizations API"),
    ("/preview", "preview API"),
    ("/regions", "regions API"),
    ("/runners", "runners API"),
    ("/shared-regions", "shared regions API"),
    ("/users", "users API"),
    ("/webhooks", "webhooks API"),
):
    _add_unsupported_api_family(_api_prefix, _api_feature)


async def toolbox_user_home_dir(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    result = await _run_command(sandbox_manager, sandbox_id, "printf %s \"$HOME\"", timeout=10)
    home = str(result.get("stdout") or "").strip() or "/root"
    return {"dir": home}


async def toolbox_work_dir(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    result = await _run_command(sandbox_manager, sandbox_id, "pwd", timeout=10)
    return {"dir": str(result.get("stdout") or "").strip() or "/"}


async def toolbox_version(sandbox_id: str) -> dict[str, str]:
    _ = sandbox_id
    return {"version": "sndbx-daytona-compat"}


async def toolbox_list_files(
    sandbox_id: str,
    path: str = "/",
    depth: Optional[int] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> list[dict[str, Any]]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    return await _list_files_recursive(sandbox_manager, sandbox_id, path, max(1, int(depth or 1)))


async def toolbox_get_file_info(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    return await _stat_file_info(sandbox_manager, sandbox_id, path)


async def toolbox_download_file(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    content = await run_io(sandbox_manager.read_file, sandbox_id, path)
    if content is None:
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    return Response(content=str(content).encode("utf-8"), media_type="application/octet-stream")


async def toolbox_bulk_download(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    boundary = f"sndbx-{secrets.token_hex(12)}"
    parts: list[bytes] = []
    for source in [str(p) for p in body.get("paths") or []]:
        content = await run_io(sandbox_manager.read_file, sandbox_id, source)
        if content is None:
            payload = json.dumps({"message": f"File not found: {source}", "status_code": 404}).encode("utf-8")
            parts.append(_multipart_part("error", source, "application/json", payload, boundary))
        else:
            parts.append(_multipart_part("file", source, "application/octet-stream", str(content).encode("utf-8"), boundary))
    payload = b"".join(parts) + f"--{boundary}--\r\n".encode("utf-8")
    return Response(content=payload, media_type=f"multipart/form-data; boundary={boundary}")


async def toolbox_upload_file(
    sandbox_id: str,
    request: Request,
    path: Optional[str] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    form = await request.form()
    upload = form.get("file")
    if upload is None:
        raise HTTPException(status_code=400, detail="file is required")
    raw = await upload.read() if hasattr(upload, "read") else bytes(upload)
    dest = path or str(form.get("path") or getattr(upload, "filename", "") or "")
    if not dest:
        raise HTTPException(status_code=400, detail="path is required")
    if not await _write_bytes(sandbox_manager, sandbox_id, dest, raw):
        raise HTTPException(status_code=500, detail=f"Failed to write file: {dest}")
    return {"path": dest, "bytes": len(raw)}


async def toolbox_bulk_upload(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    form = await request.form()
    paths: dict[int, str] = {}
    files: dict[int, Any] = {}
    for key, value in form.multi_items():
        m = re.match(r"files\[(\d+)\]\.(path|file)$", str(key))
        if not m:
            continue
        idx = int(m.group(1))
        if m.group(2) == "path":
            paths[idx] = str(value)
        else:
            files[idx] = value
    for idx, upload in files.items():
        dest = paths.get(idx) or str(getattr(upload, "filename", "") or "")
        if not dest:
            raise HTTPException(status_code=400, detail=f"files[{idx}].path is required")
        raw = await upload.read() if hasattr(upload, "read") else bytes(upload)
        if not await _write_bytes(sandbox_manager, sandbox_id, dest, raw):
            raise HTTPException(status_code=500, detail=f"Failed to write file: {dest}")
    return Response(status_code=200)


async def toolbox_delete_file(
    sandbox_id: str,
    path: str,
    recursive: Optional[bool] = False,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    ok = await run_io(sandbox_manager.delete_file, sandbox_id, path, bool(recursive))
    if not ok:
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    return Response(status_code=204)


async def toolbox_create_folder(
    sandbox_id: str,
    path: str,
    mode: str = "755",
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    try:
        numeric_mode = int(str(mode).lstrip("0") or "755", 8)
    except Exception:
        numeric_mode = 0o755
    ok = await run_io(sandbox_manager.create_directory, sandbox_id, path, numeric_mode)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to create folder: {path}")
    return Response(status_code=201)


async def toolbox_move_file(
    sandbox_id: str,
    source: str,
    destination: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    parent = str(PurePosixPath(destination).parent)
    cmd = f"mkdir -p {shlex.quote(parent)} && mv {shlex.quote(source)} {shlex.quote(destination)}"
    result = await _run_command(sandbox_manager, sandbox_id, cmd, user="root", timeout=30)
    if int(result.get("exit_code", -1)) != 0:
        raise HTTPException(status_code=500, detail=str(result.get("stderr") or "move failed"))
    return Response(status_code=204)


async def toolbox_set_permissions(
    sandbox_id: str,
    path: str,
    mode: Optional[str] = None,
    owner: Optional[str] = None,
    group: Optional[str] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    commands = []
    if mode:
        commands.append(f"chmod {shlex.quote(mode)} {shlex.quote(path)}")
    if owner or group:
        spec = f"{owner or ''}:{group or ''}"
        commands.append(f"chown {shlex.quote(spec)} {shlex.quote(path)}")
    if commands:
        result = await _run_command(sandbox_manager, sandbox_id, " && ".join(commands), user="root", timeout=30)
        if int(result.get("exit_code", -1)) != 0:
            raise HTTPException(status_code=500, detail=str(result.get("stderr") or "permission update failed"))
    return Response(status_code=204)


async def toolbox_search_files(
    sandbox_id: str,
    path: str,
    pattern: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, list[str]]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    cmd = f"find {shlex.quote(path)} -name {shlex.quote(pattern)} -print"
    result = await _run_command(sandbox_manager, sandbox_id, cmd, timeout=30)
    files = [line for line in str(result.get("stdout") or "").splitlines() if line]
    return {"files": files}


async def toolbox_find_in_files(
    sandbox_id: str,
    path: str,
    pattern: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> list[dict[str, Any]]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    cmd = f"grep -RIn -- {shlex.quote(pattern)} {shlex.quote(path)}"
    result = await _run_command(sandbox_manager, sandbox_id, cmd, timeout=30)
    if int(result.get("exit_code", 0)) not in (0, 1):
        raise HTTPException(status_code=500, detail=str(result.get("stderr") or "grep failed"))
    matches = []
    for line in str(result.get("stdout") or "").splitlines():
        file_path, _, rest = line.partition(":")
        line_no, _, content = rest.partition(":")
        try:
            n = int(line_no)
        except Exception:
            n = 0
        matches.append({"file": file_path, "line": n, "content": content})
    return matches


async def toolbox_replace_in_files(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> list[dict[str, Any]]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    pattern = str(body.get("pattern") or "")
    new_value = str(body.get("newValue") or body.get("new_value") or "")
    out = []
    for path in [str(p) for p in body.get("files") or []]:
        try:
            content = await run_io(sandbox_manager.read_file, sandbox_id, path)
            if content is None:
                out.append({"file": path, "success": False, "error": "not found"})
                continue
            ok = await run_io(sandbox_manager.write_file, sandbox_id, path, str(content).replace(pattern, new_value))
            out.append({"file": path, "success": bool(ok), "error": None if ok else "write failed"})
        except Exception as ex:
            out.append({"file": path, "success": False, "error": str(ex)})
    return out


async def toolbox_execute_command(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    result = await _run_command(
        sandbox_manager,
        sandbox_id,
        str(body.get("command") or ""),
        cwd=body.get("cwd"),
        env=_str_dict(body.get("envs") or body.get("env")),
        timeout=body.get("timeout"),
    )
    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    return {"exitCode": int(result.get("exit_code", -1)), "result": stdout if stdout else stderr, "stdout": stdout, "stderr": stderr}


async def toolbox_code_run(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    language = str(body.get("language") or "python").lower()
    code = str(body.get("code") or "")
    argv = " ".join(shlex.quote(str(x)) for x in (body.get("argv") or []))
    if language in {"python", "py"}:
        command = f"python3 -c {shlex.quote(code)} {argv}".strip()
    elif language in {"javascript", "typescript", "js", "ts"}:
        command = f"node -e {shlex.quote(code)} {argv}".strip()
    else:
        return _not_implemented(f"code-run language {language}")
    result = await _run_command(
        sandbox_manager,
        sandbox_id,
        command,
        env=_str_dict(body.get("envs")),
        timeout=body.get("timeout"),
    )
    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    return {"exitCode": int(result.get("exit_code", -1)), "result": stdout if stdout else stderr, "artifacts": {"charts": []}}


async def toolbox_create_process_session(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    session_id = str(body.get("sessionId") or body.get("session_id") or "").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="sessionId is required")
    sessions = _daytona_process_sessions(row)
    if session_id in sessions:
        raise HTTPException(status_code=409, detail=f"Session already exists: {session_id}")
    session = await _start_daytona_shell_session(
        sandbox_manager,
        sandbox_id,
        session_id,
        {
            "commands": [],
            "cwd": str(body.get("cwd") or "/"),
            "envs": _str_dict(body.get("envs")),
            "createdAt": _now_iso(),
        },
    )
    sessions[session_id] = session
    _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
    return Response(status_code=201)


async def toolbox_list_process_sessions(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> list[dict[str, Any]]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    return [_session_dto(session_id, session) for session_id, session in sessions.items() if isinstance(session, dict)]


async def toolbox_get_process_session(
    sandbox_id: str,
    session_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    return _session_dto(session_id, _session_or_404(sessions, session_id))


async def toolbox_delete_process_session(
    sandbox_id: str,
    session_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    session = sessions.get(session_id)
    if isinstance(session, dict):
        with contextlib.suppress(Exception):
            await _envd_kill_process(sandbox_manager, sandbox_id, session)
    sessions.pop(session_id, None)
    _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
    return Response(status_code=204)


async def toolbox_execute_process_session_command(
    sandbox_id: str,
    session_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    command_text = str(body.get("command") or "").strip()
    if not command_text:
        raise HTTPException(status_code=400, detail="command is required")
    sessions = _daytona_process_sessions(row)
    session = _session_or_404(sessions, session_id)
    cmd_id = f"cmd-{secrets.token_hex(8)}"
    if not session.get("pid"):
        session = await _start_daytona_shell_session(sandbox_manager, sandbox_id, session_id, session)
        sessions[session_id] = session
        _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
    start_marker = f"__DAYTONA_CMD_START_{cmd_id}__"
    end_marker = f"__DAYTONA_CMD_END_{cmd_id}__:"
    script = (
        f"printf '%s\\n' {shlex.quote(start_marker)}\n"
        f"{command_text}\n"
        "__daytona_status=$?\n"
        f"printf '%s%s\\n' {shlex.quote(end_marker)} \"$__daytona_status\"\n"
    )
    try:
        await _envd_send_stdin_input(sandbox_manager, sandbox_id, session, script.encode("utf-8"))
        exit_code, stdout, stderr = await _collect_shell_command_result(
            sandbox_manager,
            sandbox_id,
            session,
            start_marker=start_marker,
            end_marker=end_marker,
            timeout=float(body.get("timeout") or 300.0),
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            session = await _start_daytona_shell_session(sandbox_manager, sandbox_id, session_id, session)
            sessions[session_id] = session
            _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
            await _envd_send_stdin_input(sandbox_manager, sandbox_id, session, script.encode("utf-8"))
            exit_code, stdout, stderr = await _collect_shell_command_result(
                sandbox_manager,
                sandbox_id,
                session,
                start_marker=start_marker,
                end_marker=end_marker,
                timeout=float(body.get("timeout") or 300.0),
            )
        else:
            raise
    output = stdout if stdout else stderr
    command_record = {
        "id": cmd_id,
        "command": command_text,
        "exitCode": exit_code,
        "stdout": stdout,
        "stderr": stderr,
        "output": output,
    }
    commands = session.get("commands")
    if not isinstance(commands, list):
        commands = []
    commands.append(command_record)
    session["commands"] = commands
    sessions[session_id] = session
    _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
    return {"cmdId": cmd_id, "exitCode": exit_code, "output": output, "stdout": stdout, "stderr": stderr}


async def toolbox_get_process_session_command(
    sandbox_id: str,
    session_id: str,
    command_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    session = _session_or_404(sessions, session_id)
    return _session_command_dto(_session_command_or_404(session, command_id))


async def toolbox_get_process_session_command_logs(
    sandbox_id: str,
    session_id: str,
    command_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    session = _session_or_404(sessions, session_id)
    command = _session_command_or_404(session, command_id)
    stdout = str(command.get("stdout") or "")
    stderr = str(command.get("stderr") or "")
    output = str(command.get("output") or stdout or stderr)
    return {"output": output, "stdout": stdout, "stderr": stderr}


async def toolbox_get_entrypoint_session(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    session = sessions.get(_DAYTONA_ENTRYPOINT_SESSION_ID)
    if not isinstance(session, dict):
        session = {"commands": []}
    return _session_dto(_DAYTONA_ENTRYPOINT_SESSION_ID, session)


async def toolbox_get_entrypoint_logs(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    session = sessions.get(_DAYTONA_ENTRYPOINT_SESSION_ID)
    commands = session.get("commands") if isinstance(session, dict) else []
    command = commands[-1] if isinstance(commands, list) and commands else {}
    stdout = str(command.get("stdout") or "") if isinstance(command, dict) else ""
    stderr = str(command.get("stderr") or "") if isinstance(command, dict) else ""
    return {"output": stdout if stdout else stderr, "stdout": stdout, "stderr": stderr}


async def toolbox_send_process_session_command_input(
    sandbox_id: str,
    session_id: str,
    command_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    sessions = _daytona_process_sessions(row)
    session = _session_or_404(sessions, session_id)
    command = _session_command_or_404(session, command_id)
    payload = str(body.get("data") or "").encode("utf-8")
    if payload:
        await _envd_send_stdin_input(sandbox_manager, sandbox_id, session, payload)
    command["stdin"] = str(command.get("stdin") or "") + payload.decode("utf-8", "replace")
    sessions[session_id] = session
    _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
    return Response(status_code=204)


async def toolbox_create_pty_session(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    session_id = str(body.get("id") or f"pty-{secrets.token_hex(8)}").strip()
    if not session_id:
        raise HTTPException(status_code=400, detail="PTY id is required")
    sessions = _daytona_pty_sessions(row)
    if session_id in sessions:
        raise HTTPException(status_code=409, detail=f"PTY session already exists: {session_id}")
    rows = max(1, int(body.get("rows") or 24))
    cols = max(1, int(body.get("cols") or 80))
    cwd = str(body.get("cwd") or "/")
    envs = _str_dict(body.get("envs"))
    sessions[session_id] = {
        "id": session_id,
        "tag": session_id,
        "pid": 0,
        "rows": rows,
        "cols": cols,
        "cwd": cwd,
        "envs": envs,
        "active": True,
        "lazyStart": bool(body.get("lazyStart", True)),
        "started": False,
        "createdAt": _now_iso(),
    }
    _save_daytona_pty_sessions(sandbox_manager, sandbox_id, sessions)
    return {"sessionId": session_id}


async def toolbox_list_pty_sessions(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_pty_sessions(row)
    return {"sessions": [_pty_session_dto(session_id, session) for session_id, session in sessions.items() if isinstance(session, dict)]}


async def toolbox_get_pty_session(
    sandbox_id: str,
    session_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_pty_sessions(row)
    return _pty_session_dto(session_id, _pty_session_or_404(sessions, session_id))


async def toolbox_resize_pty_session(
    sandbox_id: str,
    session_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    rows = max(1, int(body.get("rows") or 24))
    cols = max(1, int(body.get("cols") or 80))
    sessions = _daytona_pty_sessions(row)
    session = _pty_session_or_404(sessions, session_id)
    if _session_has_process(session):
        await _envd_resize_pty(sandbox_manager, sandbox_id, session, rows=rows, cols=cols)
    session["rows"] = rows
    session["cols"] = cols
    sessions[session_id] = session
    _save_daytona_pty_sessions(sandbox_manager, sandbox_id, sessions)
    return _pty_session_dto(session_id, session)


async def toolbox_delete_pty_session(
    sandbox_id: str,
    session_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_pty_sessions(row)
    session = _pty_session_or_404(sessions, session_id)
    if _session_has_process(session):
        with contextlib.suppress(Exception):
            await _envd_kill_process(sandbox_manager, sandbox_id, session)
    sessions.pop(session_id, None)
    _save_daytona_pty_sessions(sandbox_manager, sandbox_id, sessions)
    return Response(status_code=204)


async def _toolbox_pty_ws_envd_to_client(
    websocket: WebSocket,
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    ready_event: asyncio.Event,
) -> None:
    try:
        session_id = str(session.get("id") or session.get("tag") or "")
        info = _envd_connection_or_503(sandbox_manager, sandbox_id)
        base_url = str(info["http_base_url"]).rstrip("/")
        starting_session = False
        if _session_has_process(session):
            url = f"{base_url}/process.Process/Connect"
            req = process_pb2.ConnectRequest(process=_process_selector(session))
            response_type = process_pb2.ConnectResponse
            logger.info("Daytona PTY bridge reconnecting sandbox=%s session=%s", sandbox_id, session_id)
        else:
            starting_session = True
            url = f"{base_url}/process.Process/Start"
            req = process_pb2.StartRequest()
            req.process.cmd = "/bin/sh"
            req.process.cwd = str(session.get("cwd") or "/")
            req.process.envs.update(_str_dict(session.get("envs")))
            req.pty.size.rows = max(1, int(session.get("rows") or 24))
            req.pty.size.cols = max(1, int(session.get("cols") or 80))
            req.tag = session_id
            req.stdin = True
            response_type = process_pb2.StartResponse
            logger.info("Daytona PTY bridge starting sandbox=%s session=%s", sandbox_id, session_id)

        async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=5.0)) as client:
            async with client.stream(
                "POST",
                url,
                headers=_envd_headers(info, stream=True),
                content=_connect_envelope(req),
            ) as response:
                if response.status_code >= 400:
                    detail = (await response.aread()).decode("utf-8", "replace")
                    ready_event.set()
                    await websocket.send_text(json.dumps({"type": "control", "status": "error", "error": detail}))
                    return
                session["active"] = True
                session["started"] = True
                with contextlib.suppress(Exception):
                    _save_daytona_pty_session(sandbox_manager, sandbox_id, session_id, session)
                if not starting_session:
                    ready_event.set()
                buffer = bytearray()
                async for chunk in response.aiter_raw():
                    buffer.extend(chunk)
                    for msg in _parse_connect_messages(buffer, response_type):
                        if not msg.HasField("event"):
                            continue
                        event = msg.event
                        if event.HasField("start"):
                            pid = int(event.start.pid)
                            try:
                                previous_pid = int(session.get("pid") or 0)
                            except Exception:
                                previous_pid = 0
                            if pid > 0 and previous_pid != pid:
                                session["pid"] = pid
                                session["started"] = True
                                with contextlib.suppress(Exception):
                                    _save_daytona_pty_session(sandbox_manager, sandbox_id, session_id, session)
                            ready_event.set()
                        if event.HasField("data"):
                            payload = bytes(event.data.pty or event.data.stdout or event.data.stderr)
                            if payload:
                                await websocket.send_bytes(payload)
                        if event.HasField("end"):
                            session["active"] = False
                            session["exitCode"] = int(event.end.exit_code)
                            with contextlib.suppress(Exception):
                                _save_daytona_pty_session(sandbox_manager, sandbox_id, session_id, session)
                            exit_code = int(event.end.exit_code)
                            exit_reason = str(event.end.error or "")
                            if exit_code != 0 and not exit_reason:
                                exit_reason = str(event.end.status or "")
                            reason = json.dumps({"exitCode": exit_code, "exitReason": exit_reason})
                            await websocket.close(code=1000, reason=reason)
                            return
    except WebSocketDisconnect:
        return
    except HTTPException as exc:
        detail = str(exc.detail or "envd PTY connect failed")
        logger.warning("Daytona PTY envd connect failed sandbox=%s session=%s: %s", sandbox_id, session.get("id"), detail)
        ready_event.set()
        with contextlib.suppress(Exception):
            await websocket.send_text(json.dumps({"type": "control", "status": "error", "error": detail}))
    except Exception as exc:  # noqa: BLE001
        detail = f"{type(exc).__name__}: {exc}"
        logger.warning("Daytona PTY bridge failed sandbox=%s session=%s: %s", sandbox_id, session.get("id"), detail)
        ready_event.set()
        with contextlib.suppress(Exception):
            await websocket.send_text(json.dumps({"type": "control", "status": "error", "error": detail}))


async def _toolbox_pty_ws_client_to_envd(
    websocket: WebSocket,
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    ready_event: asyncio.Event,
) -> None:
    while True:
        message = await websocket.receive()
        mtype = message.get("type")
        if mtype == "websocket.disconnect":
            return
        if mtype != "websocket.receive":
            continue
        payload = message.get("bytes")
        if payload is None and message.get("text") is not None:
            payload = str(message.get("text") or "").encode("utf-8")
        if payload:
            await ready_event.wait()
            await _envd_send_pty_input(sandbox_manager, sandbox_id, session, bytes(payload))


async def toolbox_connect_pty_session_ws(websocket: WebSocket, sandbox_id: str, session_id: str):
    sandbox_manager = SandboxManager.__dict__.get("instance")
    if sandbox_manager is None:
        await websocket.close(code=1011, reason="sandbox manager unavailable")
        return
    try:
        principal = await _websocket_principal(websocket, sandbox_manager)
        row = _ensure_live(sandbox_manager, principal, sandbox_id)
        session = _pty_session_or_404(_daytona_pty_sessions(row), session_id)
    except HTTPException as exc:
        await websocket.close(code=4401 if exc.status_code in (401, 403) else 1011, reason=str(exc.detail)[:120])
        return
    await websocket.accept()
    await websocket.send_text(json.dumps({"type": "control", "status": "connected"}))
    ready_event = asyncio.Event()
    upstream_task = asyncio.create_task(_toolbox_pty_ws_envd_to_client(websocket, sandbox_manager, sandbox_id, session, ready_event))
    downstream_task = asyncio.create_task(_toolbox_pty_ws_client_to_envd(websocket, sandbox_manager, sandbox_id, session, ready_event))
    done, pending = await asyncio.wait({upstream_task, downstream_task}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    for task in pending:
        with contextlib.suppress(asyncio.CancelledError):
            await task
    for task in done:
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
        if task.cancelled():
            continue
        if exc and not isinstance(exc, WebSocketDisconnect):
            with contextlib.suppress(Exception):
                await websocket.close(code=1011, reason=str(exc)[:120])


async def _toolbox_logs_ws_send(websocket: WebSocket, stdout: str, stderr: str) -> None:
    await websocket.accept()
    if stdout:
        await websocket.send_bytes(b"\x01\x01\x01" + stdout.encode("utf-8"))
    if stderr:
        await websocket.send_bytes(b"\x02\x02\x02" + stderr.encode("utf-8"))
    await websocket.close(code=1000)


async def toolbox_process_command_logs_ws(websocket: WebSocket, sandbox_id: str, session_id: str, command_id: str):
    sandbox_manager = SandboxManager.__dict__.get("instance")
    if sandbox_manager is None:
        await websocket.close(code=1011, reason="sandbox manager unavailable")
        return
    try:
        principal = await _websocket_principal(websocket, sandbox_manager)
        row = _ensure_live(sandbox_manager, principal, sandbox_id)
        session = _session_or_404(_daytona_process_sessions(row), session_id)
        command = _session_command_or_404(session, command_id)
    except HTTPException as exc:
        await websocket.close(code=4401 if exc.status_code in (401, 403) else 1011, reason=str(exc.detail)[:120])
        return
    await _toolbox_logs_ws_send(websocket, str(command.get("stdout") or ""), str(command.get("stderr") or ""))


async def toolbox_entrypoint_logs_ws(websocket: WebSocket, sandbox_id: str):
    sandbox_manager = SandboxManager.__dict__.get("instance")
    if sandbox_manager is None:
        await websocket.close(code=1011, reason="sandbox manager unavailable")
        return
    try:
        principal = await _websocket_principal(websocket, sandbox_manager)
        row = _ensure_live(sandbox_manager, principal, sandbox_id)
        session = _daytona_process_sessions(row).get(_DAYTONA_ENTRYPOINT_SESSION_ID)
        commands = session.get("commands") if isinstance(session, dict) else []
        command = commands[-1] if isinstance(commands, list) and commands else {}
    except HTTPException as exc:
        await websocket.close(code=4401 if exc.status_code in (401, 403) else 1011, reason=str(exc.detail)[:120])
        return
    await _toolbox_logs_ws_send(
        websocket,
        str(command.get("stdout") or "") if isinstance(command, dict) else "",
        str(command.get("stderr") or "") if isinstance(command, dict) else "",
    )


def _git_scope_args(scope: Optional[str]) -> list[str]:
    scope = str(scope or "global").strip().lower()
    if scope == "local":
        return ["--local"]
    if scope == "system":
        return ["--system"]
    return ["--global"]


def _git_url_with_credentials(url: str, username: Optional[str], password: Optional[str]) -> str:
    if not username or password is None:
        return url
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return url
    userinfo = f"{url_quote(username, safe='')}:{url_quote(password, safe='')}"
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, f"{userinfo}@{host}", parts.path, parts.query, parts.fragment))


async def _git_run(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    args: list[Any],
    *,
    timeout: float = 120.0,
    allow_exit: tuple[int, ...] = (0,),
) -> dict[str, Any]:
    command = " ".join(shlex.quote(str(arg)) for arg in args)
    result = await _run_command(sandbox_manager, sandbox_id, command, user="root", timeout=timeout)
    exit_code = int(result.get("exit_code", -1))
    if exit_code not in allow_exit:
        detail = str(result.get("stderr") or result.get("stdout") or f"git exited {exit_code}")
        raise HTTPException(status_code=500, detail=detail)
    return result


async def _git_shell(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    command: str,
    *,
    timeout: float = 120.0,
    allow_exit: tuple[int, ...] = (0,),
) -> dict[str, Any]:
    result = await _run_command(sandbox_manager, sandbox_id, command, user="root", timeout=timeout)
    exit_code = int(result.get("exit_code", -1))
    if exit_code not in allow_exit:
        detail = str(result.get("stderr") or result.get("stdout") or f"git exited {exit_code}")
        raise HTTPException(status_code=500, detail=detail)
    return result


def _git_file_status(raw: str) -> str:
    return {
        " ": "Unmodified",
        "?": "Untracked",
        "M": "Modified",
        "A": "Added",
        "D": "Deleted",
        "R": "Renamed",
        "C": "Copied",
        "U": "Updated but unmerged",
    }.get(raw or " ", "Unmodified")


async def toolbox_git_add(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    files = [str(f) for f in body.get("files") or []] or ["."]
    await _git_run(sandbox_manager, sandbox_id, ["git", "-C", str(body.get("path") or "/"), "add", "--", *files])
    return Response(status_code=204)


async def toolbox_git_branches(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    branches = await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "branch", "--format", "%(refname:short)"])
    current = await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "branch", "--show-current"], allow_exit=(0, 1))
    return {
        "branches": [line.strip() for line in str(branches.get("stdout") or "").splitlines() if line.strip()],
        "current": str(current.get("stdout") or "").strip() or None,
    }


async def toolbox_git_clone(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    url = _git_url_with_credentials(str(body.get("url") or ""), body.get("username"), body.get("password"))
    args: list[Any] = ["git"]
    if body.get("insecureSkipTls") or body.get("insecure_skip_tls"):
        args.extend(["-c", "http.sslVerify=false"])
    args.append("clone")
    if body.get("depth"):
        args.extend(["--depth", int(body.get("depth"))])
    if body.get("branch"):
        args.extend(["--branch", str(body.get("branch"))])
    args.extend([url, str(body.get("path") or "")])
    await _git_run(sandbox_manager, sandbox_id, args, timeout=600.0)
    commit_id = str(body.get("commitId") or body.get("commit_id") or "").strip()
    if commit_id:
        await _git_run(sandbox_manager, sandbox_id, ["git", "-C", str(body.get("path") or ""), "checkout", commit_id], timeout=120.0)
    return Response(status_code=204)


async def toolbox_git_commit(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, str]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    path = str(body.get("path") or "/")
    args: list[Any] = [
        "git",
        "-C",
        path,
        "-c",
        f"user.name={str(body.get('author') or '')}",
        "-c",
        f"user.email={str(body.get('email') or '')}",
        "commit",
        "-m",
        str(body.get("message") or ""),
    ]
    if body.get("allowEmpty") or body.get("allow_empty"):
        args.append("--allow-empty")
    await _git_run(sandbox_manager, sandbox_id, args)
    head = await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "rev-parse", "HEAD"])
    return {"hash": str(head.get("stdout") or "").strip()}


async def toolbox_git_status(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    result = await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "status", "--porcelain=v1", "-b"], allow_exit=(0,))
    branch = ""
    upstream = None
    ahead = 0
    behind = 0
    detached = False
    files: list[dict[str, str]] = []
    for line in str(result.get("stdout") or "").splitlines():
        if line.startswith("## "):
            head = line[3:]
            if "HEAD" in head and "no branch" in head:
                branch = "HEAD"
                detached = True
            else:
                branch = head.split("...", 1)[0].strip()
                if "..." in head:
                    upstream_part = head.split("...", 1)[1]
                    upstream = upstream_part.split(" ", 1)[0].strip() or None
                m_ahead = re.search(r"ahead (\d+)", head)
                m_behind = re.search(r"behind (\d+)", head)
                ahead = int(m_ahead.group(1)) if m_ahead else 0
                behind = int(m_behind.group(1)) if m_behind else 0
            continue
        if len(line) < 4:
            continue
        staging = line[0]
        worktree = line[1]
        name = line[3:]
        extra = ""
        if " -> " in name:
            extra, _, name = name.partition(" -> ")
        files.append(
            {
                "name": name,
                "extra": extra,
                "staging": _git_file_status(staging),
                "worktree": _git_file_status(worktree if staging != "?" else "?"),
            }
        )
    return {
        "ahead": ahead,
        "behind": behind,
        "branchPublished": bool(upstream),
        "currentBranch": branch,
        "detached": detached,
        "fileStatus": files,
        "upstream": upstream,
    }


async def toolbox_git_simple_body_command(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    path = str(body.get("path") or "/")
    request_path = request.url.path
    if request_path.endswith("/git/checkout"):
        args = ["git", "-C", path, "checkout", str(body.get("branch") or "")]
    elif request_path.endswith("/git/branches") and request.method.upper() == "POST":
        args = ["git", "-C", path, "checkout", "-b", str(body.get("name") or "")]
    elif request_path.endswith("/git/branches") and request.method.upper() == "DELETE":
        args = ["git", "-C", path, "branch", "-D", str(body.get("name") or "")]
    elif request_path.endswith("/git/pull"):
        args = ["git", "-C", path, "pull"]
        if body.get("remote"):
            args.append(str(body.get("remote")))
        if body.get("branch"):
            args.append(str(body.get("branch")))
    elif request_path.endswith("/git/push"):
        args = ["git", "-C", path, "push"]
        if body.get("setUpstream") or body.get("set_upstream"):
            args.append("-u")
        if body.get("remote"):
            args.append(str(body.get("remote")))
        if body.get("branch"):
            args.append(str(body.get("branch")))
    else:
        raise HTTPException(status_code=404, detail="unknown git route")
    await _git_run(sandbox_manager, sandbox_id, args, timeout=600.0)
    return Response(status_code=204)


async def toolbox_git_init(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    path = str(body.get("path") or "/")
    args: list[Any] = ["git", "-C", path, "init"]
    if body.get("bare"):
        args.append("--bare")
    if body.get("initialBranch") or body.get("initial_branch"):
        args.extend(["--initial-branch", str(body.get("initialBranch") or body.get("initial_branch"))])
    await _git_shell(
        sandbox_manager,
        sandbox_id,
        f"mkdir -p {shlex.quote(path)} && {' '.join(shlex.quote(str(a)) for a in args)}",
    )
    return Response(status_code=204)


async def toolbox_git_reset(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    args: list[Any] = ["git", "-C", str(body.get("path") or "/"), "reset"]
    mode = str(body.get("mode") or "").strip()
    if mode:
        args.append(f"--{mode}")
    if body.get("target"):
        args.append(str(body.get("target")))
    files = [str(f) for f in body.get("files") or []]
    if files:
        args.extend(["--", *files])
    await _git_run(sandbox_manager, sandbox_id, args)
    return Response(status_code=204)


async def toolbox_git_restore(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    args: list[Any] = ["git", "-C", str(body.get("path") or "/"), "restore"]
    if body.get("staged"):
        args.append("--staged")
    if body.get("worktree"):
        args.append("--worktree")
    if body.get("source"):
        args.extend(["--source", str(body.get("source"))])
    args.extend(["--", *[str(f) for f in body.get("files") or []]])
    await _git_run(sandbox_manager, sandbox_id, args)
    return Response(status_code=204)


async def toolbox_git_remote_add(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    path = str(body.get("path") or "/")
    name = str(body.get("name") or "")
    url = str(body.get("url") or "")
    if body.get("overwrite"):
        await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "remote", "remove", name], allow_exit=(0, 2))
    await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "remote", "add", name, url])
    if body.get("fetch"):
        await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "fetch", name], timeout=600.0)
    return Response(status_code=204)


async def toolbox_git_remotes(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    result = await _git_run(sandbox_manager, sandbox_id, ["git", "-C", path, "remote", "-v"], allow_exit=(0,))
    remotes: dict[str, str] = {}
    for line in str(result.get("stdout") or "").splitlines():
        parts = line.split()
        if len(parts) >= 2 and (len(parts) < 3 or parts[2] == "(fetch)"):
            remotes[parts[0]] = parts[1]
    return {"remotes": [{"name": name, "url": url} for name, url in remotes.items()]}


async def toolbox_git_set_config(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    args: list[Any] = ["git"]
    if str(body.get("scope") or "global").strip().lower() == "local":
        args.extend(["-C", str(body.get("path") or "/")])
    args.extend(["config", *_git_scope_args(body.get("scope")), str(body.get("key") or ""), str(body.get("value") or "")])
    await _git_run(sandbox_manager, sandbox_id, args)
    return Response(status_code=204)


async def toolbox_git_get_config(
    sandbox_id: str,
    key: str,
    path: Optional[str] = None,
    scope: Optional[str] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Optional[str]]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    args: list[Any] = ["git"]
    if str(scope or "global").strip().lower() == "local":
        args.extend(["-C", str(path or "/")])
    args.extend(["config", *_git_scope_args(scope), "--get", key])
    result = await _git_run(sandbox_manager, sandbox_id, args, allow_exit=(0, 1))
    value = str(result.get("stdout") or "").strip()
    return {"value": value or None}


async def toolbox_git_configure_user(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    for key, value in (("user.name", str(body.get("name") or "")), ("user.email", str(body.get("email") or ""))):
        args: list[Any] = ["git"]
        if str(body.get("scope") or "global").strip().lower() == "local":
            args.extend(["-C", str(body.get("path") or "/")])
        args.extend(["config", *_git_scope_args(body.get("scope")), key, value])
        await _git_run(sandbox_manager, sandbox_id, args)
    return Response(status_code=204)


async def toolbox_git_authenticate(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    _ensure_live(sandbox_manager, principal, sandbox_id)
    body = await _json_body(request)
    protocol = str(body.get("protocol") or "https")
    host = str(body.get("host") or "github.com")
    credential = f"{protocol}://{url_quote(str(body.get('username') or ''), safe='')}:{url_quote(str(body.get('password') or ''), safe='')}@{host}"
    await _git_shell(
        sandbox_manager,
        sandbox_id,
        "git config --global credential.helper store && "
        f"mkdir -p \"$HOME\" && printf '%s\\n' {shlex.quote(credential)} >> \"$HOME/.git-credentials\"",
    )
    return Response(status_code=204)


async def toolbox_git_history(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> list[dict[str, str]]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    result = await _git_run(
        sandbox_manager,
        sandbox_id,
        ["git", "-C", path, "log", "--pretty=format:%H%x09%an%x09%ae%x09%cI%x09%s", "-n", "50"],
        allow_exit=(0, 128),
    )
    out: list[dict[str, str]] = []
    for line in str(result.get("stdout") or "").splitlines():
        parts = line.split("\t", 4)
        if len(parts) == 5:
            out.append({"hash": parts[0], "author": parts[1], "email": parts[2], "timestamp": parts[3], "message": parts[4]})
    return out


async def toolbox_ports(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    return {"ports": ports_from_metadata(row.get("metadata") or {})}


async def toolbox_port_in_use(
    sandbox_id: str,
    port: int,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, bool]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    return {"isInUse": int(port) in set(ports_from_metadata(row.get("metadata") or {}))}


async def toolbox_system_metrics(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    _ensure_live(sandbox_manager, principal, sandbox_id)
    metrics = await run_io(sandbox_manager.get_metrics, sandbox_id)
    now = datetime.now(timezone.utc)
    return {
        "cpuCount": 1,
        "cpuUsedPct": float((metrics or {}).get("cpu_percent") or 0.0),
        "diskFree": 0,
        "diskTotal": 0,
        "diskUsed": 0,
        "memCache": 0,
        "memTotal": int((metrics or {}).get("memory_limit") or 0),
        "memUsed": int((metrics or {}).get("memory_usage") or 0),
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "timestampUnix": int(now.timestamp()),
    }


_add_toolbox_route("/user-home-dir", toolbox_user_home_dir, ["GET"])
_add_toolbox_route("/work-dir", toolbox_work_dir, ["GET"])
_add_toolbox_route("/version", toolbox_version, ["GET"])
_add_toolbox_route("/files", toolbox_list_files, ["GET"])
_add_toolbox_route("/files", toolbox_delete_file, ["DELETE"])
_add_toolbox_route("/files/download", toolbox_download_file, ["GET"])
_add_toolbox_route("/files/bulk-download", toolbox_bulk_download, ["POST"])
_add_toolbox_route("/files/upload", toolbox_upload_file, ["POST"])
_add_toolbox_route("/files/bulk-upload", toolbox_bulk_upload, ["POST"])
_add_toolbox_route("/files/folder", toolbox_create_folder, ["POST"], status_code=201)
_add_toolbox_route("/files/info", toolbox_get_file_info, ["GET"])
_add_toolbox_route("/files/move", toolbox_move_file, ["POST"])
_add_toolbox_route("/files/permissions", toolbox_set_permissions, ["POST"])
_add_toolbox_route("/files/search", toolbox_search_files, ["GET"])
_add_toolbox_route("/files/find", toolbox_find_in_files, ["GET"])
_add_toolbox_route("/files/replace", toolbox_replace_in_files, ["POST"])
_add_toolbox_route("/process/execute", toolbox_execute_command, ["POST"])
_add_toolbox_route("/process/code-run", toolbox_code_run, ["POST"])
_add_toolbox_route("/process/session", toolbox_create_process_session, ["POST"], status_code=201)
_add_toolbox_route("/process/session", toolbox_list_process_sessions, ["GET"])
_add_toolbox_route("/process/session/entrypoint", toolbox_get_entrypoint_session, ["GET"])
_add_toolbox_route("/process/session/entrypoint/logs", toolbox_get_entrypoint_logs, ["GET"])
_add_toolbox_route("/process/session/{session_id}", toolbox_get_process_session, ["GET"])
_add_toolbox_route("/process/session/{session_id}", toolbox_delete_process_session, ["DELETE"])
_add_toolbox_route("/process/session/{session_id}/exec", toolbox_execute_process_session_command, ["POST"])
_add_toolbox_route("/process/session/{session_id}/command/{command_id}", toolbox_get_process_session_command, ["GET"])
_add_toolbox_route("/process/session/{session_id}/command/{command_id}/logs", toolbox_get_process_session_command_logs, ["GET"])
_add_toolbox_ws_route("/process/session/{session_id}/command/{command_id}/logs", toolbox_process_command_logs_ws)
_add_toolbox_route("/process/session/{session_id}/command/{command_id}/input", toolbox_send_process_session_command_input, ["POST"])
_add_toolbox_ws_route("/process/session/entrypoint/logs", toolbox_entrypoint_logs_ws)
_add_toolbox_route("/process/pty", toolbox_create_pty_session, ["POST"], status_code=201)
_add_toolbox_route("/process/pty", toolbox_list_pty_sessions, ["GET"])
_add_toolbox_route("/process/pty/{session_id}", toolbox_get_pty_session, ["GET"])
_add_toolbox_route("/process/pty/{session_id}", toolbox_delete_pty_session, ["DELETE"])
_add_toolbox_route("/process/pty/{session_id}/resize", toolbox_resize_pty_session, ["POST"])
_add_toolbox_ws_route("/process/pty/{session_id}/connect", toolbox_connect_pty_session_ws)
_add_toolbox_route("/git/add", toolbox_git_add, ["POST"])
_add_toolbox_route("/git/branches", toolbox_git_branches, ["GET"])
_add_toolbox_route("/git/branches", toolbox_git_simple_body_command, ["POST", "DELETE"])
_add_toolbox_route("/git/checkout", toolbox_git_simple_body_command, ["POST"])
_add_toolbox_route("/git/clone", toolbox_git_clone, ["POST"])
_add_toolbox_route("/git/commit", toolbox_git_commit, ["POST"])
_add_toolbox_route("/git/status", toolbox_git_status, ["GET"])
_add_toolbox_route("/git/init", toolbox_git_init, ["POST"])
_add_toolbox_route("/git/pull", toolbox_git_simple_body_command, ["POST"])
_add_toolbox_route("/git/push", toolbox_git_simple_body_command, ["POST"])
_add_toolbox_route("/git/reset", toolbox_git_reset, ["POST"])
_add_toolbox_route("/git/restore", toolbox_git_restore, ["POST"])
_add_toolbox_route("/git/remotes", toolbox_git_remotes, ["GET"])
_add_toolbox_route("/git/remotes", toolbox_git_remote_add, ["POST"])
_add_toolbox_route("/git/config", toolbox_git_get_config, ["GET"])
_add_toolbox_route("/git/config", toolbox_git_set_config, ["POST"])
_add_toolbox_route("/git/config/user", toolbox_git_configure_user, ["POST"])
_add_toolbox_route("/git/credentials", toolbox_git_authenticate, ["POST"])
_add_toolbox_route("/git/history", toolbox_git_history, ["GET"])
_add_toolbox_route("/port", toolbox_ports, ["GET"])
_add_toolbox_route("/port/{port}/in-use", toolbox_port_in_use, ["GET"])
_add_toolbox_route("/system/metrics", toolbox_system_metrics, ["GET"])

for _prefix, _feature in (
    ("lsp", "LSP"),
    ("computer-use", "computer-use"),
    ("process/interpreter", "stateful interpreter"),
):
    _add_toolbox_route(f"/{_prefix}", _unsupported_toolbox_endpoint(_feature), _ALL_METHODS)
    _add_toolbox_route(f"/{_prefix}/{{path:path}}", _unsupported_toolbox_endpoint(_feature, with_path=True), _ALL_METHODS)

router.include_router(toolbox_router)
router.include_router(deprecated_toolbox_router)
