"""Daytona SDK compatibility wrapper over the generic local API handlers."""

from __future__ import annotations

import base64
import json
import re
import secrets
import shlex
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from async_runner import run_io
from handlers import sandboxes as sandbox_handlers
from handlers import templates as template_handlers
from middleware import ApiKeyPrincipal, SandboxNotFoundException, validate_api_key
from models import CreateSandboxRequest, RegisterTemplateFromDockerfileRequest
from orchestrator import SandboxManager
from orchestrator.guest_ports import ports_from_metadata
from orchestrator.sandbox_connections import data_plane_base_url, traffic_access_token_for_row

router = APIRouter(tags=["daytona-compat"])
toolbox_router = APIRouter(prefix="/{sandbox_id}", tags=["daytona-toolbox-compat"])
deprecated_toolbox_router = APIRouter(prefix="/toolbox/{sandbox_id}/toolbox", tags=["daytona-toolbox-compat"])
_TOOLBOX_ROUTERS = (toolbox_router, deprecated_toolbox_router)

_CODE_TOOLBOX_LANGUAGE_LABEL = "code-toolbox-language"
_DAYTONA_ORG_ID = "local"
_DEFAULT_SNAPSHOT = "python:3.11"
_ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _api_base_url(request: Request) -> str:
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    prefix = (request.headers.get("x-forwarded-prefix") or request.scope.get("root_path") or "").rstrip("/")
    if host:
        return f"{proto}://{host}{prefix}".rstrip("/")
    return str(request.base_url).rstrip("/")


def _daytona_error(status_code: int, message: str, *, code: str = "not_implemented"):
    return JSONResponse(
        status_code=status_code,
        content={
            "error": code,
            "message": message,
            "status_code": status_code,
            "details": {"detail": message},
        },
    )


def _not_implemented(feature: str):
    return _daytona_error(501, f"Daytona {feature} is not implemented yet in this sandbox API.")


def _metadata(row: Optional[dict]) -> dict[str, Any]:
    md = (row or {}).get("metadata")
    return dict(md) if isinstance(md, dict) else {}


def _daytona_meta(row: Optional[dict]) -> dict[str, Any]:
    md = _metadata(row)
    dm = md.get("daytona")
    return dict(dm) if isinstance(dm, dict) else {}


def _str_dict(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(k): str(v) for k, v in value.items() if v is not None}


def _parse_cpu(value: Any, default: float = 1.0) -> float:
    try:
        return max(0.0, float(str(value or default).strip()))
    except Exception:
        return default


def _limit_to_gib(value: Any, default_gib: float) -> float:
    raw = str(value or "").strip().lower()
    if not raw:
        return default_gib
    try:
        if raw.endswith("gb") or raw.endswith("g"):
            return max(0.0, float(raw.rstrip("gbg")))
        if raw.endswith("mb") or raw.endswith("m"):
            return max(0.0, float(raw.rstrip("mbm")) / 1024.0)
        if raw.endswith("k"):
            return max(0.0, float(raw[:-1]) / 1024.0 / 1024.0)
        n = float(raw)
        if n > 1024 * 1024:
            return n / 1024.0 / 1024.0 / 1024.0
        if n > 1024:
            return n / 1024.0
        return n
    except Exception:
        return default_gib


def _memory_limit_from_daytona(value: Any) -> str:
    if value is None:
        return "512m"
    try:
        n = float(value)
    except Exception:
        return "512m"
    if n <= 0:
        return "512m"
    if n < 1:
        return f"{max(128, int(n * 1024))}m"
    return f"{int(n)}g"


def _daytona_state(row: Optional[dict]) -> str:
    md = _metadata(row)
    override = str(md.get("daytona_state") or "").strip().lower()
    state = str((row or {}).get("state") or "").strip().lower()
    if state == "running":
        return "started"
    if state in {"pausing", "resuming"}:
        return state
    if state == "paused":
        return override if override in {"paused", "stopped"} else "paused"
    if state == "killed":
        return "destroyed"
    if state == "failed":
        return "error"
    return "unknown"


def _daytona_labels(row: Optional[dict]) -> dict[str, str]:
    md = _metadata(row)
    labels = _str_dict(md.get("labels"))
    labels.setdefault(_CODE_TOOLBOX_LANGUAGE_LABEL, "python")
    return labels


def _daytona_public(row: Optional[dict], sandbox_manager: SandboxManager) -> bool:
    try:
        return bool(sandbox_handlers.sandbox_response_payload(row or {}, sandbox_manager, include_secrets=False).get("allow_public_traffic"))
    except Exception:
        return False


def _daytona_build_info(value: Any, *, created_at: str, updated_at: str, snapshot_ref: str) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    dto: dict[str, Any] = {
        "createdAt": value.get("createdAt") or created_at,
        "updatedAt": value.get("updatedAt") or updated_at,
        "snapshotRef": str(value.get("snapshotRef") or snapshot_ref),
    }
    if value.get("dockerfileContent") is not None:
        dto["dockerfileContent"] = value.get("dockerfileContent")
    if value.get("contextHashes") is not None:
        dto["contextHashes"] = value.get("contextHashes")
    return dto


def _sandbox_dto(
    row: dict,
    sandbox_manager: SandboxManager,
    request: Request,
    *,
    include_full_fields: bool = True,
) -> dict[str, Any]:
    md = _metadata(row)
    dm = _daytona_meta(row)
    sandbox_id = str(row.get("sandbox_id") or row.get("id") or "")
    created_at = str(row.get("created_at") or _now_iso())
    updated_at = str(row.get("updated_at") or created_at)
    labels = _daytona_labels(row)
    env = _str_dict(md.get("env"))
    snapshot = str(dm.get("snapshot") or row.get("template_id") or _DEFAULT_SNAPSHOT)
    public = _daytona_public(row, sandbox_manager)
    state = _daytona_state(row)
    dto: dict[str, Any] = {
        "id": sandbox_id,
        "organizationId": str(row.get("owner_client_id") or _DAYTONA_ORG_ID),
        "name": str(dm.get("name") or md.get("name") or sandbox_id),
        "snapshot": snapshot,
        "user": str(dm.get("user") or md.get("user") or "root"),
        "labels": labels,
        "public": public,
        "target": str(dm.get("target") or md.get("target") or "local"),
        "cpu": _parse_cpu(row.get("cpu_limit"), 1.0),
        "gpu": 0,
        "memory": _limit_to_gib(row.get("memory_limit"), 0.5),
        "disk": _limit_to_gib(row.get("disk_limit"), 0.0),
        "state": state,
        "desiredState": state if state in {"started", "stopped", "paused", "destroyed", "archived"} else None,
        "errorReason": None,
        "recoverable": False,
        "backupState": None,
        "autoStopInterval": dm.get("autoStopInterval"),
        "autoArchiveInterval": dm.get("autoArchiveInterval"),
        "autoDeleteInterval": dm.get("autoDeleteInterval"),
        "createdAt": created_at,
        "updatedAt": updated_at,
        "lastActivityAt": str(row.get("last_activity_at") or updated_at),
        "daemonVersion": "sndbx",
        "toolboxProxyUrl": _api_base_url(request),
    }
    if include_full_fields:
        dto.update(
            {
                "env": env,
                "networkBlockAll": bool(dm.get("networkBlockAll", False)),
                "networkAllowList": dm.get("networkAllowList"),
                "domainAllowList": dm.get("domainAllowList"),
                "volumes": dm.get("volumes") or [],
                "buildInfo": _daytona_build_info(
                    dm.get("buildInfo"),
                    created_at=created_at,
                    updated_at=updated_at,
                    snapshot_ref=snapshot,
                ),
                "backupCreatedAt": None,
            }
        )
    return dto


def _snapshot_dto(row: dict, *, owner_client_id: str = _DAYTONA_ORG_ID) -> dict[str, Any]:
    created_at = str(row.get("created_at") or _now_iso())
    updated_at = str(row.get("updated_at") or created_at)
    name = str(row.get("template_alias") or row.get("label") or row.get("snapshot_id") or row.get("template_id") or "")
    image_name = str(row.get("warm_snapshot_image") or row.get("image_ref") or row.get("base_image") or "")
    dockerfile = str(row.get("dockerfile_text") or "").strip()
    return {
        "id": str(row.get("template_id") or row.get("snapshot_id") or name),
        "organizationId": str(row.get("owner_client_id") or owner_client_id or _DAYTONA_ORG_ID),
        "general": False,
        "name": name,
        "imageName": image_name or None,
        "state": "active" if not row.get("build_error") else "build_failed",
        "size": None,
        "entrypoint": None,
        "cpu": 1,
        "gpu": 0,
        "mem": 1,
        "disk": 0,
        "errorReason": row.get("build_error"),
        "createdAt": created_at,
        "updatedAt": updated_at,
        "lastUsedAt": None,
        "buildInfo": _daytona_build_info(
            {"dockerfileContent": dockerfile},
            created_at=created_at,
            updated_at=updated_at,
            snapshot_ref=image_name or name,
        )
        if dockerfile
        else None,
        "regionIds": ["local"],
        "ref": image_name or None,
    }


def _snapshot_row_or_404(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    snapshot_id: str,
) -> dict[str, Any]:
    row = (
        sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, snapshot_id)
        or sandbox_manager.db.get_sandbox_template(snapshot_id)
        or sandbox_manager.db.get_sandbox_snapshot(snapshot_id, owner_client_id=principal.client_id)
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown snapshot: {snapshot_id}")
    return row


def _latest_template_build_for_snapshot(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    row: dict[str, Any],
) -> Optional[dict[str, Any]]:
    template_id = str(row.get("template_id") or row.get("snapshot_id") or "").strip()
    alias = str(row.get("template_alias") or row.get("label") or "").strip()
    if not template_id and not alias:
        return None
    builds = sandbox_manager.db.list_template_builds_for_client(principal.client_id, limit=200)
    for build in builds:
        if template_id and str(build.get("template_id") or "") == template_id:
            return build
        if alias and str(build.get("template_alias") or "") == alias:
            return build
    return None


def _resolve_daytona_snapshot_reference(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    snapshot: str,
) -> str:
    requested = (snapshot or "").strip()
    if not requested:
        return requested
    row = sandbox_manager.db.get_sandbox_snapshot(requested, owner_client_id=principal.client_id)
    if row:
        return str(row.get("snapshot_id") or requested)
    list_all = getattr(sandbox_manager.db, "list_all_sandbox_snapshots", None)
    rows = list_all(500, owner_client_id=principal.client_id) if callable(list_all) else []
    for candidate in rows:
        if str(candidate.get("label") or "") == requested:
            return str(candidate.get("snapshot_id") or requested)
    return requested


def _safe_template_alias(raw: str) -> str:
    alias = re.sub(r"[^a-zA-Z0-9._-]+", "-", (raw or "").strip()).strip("-._")
    if not alias or not alias[0].isalpha():
        alias = f"daytona-{alias or secrets.token_hex(6)}"
    return alias[:63]


async def _ensure_daytona_build_template(
    body: dict[str, Any],
    principal: ApiKeyPrincipal,
    sandbox_manager: SandboxManager,
) -> Optional[str]:
    build = body.get("buildInfo") or body.get("build_info")
    if not isinstance(build, dict):
        return None
    dockerfile = str(build.get("dockerfileContent") or build.get("dockerfile_content") or "").strip()
    if not dockerfile:
        return None
    alias = _safe_template_alias(str(body.get("snapshot") or body.get("name") or f"daytona-{secrets.token_hex(6)}"))
    req = RegisterTemplateFromDockerfileRequest(
        template_id=alias,
        dockerfile=dockerfile,
        build_args=_str_dict(build.get("buildArgs") or build.get("build_args")),
        env=_str_dict(body.get("env")),
        settle_seconds=20,
    )
    await template_handlers.register_template_from_dockerfile(req, principal, sandbox_manager)
    return alias


def _update_daytona_metadata(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    updates: dict[str, Any],
) -> None:
    row = sandbox_manager.get_sandbox(sandbox_id)
    if not row:
        raise SandboxNotFoundException(sandbox_id)
    md = _metadata(row)
    daytona = dict(md.get("daytona") or {})
    daytona.update({k: v for k, v in updates.items() if not k.startswith("_")})
    md["daytona"] = daytona
    if "_daytona_state" in updates:
        md["daytona_state"] = updates["_daytona_state"]
    sandbox_manager.db.merge_sandbox_metadata(sandbox_id, md)


def _daytona_process_sessions(row: Optional[dict]) -> dict[str, Any]:
    sessions = _metadata(row).get("daytona_process_sessions")
    return dict(sessions) if isinstance(sessions, dict) else {}


def _save_daytona_process_sessions(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    sessions: dict[str, Any],
) -> None:
    row = sandbox_manager.get_sandbox(sandbox_id)
    if not row:
        raise SandboxNotFoundException(sandbox_id)
    md = _metadata(row)
    md["daytona_process_sessions"] = sessions
    sandbox_manager.db.merge_sandbox_metadata(sandbox_id, md)


def _session_command_dto(command: dict[str, Any]) -> dict[str, Any]:
    dto = {
        "id": str(command.get("id") or ""),
        "command": str(command.get("command") or ""),
    }
    if command.get("exitCode") is not None:
        dto["exitCode"] = int(command.get("exitCode"))
    return dto


def _session_dto(session_id: str, session: dict[str, Any]) -> dict[str, Any]:
    commands = session.get("commands")
    if not isinstance(commands, list):
        commands = []
    return {
        "sessionId": session_id,
        "commands": [_session_command_dto(cmd) for cmd in commands if isinstance(cmd, dict)],
    }


def _session_or_404(sessions: dict[str, Any], session_id: str) -> dict[str, Any]:
    session = sessions.get(session_id)
    if not isinstance(session, dict):
        raise HTTPException(status_code=404, detail=f"Session not found: {session_id}")
    return session


def _session_command_or_404(session: dict[str, Any], command_id: str) -> dict[str, Any]:
    commands = session.get("commands")
    if not isinstance(commands, list):
        raise HTTPException(status_code=404, detail=f"Command not found: {command_id}")
    for command in commands:
        if isinstance(command, dict) and str(command.get("id") or "") == command_id:
            return command
    raise HTTPException(status_code=404, detail=f"Command not found: {command_id}")


def _owned_row(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    sandbox_id: str,
) -> dict:
    return sandbox_handlers.owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)


def _ensure_live(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    sandbox_id: str,
) -> dict:
    row = _owned_row(sandbox_manager, principal, sandbox_id)
    sandbox_handlers.ensure_live_sandbox(sandbox_manager, sandbox_id)
    return row


async def _run_command(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    command: str,
    *,
    cwd: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
    timeout: Optional[float] = None,
    user: Optional[str] = None,
) -> dict[str, Any]:
    result = await run_io(sandbox_manager.run_command, sandbox_id, command, cwd, env, timeout, user)
    return result or {"exit_code": -1, "stdout": "", "stderr": "Command failed", "pid": -1}


async def _write_bytes(sandbox_manager: SandboxManager, sandbox_id: str, path: str, payload: bytes) -> bool:
    quoted = shlex.quote(path)
    parent = str(PurePosixPath(path).parent)
    await _run_command(sandbox_manager, sandbox_id, f"mkdir -p {shlex.quote(parent)}", user="root", timeout=30)
    await _run_command(sandbox_manager, sandbox_id, f": > {quoted}", user="root", timeout=30)
    if not payload:
        return True
    for offset in range(0, len(payload), 2048):
        enc = base64.b64encode(payload[offset : offset + 2048]).decode("ascii")
        result = await _run_command(
            sandbox_manager,
            sandbox_id,
            f"printf '%s' {shlex.quote(enc)} | base64 -d >> {quoted}",
            user="root",
            timeout=30,
        )
        if int(result.get("exit_code", -1)) != 0:
            return False
    return True


def _file_info_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    typ = str(entry.get("type") or "").lower()
    path = str(entry.get("path") or "")
    permissions = str(entry.get("permissions") or "")
    modified = str(entry.get("modified_at") or _now_iso())
    return {
        "group": "root",
        "isDir": typ == "directory",
        "modTime": modified,
        "mode": permissions,
        "modifiedAt": modified if "T" in modified else _now_iso(),
        "name": str(entry.get("name") or PurePosixPath(path).name),
        "owner": "root",
        "path": path,
        "permissions": permissions,
        "size": int(entry.get("size") or 0),
    }


async def _stat_file_info(sandbox_manager: SandboxManager, sandbox_id: str, path: str) -> dict[str, Any]:
    cmd = "stat -c '%F\t%s\t%a\t%U\t%G\t%Y\t%n' " + shlex.quote(path)
    result = await _run_command(sandbox_manager, sandbox_id, cmd, user="root", timeout=10)
    if int(result.get("exit_code", -1)) != 0:
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    parts = str(result.get("stdout") or "").rstrip("\n").split("\t", 6)
    if len(parts) < 7:
        raise HTTPException(status_code=500, detail=f"Unable to stat file: {path}")
    ftype, size, mode, owner, group, mtime, name = parts
    try:
        modified = datetime.fromtimestamp(int(mtime), timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        modified = _now_iso()
    return {
        "group": group or "root",
        "isDir": "directory" in ftype.lower(),
        "modTime": modified,
        "mode": mode,
        "modifiedAt": modified,
        "name": PurePosixPath(name).name,
        "owner": owner or "root",
        "path": path,
        "permissions": mode,
        "size": int(size or 0),
    }


async def _list_files_recursive(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    path: str,
    depth: int,
) -> list[dict[str, Any]]:
    entries = await run_io(sandbox_manager.list_files, sandbox_id, path)
    if entries is None:
        raise HTTPException(status_code=404, detail=f"Path not found: {path}")
    out = [_file_info_from_entry(entry) for entry in entries]
    if depth > 1:
        for entry in entries:
            if str(entry.get("type") or "").lower() == "directory":
                out.extend(await _list_files_recursive(sandbox_manager, sandbox_id, str(entry.get("path")), depth - 1))
    return out


def _multipart_part(name: str, filename: str, content_type: str, payload: bytes, boundary: str) -> bytes:
    safe_filename = filename.replace("\\", "\\\\").replace('"', '\\"')
    headers = [
        f"--{boundary}",
        f'Content-Disposition: form-data; name="{name}"; filename="{safe_filename}"',
        f"Content-Type: {content_type}",
        f"Content-Length: {len(payload)}",
        "",
        "",
    ]
    return "\r\n".join(headers).encode("utf-8") + payload + b"\r\n"


def _add_toolbox_route(
    path: str,
    endpoint: Callable[..., Awaitable[Any]],
    methods: list[str],
    *,
    status_code: Optional[int] = None,
) -> None:
    for r in _TOOLBOX_ROUTERS:
        r.add_api_route(path, endpoint, methods=methods, status_code=status_code)


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
    if isinstance(build, dict):
        dockerfile = str(build.get("dockerfileContent") or build.get("dockerfile_content") or "").strip()
    if not dockerfile:
        image = str(body.get("imageName") or body.get("image_name") or _DEFAULT_SNAPSHOT).strip()
        dockerfile = f"FROM {image}\n"
    req = RegisterTemplateFromDockerfileRequest(
        template_id=name,
        dockerfile=dockerfile,
        env={},
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
@router.post("/sandbox/{sandbox_id_or_name}/ssh-access")
@router.delete("/sandbox/{sandbox_id_or_name}/ssh-access")
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


@router.get("/sandbox/ssh-access/validate")
@router.get("/sandbox/for-runner")
async def unsupported_static_sandbox_control_plane():
    return _not_implemented("control-plane operation")


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
    sessions[session_id] = {"commands": []}
    _save_daytona_process_sessions(sandbox_manager, sandbox_id, sessions)
    return Response(status_code=201)


async def toolbox_list_process_sessions(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
) -> dict[str, Any]:
    row = _ensure_live(sandbox_manager, principal, sandbox_id)
    sessions = _daytona_process_sessions(row)
    return {"sessions": [_session_dto(session_id, session) for session_id, session in sessions.items() if isinstance(session, dict)]}


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
    result = await _run_command(sandbox_manager, sandbox_id, command_text)
    stdout = str(result.get("stdout") or "")
    stderr = str(result.get("stderr") or "")
    exit_code = int(result.get("exit_code", -1))
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
_add_toolbox_route("/process/session/{session_id}", toolbox_get_process_session, ["GET"])
_add_toolbox_route("/process/session/{session_id}", toolbox_delete_process_session, ["DELETE"])
_add_toolbox_route("/process/session/{session_id}/exec", toolbox_execute_process_session_command, ["POST"])
_add_toolbox_route("/process/session/{session_id}/command/{command_id}", toolbox_get_process_session_command, ["GET"])
_add_toolbox_route("/process/session/{session_id}/command/{command_id}/logs", toolbox_get_process_session_command_logs, ["GET"])
_add_toolbox_route("/port", toolbox_ports, ["GET"])
_add_toolbox_route("/port/{port}/in-use", toolbox_port_in_use, ["GET"])
_add_toolbox_route("/system/metrics", toolbox_system_metrics, ["GET"])

for _prefix, _feature in (
    ("git", "Git"),
    ("lsp", "LSP"),
    ("computer-use", "computer-use"),
    ("process/pty", "PTY sessions"),
    ("process/interpreter", "stateful interpreter"),
):
    _add_toolbox_route(f"/{_prefix}", _unsupported_toolbox_endpoint(_feature), _ALL_METHODS)
    _add_toolbox_route(f"/{_prefix}/{{path:path}}", _unsupported_toolbox_endpoint(_feature, with_path=True), _ALL_METHODS)

router.include_router(toolbox_router)
router.include_router(deprecated_toolbox_router)
