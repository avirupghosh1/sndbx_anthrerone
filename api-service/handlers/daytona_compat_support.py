"""Support helpers for the Daytona REST/toolbox compatibility routes."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import re
import secrets
import shlex
import struct
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Optional

import httpx
from fastapi import HTTPException, Request, WebSocket
from fastapi.responses import JSONResponse

from async_runner import run_io
from config import get_config
from envd_guest.proto import process_pb2
from handlers import sandboxes as sandbox_handlers
from handlers import templates as template_handlers
from handlers.build_context import (
    DAYTONA_UPLOAD_NAMESPACE,
    daytona_context_key,
    merged_context_from_uploads,
)
from middleware import (
    ApiKeyPrincipal,
    ClientAuthError,
    SandboxNotFoundException,
    authenticate_client_credential,
)
from models import RegisterTemplateFromDockerfileRequest
from orchestrator import SandboxManager

logger = logging.getLogger(__name__)

_CODE_TOOLBOX_LANGUAGE_LABEL = "code-toolbox-language"
_DAYTONA_ORG_ID = "local"
_DEFAULT_SNAPSHOT = "python:3.11"
_CONNECT_HEADER = struct.Struct(">BI")
_CONNECT_FLAG_END_STREAM = 0b00000010
_DAYTONA_ENTRYPOINT_SESSION_ID = "entrypoint"


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

def _storage_token(organization_id: str) -> str:
    cfg = get_config()
    secret = str(
        getattr(cfg, "INTERNAL_API_KEY", "")
        or getattr(cfg, "API_KEY", "")
        or "sndbx-daytona-object-storage"
    )
    msg = f"{DAYTONA_UPLOAD_NAMESPACE}\0{organization_id or ''}"
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

def _storage_token_ok(request: Request, organization_id: str) -> bool:
    token = (
        request.headers.get("x-amz-security-token")
        or request.query_params.get("X-Amz-Security-Token")
        or request.query_params.get("x-amz-security-token")
        or ""
    )
    if not token:
        return False
    return hmac.compare_digest(_storage_token(organization_id), token)

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

def _api_key_from_headers(headers: Any) -> str:
    api_key = (headers.get("x-api-key") or headers.get("X-API-Key") or "").strip()
    if api_key:
        return api_key
    auth = (headers.get("authorization") or headers.get("Authorization") or "").strip()
    scheme, _, token = auth.partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else ""

def _principal_from_api_key(api_key: str, sandbox_manager: SandboxManager) -> ApiKeyPrincipal:
    if not api_key:
        raise HTTPException(status_code=401, detail="X-API-Key or Authorization Bearer token required")
    try:
        return authenticate_client_credential(api_key)
    except ClientAuthError as ex:
        raise HTTPException(status_code=ex.status_code, detail=ex.detail) from ex

async def _websocket_principal(websocket: WebSocket, sandbox_manager: SandboxManager) -> ApiKeyPrincipal:
    return _principal_from_api_key(_api_key_from_headers(websocket.headers), sandbox_manager)

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

async def _daytona_context_tar_gzip_base64(
    build: dict[str, Any],
    principal: ApiKeyPrincipal,
    sandbox_manager: SandboxManager,
) -> Optional[str]:
    raw_hashes = build.get("contextHashes") or build.get("context_hashes") or []
    if not isinstance(raw_hashes, list) or not raw_hashes:
        return None
    hashes = [str(h).strip() for h in raw_hashes if str(h or "").strip()]
    if not hashes:
        return None
    keys = [daytona_context_key(principal.client_id, h) for h in hashes]
    try:
        return await run_io(
            merged_context_from_uploads,
            sandbox_manager.db,
            owner_client_id=principal.client_id,
            namespace=DAYTONA_UPLOAD_NAMESPACE,
            object_keys=keys,
        )
    except KeyError as ex:
        raise HTTPException(status_code=400, detail=f"Missing Daytona image context upload(s): {ex}") from ex

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
    context_tar_gzip_base64 = await _daytona_context_tar_gzip_base64(build, principal, sandbox_manager)
    alias = _safe_template_alias(str(body.get("snapshot") or body.get("name") or f"daytona-{secrets.token_hex(6)}"))
    req = RegisterTemplateFromDockerfileRequest(
        template_id=alias,
        dockerfile=dockerfile,
        build_args=_str_dict(build.get("buildArgs") or build.get("build_args")),
        env=_str_dict(body.get("env")),
        context_tar_gzip_base64=context_tar_gzip_base64,
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

def _daytona_pty_sessions(row: Optional[dict]) -> dict[str, Any]:
    sessions = _metadata(row).get("daytona_pty_sessions")
    return dict(sessions) if isinstance(sessions, dict) else {}

def _save_daytona_pty_sessions(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    sessions: dict[str, Any],
) -> None:
    row = sandbox_manager.get_sandbox(sandbox_id)
    if not row:
        raise SandboxNotFoundException(sandbox_id)
    md = _metadata(row)
    md["daytona_pty_sessions"] = sessions
    sandbox_manager.db.merge_sandbox_metadata(sandbox_id, md)

def _save_daytona_pty_session(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session_id: str,
    session: dict[str, Any],
) -> None:
    row = sandbox_manager.get_sandbox(sandbox_id)
    sessions = _daytona_pty_sessions(row)
    sessions[session_id] = session
    _save_daytona_pty_sessions(sandbox_manager, sandbox_id, sessions)

def _pty_session_or_404(sessions: dict[str, Any], session_id: str) -> dict[str, Any]:
    session = sessions.get(session_id)
    if not isinstance(session, dict):
        raise HTTPException(status_code=404, detail=f"PTY session not found: {session_id}")
    return session

def _pty_session_dto(session_id: str, session: dict[str, Any]) -> dict[str, Any]:
    return {
        "active": bool(session.get("active", True)),
        "cols": int(session.get("cols") or 80),
        "createdAt": str(session.get("createdAt") or _now_iso()),
        "cwd": str(session.get("cwd") or "/"),
        "envs": _str_dict(session.get("envs")),
        "id": session_id,
        "lazyStart": bool(session.get("lazyStart", False)),
        "rows": int(session.get("rows") or 24),
    }

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

def _envd_connection_or_503(sandbox_manager: SandboxManager, sandbox_id: str) -> dict[str, Any]:
    info, reason = sandbox_manager.get_envd_connection_ex(sandbox_id)
    if not info:
        raise HTTPException(status_code=503, detail=f"envd unavailable: {reason or 'unknown'}")
    return info

def _envd_headers(info: dict[str, Any], *, stream: bool = False) -> dict[str, str]:
    headers = {
        "X-Access-Token": str(info.get("access_token") or ""),
        "Content-Type": "application/connect+proto" if stream else "application/proto",
        "Connect-Protocol-Version": "1",
    }
    traffic_token = str(info.get("traffic_access_token") or "").strip()
    if traffic_token:
        headers["e2b-traffic-access-token"] = traffic_token
    internal_route_headers = info.get("internal_route_headers")
    if isinstance(internal_route_headers, dict):
        for key, value in internal_route_headers.items():
            k = str(key or "").strip()
            v = str(value or "").strip()
            if k and v:
                headers[k] = v
    return headers

def _connect_envelope(message: Any) -> bytes:
    payload = message.SerializeToString()
    return _CONNECT_HEADER.pack(0, len(payload)) + payload

def _parse_connect_messages(buffer: bytearray, response_type: Any) -> list[Any]:
    messages: list[Any] = []
    while len(buffer) >= _CONNECT_HEADER.size:
        flags, size = _CONNECT_HEADER.unpack(bytes(buffer[: _CONNECT_HEADER.size]))
        total = _CONNECT_HEADER.size + int(size)
        if len(buffer) < total:
            break
        payload = bytes(buffer[_CONNECT_HEADER.size:total])
        del buffer[:total]
        if flags & _CONNECT_FLAG_END_STREAM:
            continue
        msg = response_type()
        msg.ParseFromString(payload)
        messages.append(msg)
    return messages

async def _envd_unary(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    rpc_path: str,
    request_message: Any,
    response_type: Any,
    *,
    timeout: float = 30.0,
) -> Any:
    info = _envd_connection_or_503(sandbox_manager, sandbox_id)
    url = f"{str(info['http_base_url']).rstrip('/')}{rpc_path}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=5.0)) as client:
        response = await client.post(
            url,
            headers=_envd_headers(info),
            content=request_message.SerializeToString(),
        )
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    out = response_type()
    out.ParseFromString(response.content)
    return out

async def _envd_stream_first(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    rpc_path: str,
    request_message: Any,
    response_type: Any,
    *,
    timeout: float = 30.0,
) -> Any:
    info = _envd_connection_or_503(sandbox_manager, sandbox_id)
    url = f"{str(info['http_base_url']).rstrip('/')}{rpc_path}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=5.0, read=timeout)) as client:
        async with client.stream(
            "POST",
            url,
            headers=_envd_headers(info, stream=True),
            content=_connect_envelope(request_message),
        ) as response:
            if response.status_code >= 400:
                detail = await response.aread()
                raise HTTPException(status_code=response.status_code, detail=detail.decode("utf-8", "replace"))
            buffer = bytearray()
            async for chunk in response.aiter_raw():
                buffer.extend(chunk)
                messages = _parse_connect_messages(buffer, response_type)
                if messages:
                    return messages[0]
    raise HTTPException(status_code=502, detail="envd stream closed before first event")

def _process_selector(session: dict[str, Any]) -> Any:
    selector = process_pb2.ProcessSelector()
    pid = session.get("pid")
    try:
        if pid is not None:
            pid_int = int(pid)
            if pid_int > 0:
                selector.pid = pid_int
                return selector
    except Exception:
        pass
    selector.tag = str(session.get("tag") or session.get("id") or "")
    return selector

def _session_has_process(session: dict[str, Any]) -> bool:
    try:
        if int(session.get("pid") or 0) > 0:
            return True
    except Exception:
        pass
    return bool(session.get("started"))

async def _envd_send_pty_input(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    payload: bytes,
) -> None:
    req = process_pb2.SendInputRequest(process=_process_selector(session))
    req.input.pty = payload
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/SendInput", req, process_pb2.SendInputResponse, timeout=15.0)

async def _envd_send_stdin_input(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    payload: bytes,
) -> None:
    req = process_pb2.SendInputRequest(process=_process_selector(session))
    req.input.stdin = payload
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/SendInput", req, process_pb2.SendInputResponse, timeout=15.0)

async def _envd_resize_pty(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    *,
    rows: int,
    cols: int,
) -> None:
    req = process_pb2.UpdateRequest(process=_process_selector(session))
    req.pty.size.rows = max(1, int(rows or 24))
    req.pty.size.cols = max(1, int(cols or 80))
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/Update", req, process_pb2.UpdateResponse, timeout=15.0)

async def _envd_kill_process(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
) -> None:
    req = process_pb2.SendSignalRequest(process=_process_selector(session), signal=process_pb2.SIGNAL_SIGKILL)
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/SendSignal", req, process_pb2.SendSignalResponse, timeout=15.0)

async def _start_daytona_shell_session(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session_id: str,
    session: dict[str, Any],
) -> dict[str, Any]:
    cwd = str(session.get("cwd") or "/")
    envs = _str_dict(session.get("envs"))
    req = process_pb2.StartRequest()
    req.process.cmd = "/bin/sh"
    req.process.cwd = cwd
    req.process.envs.update(envs)
    req.tag = session_id
    req.stdin = True
    first = await _envd_stream_first(
        sandbox_manager,
        sandbox_id,
        "/process.Process/Start",
        req,
        process_pb2.StartResponse,
        timeout=30.0,
    )
    pid = 0
    if first and first.HasField("event") and first.event.HasField("start"):
        pid = int(first.event.start.pid)
    if pid <= 0:
        raise HTTPException(status_code=502, detail="envd did not return a session shell pid")
    session.update(
        {
            "id": session_id,
            "tag": session_id,
            "pid": pid,
            "cwd": cwd,
            "envs": envs,
            "active": True,
            "createdAt": session.get("createdAt") or _now_iso(),
            "commands": session.get("commands") if isinstance(session.get("commands"), list) else [],
        }
    )
    return session

def _strip_one_leading_newline(value: str) -> str:
    if value.startswith("\r\n"):
        return value[2:]
    if value.startswith("\n"):
        return value[1:]
    return value

async def _collect_shell_command_result(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    *,
    start_marker: str,
    end_marker: str,
    timeout: float,
) -> tuple[int, str, str]:
    async def _read() -> tuple[int, str, str]:
        info = _envd_connection_or_503(sandbox_manager, sandbox_id)
        url = f"{str(info['http_base_url']).rstrip('/')}/process.Process/Connect"
        req = process_pb2.ConnectRequest(process=_process_selector(session))
        stdout_accum = ""
        stderr_chunks: list[str] = []
        seen_start = False
        async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=5.0)) as client:
            async with client.stream(
                "POST",
                url,
                headers=_envd_headers(info, stream=True),
                content=_connect_envelope(req),
            ) as response:
                if response.status_code >= 400:
                    detail = (await response.aread()).decode("utf-8", "replace")
                    raise HTTPException(status_code=response.status_code, detail=detail)
                buffer = bytearray()
                async for chunk in response.aiter_raw():
                    buffer.extend(chunk)
                    for msg in _parse_connect_messages(buffer, process_pb2.ConnectResponse):
                        if not msg.HasField("event"):
                            continue
                        event = msg.event
                        if event.HasField("data"):
                            if event.data.stdout:
                                text = bytes(event.data.stdout).decode("utf-8", "replace")
                                stdout_accum += text
                                if not seen_start:
                                    idx = stdout_accum.find(start_marker)
                                    if idx >= 0:
                                        seen_start = True
                                        stdout_accum = _strip_one_leading_newline(stdout_accum[idx + len(start_marker):])
                                if seen_start:
                                    end_idx = stdout_accum.find(end_marker)
                                    if end_idx >= 0:
                                        stdout = stdout_accum[:end_idx]
                                        tail = stdout_accum[end_idx + len(end_marker):]
                                        match = re.match(r"(-?\d+)", tail.strip())
                                        exit_code = int(match.group(1)) if match else 0
                                        return exit_code, stdout, "".join(stderr_chunks)
                            if event.data.stderr and seen_start:
                                stderr_chunks.append(bytes(event.data.stderr).decode("utf-8", "replace"))
                        if event.HasField("end"):
                            raise HTTPException(status_code=409, detail="session shell exited before command completed")
        raise HTTPException(status_code=502, detail="envd stream closed before command marker")

    try:
        return await asyncio.wait_for(_read(), timeout=max(1.0, float(timeout or 30.0)))
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="session command timed out") from exc

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
