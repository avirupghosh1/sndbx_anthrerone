"""Support helpers for the E2B SDK compatibility routes."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import parse_qsl, unquote

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from async_runner import run_io
from config import get_config
from handlers import sandboxes as sandbox_handlers
from handlers import templates as template_handlers
from handlers.build_context import e2b_upload_key
from middleware import ApiKeyPrincipal
from models import RegisterTemplateFromDockerfileRequest
from orchestrator import SandboxManager
from orchestrator.sandbox_constants import ENVD_TEMPLATE_BAKED_ENV
from orchestrator.runtime_gateway_templates import (
    gateway_template_build_enabled,
    stream_dockerfile_template_via_gateway,
)

logger = logging.getLogger(__name__)

_E2B_ENVD_VERSION = "0.6.7"


def _error_response(status_code: int, message: str, *, error: str = "HTTPException") -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": error,
            "code": status_code,
            "message": message,
            "status_code": status_code,
            "details": {"detail": message},
        },
    )

def _is_e2b_request(request: Request) -> bool:
    ua = (request.headers.get("user-agent") or "").lower()
    publisher = (request.headers.get("publisher") or "").lower()
    sdk_runtime = (request.headers.get("sdk_runtime") or "").lower()
    return "e2b" in ua or publisher == "e2b" or sdk_runtime == "python"

async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}

def _api_base_url(request: Request) -> str:
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc).split(",")[0].strip()
    prefix = (request.headers.get("x-forwarded-prefix") or request.scope.get("root_path") or "").rstrip("/")
    if host:
        return f"{proto}://{host}{prefix}".rstrip("/")
    return str(request.base_url).rstrip("/")

def _looks_like_e2b_create(body: dict[str, Any]) -> bool:
    return any(
        key in body
        for key in (
            "templateID",
            "envVars",
            "allowInternetAccess",
            "autoPause",
            "secure",
            "lifecycle",
            "volumeMounts",
        )
    )

def _e2b_allow_public_traffic(body: dict[str, Any]) -> bool:
    network = body.get("network")
    if isinstance(network, dict):
        for key in ("allowPublicTraffic", "allow_public_traffic"):
            if key in network:
                return bool(network.get(key))
    for key in ("allowPublicTraffic", "allow_public_traffic"):
        if key in body:
            return bool(body.get(key))
    return True

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _coerce_iso(value: Any, fallback: Optional[str] = None) -> str:
    if value:
        return str(value)
    return fallback or _now_iso()

def _parse_cpu_count(value: Any) -> int:
    try:
        return max(1, int(float(str(value or "1").strip())))
    except Exception:
        return 1

def _parse_bytes_or_mb(value: Any, default_mb: int) -> int:
    raw = str(value or "").strip().lower()
    if not raw:
        return int(default_mb)
    try:
        if raw.endswith("mb") or raw.endswith("m"):
            return max(0, int(float(raw.rstrip("mbm"))))
        if raw.endswith("gb") or raw.endswith("g"):
            return max(0, int(float(raw.rstrip("gbg")) * 1024))
        if raw.endswith("k"):
            return max(0, int(float(raw[:-1]) / 1024))
        n = float(raw)
        return max(0, int(n / (1024 * 1024))) if n > 1024 * 1024 else max(0, int(n))
    except Exception:
        return int(default_mb)

def _e2b_sandbox_payload(
    sandbox: Optional[dict],
    sandbox_manager: SandboxManager,
    *,
    include_secrets: bool,
    default_client_id: str = "",
) -> dict:
    raw = sandbox_handlers.sandbox_response_payload(
        sandbox or {},
        sandbox_manager,
        include_secrets=include_secrets,
    )
    metadata = sandbox_handlers.strip_secret_metadata(raw.get("metadata"))
    raw["metadata"] = metadata
    sandbox_id = str(raw.get("sandbox_id") or (sandbox or {}).get("sandbox_id") or "")
    template_id = str((sandbox or {}).get("template_id") or metadata.get("template_id") or "python:3.11")
    client_id = str((sandbox or {}).get("owner_client_id") or default_client_id or "default")
    timeout = int((sandbox or {}).get("timeout") or 3600)
    end_at = raw.get("lease_expires_at") or (
        datetime.now(timezone.utc) + timedelta(seconds=timeout)
    ).isoformat().replace("+00:00", "Z")
    envd_token = raw.get("envd_access_token") if include_secrets else None
    traffic_token = raw.get("traffic_access_token") if include_secrets else None
    e2b = {
        "clientID": client_id,
        "cpuCount": _parse_cpu_count((sandbox or {}).get("cpu_limit")),
        "diskSizeMB": _parse_bytes_or_mb((sandbox or {}).get("disk_limit"), 0),
        "endAt": _coerce_iso(end_at),
        "envdVersion": _E2B_ENVD_VERSION,
        "memoryMB": _parse_bytes_or_mb((sandbox or {}).get("memory_limit"), 512),
        "sandboxID": sandbox_id,
        "sandboxDomain": raw.get("sandbox_domain"),
        "startedAt": _coerce_iso(raw.get("created_at") or (sandbox or {}).get("created_at")),
        "state": str(raw.get("state") or (sandbox or {}).get("state") or "running"),
        "templateID": template_id,
        "alias": template_id,
        "allowInternetAccess": True,
        "domain": raw.get("sandbox_domain"),
        "metadata": metadata,
        "volumeMounts": [],
    }
    if envd_token:
        e2b["envdAccessToken"] = envd_token
    if traffic_token:
        e2b["trafficAccessToken"] = traffic_token
    return {**raw, **e2b}

def _e2b_snapshot_payload(row: dict) -> dict:
    label = str(row.get("label") or "").strip()
    image_ref = str(row.get("image_ref") or "").strip()
    names = [x for x in (label, image_ref) if x]
    return {"snapshotID": str(row.get("snapshot_id") or ""), "names": names}

def _metadata_filter_matches(metadata: Any, encoded_filter: Optional[str]) -> bool:
    if not encoded_filter:
        return True
    md = metadata if isinstance(metadata, dict) else {}
    for key, value in parse_qsl(encoded_filter, keep_blank_values=True):
        if str(md.get(unquote(key), "")) != unquote(value):
            return False
    return True

def _e2b_build_name_parts(name: str, tags: Optional[list[str]] = None) -> tuple[str, list[str], list[str]]:
    raw = (name or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="template name is required")
    alias, _, tag = raw.partition(":")
    alias = template_handlers._validate_template_id(alias)
    out_tags = list(tags or [])
    if tag and tag not in out_tags:
        out_tags.append(tag)
    names = [raw]
    names.extend(f"{alias}:{t}" for t in out_tags if ":" not in raw or t != tag)
    return alias, out_tags, names

def _quote_docker_env(value: Any) -> str:
    return json.dumps(str(value))

def _upload_token(owner_client_id: str, namespace: str, object_key: str) -> str:
    cfg = get_config()
    secret = str(
        getattr(cfg, "INTERNAL_API_KEY", "")
        or getattr(cfg, "API_KEY", "")
        or "sndbx-upload-token"
    )
    msg = "\0".join((owner_client_id or "", namespace or "", object_key or ""))
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

def _verify_upload_token(owner_client_id: str, namespace: str, object_key: str, token: str) -> bool:
    return hmac.compare_digest(_upload_token(owner_client_id, namespace, object_key), str(token or ""))

def _docker_copy_line(args: list[str]) -> str:
    if len(args) < 2 or not args[0] or not args[1]:
        raise HTTPException(status_code=400, detail="E2B COPY step requires source and destination")
    src, dest = args[0], args[1]
    user = args[2].strip() if len(args) > 2 else ""
    mode = args[3].strip() if len(args) > 3 else ""
    flags = []
    if user:
        flags.append(f"--chown={user}")
    if mode:
        flags.append(f"--chmod={mode}")
    return "COPY " + " ".join([*flags, json.dumps([src, dest])])

def _dockerfile_from_e2b_template_payload(
    template_id: str,
    body: dict[str, Any],
) -> tuple[str, dict[str, str], str, str, list[str]]:
    base_image = str(body.get("fromImage") or "python:3.11").strip()
    if body.get("fromTemplate"):
        base_image = str(body.get("fromTemplate") or "").strip()
    if not base_image:
        base_image = "python:3.11"
    env: dict[str, str] = {}
    lines = [f"FROM {base_image}"]
    context_keys: list[str] = []
    for step in body.get("steps") or []:
        if not isinstance(step, dict):
            continue
        typ = str(step.get("type") or "").upper()
        args = [str(x) for x in (step.get("args") or [])]
        if typ == "COPY":
            files_hash = str(step.get("filesHash") or "").strip()
            if not files_hash:
                raise HTTPException(status_code=400, detail="E2B COPY step is missing filesHash")
            context_keys.append(e2b_upload_key(template_id, files_hash))
            lines.append(_docker_copy_line(args))
            continue
        if typ == "ENV":
            for i in range(0, len(args), 2):
                key = args[i] if i < len(args) else ""
                value = args[i + 1] if i + 1 < len(args) else ""
                if key:
                    env[key] = value
                    lines.append(f"ENV {key}={_quote_docker_env(value)}")
            continue
        if typ == "WORKDIR" and args:
            lines.append(f"WORKDIR {args[0]}")
            continue
        if typ == "USER" and args:
            lines.append(f"USER {args[0]}")
            continue
        if typ == "RUN" and args:
            user = args[1].strip() if len(args) > 1 else ""
            if user:
                lines.append(f"USER {user}")
            lines.append(f"RUN {args[0]}")
            continue
        if typ:
            raise HTTPException(status_code=400, detail=f"Unsupported E2B template step: {typ}")
    return "\n".join(lines) + "\n", env, str(body.get("startCmd") or ""), str(body.get("readyCmd") or ""), context_keys

def _e2b_template_response(template_id: str, build_id: str, alias: str, names: list[str], tags: list[str]) -> dict[str, Any]:
    return {
        "aliases": [alias],
        "buildID": build_id,
        "names": names or [alias],
        "public": False,
        "tags": tags,
        "templateID": template_id,
    }

def _e2b_build_log_entries(row: dict, *, logs_offset: int = 0) -> tuple[list[dict[str, Any]], list[str]]:
    created = str(row.get("created_at") or _now_iso())
    lines = [x for x in str(row.get("build_log") or "").splitlines() if x.strip()]
    if not lines:
        status = str(row.get("status") or "")
        lines = [f"Template build {status or 'started'}"]
    sliced = lines[max(0, int(logs_offset or 0)) :]
    entries = [{"timestamp": created, "level": "info", "message": line} for line in sliced]
    if row.get("error_text"):
        entries.append({"timestamp": created, "level": "error", "message": str(row.get("error_text"))})
    return entries, lines

def _e2b_build_status_payload(row: dict, *, logs_offset: int = 0) -> dict[str, Any]:
    status = str(row.get("status") or "").lower()
    e2b_status = "ready" if status == "success" else "error" if status == "failed" else "building"
    entries, logs = _e2b_build_log_entries(row, logs_offset=logs_offset)
    out: dict[str, Any] = {
        "buildID": str(row.get("build_id") or ""),
        "templateID": str(row.get("template_id") or ""),
        "status": e2b_status,
        "logEntries": entries,
        "logs": logs,
    }
    if e2b_status == "error":
        out["reason"] = {"message": str(row.get("error_text") or "Build failed"), "logEntries": entries}
    return out

def _uuid_for_e2b(value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, value or "sndbx"))

def _split_e2b_template_ref(name: str) -> tuple[str, Optional[str]]:
    raw = (name or "").strip()
    base, sep, tag = raw.partition(":")
    return base.strip(), tag.strip() if sep and tag.strip() else None

def _tagged_alias(base_alias: str, tag: str) -> str:
    return f"{base_alias}:{tag.strip()}"

async def _latest_build_for_template(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    template_id: str,
) -> Optional[dict[str, Any]]:
    builds = await run_io(
        sandbox_manager.db.list_template_builds_for_client,
        principal.client_id,
        limit=200,
    )
    for build in builds:
        if str(build.get("template_id") or "") == template_id:
            return build
    return None

async def _update_e2b_build_log(
    sandbox_manager: SandboxManager,
    build_id: str,
    log_parts: list[str],
    *,
    status: str = "running",
    effective_mode: str = "e2b_sdk",
) -> None:
    await run_io(
        sandbox_manager.db.update_template_build,
        build_id,
        status=status,
        effective_mode=effective_mode,
        build_log="\n".join(part for part in log_parts if part),
    )

async def _run_e2b_template_build_background(
    *,
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    template_id: str,
    build_id: str,
    alias: str,
    dockerfile: str,
    env: dict[str, str],
    start_cmd: str,
    ready_cmd: str,
    context_tar_gzip_base64: Optional[str],
) -> None:
    cfg = get_config()
    mode = (cfg.TEMPLATE_DOCKERFILE_BUILD_MODE or "parsed").strip().lower()
    log_parts: list[str] = []

    async def append(line: str) -> None:
        text = str(line or "").rstrip("\n")
        if not text:
            return
        log_parts.append(text)
        await _update_e2b_build_log(
            sandbox_manager,
            build_id,
            log_parts,
            status="running",
            effective_mode="runtime_gateway" if gateway_template_build_enabled(cfg) else mode,
        )

    try:
        await append("Starting Docker build")
        if not gateway_template_build_enabled(cfg):
            await append("Streaming build logs unavailable without TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY=true")
            req = RegisterTemplateFromDockerfileRequest(
                template_id=alias,
                dockerfile=dockerfile,
                env=env,
                start_cmd=start_cmd,
                ready_cmd=ready_cmd,
                context_tar_gzip_base64=context_tar_gzip_base64,
                settle_seconds=20,
            )
            row_response = await template_handlers.register_template_from_dockerfile(req, principal, sandbox_manager)
            row = await run_io(sandbox_manager.db.get_sandbox_template, template_id)
            await template_handlers._finish_build_record(
                sandbox_manager,
                build_id,
                status="success",
                effective_mode="e2b_sdk",
                image_tag=str((row or {}).get("warm_snapshot_image") or (row or {}).get("base_image") or ""),
                registry_image_ref=str((row or {}).get("registry_image_ref") or "") or None,
                gateway_instance_id=str((row or {}).get("materialized_gateway_instance_id") or "") or None,
                build_log="\n".join(log_parts + [f"Template registered: {getattr(row_response, 'template_id', template_id)}"]),
            )
            return

        existing_template = await run_io(sandbox_manager.db.get_sandbox_template, template_id)
        gateway_target = sandbox_manager._gateway_target_for_template_row(existing_template)
        saw_result = False
        async for event in stream_dockerfile_template_via_gateway(
            cfg,
            template_id=template_id,
            dockerfile=dockerfile,
            image_tag=None,
            build_args=None,
            context_tar_gzip_base64=context_tar_gzip_base64,
            build_mode=mode,
            embed_envd=bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
            gateway_api_base=(gateway_target.api_base if gateway_target else None),
        ):
            typ = str(event.get("type") or "")
            if typ == "log":
                await append(str(event.get("line") or ""))
                continue
            if typ == "error":
                detail = str(event.get("detail") or "build failed")
                log_parts.append(detail)
                await template_handlers._finish_build_record(
                    sandbox_manager,
                    build_id,
                    status="failed",
                    effective_mode=str(event.get("effective_mode") or "runtime_gateway"),
                    build_log="\n".join(log_parts),
                    error_text=detail,
                )
                return
            if typ != "result":
                continue

            saw_result = True
            tag = str(event.get("image_tag") or "").strip()
            registry_ref = str(event.get("registry_image_ref") or "").strip()
            gateway_instance_id = str(event.get("gateway_instance_id") or "").strip()
            build_log = str(event.get("build_log") or "").strip()
            if build_log and len(log_parts) <= 1:
                log_parts.append(build_log)
            if not tag:
                raise RuntimeError("runtime-gateway build produced no image tag")

            reg_env, reg_start = template_handlers._fields_from_dockerfile_request(dockerfile, env, start_cmd)
            if bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)):
                reg_env[ENVD_TEMPLATE_BAKED_ENV] = "1"
            row = await run_io(
                sandbox_manager.db.upsert_sandbox_template,
                template_id,
                tag,
                reg_env,
                reg_start,
                20,
                ready_cmd,
                principal.client_id,
                principal.key_id,
                alias,
            )
            await run_io(
                sandbox_manager.db.set_template_build_source,
                template_id,
                source_kind="dockerfile",
                source_build_mode=mode,
                dockerfile_text=dockerfile,
                build_args={},
                context_tar_gzip_base64=context_tar_gzip_base64,
            )
            await run_io(
                sandbox_manager.db.set_template_warm_snapshot,
                template_id,
                tag,
                None,
                registry_image_ref=registry_ref or None,
                materialized_gateway_instance_id=gateway_instance_id or None,
            )
            await run_io(sandbox_manager.sync_warm_pool_default_segment, template_id, tag)
            await template_handlers._finish_build_record(
                sandbox_manager,
                build_id,
                status="success",
                effective_mode=str(event.get("effective_mode") or "runtime_gateway"),
                image_tag=tag,
                registry_image_ref=registry_ref or None,
                gateway_instance_id=gateway_instance_id or None,
                build_log="\n".join(log_parts),
            )
            logger.info("E2B template build completed template=%s build=%s", row.get("template_id"), build_id)
            return

        if not saw_result:
            raise RuntimeError("runtime-gateway build stream ended without a result")
    except Exception as ex:  # noqa: BLE001
        logger.warning("E2B template build failed template=%s build=%s: %s", template_id, build_id, ex)
        await template_handlers._finish_build_record(
            sandbox_manager,
            build_id,
            status="failed",
            effective_mode="e2b_sdk",
            build_log="\n".join(log_parts),
            error_text=str(ex),
        )
