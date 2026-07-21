"""Logical sandbox templates (Docker): base image + env + start_cmd + one-time warm snapshot."""

import base64
import json
import re
import time
import uuid

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, StreamingResponse
from typing import List

from async_runner import run_io
from config import get_config
from models import RegisterTemplateFromDockerfileRequest, RegisterTemplateRequest
from models.responses import TemplateDefinitionResponse
from middleware import ApiKeyPrincipal, ensure_template_access, public_template_id_for_row, validate_api_key
from orchestrator import SandboxManager
from orchestrator.runtime_gateway_templates import (
    gateway_template_build_enabled,
    stream_dockerfile_template_via_gateway,
)
from orchestrator.sandbox_constants import ENVD_TEMPLATE_BAKED_ENV
from orchestrator.template_docker_build import build_image_from_dockerfile

router = APIRouter(prefix="/templates", tags=["templates"])

_TEMPLATE_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9._-]{0,62}$")


def _fields_from_dockerfile_request(
    dockerfile: str,
    env: dict | None,
    start_cmd: str,
) -> tuple[dict, str]:
    """Merge API request fields with ``CMD``/``ENV`` parsed from the Dockerfile text."""
    from orchestrator.template_dockerfile_builder import (
        extract_env_from_dockerfile,
        extract_start_cmd_from_dockerfile,
    )

    merged_env = dict(extract_env_from_dockerfile(dockerfile))
    merged_env.update(dict(env or {}))
    sc = (start_cmd or "").strip() or extract_start_cmd_from_dockerfile(dockerfile)
    return merged_env, sc


def _validate_template_id(template_id: str) -> str:
    tid = template_id.strip()
    if not _TEMPLATE_ID_RE.match(tid):
        raise HTTPException(
            status_code=400,
            detail=(
                "template_id must be 1-63 chars, start with a letter, "
                "and use only [a-zA-Z0-9._-] (no `/`; use base_image for the Docker ref)."
            ),
        )
    return tid


def _storage_template_id(principal: ApiKeyPrincipal, template_alias: str) -> str:
    client_part = re.sub(r"[^a-zA-Z0-9]+", "-", principal.client_id).strip("-").lower() or "client"
    alias_part = re.sub(r"[^a-zA-Z0-9._-]+", "-", template_alias).strip("-") or "tpl"
    return f"tpl-{client_part[:18]}-{alias_part[:36]}"


def _resolve_template_row_for_principal(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    template_id: str,
) -> dict | None:
    requested = (template_id or "").strip()
    owned = sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, requested)
    if owned:
        return owned
    row = sandbox_manager.db.get_sandbox_template(requested)
    if row and not row.get("owner_client_id"):
        return row
    return None


def _owned_template_row_for_principal(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    template_id: str,
) -> dict | None:
    requested = (template_id or "").strip()
    if not requested:
        return None
    owned = sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, requested)
    if owned:
        return owned
    row = sandbox_manager.db.get_sandbox_template(requested)
    if row and str(row.get("owner_client_id") or "") == principal.client_id:
        return row
    return None


def _disable_template_warm_pool_segments(sandbox_manager: SandboxManager, template_id: str) -> int:
    tid = (template_id or "").strip()
    if not tid:
        return 0
    list_segments = getattr(sandbox_manager.db, "list_warm_pool_segments", None)
    disable_segment = getattr(sandbox_manager.db, "disable_warm_pool_segment", None)
    if not callable(list_segments) or not callable(disable_segment):
        return 0
    disabled = 0
    for segment in list_segments():
        key = str(segment.get("warm_pool_key") or "").strip()
        segment_tid = str(segment.get("template_id") or "").strip()
        if not key:
            continue
        if segment_tid != tid and not key.startswith(f"{tid}|"):
            continue
        pool = getattr(sandbox_manager, "warm_pool", None)
        if pool is not None:
            cfg = getattr(sandbox_manager, "_config", None)
            try:
                pool.ensure_pool_for(
                    segment_tid or tid,
                    str(segment.get("cpu_limit") or getattr(cfg, "DEFAULT_CPU_LIMIT", "1")),
                    str(segment.get("memory_limit") or getattr(cfg, "DEFAULT_MEMORY_LIMIT", "512m")),
                    int(segment.get("timeout") or getattr(cfg, "DEFAULT_TIMEOUT", 3600)),
                    None,
                    desired_size=0,
                )
            except Exception:
                pass
        if disable_segment(key, f"template {tid} deleted"):
            disabled += 1
        try:
            sandbox_manager.trim_warm_pool_to_size(key, 0)
        except Exception:
            pass
    return disabled


async def delete_template_for_principal(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    template_id: str,
) -> bool:
    requested = (template_id or "").strip()
    row = await run_io(_owned_template_row_for_principal, sandbox_manager, principal, template_id)
    if not row:
        client_part = re.sub(r"[^a-zA-Z0-9]+", "-", principal.client_id).strip("-").lower() or "client"
        owner_prefix = f"tpl-{client_part[:18]}-"
        candidate_ids = [_storage_template_id(principal, requested)]
        if requested.startswith(owner_prefix):
            candidate_ids.append(requested)
        disabled = 0
        for candidate in dict.fromkeys(candidate_ids):
            disabled += await run_io(_disable_template_warm_pool_segments, sandbox_manager, candidate)
        return disabled > 0
    if str(row.get("owner_client_id") or "") != principal.client_id:
        return False
    tid = str(row.get("template_id") or "").strip()
    if not tid:
        return False
    await run_io(_disable_template_warm_pool_segments, sandbox_manager, tid)
    return bool(await run_io(sandbox_manager.db.delete_sandbox_template, tid, principal.client_id))


async def _create_build_record(
    sandbox_manager: SandboxManager,
    *,
    template_id: str,
    template_alias: str,
    principal: ApiKeyPrincipal,
    requested_mode: str,
    effective_mode: str = "",
    status: str = "running",
) -> str:
    build_id = f"tb-{uuid.uuid4().hex[:16]}"
    await run_io(
        sandbox_manager.db.create_template_build,
        build_id=build_id,
        template_id=template_id,
        template_alias=template_alias,
        owner_client_id=principal.client_id,
        owner_api_key_id=principal.key_id,
        requested_mode=requested_mode,
        effective_mode=effective_mode,
        status=status,
        image_tag=None,
        build_log="",
        error_text=None,
    )
    return build_id


async def _finish_build_record(
    sandbox_manager: SandboxManager,
    build_id: str,
    *,
    status: str,
    effective_mode: str,
    image_tag: str | None = None,
    registry_image_ref: str | None = None,
    gateway_instance_id: str | None = None,
    build_log: str = "",
    error_text: str | None = None,
) -> None:
    await run_io(
        sandbox_manager.db.update_template_build,
        build_id,
        status=status,
        effective_mode=effective_mode,
        image_tag=image_tag,
        registry_image_ref=registry_image_ref,
        gateway_instance_id=gateway_instance_id,
        build_log=build_log,
        error_text=error_text,
    )


class _BuildLogRecorder:
    def __init__(
        self,
        sandbox_manager: SandboxManager,
        build_id: str,
        *,
        effective_mode: str,
        min_interval_sec: float = 0.75,
    ) -> None:
        self.sandbox_manager = sandbox_manager
        self.build_id = build_id
        self.effective_mode = effective_mode
        self.min_interval_sec = min_interval_sec
        self.parts: list[str] = []
        self._last_flush = 0.0

    def append_sync(self, chunk: object, *, force: bool = False) -> None:
        text = str(chunk or "").rstrip("\n")
        if text:
            self.parts.append(text)
        now = time.monotonic()
        if not force and now - self._last_flush < self.min_interval_sec:
            return
        self._last_flush = now
        self.sandbox_manager.db.update_template_build(
            self.build_id,
            status="running",
            effective_mode=self.effective_mode,
            build_log=self.text,
        )

    async def append(self, chunk: object, *, force: bool = False) -> None:
        text = str(chunk or "").rstrip("\n")
        if text:
            self.parts.append(text)
        now = time.monotonic()
        if not force and now - self._last_flush < self.min_interval_sec:
            return
        self._last_flush = now
        await run_io(
            self.sandbox_manager.db.update_template_build,
            self.build_id,
            status="running",
            effective_mode=self.effective_mode,
            build_log=self.text,
        )

    @property
    def text(self) -> str:
        return "\n".join(part for part in self.parts if part)


@router.post("", response_model=TemplateDefinitionResponse)
async def register_template(
    request: RegisterTemplateRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Register or update a logical template (clears any previous warm snapshot)."""
    alias = _validate_template_id(request.template_id)
    existing = await run_io(sandbox_manager.db.get_sandbox_template_by_alias, principal.client_id, alias)
    tid = str(existing["template_id"]) if existing else _storage_template_id(principal, alias)
    row = await run_io(
        sandbox_manager.db.upsert_sandbox_template,
        tid,
        request.base_image.strip(),
        request.env or {},
        (request.start_cmd or "").strip(),
        int(request.settle_seconds),
        (request.ready_cmd or "").strip(),
        principal.client_id,
        principal.key_id,
        alias,
    )
    warm = (request.warm_snapshot_image or "").strip()
    if warm:
        await run_io(sandbox_manager.db.set_template_warm_snapshot, tid, warm, None)
        await run_io(sandbox_manager.sync_warm_pool_default_segment, tid, warm)
        row = await run_io(sandbox_manager.db.get_sandbox_template, tid) or row
    return TemplateDefinitionResponse(**_row_to_response(row))


@router.post("/from-dockerfile", response_model=TemplateDefinitionResponse)
async def register_template_from_dockerfile(
    request: RegisterTemplateFromDockerfileRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Register a template from a Dockerfile.

    ``warm_snapshot_image`` is the local built OCI image tag. In production the build is
    delegated to runtime-gateway, and ``registry_image_ref`` stores the published copy if enabled.
    """
    alias = _validate_template_id(request.template_id)
    existing = await run_io(sandbox_manager.db.get_sandbox_template_by_alias, principal.client_id, alias)
    tid = str(existing["template_id"]) if existing else _storage_template_id(principal, alias)

    raw = (request.context_tar_gzip_base64 or "").strip()
    ctx: bytes | None = None
    if raw:
        try:
            ctx = base64.b64decode(raw, validate=True)
        except Exception as ex:
            raise HTTPException(status_code=400, detail=f"Invalid context_tar_gzip_base64: {ex}") from ex

    cfg = get_config()
    mode = (cfg.TEMPLATE_DOCKERFILE_BUILD_MODE or "parsed").strip().lower()
    if mode not in ("docker_cli", "parsed"):
        raise HTTPException(
            status_code=400,
            detail="Unsupported TEMPLATE_DOCKERFILE_BUILD_MODE. Use docker_cli or parsed.",
        )
    build_id = await _create_build_record(
        sandbox_manager,
        template_id=tid,
        template_alias=alias,
        principal=principal,
        requested_mode=mode,
    )
    if gateway_template_build_enabled(cfg):
        existing_template = await run_io(sandbox_manager.db.get_sandbox_template, tid)
        gateway_target = sandbox_manager._gateway_target_for_template_row(existing_template)
        recorder = _BuildLogRecorder(sandbox_manager, build_id, effective_mode="runtime_gateway")
        await recorder.append("Starting runtime-gateway Docker build", force=True)
        build_res: dict = {}
        try:
            async for event in stream_dockerfile_template_via_gateway(
                cfg,
                template_id=tid,
                dockerfile=request.dockerfile,
                image_tag=request.image_tag,
                build_args=request.build_args,
                context_tar_gzip_base64=request.context_tar_gzip_base64,
                build_mode=mode,
                embed_envd=bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
                gateway_api_base=(gateway_target.api_base if gateway_target else None),
            ):
                typ = str(event.get("type") or "")
                if typ == "log":
                    await recorder.append(str(event.get("line") or ""))
                elif typ == "error":
                    detail = str(event.get("detail") or "build failed")
                    await recorder.append(detail, force=True)
                    build_res = event
                    break
                elif typ == "result":
                    build_log = str(event.get("build_log") or "")
                    if build_log and len(recorder.parts) <= 1:
                        await recorder.append(build_log, force=True)
                    build_res = event
                    break
        except RuntimeError as ex:
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode="runtime_gateway",
                build_log=recorder.text,
                error_text=str(ex),
            )
            raise HTTPException(status_code=400, detail=str(ex)) from ex
        if str(build_res.get("type") or "") == "error":
            detail = str(build_res.get("detail") or "build failed")
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode=str(build_res.get("effective_mode") or "runtime_gateway"),
                build_log=recorder.text,
                error_text=detail,
            )
            raise HTTPException(status_code=400, detail=detail)
        if not build_res:
            detail = "runtime-gateway build stream ended without a result"
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode="runtime_gateway",
                build_log=recorder.text,
                error_text=detail,
            )
            raise HTTPException(status_code=400, detail=detail)

        tag = str(build_res.get("image_tag") or "").strip()
        registry_ref = str(build_res.get("registry_image_ref") or "").strip()
        gateway_instance_id = str(build_res.get("gateway_instance_id") or "").strip()
        if not tag:
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode="runtime_gateway",
                build_log=recorder.text or str(build_res.get("build_log") or ""),
                error_text="runtime-gateway build produced no image tag",
            )
            raise HTTPException(status_code=400, detail="runtime-gateway build produced no image tag")
        await recorder.append("Docker build finished; registering template", force=True)
        reg_env, reg_start = _fields_from_dockerfile_request(
            request.dockerfile,
            request.env,
            request.start_cmd or "",
        )
        if bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)):
            reg_env[ENVD_TEMPLATE_BAKED_ENV] = "1"
        row = await run_io(
            sandbox_manager.db.upsert_sandbox_template,
            tid,
            tag,
            reg_env,
            reg_start,
            int(request.settle_seconds),
            (request.ready_cmd or "").strip(),
            principal.client_id,
            principal.key_id,
            alias,
        )
        await run_io(
            sandbox_manager.db.set_template_build_source,
            tid,
            source_kind="dockerfile",
            source_build_mode=mode,
            dockerfile_text=request.dockerfile,
            build_args=request.build_args or {},
            context_tar_gzip_base64=request.context_tar_gzip_base64,
        )
        await run_io(
            sandbox_manager.db.set_template_warm_snapshot,
            tid,
            tag,
            None,
            registry_image_ref=registry_ref or None,
            materialized_gateway_instance_id=gateway_instance_id or None,
        )
        await run_io(sandbox_manager.sync_warm_pool_default_segment, tid, tag)
        row = await run_io(sandbox_manager.db.get_sandbox_template, tid) or row
        await _finish_build_record(
            sandbox_manager,
            build_id,
            status="success",
            effective_mode=str(build_res.get("effective_mode") or "runtime_gateway"),
            image_tag=tag,
            registry_image_ref=registry_ref or None,
            gateway_instance_id=gateway_instance_id or None,
            build_log=recorder.text or str(build_res.get("build_log") or ""),
        )
        return TemplateDefinitionResponse(**_row_to_response(row))

    if mode != "docker_cli" and not ctx:
        if re.search(r"^\s*COPY\s", request.dockerfile, re.I | re.M) or re.search(
            r"^\s*ADD\s+(?!https?://)", request.dockerfile, re.I | re.M
        ):
            detail = "Parsed Dockerfile build: COPY/ADD (local) requires context_tar_gzip_base64."
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode="parsed",
                build_log=detail,
                error_text=detail,
            )
            raise HTTPException(
                status_code=400,
                detail=detail,
            )

    if mode == "docker_cli":
        recorder = _BuildLogRecorder(sandbox_manager, build_id, effective_mode="docker_cli")

        def _do_build() -> tuple[str, str]:
            recorder.append_sync("Starting local Docker build", force=True)
            return build_image_from_dockerfile(
                dockerfile=request.dockerfile,
                image_tag=request.image_tag,
                template_id=tid,
                build_args=request.build_args,
                context_tar_gzip=ctx,
                build_timeout_sec=cfg.TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC,
                embed_envd=bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
                log_callback=lambda chunk: recorder.append_sync(chunk),
            )

        try:
            tag, build_log = await run_io(_do_build)
            await recorder.append("Docker build finished; registering template", force=True)
        except RuntimeError as ex:
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode="docker_cli",
                build_log=recorder.text,
                error_text=str(ex),
            )
            raise HTTPException(status_code=400, detail=str(ex)) from ex

        reg_env, reg_start = _fields_from_dockerfile_request(
            request.dockerfile,
            request.env,
            request.start_cmd or "",
        )
        if bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)):
            reg_env[ENVD_TEMPLATE_BAKED_ENV] = "1"

    if mode == "docker_cli":
        row = await run_io(
            sandbox_manager.db.upsert_sandbox_template,
            tid,
            tag,
            reg_env,
            reg_start,
            int(request.settle_seconds),
            (request.ready_cmd or "").strip(),
            principal.client_id,
            principal.key_id,
            alias,
        )
        await run_io(
            sandbox_manager.db.set_template_build_source,
            tid,
            source_kind="dockerfile",
            source_build_mode=mode,
            dockerfile_text=request.dockerfile,
            build_args=request.build_args or {},
            context_tar_gzip_base64=request.context_tar_gzip_base64,
        )
        await run_io(sandbox_manager.db.set_template_warm_snapshot, tid, tag, None)
        await run_io(sandbox_manager.sync_warm_pool_default_segment, tid, tag)
        await _finish_build_record(
            sandbox_manager,
            build_id,
            status="success",
            effective_mode=mode,
            image_tag=tag,
            build_log=recorder.text or build_log,
        )
        return TemplateDefinitionResponse(**_row_to_response(row))

    parsed_recorder = _BuildLogRecorder(sandbox_manager, build_id, effective_mode="parsed")
    await parsed_recorder.append("Starting parsed Dockerfile build", force=True)
    try:
        sm = sandbox_manager
        row = await run_io(
            lambda: sm.build_template_from_dockerfile_parsed(
                tid,
                request.dockerfile,
                request.env or {},
                (request.start_cmd or "").strip(),
                int(request.settle_seconds),
                (request.ready_cmd or "").strip(),
                request.build_args,
                ctx,
                request.image_tag,
                principal.client_id,
                principal.key_id,
                alias,
            )
        )
    except RuntimeError as ex:
        await _finish_build_record(
            sandbox_manager,
            build_id,
            status="failed",
            effective_mode="parsed",
            build_log=parsed_recorder.text,
            error_text=str(ex),
        )
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    await parsed_recorder.append("Parsed Dockerfile build finished; syncing warm pool", force=True)
    await _finish_build_record(
        sandbox_manager,
        build_id,
        status="success",
        effective_mode="parsed",
        image_tag=str(row.get("warm_snapshot_image") or row.get("base_image") or ""),
        build_log=parsed_recorder.text or "parsed Dockerfile build completed",
    )
    return TemplateDefinitionResponse(**_row_to_response(row))


@router.post("/from-dockerfile/stream")
async def register_template_from_dockerfile_stream(
    request: RegisterTemplateFromDockerfileRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    cfg = get_config()
    if not gateway_template_build_enabled(cfg):
        raise HTTPException(
            status_code=400,
            detail="Streaming template build requires TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY=true",
        )
    alias = _validate_template_id(request.template_id)
    existing = await run_io(sandbox_manager.db.get_sandbox_template_by_alias, principal.client_id, alias)
    tid = str(existing["template_id"]) if existing else _storage_template_id(principal, alias)
    mode = (cfg.TEMPLATE_DOCKERFILE_BUILD_MODE or "parsed").strip().lower()
    build_id = await _create_build_record(
        sandbox_manager,
        template_id=tid,
        template_alias=alias,
        principal=principal,
        requested_mode=mode,
    )

    async def _events():
        recorder = _BuildLogRecorder(sandbox_manager, build_id, effective_mode="runtime_gateway")
        await recorder.append("Starting runtime-gateway Docker build", force=True)
        existing_template = await run_io(sandbox_manager.db.get_sandbox_template, tid)
        gateway_target = sandbox_manager._gateway_target_for_template_row(existing_template)
        try:
            async for event in stream_dockerfile_template_via_gateway(
                cfg,
                template_id=tid,
                dockerfile=request.dockerfile,
                image_tag=request.image_tag,
                build_args=request.build_args,
                context_tar_gzip_base64=request.context_tar_gzip_base64,
                build_mode=mode,
                embed_envd=bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
                gateway_api_base=(gateway_target.api_base if gateway_target else None),
            ):
                if event.get("type") == "log":
                    await recorder.append(str(event.get("line") or ""))
                if event.get("type") == "error":
                    detail = str(event.get("detail") or "build failed")
                    await recorder.append(detail, force=True)
                    await _finish_build_record(
                        sandbox_manager,
                        build_id,
                        status="failed",
                        effective_mode=str(event.get("effective_mode") or "runtime_gateway"),
                        build_log=recorder.text,
                        error_text=detail,
                    )
                if event.get("type") == "result":
                    tag = str(event.get("image_tag") or "").strip()
                    registry_ref = str(event.get("registry_image_ref") or "").strip()
                    gateway_instance_id = str(event.get("gateway_instance_id") or "").strip()
                    build_log = str(event.get("build_log") or "")
                    if build_log and len(recorder.parts) <= 1:
                        await recorder.append(build_log, force=True)
                    await recorder.append("Docker build finished; registering template", force=True)
                    reg_env, reg_start = _fields_from_dockerfile_request(
                        request.dockerfile,
                        request.env,
                        request.start_cmd or "",
                    )
                    if bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)):
                        reg_env[ENVD_TEMPLATE_BAKED_ENV] = "1"
                    row = await run_io(
                        sandbox_manager.db.upsert_sandbox_template,
                        tid,
                        tag,
                        reg_env,
                        reg_start,
                        int(request.settle_seconds),
                        (request.ready_cmd or "").strip(),
                        principal.client_id,
                        principal.key_id,
                        alias,
                    )
                    await run_io(
                        sandbox_manager.db.set_template_build_source,
                        tid,
                        source_kind="dockerfile",
                        source_build_mode=mode,
                        dockerfile_text=request.dockerfile,
                        build_args=request.build_args or {},
                        context_tar_gzip_base64=request.context_tar_gzip_base64,
                    )
                    await run_io(
                        sandbox_manager.db.set_template_warm_snapshot,
                        tid,
                        tag,
                        None,
                        registry_image_ref=registry_ref or None,
                        materialized_gateway_instance_id=gateway_instance_id or None,
                    )
                    await run_io(sandbox_manager.sync_warm_pool_default_segment, tid, tag)
                    row = await run_io(sandbox_manager.db.get_sandbox_template, tid) or row
                    await _finish_build_record(
                        sandbox_manager,
                        build_id,
                        status="success",
                        effective_mode=str(event.get("effective_mode") or "runtime_gateway"),
                        image_tag=tag,
                        registry_image_ref=registry_ref or None,
                        gateway_instance_id=gateway_instance_id or None,
                        build_log=recorder.text,
                    )
                    event = {
                        "type": "registered",
                        "template": _row_to_response(row),
                        "build_id": build_id,
                    }
                yield f"data: {json.dumps(event, ensure_ascii=True)}\n\n"
        except Exception as ex:  # noqa: BLE001
            await _finish_build_record(
                sandbox_manager,
                build_id,
                status="failed",
                effective_mode="runtime_gateway",
                build_log=recorder.text,
                error_text=str(ex),
            )
            yield f"data: {json.dumps({'type': 'error', 'detail': str(ex)}, ensure_ascii=True)}\n\n"

    return StreamingResponse(_events(), media_type="text/event-stream")


@router.get("", response_model=List[TemplateDefinitionResponse])
async def list_templates(
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    rows = await run_io(sandbox_manager.db.list_sandbox_templates, principal.client_id)
    return [TemplateDefinitionResponse(**_row_to_response(r)) for r in rows]


@router.get("/{template_id}", response_model=TemplateDefinitionResponse)
async def get_template(
    template_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = await run_io(_resolve_template_row_for_principal, sandbox_manager, principal, template_id.strip())
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown template_id: {template_id}")
    ensure_template_access(principal, row, template_id)
    return TemplateDefinitionResponse(**_row_to_response(row))


@router.delete("/{template_id}")
async def delete_template(
    template_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    if not await delete_template_for_principal(sandbox_manager, principal, template_id):
        raise HTTPException(status_code=404, detail=f"Unknown template_id: {template_id}")
    return Response(status_code=204)


def _row_to_response(row: dict) -> dict:
    return {
        "template_id": public_template_id_for_row(row),
        "base_image": row["base_image"],
        "env": dict(row.get("env") or {}),
        "start_cmd": row.get("start_cmd") or "",
        "settle_seconds": int(row.get("settle_seconds") or 20),
        "ready_cmd": row.get("ready_cmd") or "",
        "warm_snapshot_image": row.get("warm_snapshot_image"),
        "registry_image_ref": row.get("registry_image_ref"),
        "build_error": row.get("build_error"),
        "created_at": row.get("created_at") or "",
        "updated_at": row.get("updated_at") or "",
    }
