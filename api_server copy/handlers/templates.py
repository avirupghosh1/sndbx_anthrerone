"""Logical sandbox templates (Docker): base image + env + start_cmd + one-time warm snapshot."""

import base64
import re

from fastapi import APIRouter, Depends, HTTPException
from typing import List

from async_runner import run_io
from config import get_config
from models import RegisterTemplateFromDockerfileRequest, RegisterTemplateRequest
from models.responses import TemplateDefinitionResponse
from middleware import validate_api_key
from orchestrator import SandboxManager
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
                "template_id must be 1–63 chars, start with a letter, "
                "and use only [a-zA-Z0-9._-] (no `/` — use base_image for the Docker ref)."
            ),
        )
    return tid


@router.post("", response_model=TemplateDefinitionResponse)
async def register_template(
    request: RegisterTemplateRequest,
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Register or update a logical template (clears any previous warm snapshot)."""
    tid = _validate_template_id(request.template_id)
    row = await run_io(
        sandbox_manager.db.upsert_sandbox_template,
        tid,
        request.base_image.strip(),
        request.env or {},
        (request.start_cmd or "").strip(),
        int(request.settle_seconds),
        (request.ready_cmd or "").strip(),
    )
    return TemplateDefinitionResponse(**_row_to_response(row))


@router.post("/from-dockerfile", response_model=TemplateDefinitionResponse)
async def register_template_from_dockerfile(
    request: RegisterTemplateFromDockerfileRequest,
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Register a template from a Dockerfile.

    **Docker / gVisor sandboxes:** ``warm_snapshot_image`` is the built OCI image tag (same as before).

    **Firecracker sandboxes:** the API still builds with **Docker Engine** on the host, then exports
    the image to a host ``*.ext4`` and stores that path in ``warm_snapshot_image`` (see
    ``docs/FIRECRACKER.md``). Requires ``docker`` on ``PATH``, a working engine (``DOCKER_HOST``), and
    a privileged one-shot container for ``mkfs.ext4`` (default builder image ``alpine:3.19``).

    **Lima VM sandboxes** (``SANDBOX_ISOLATION=lima``): not supported from this endpoint.
    """
    tid = _validate_template_id(request.template_id)
    kind = sandbox_manager.execution.get_backend_kind()
    if kind == "lima":
        raise HTTPException(
            status_code=400,
            detail="POST /templates/from-dockerfile requires Docker Engine on the host (Lima VM isolation has no Docker build path).",
        )

    raw = (request.context_tar_gzip_base64 or "").strip()
    ctx: bytes | None = None
    if raw:
        try:
            ctx = base64.b64decode(raw, validate=True)
        except Exception as ex:
            raise HTTPException(status_code=400, detail=f"Invalid context_tar_gzip_base64: {ex}") from ex

    cfg = get_config()
    mode = (cfg.TEMPLATE_DOCKERFILE_BUILD_MODE or "parsed").strip().lower()
    if mode != "docker_cli" and not ctx:
        if re.search(r"^\s*COPY\s", request.dockerfile, re.I | re.M) or re.search(
            r"^\s*ADD\s+(?!https?://)", request.dockerfile, re.I | re.M
        ):
            raise HTTPException(
                status_code=400,
                detail="Parsed Dockerfile build: COPY/ADD (local) requires context_tar_gzip_base64.",
            )

    if mode == "docker_cli":
        def _do_build() -> tuple[str, str]:
            return build_image_from_dockerfile(
                dockerfile=request.dockerfile,
                image_tag=request.image_tag,
                template_id=tid,
                build_args=request.build_args,
                context_tar_gzip=ctx,
                build_timeout_sec=cfg.TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC,
                embed_envd=bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
            )

        try:
            tag, _build_log = await run_io(_do_build)
        except RuntimeError as ex:
            raise HTTPException(status_code=400, detail=str(ex)) from ex

        reg_env, reg_start = _fields_from_dockerfile_request(
            request.dockerfile,
            request.env,
            request.start_cmd or "",
        )
        
    elif mode == "kaniko":
        from orchestrator.kaniko_builder import build_with_kaniko
        df_content = request.dockerfile
        if request.start_cmd:
            df_content += f"\nRUN {request.start_cmd}\n"
        
        try:
            tag = await run_io(
                build_with_kaniko,
                dockerfile=df_content,
                template_id=tid,
                context_tar_gzip=ctx,
                image_tag=request.image_tag,
            )
        except RuntimeError as ex:
            raise HTTPException(status_code=400, detail=str(ex)) from ex
            
        reg_env, _ = _fields_from_dockerfile_request(
            request.dockerfile,
            request.env,
            request.start_cmd or "",
        )
        reg_start = ""  # Pre-baked into image via Kaniko

    if mode in ("docker_cli", "kaniko"):
        if kind == "firecracker":
            try:

                def _fc_cli() -> str:
                    return sandbox_manager.materialize_firecracker_rootfs_from_oci(tag, tid)

                ext4_path = await run_io(_fc_cli)
            except RuntimeError as ex:
                raise HTTPException(status_code=400, detail=str(ex)) from ex
            row = await run_io(
                sandbox_manager.db.upsert_sandbox_template,
                tid,
                tag,
                reg_env,
                reg_start,
                int(request.settle_seconds),
                (request.ready_cmd or "").strip(),
            )
            await run_io(sandbox_manager.db.set_template_warm_snapshot, tid, ext4_path, None)
            await run_io(sandbox_manager.sync_warm_pool_default_segment, tid, ext4_path)
            return TemplateDefinitionResponse(**_row_to_response(row))

        row = await run_io(
            sandbox_manager.db.upsert_sandbox_template,
            tid,
            tag,
            reg_env,
            reg_start,
            int(request.settle_seconds),
            (request.ready_cmd or "").strip(),
        )
        await run_io(sandbox_manager.db.set_template_warm_snapshot, tid, tag, None)
        await run_io(sandbox_manager.sync_warm_pool_default_segment, tid, tag)
        return TemplateDefinitionResponse(**_row_to_response(row))

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
            )
        )
    except RuntimeError as ex:
        raise HTTPException(status_code=400, detail=str(ex)) from ex
    return TemplateDefinitionResponse(**_row_to_response(row))


@router.get("", response_model=List[TemplateDefinitionResponse])
async def list_templates(
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    rows = await run_io(sandbox_manager.db.list_sandbox_templates)
    return [TemplateDefinitionResponse(**_row_to_response(r)) for r in rows]


@router.get("/{template_id}", response_model=TemplateDefinitionResponse)
async def get_template(
    template_id: str,
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    row = await run_io(sandbox_manager.db.get_sandbox_template, template_id.strip())
    if not row:
        raise HTTPException(status_code=404, detail=f"Unknown template_id: {template_id}")
    return TemplateDefinitionResponse(**_row_to_response(row))


def _row_to_response(row: dict) -> dict:
    return {
        "template_id": row["template_id"],
        "base_image": row["base_image"],
        "env": dict(row.get("env") or {}),
        "start_cmd": row.get("start_cmd") or "",
        "settle_seconds": int(row.get("settle_seconds") or 20),
        "ready_cmd": row.get("ready_cmd") or "",
        "warm_snapshot_image": row.get("warm_snapshot_image"),
        "build_error": row.get("build_error"),
        "created_at": row.get("created_at") or "",
        "updated_at": row.get("updated_at") or "",
    }
