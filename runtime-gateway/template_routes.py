from __future__ import annotations

import base64
import json
from typing import Any, Optional

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from config import get_config
from internal_auth import internal_api_key_valid, unauthorized_response
from template_builder import (
    build_image_from_dockerfile,
    build_registered_template_snapshot,
    push_image_to_registry,
    stream_build_image_from_dockerfile,
)


def _json_error(message: str, *, status: int = 400) -> JSONResponse:
    return JSONResponse({"detail": message}, status_code=status)


def _build_dockerfile_sync(payload: dict[str, Any], ctx: Optional[bytes], cfg: Any) -> dict[str, Any]:
    image_tag, build_log = build_image_from_dockerfile(
        dockerfile=str(payload.get("dockerfile") or ""),
        image_tag=payload.get("image_tag"),
        template_id=str(payload.get("template_id") or ""),
        build_args=payload.get("build_args") if isinstance(payload.get("build_args"), dict) else None,
        context_tar_gzip=ctx,
        build_timeout_sec=int(payload.get("build_timeout_sec") or 3600),
        embed_envd=bool(payload.get("embed_envd", True)),
        restore_user_mode=str(getattr(cfg, "ENVD_DOCKERFILE_RESTORE_USER", "auto") or "auto"),
    )
    registry_image_ref = None
    if bool(getattr(cfg, "TEMPLATE_REGISTRY_PUSH_ENABLED", False)):
        registry_image_ref = push_image_to_registry(
            local_ref=image_tag,
            template_id=str(payload.get("template_id") or ""),
            repo_prefix=str(getattr(cfg, "TEMPLATE_REGISTRY_REPO_PREFIX", "") or ""),
            timeout=max(600, int(payload.get("build_timeout_sec") or 3600)),
        )
    return {
        "image_tag": image_tag,
        "registry_image_ref": registry_image_ref,
        "gateway_instance_id": str(getattr(cfg, "GATEWAY_INSTANCE_ID", "") or ""),
        "build_log": build_log,
        "requested_mode": str(payload.get("build_mode") or "docker_cli"),
        "effective_mode": "docker_cli",
    }


def _build_template_snapshot_sync(payload: dict[str, Any], cfg: Any) -> dict[str, Any]:
    result = build_registered_template_snapshot(
        template_id=str(payload.get("template_id") or ""),
        base_image=str(payload.get("base_image") or ""),
        env=payload.get("env") if isinstance(payload.get("env"), dict) else None,
        start_cmd=str(payload.get("start_cmd") or ""),
        settle_seconds=int(payload.get("settle_seconds") or 20),
        ready_cmd=str(payload.get("ready_cmd") or ""),
        embed_envd=bool(payload.get("embed_envd", True)),
        envd_pip_timeout_sec=float(payload.get("envd_pip_timeout_sec") or 300.0),
        snapshot_repo=str(payload.get("snapshot_repo") or "mysandbox-snap"),
    )
    if bool(getattr(cfg, "TEMPLATE_REGISTRY_PUSH_ENABLED", False)):
        result["registry_image_ref"] = push_image_to_registry(
            local_ref=str(result.get("image_ref") or ""),
            template_id=str(payload.get("template_id") or ""),
            repo_prefix=str(getattr(cfg, "TEMPLATE_REGISTRY_REPO_PREFIX", "") or ""),
            timeout=max(600, int(payload.get("settle_seconds") or 20) + 900),
        )
    result["gateway_instance_id"] = str(getattr(cfg, "GATEWAY_INSTANCE_ID", "") or "")
    return result


async def build_dockerfile(request: Request) -> Response:
    if not internal_api_key_valid(request):
        return unauthorized_response()
    try:
        payload = await request.json()
    except Exception as ex:  # noqa: BLE001
        return _json_error(f"invalid json: {ex}")
    raw_ctx = str(payload.get("context_tar_gzip_base64") or "").strip()
    ctx: Optional[bytes] = None
    if raw_ctx:
        try:
            ctx = base64.b64decode(raw_ctx, validate=True)
        except Exception as ex:  # noqa: BLE001
            return _json_error(f"invalid context_tar_gzip_base64: {ex}")
    cfg = get_config()
    try:
        result = await run_in_threadpool(_build_dockerfile_sync, payload, ctx, cfg)
    except RuntimeError as ex:
        return _json_error(str(ex))
    return JSONResponse(result)


async def build_dockerfile_stream(request: Request) -> Response:
    if not internal_api_key_valid(request):
        return unauthorized_response()
    try:
        payload = await request.json()
    except Exception as ex:  # noqa: BLE001
        return _json_error(f"invalid json: {ex}")
    raw_ctx = str(payload.get("context_tar_gzip_base64") or "").strip()
    ctx: Optional[bytes] = None
    if raw_ctx:
        try:
            ctx = base64.b64decode(raw_ctx, validate=True)
        except Exception as ex:  # noqa: BLE001
            return _json_error(f"invalid context_tar_gzip_base64: {ex}")
    cfg = get_config()

    def event_stream():
        try:
            for event in stream_build_image_from_dockerfile(
                dockerfile=str(payload.get("dockerfile") or ""),
                image_tag=payload.get("image_tag"),
                template_id=str(payload.get("template_id") or ""),
                build_args=payload.get("build_args") if isinstance(payload.get("build_args"), dict) else None,
                context_tar_gzip=ctx,
                build_timeout_sec=int(payload.get("build_timeout_sec") or 3600),
                embed_envd=bool(payload.get("embed_envd", True)),
                restore_user_mode=str(getattr(cfg, "ENVD_DOCKERFILE_RESTORE_USER", "auto") or "auto"),
            ):
                if event.get("type") == "result":
                    registry_image_ref = None
                    if bool(getattr(cfg, "TEMPLATE_REGISTRY_PUSH_ENABLED", False)):
                        yield f"data: {json.dumps({'type': 'log', 'line': 'Pushing template image to registry\\n'}, ensure_ascii=True)}\n\n".encode("utf-8")
                        registry_image_ref = push_image_to_registry(
                            local_ref=str(event.get("image_tag") or ""),
                            template_id=str(payload.get("template_id") or ""),
                            repo_prefix=str(getattr(cfg, "TEMPLATE_REGISTRY_REPO_PREFIX", "") or ""),
                            timeout=max(600, int(payload.get("build_timeout_sec") or 3600)),
                        )
                        yield f"data: {json.dumps({'type': 'log', 'line': f'Registry image: {registry_image_ref}\\n'}, ensure_ascii=True)}\n\n".encode("utf-8")
                    event = {
                        **event,
                        "registry_image_ref": registry_image_ref,
                        "gateway_instance_id": str(getattr(cfg, "GATEWAY_INSTANCE_ID", "") or ""),
                        "requested_mode": str(payload.get("build_mode") or "docker_cli"),
                        "effective_mode": "docker_cli",
                    }
                yield f"data: {json.dumps(event, ensure_ascii=True)}\n\n".encode("utf-8")
        except Exception as ex:  # noqa: BLE001
            yield f"data: {json.dumps({'type': 'error', 'detail': str(ex)}, ensure_ascii=True)}\n\n".encode("utf-8")

    return StreamingResponse(event_stream(), media_type="text/event-stream")


async def build_template_snapshot(request: Request) -> Response:
    if not internal_api_key_valid(request):
        return unauthorized_response()
    try:
        payload = await request.json()
    except Exception as ex:  # noqa: BLE001
        return _json_error(f"invalid json: {ex}")
    cfg = get_config()
    try:
        result = await run_in_threadpool(_build_template_snapshot_sync, payload, cfg)
    except RuntimeError as ex:
        return _json_error(str(ex))
    return JSONResponse(result)
