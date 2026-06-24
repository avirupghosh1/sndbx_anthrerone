from __future__ import annotations

import base64
import json
from typing import Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from config import get_config
from internal_auth import internal_api_key_valid, unauthorized_response
from template_builder import (
    build_image_from_dockerfile,
    build_registered_template_snapshot,
    stream_build_image_from_dockerfile,
)


def _json_error(message: str, *, status: int = 400) -> JSONResponse:
    return JSONResponse({"detail": message}, status_code=status)


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
    except RuntimeError as ex:
        return _json_error(str(ex))
    return JSONResponse(
        {
            "image_tag": image_tag,
            "build_log": build_log,
            "requested_mode": str(payload.get("build_mode") or "docker_cli"),
            "effective_mode": "docker_cli",
        }
    )


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
                    event = {
                        **event,
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
    except RuntimeError as ex:
        return _json_error(str(ex))
    return JSONResponse(result)
