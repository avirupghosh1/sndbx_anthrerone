from __future__ import annotations

import json
from typing import Any, AsyncIterator, Dict, Optional

import httpx


def _gateway_headers(config: Any) -> dict[str, str]:
    key = (getattr(config, "RUNTIME_GATEWAY_API_KEY", None) or "").strip()
    if not key:
        raise RuntimeError("RUNTIME_GATEWAY_API_KEY is not configured")
    return {"X-API-Key": key}


def gateway_template_build_enabled(config: Any) -> bool:
    return bool(getattr(config, "TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY", True))


def build_dockerfile_template_via_gateway(
    config: Any,
    *,
    template_id: str,
    dockerfile: str,
    image_tag: Optional[str],
    build_args: Optional[Dict[str, str]],
    context_tar_gzip_base64: Optional[str],
    build_mode: str,
    embed_envd: bool,
) -> dict[str, Any]:
    base = (getattr(config, "RUNTIME_GATEWAY_URL", None) or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("RUNTIME_GATEWAY_URL is not configured")
    body = {
        "template_id": template_id,
        "dockerfile": dockerfile,
        "image_tag": image_tag,
        "build_args": build_args or None,
        "context_tar_gzip_base64": context_tar_gzip_base64 or None,
        "build_timeout_sec": int(getattr(config, "TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC", 3600) or 3600),
        "build_mode": build_mode,
        "embed_envd": bool(embed_envd),
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(float(body["build_timeout_sec"]) + 30.0)) as client:
            resp = client.post(
                f"{base}/internal/templates/build/dockerfile",
                json=body,
                headers=_gateway_headers(config),
            )
    except httpx.RequestError as ex:
        raise RuntimeError(f"runtime-gateway template build request failed: {ex}") from ex
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text
        raise RuntimeError(str(detail or f"runtime-gateway template build failed HTTP {resp.status_code}"))
    return resp.json()


def build_template_snapshot_via_gateway(
    config: Any,
    *,
    template_id: str,
    base_image: str,
    env: Optional[Dict[str, str]],
    start_cmd: str,
    settle_seconds: int,
    ready_cmd: str,
    embed_envd: bool,
    envd_pip_timeout_sec: float,
    snapshot_repo: str,
) -> dict[str, Any]:
    base = (getattr(config, "RUNTIME_GATEWAY_URL", None) or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("RUNTIME_GATEWAY_URL is not configured")
    body = {
        "template_id": template_id,
        "base_image": base_image,
        "env": env or None,
        "start_cmd": start_cmd,
        "settle_seconds": int(settle_seconds),
        "ready_cmd": ready_cmd,
        "embed_envd": bool(embed_envd),
        "envd_pip_timeout_sec": float(envd_pip_timeout_sec),
        "snapshot_repo": snapshot_repo,
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(max(600.0, float(settle_seconds) + 630.0))) as client:
            resp = client.post(
                f"{base}/internal/templates/build/snapshot",
                json=body,
                headers=_gateway_headers(config),
            )
    except httpx.RequestError as ex:
        raise RuntimeError(f"runtime-gateway template snapshot request failed: {ex}") from ex
    if resp.status_code >= 400:
        try:
            detail = resp.json().get("detail")
        except Exception:
            detail = resp.text
        raise RuntimeError(str(detail or f"runtime-gateway template snapshot failed HTTP {resp.status_code}"))
    return resp.json()


async def stream_dockerfile_template_via_gateway(
    config: Any,
    *,
    template_id: str,
    dockerfile: str,
    image_tag: Optional[str],
    build_args: Optional[Dict[str, str]],
    context_tar_gzip_base64: Optional[str],
    build_mode: str,
    embed_envd: bool,
) -> AsyncIterator[dict[str, Any]]:
    base = (getattr(config, "RUNTIME_GATEWAY_URL", None) or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("RUNTIME_GATEWAY_URL is not configured")
    body = {
        "template_id": template_id,
        "dockerfile": dockerfile,
        "image_tag": image_tag,
        "build_args": build_args or None,
        "context_tar_gzip_base64": context_tar_gzip_base64 or None,
        "build_timeout_sec": int(getattr(config, "TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC", 3600) or 3600),
        "build_mode": build_mode,
        "embed_envd": bool(embed_envd),
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(float(body["build_timeout_sec"]) + 30.0)) as client:
            async with client.stream(
                "POST",
                f"{base}/internal/templates/build/dockerfile/stream",
                json=body,
                headers=_gateway_headers(config),
            ) as resp:
                if resp.status_code >= 400:
                    text = await resp.aread()
                    raise RuntimeError(
                        f"runtime-gateway template build stream failed HTTP {resp.status_code}: "
                        f"{text.decode('utf-8', errors='replace')}"
                    )
                async for line in resp.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    raw = line[6:].strip()
                    if not raw:
                        continue
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError:
                        continue
    except httpx.RequestError as ex:
        raise RuntimeError(f"runtime-gateway template build stream request failed: {ex}") from ex
