"""Sandbox data-plane gateway (standalone proxy-service or runtime-gateway sidecar)."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from config import get_config
from middleware import SandboxDataPlaneMiddleware
from runtime_routes import (
    container_commit,
    container_exec,
    container_exec_stream,
    container_kill,
    container_network,
    container_pause,
    container_ports,
    container_resume,
    container_state,
    container_stats,
    create_container,
    docker_check,
    file_delete,
    file_archive_upload,
    file_list,
    file_mkdir,
    file_read,
    file_write,
    image_exists,
    image_metadata,
    image_pull,
    prune_containers,
    prune_images,
)
from template_routes import build_dockerfile, build_dockerfile_stream, build_template_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_registry_ready = False
_registry_error = ""
_registry_lock = threading.Lock()


def _registry_login_required() -> bool:
    cfg = get_config()
    return bool(getattr(cfg, "TEMPLATE_REGISTRY_PUSH_ENABLED", False)) and bool(
        getattr(cfg, "TEMPLATE_REGISTRY_AUTH_REQUIRED", False)
    )


def _set_registry_status(*, ready: bool, error: str = "") -> None:
    global _registry_ready, _registry_error
    with _registry_lock:
        _registry_ready = bool(ready)
        _registry_error = (error or "").strip()


def _get_registry_status() -> tuple[bool, str]:
    with _registry_lock:
        return _registry_ready, _registry_error


def _registry_login_loop() -> None:
    cfg = get_config()
    if not bool(getattr(cfg, "TEMPLATE_REGISTRY_PUSH_ENABLED", False)):
        _set_registry_status(ready=True)
        return
    if not _registry_login_required() and not (
        getattr(cfg, "TEMPLATE_REGISTRY_USERNAME", "") and getattr(cfg, "TEMPLATE_REGISTRY_PASSWORD", "")
    ):
        _set_registry_status(ready=True)
        return
    from template_builder import ensure_registry_login

    while True:
        try:
            ensure_registry_login(timeout=60)
            _set_registry_status(ready=True)
            logger.info("template registry login ready")
            return
        except Exception as exc:  # noqa: BLE001
            _set_registry_status(ready=False, error=f"{type(exc).__name__}: {exc}")
            logger.warning("template registry login failed; retrying: %s", exc)
            time.sleep(5.0)


async def startup() -> None:
    threading.Thread(target=_registry_login_loop, name="template-registry-login", daemon=True).start()


@asynccontextmanager
async def lifespan(_app: Starlette):
    await startup()
    yield


async def health(_request: Request) -> JSONResponse:
    cfg = get_config()
    registry_ready, registry_error = _get_registry_status()
    return JSONResponse(
        {
            "status": "ok" if registry_ready or not _registry_login_required() else "degraded",
            "role": "runtime-gateway",
            "sandbox_domain": cfg.SANDBOX_DOMAIN,
            "upstream_resolve_mode": cfg.UPSTREAM_RESOLVE_MODE,
            "control_plane_url": cfg.CONTROL_PLANE_URL,
            "template_registry_ready": registry_ready,
            "template_registry_error": registry_error,
        }
    )


async def ready(_request: Request) -> JSONResponse:
    cfg = get_config()
    registry_ready, registry_error = _get_registry_status()
    if _registry_login_required() and not registry_ready:
        return JSONResponse(
            {
                "status": "not_ready",
                "role": "runtime-gateway",
                "reason": "template registry login not ready",
                "error": registry_error,
            },
            status_code=503,
        )
    return JSONResponse(
        {
            "status": "ready",
            "role": "runtime-gateway",
            "sandbox_domain": cfg.SANDBOX_DOMAIN,
        }
    )


async def root(_request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "message": "Sandbox data-plane gateway",
            "health": "/health",
            "ready": "/ready",
            "routing": "{port}-{sandbox_id}." + get_config().SANDBOX_DOMAIN,
        }
    )


async def runtime_status(_request: Request) -> JSONResponse:
    from internal_auth import internal_api_key_valid, unauthorized_response

    if not internal_api_key_valid(_request):
        return unauthorized_response()
    cfg = get_config()
    graph = cfg.DOCKER_GRAPH_PATH or "/var/lib/docker"
    capacity = int(getattr(cfg, "DOCKER_GRAPH_CAPACITY_BYTES", 0) or 0)
    source = "statvfs"
    if capacity > 0:
        total = capacity
        used = _docker_graph_used_bytes(graph)
        free = max(0, total - used)
        source = "configured_capacity"
    else:
        st = os.statvfs(graph)
        total = int(st.f_blocks * st.f_frsize)
        free = int(st.f_bavail * st.f_frsize)
        used = max(0, total - free)
    return JSONResponse(
        {
            "gateway_instance_id": cfg.GATEWAY_INSTANCE_ID,
            "docker_graph_path": graph,
            "disk_metric_source": source,
            "disk_total_bytes": total,
            "disk_used_bytes": used,
            "disk_free_bytes": free,
            "disk_used_ratio": (float(used) / float(total)) if total > 0 else 0.0,
        }
    )


async def runtime_probe(request: Request) -> JSONResponse:
    from internal_auth import internal_api_key_valid, unauthorized_response

    if not internal_api_key_valid(request):
        return unauthorized_response()
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    raw_targets = payload.get("targets") if isinstance(payload, dict) else []
    if not isinstance(raw_targets, list):
        raw_targets = []
    timeout_seconds = max(0.1, min(30.0, float((payload or {}).get("timeout_seconds") or 5.0)))
    poll_seconds = max(0.02, min(1.0, float((payload or {}).get("poll_seconds") or 0.05)))
    started = time.monotonic()

    async def _probe_one(raw: dict) -> dict:
        label = str(raw.get("label") or "").strip()
        host = str(raw.get("host") or "").strip()
        mode = str(raw.get("mode") or "tcp").strip().lower()
        path = str(raw.get("path") or "/").strip() or "/"
        try:
            port = max(1, min(65535, int(raw.get("port") or 0)))
        except (TypeError, ValueError):
            port = 0
        result = {
            "label": label,
            "host": host,
            "port": port,
            "mode": mode,
            "ok": False,
            "attempts": 0,
            "elapsed_seconds": 0.0,
        }
        if not host or port <= 0:
            result["error"] = "host and port are required"
            return result

        deadline = started + timeout_seconds
        last_error = ""
        async with httpx.AsyncClient(timeout=httpx.Timeout(1.0, connect=0.5)) as client:
            while time.monotonic() <= deadline:
                result["attempts"] = int(result["attempts"]) + 1
                try:
                    if mode in ("http", "http_get", "health"):
                        url = f"http://{host}:{port}{path if path.startswith('/') else '/' + path}"
                        resp = await client.get(url)
                        if 200 <= resp.status_code < 500:
                            if label.lower() == "envd" and (path or "/").rstrip("/") == "/health":
                                try:
                                    body = resp.json()
                                except Exception:  # noqa: BLE001
                                    body = {}
                                phase = str(body.get("phase") or "").strip() if isinstance(body, dict) else ""
                                if phase != "connect-v1":
                                    last_error = f"envd health phase {phase or '<missing>'}"
                                    await asyncio.sleep(poll_seconds)
                                    continue
                                result["phase"] = phase
                            result["ok"] = True
                            result["status_code"] = resp.status_code
                            break
                        last_error = f"http {resp.status_code}"
                    else:
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(host, port),
                            timeout=0.5,
                        )
                        writer.close()
                        try:
                            await writer.wait_closed()
                        except Exception:
                            pass
                        del reader
                        result["ok"] = True
                        break
                except Exception as exc:  # noqa: BLE001
                    last_error = f"{type(exc).__name__}: {exc}"
                await asyncio.sleep(poll_seconds)
        result["elapsed_seconds"] = round(max(0.0, time.monotonic() - started), 3)
        if not result["ok"] and last_error:
            result["error"] = last_error[:500]
        return result

    targets = [t for t in raw_targets if isinstance(t, dict)]
    results = await asyncio.gather(*(_probe_one(t) for t in targets)) if targets else []
    ok = bool(results) and all(bool(r.get("ok")) for r in results)
    return JSONResponse(
        {
            "ok": ok,
            "elapsed_seconds": round(max(0.0, time.monotonic() - started), 3),
            "results": results,
        },
        status_code=200 if ok else 504,
    )


def _docker_graph_used_bytes(graph: str) -> int:
    """Return bytes consumed by this shard's Docker graph directory."""
    try:
        out = subprocess.check_output(
            ["du", "-sb", graph],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=15,
        )
        first = (out or "").split()[0]
        return max(0, int(first))
    except Exception:
        return 0


routes = [
    Route("/health", health),
    Route("/ready", ready),
    Route("/", root),
    Route("/internal/runtime/docker/check", docker_check),
    Route("/internal/runtime/status", runtime_status),
    Route("/internal/runtime/probe", runtime_probe, methods=["POST"]),
    Route("/internal/runtime/images/pull", image_pull, methods=["POST"]),
    Route("/internal/runtime/images/exists", image_exists, methods=["POST"]),
    Route("/internal/runtime/images/metadata", image_metadata, methods=["POST"]),
    Route("/internal/runtime/containers/create", create_container, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/exec", container_exec, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/exec/stream", container_exec_stream, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/files/read", file_read, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/files/write", file_write, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/files/list", file_list, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/files/delete", file_delete, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/files/mkdir", file_mkdir, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/files/archive", file_archive_upload, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/stats", container_stats),
    Route("/internal/runtime/containers/{container_id}/state", container_state),
    Route("/internal/runtime/containers/{container_id}/network", container_network),
    Route("/internal/runtime/containers/{container_id}/ports", container_ports, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/commit", container_commit, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/kill", container_kill, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/pause", container_pause, methods=["POST"]),
    Route("/internal/runtime/containers/{container_id}/resume", container_resume, methods=["POST"]),
    Route("/internal/runtime/prune/containers", prune_containers, methods=["POST"]),
    Route("/internal/runtime/prune/images", prune_images, methods=["POST"]),
    Route("/internal/templates/build/dockerfile", build_dockerfile, methods=["POST"]),
    Route("/internal/templates/build/dockerfile/stream", build_dockerfile_stream, methods=["POST"]),
    Route("/internal/templates/build/snapshot", build_template_snapshot, methods=["POST"]),
]

app = Starlette(routes=routes, lifespan=lifespan)
app = SandboxDataPlaneMiddleware(app)


if __name__ == "__main__":
    import uvicorn

    cfg = get_config()
    uvicorn.run(
        "main:app",
        host=cfg.HOST,
        port=cfg.PORT,
        log_level=cfg.LOG_LEVEL.lower(),
        access_log=False
    )
