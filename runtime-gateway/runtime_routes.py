from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse

from config import get_config
from internal_auth import internal_api_key_valid, unauthorized_response
from runtime_container_manager import ContainerConfig, ContainerManager
from template_builder import LocalDockerExecution

logger = logging.getLogger(__name__)
_manager: ContainerManager | None = None


def _runtime_manager() -> ContainerManager:
    global _manager
    if _manager is None:
        cfg = get_config()
        oci_runtime = (getattr(cfg, "SANDBOX_DOCKER_OCI_RUNTIME", "") or "").strip()
        _manager = ContainerManager(oci_runtime=oci_runtime)
    return _manager


def _auth(request: Request):
    if not internal_api_key_valid(request):
        return unauthorized_response()
    return None


async def docker_check(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    ok = await run_in_threadpool(_runtime_manager().check_docker)
    return JSONResponse({"ok": bool(ok)})


async def image_pull(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    image = str(payload.get("image") or "").strip()
    if not image:
        return JSONResponse({"ok": False, "detail": "image is required"}, status_code=400)
    started = time.monotonic()

    def _pull() -> None:
        plane = LocalDockerExecution(timeout=600)
        try:
            plane.ensure_image(image)
        finally:
            plane.close()

    try:
        await run_in_threadpool(_pull)
    except Exception as exc:  # noqa: BLE001
        elapsed = round(time.monotonic() - started, 3)
        logger.warning("Runtime image pull failed image=%s elapsed_seconds=%.3f: %s", image, elapsed, exc)
        return JSONResponse(
            {
                "ok": False,
                "image": image,
                "detail": f"{type(exc).__name__}: {exc}",
                "elapsed_seconds": elapsed,
            },
            status_code=502,
        )
    elapsed = round(time.monotonic() - started, 3)
    logger.info("Runtime image pull: image=%s elapsed_seconds=%.3f", image, elapsed)
    return JSONResponse({"ok": True, "image": image, "elapsed_seconds": elapsed})


async def image_exists(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    image = str(payload.get("image") or "").strip()
    exists = await run_in_threadpool(_runtime_manager().image_exists, image)
    return JSONResponse({"ok": True, "exists": bool(exists)})


async def image_metadata(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    image = str(payload.get("image") or "").strip()
    mgr = _runtime_manager()
    start_cmd, env, user = await run_in_threadpool(
        lambda: (
            mgr.image_start_cmd_shell(image),
            mgr.image_env_dict(image),
            mgr.image_default_user(image),
        )
    )
    return JSONResponse(
        {
            "ok": True,
            "start_cmd_shell": start_cmd,
            "env": env,
            "default_user": user,
        }
    )


async def create_container(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    started = time.monotonic()
    payload = await _json_payload(request)
    name = str(payload.get("name") or "").strip()
    cfg_raw = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    if not name:
        return JSONResponse({"ok": False, "detail": "name is required"}, status_code=400)
    config = ContainerConfig(**_container_config_kwargs(cfg_raw))
    pull = await image_pull_from_value(config.image)
    if pull is not None:
        return pull
    cid = await run_in_threadpool(_runtime_manager().create_container, name, config)
    if not cid:
        logger.warning(
            "Runtime create failed name=%s image=%s elapsed_seconds=%.3f",
            name,
            config.image,
            max(0.0, time.monotonic() - started),
        )
        return JSONResponse({"ok": False, "detail": "container create failed"}, status_code=502)
    logger.info(
        "Runtime create: name=%s container=%s image=%s elapsed_seconds=%.3f",
        name,
        cid[:12],
        config.image,
        max(0.0, time.monotonic() - started),
    )
    return JSONResponse({"ok": True, "container_id": cid})


async def container_exec(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    container_id = request.path_params["container_id"]
    payload = await _json_payload(request)
    result = await run_in_threadpool(
        _runtime_manager().run_command,
        container_id,
        str(payload.get("command") or ""),
        payload.get("cwd"),
        payload.get("env") if isinstance(payload.get("env"), dict) else None,
        payload.get("timeout"),
        payload.get("user"),
    )
    return JSONResponse(result or {"exit_code": -1, "stdout": "", "stderr": "exec failed", "pid": -1})


async def container_exec_stream(request: Request) -> StreamingResponse:
    denied = _auth(request)
    if denied:
        return denied
    container_id = request.path_params["container_id"]
    payload = await _json_payload(request)

    def _events():
        for ev in _runtime_manager().run_command_stream(
            container_id,
            str(payload.get("command") or ""),
            cwd=payload.get("cwd"),
            env=payload.get("env") if isinstance(payload.get("env"), dict) else None,
            timeout=payload.get("timeout"),
            user=payload.get("user"),
        ):
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"

    return StreamingResponse(_events(), media_type="text/event-stream")


async def file_read(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    content = await run_in_threadpool(_runtime_manager().read_file, request.path_params["container_id"], str(payload.get("path") or ""))
    return JSONResponse({"ok": content is not None, "content": content})


async def file_write(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    ok = await run_in_threadpool(
        _runtime_manager().write_file,
        request.path_params["container_id"],
        str(payload.get("path") or ""),
        str(payload.get("content") or ""),
    )
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def file_list(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    entries = await run_in_threadpool(_runtime_manager().list_files, request.path_params["container_id"], str(payload.get("path") or "/"))
    return JSONResponse({"ok": entries is not None, "entries": entries or []})


async def file_delete(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    ok = await run_in_threadpool(
        _runtime_manager().delete_file,
        request.path_params["container_id"],
        str(payload.get("path") or ""),
        bool(payload.get("recursive")),
    )
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def file_mkdir(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    ok = await run_in_threadpool(
        _runtime_manager().create_directory,
        request.path_params["container_id"],
        str(payload.get("path") or ""),
        int(payload.get("mode") or 0o755),
    )
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def file_archive_upload(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    container_id = request.path_params["container_id"]
    path = str(request.query_params.get("path") or "/").strip() or "/"
    data = await request.body()
    ok = await run_in_threadpool(_runtime_manager().put_archive_to_container, container_id, path, data)
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def container_stats(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    stats = await run_in_threadpool(_runtime_manager().get_container_stats, request.path_params["container_id"])
    return JSONResponse({"ok": stats is not None, "stats": stats})


async def container_state(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    state = await run_in_threadpool(_runtime_manager().get_container_state, request.path_params["container_id"])
    return JSONResponse({"ok": True, "state": state})


async def container_network(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    ip = await run_in_threadpool(_runtime_manager().get_container_internal_ipv4, request.path_params["container_id"])
    return JSONResponse({"ok": bool(ip), "internal_ipv4": ip or ""})


async def container_ports(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    port = int(payload.get("container_port") or 0)
    host_port = await run_in_threadpool(_runtime_manager().get_container_tcp_host_port, request.path_params["container_id"], port)
    return JSONResponse({"ok": host_port is not None, "host_port": host_port})


async def container_commit(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    image_ref = await run_in_threadpool(
        _runtime_manager().commit_filesystem_snapshot,
        request.path_params["container_id"],
        str(payload.get("repository") or ""),
        str(payload.get("tag") or ""),
        pause_during_commit=bool(payload.get("pause_during_commit", True)),
    )
    return JSONResponse({"ok": bool(image_ref), "image_ref": image_ref or ""}, status_code=200 if image_ref else 502)


async def container_kill(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    ok = await run_in_threadpool(_runtime_manager().kill_container, request.path_params["container_id"], bool(payload.get("force", True)))
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def container_pause(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    ok = await run_in_threadpool(_runtime_manager().pause_instance, request.path_params["container_id"])
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def container_resume(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    ok = await run_in_threadpool(_runtime_manager().resume_instance, request.path_params["container_id"])
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 502)


async def prune_containers(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    removed = await run_in_threadpool(_runtime_manager().prune_exited_containers, int(payload.get("older_than_seconds") or 1800))
    return JSONResponse({"ok": True, "removed": int(removed or 0)})


async def prune_images(request: Request) -> JSONResponse:
    denied = _auth(request)
    if denied:
        return denied
    payload = await _json_payload(request)
    removed = await run_in_threadpool(
        _runtime_manager().prune_generated_images,
        keep_refs=set(str(x) for x in payload.get("keep_refs") or []),
        older_than_seconds=int(payload.get("older_than_seconds") or 172800),
        repo_prefixes=[str(x) for x in payload.get("repo_prefixes") or []],
    )
    return JSONResponse({"ok": True, "removed": int(removed or 0)})


async def _json_payload(request: Request) -> Dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


async def image_pull_from_value(image: str) -> JSONResponse | None:
    if not (image or "").strip():
        return JSONResponse({"ok": False, "detail": "config.image is required"}, status_code=400)
    started = time.monotonic()

    def _pull() -> None:
        plane = LocalDockerExecution(timeout=600)
        try:
            plane.ensure_image(image)
        finally:
            plane.close()

    try:
        await run_in_threadpool(_pull)
        logger.info(
            "Runtime image ensure: image=%s elapsed_seconds=%.3f",
            image,
            max(0.0, time.monotonic() - started),
        )
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Runtime image ensure failed image=%s elapsed_seconds=%.3f: %s",
            image,
            max(0.0, time.monotonic() - started),
            exc,
        )
        return JSONResponse(
            {"ok": False, "detail": f"image pull failed: {type(exc).__name__}: {exc}"},
            status_code=502,
        )


def _container_config_kwargs(raw: Dict[str, Any]) -> Dict[str, Any]:
    allowed = set(ContainerConfig.__dataclass_fields__.keys())
    return {k: v for k, v in raw.items() if k in allowed}
