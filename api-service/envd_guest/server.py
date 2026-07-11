"""HTTP envd-style guest daemon (Phase 1: filesystem + process stream, no gRPC yet).

Run: ``uvicorn envd_guest.server:app --host 0.0.0.0 --port 49983``
Auth: header ``X-Access-Token`` must match env ``ENVD_ACCESS_TOKEN``.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import os
import stat
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route


def _expect_token() -> str:
    return (os.environ.get("ENVD_ACCESS_TOKEN") or "").strip()


def _safe_abs_path(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        raise ValueError("path required")
    # ``root + os.sep`` is wrong when root is ``/`` (becomes ``//``); use path relative_to.
    root = Path("/").resolve()
    candidate = (root / str(raw).lstrip("/")).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ValueError("path escapes filesystem root") from None
    return str(candidate)


def _entry_info(p: Path) -> Dict[str, Any]:
    st = p.stat()
    mode = st.st_mode
    ftype = "FILE_TYPE_DIRECTORY" if stat.S_ISDIR(mode) else "FILE_TYPE_FILE"
    if stat.S_ISLNK(mode):
        ftype = "FILE_TYPE_FILE"
    return {
        "name": p.name,
        "type": ftype,
        "path": str(p),
        "size": int(st.st_size),
        "mode": int(mode & 0o777),
        "permissions": oct(mode & 0o777),
    }


def _is_e2b_request(request: Request) -> bool:
    return "e2b" in (request.headers.get("user-agent") or "").lower()


def _write_info(p: Path) -> Dict[str, Any]:
    return {
        "name": p.name,
        "type": "dir" if p.is_dir() else "file",
        "path": str(p),
    }


class _AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[no-untyped-def]
        if request.url.path == "/health":
            return await call_next(request)
        tok = _expect_token()
        if not tok:
            return JSONResponse({"detail": "ENVD_ACCESS_TOKEN not set in guest"}, status_code=503)
        if (request.headers.get("x-access-token") or "").strip() != tok:
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        return await call_next(request)


async def health(_: Request) -> JSONResponse:
    return JSONResponse({"ok": True, "service": "envd-guest", "phase": "http-v1"})


async def fs_stat(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        path = _safe_abs_path(str(body.get("path") or ""))
        p = Path(path)
        if not p.exists():
            return JSONResponse({"detail": "not found"}, status_code=404)
        return JSONResponse({"entry": _entry_info(p)})
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def fs_list_dir(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        path = _safe_abs_path(str(body.get("path") or "/"))
        root = Path(path)
        if not root.exists():
            return JSONResponse({"detail": "not found"}, status_code=404)
        if not root.is_dir():
            return JSONResponse({"detail": "not a directory"}, status_code=400)
        entries: List[Dict[str, Any]] = []
        for ch in sorted(root.iterdir(), key=lambda x: x.name):
            if len(entries) >= 5000:
                break
            entries.append(_entry_info(ch))
        return JSONResponse({"entries": entries})
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def fs_mkdir(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        path = _safe_abs_path(str(body.get("path") or ""))
        Path(path).mkdir(parents=True, exist_ok=False)
        return JSONResponse({"entry": _entry_info(Path(path))})
    except FileExistsError:
        return JSONResponse({"detail": "exists"}, status_code=409)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def fs_remove(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        path = _safe_abs_path(str(body.get("path") or ""))
        p = Path(path)
        if p.is_dir() and not p.is_symlink():
            p.rmdir()
        else:
            p.unlink(missing_ok=False)
        return JSONResponse({})
    except FileNotFoundError:
        return JSONResponse({"detail": "not found"}, status_code=404)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def fs_move(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        src = _safe_abs_path(str(body.get("source") or ""))
        dst = _safe_abs_path(str(body.get("destination") or ""))
        Path(src).rename(dst)
        return JSONResponse({"entry": _entry_info(Path(dst))})
    except FileNotFoundError:
        return JSONResponse({"detail": "not found"}, status_code=404)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def files_get(request: Request) -> Response:
    try:
        raw = request.query_params.get("path") or "/"
        path = _safe_abs_path(raw)
        p = Path(path)
        if not p.is_file():
            return JSONResponse({"detail": "not a file"}, status_code=400)
        data = p.read_bytes()
        return Response(content=data, media_type="application/octet-stream")
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def files_post(request: Request) -> JSONResponse:
    try:
        ct = (request.headers.get("content-type") or "").lower()
        wrote: List[Dict[str, Any]] = []
        if "multipart/form-data" in ct:
            form = await request.form()
            uploads = form.getlist("file") if hasattr(form, "getlist") else [form.get("file")]
            uploads = [up for up in uploads if up is not None]
            if not uploads:
                return JSONResponse({"detail": "file required"}, status_code=400)
            param_dest = request.query_params.get("path") or str(form.get("path") or "")
            for up in uploads:
                dest = param_dest if len(uploads) == 1 and param_dest else str(getattr(up, "filename", "") or param_dest)
                raw = await up.read() if hasattr(up, "read") else b""
                path = _safe_abs_path(dest)
                parent = Path(path).parent
                parent.mkdir(parents=True, exist_ok=True)
                p = Path(path)
                p.write_bytes(raw)
                wrote.append({"path": path, "bytes_written": len(raw), "entry": _write_info(p)})
        else:
            dest = request.query_params.get("path") or "/"
            raw = await request.body()
            if (request.headers.get("content-encoding") or "").lower() == "gzip":
                raw = gzip.decompress(raw)
            path = _safe_abs_path(dest)
            parent = Path(path).parent
            parent.mkdir(parents=True, exist_ok=True)
            p = Path(path)
            p.write_bytes(raw)
            wrote.append({"path": path, "bytes_written": len(raw), "entry": _write_info(p)})
        if _is_e2b_request(request) or "multipart/form-data" in ct:
            return JSONResponse([item["entry"] for item in wrote])
        if len(wrote) == 1:
            return JSONResponse({"path": wrote[0]["path"], "bytes_written": wrote[0]["bytes_written"]})
        return JSONResponse({"files": [{"path": item["path"], "bytes_written": item["bytes_written"]} for item in wrote]})
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def process_start(request: Request) -> Response:
    try:
        body = await request.json()
        cwd = str(body.get("cwd") or "/")
        cwd = _safe_abs_path(cwd) if cwd else "/"
        env = body.get("env")
        if isinstance(env, dict):
            child_env = {**os.environ, **{str(k): str(v) for k, v in env.items()}}
        else:
            child_env = os.environ.copy()

        argv: Optional[List[str]] = None
        if isinstance(body.get("argv"), list) and body["argv"]:
            argv = [str(x) for x in body["argv"]]
        elif isinstance(body.get("command"), str) and body["command"].strip():
            argv = ["/bin/sh", "-c", body["command"].strip()]
        else:
            return JSONResponse({"detail": "need argv or command"}, status_code=400)

        async def combined() -> AsyncIterator[bytes]:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=cwd,
                env=child_env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            yield (json.dumps({"type": "start", "pid": proc.pid}) + "\n").encode()
            assert proc.stdout and proc.stderr
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                payload = base64.b64encode(chunk).decode("ascii")
                yield (json.dumps({"type": "stdout", "b64": payload}) + "\n").encode()
            while True:
                chunk = await proc.stderr.read(65536)
                if not chunk:
                    break
                payload = base64.b64encode(chunk).decode("ascii")
                yield (json.dumps({"type": "stderr", "b64": payload}) + "\n").encode()
            code = await proc.wait()
            yield (json.dumps({"type": "exit", "code": int(code)}) + "\n").encode()

        return StreamingResponse(combined(), media_type="application/x-ndjson")
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def connect_unimplemented(_: Request) -> JSONResponse:
    return JSONResponse(
        {
            "code": "unimplemented",
            "message": "Connect RPC is not implemented by this HTTP envd guest yet.",
        },
        status_code=501,
    )


_PROCESS_RPC_METHODS = (
    "List",
    "Connect",
    "Start",
    "Update",
    "StreamInput",
    "SendInput",
    "SendSignal",
    "CloseStdin",
)

_FILESYSTEM_RPC_METHODS = (
    "Stat",
    "MakeDir",
    "Move",
    "ListDir",
    "Remove",
    "WatchDir",
    "CreateWatcher",
    "GetWatcherEvents",
    "RemoveWatcher",
)


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/v1/fs/stat", fs_stat, methods=["POST"]),
    Route("/v1/fs/list_dir", fs_list_dir, methods=["POST"]),
    Route("/v1/fs/mkdir", fs_mkdir, methods=["POST"]),
    Route("/v1/fs/remove", fs_remove, methods=["POST"]),
    Route("/v1/fs/move", fs_move, methods=["POST"]),
    Route("/files", files_get, methods=["GET"]),
    Route("/files", files_post, methods=["POST"]),
    Route("/v1/process/start", process_start, methods=["POST"]),
    *[
        Route(f"/process.Process/{method}", connect_unimplemented, methods=["POST"])
        for method in _PROCESS_RPC_METHODS
    ],
    *[
        Route(f"/filesystem.Filesystem/{method}", connect_unimplemented, methods=["POST"])
        for method in _FILESYSTEM_RPC_METHODS
    ],
]

app = Starlette(routes=routes, middleware=[Middleware(_AuthMiddleware)])
