"""HTTP envd-style guest daemon for E2B-compatible filesystem/process APIs.

Run: ``uvicorn envd_guest.server:app --host 0.0.0.0 --port 49983``
Auth: header ``X-Access-Token`` must match env ``ENVD_ACCESS_TOKEN``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import errno
import fcntl
import gzip
import json
import os
import pty
import pwd
import grp
import shutil
import signal
import stat
import struct
import termios
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from google.protobuf import json_format
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

try:  # Package import when started as ``envd_guest.server``.
    from .proto import filesystem_pb2, process_pb2
except Exception:  # pragma: no cover - direct module execution fallback.
    from proto import filesystem_pb2, process_pb2  # type: ignore


_CONNECT_HEADER = struct.Struct(">BI")
_CONNECT_FLAG_COMPRESSED = 0b00000001
_CONNECT_FLAG_END_STREAM = 0b00000010
_MAX_LIST_ENTRIES = 5000


def _expect_token() -> str:
    return (os.environ.get("ENVD_ACCESS_TOKEN") or "").strip()


def _safe_abs_path(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        raise ValueError("path required")
    root = Path("/").resolve()
    candidate = (root / str(raw).lstrip("/")).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ValueError("path escapes filesystem root") from None
    return str(candidate)


def _is_json_content(request: Request) -> bool:
    return "json" in (request.headers.get("content-type") or "").lower()


def _message_from_bytes(request: Request, body: bytes, msg: Any) -> Any:
    if _is_json_content(request):
        json_format.Parse(body.decode("utf-8") or "{}", msg, ignore_unknown_fields=True)
    else:
        msg.ParseFromString(body)
    return msg


def _message_to_bytes(request: Request, msg: Any) -> bytes:
    if _is_json_content(request):
        return json_format.MessageToJson(msg).encode("utf-8")
    return msg.SerializeToString()


async def _read_unary_message(request: Request, msg_type: Any) -> Any:
    return _message_from_bytes(request, await request.body(), msg_type())


async def _read_stream_message(request: Request, msg_type: Any) -> Any:
    body = await request.body()
    if len(body) < _CONNECT_HEADER.size:
        raise ValueError("invalid connect envelope")
    flags, size = _CONNECT_HEADER.unpack(body[: _CONNECT_HEADER.size])
    if flags & _CONNECT_FLAG_COMPRESSED:
        raise ValueError("compressed connect requests are not supported")
    if flags & _CONNECT_FLAG_END_STREAM:
        raise ValueError("unexpected end-stream envelope")
    payload = body[_CONNECT_HEADER.size : _CONNECT_HEADER.size + size]
    if len(payload) != size:
        raise ValueError("truncated connect envelope")
    return _message_from_bytes(request, payload, msg_type())


def _connect_media_type(request: Request, *, stream: bool = False) -> str:
    suffix = "json" if _is_json_content(request) else "proto"
    return f"application/connect+{suffix}" if stream else f"application/{suffix}"


def _connect_unary(request: Request, msg: Any) -> Response:
    return Response(
        content=_message_to_bytes(request, msg),
        media_type=_connect_media_type(request),
        headers={"connect-protocol-version": "1"},
    )


def _connect_error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"code": code, "message": message})


def _encode_connect_envelope(request: Request, msg: Any) -> bytes:
    data = _message_to_bytes(request, msg)
    return _CONNECT_HEADER.pack(0, len(data)) + data


def _encode_connect_end() -> bytes:
    data = b"{}"
    return _CONNECT_HEADER.pack(_CONNECT_FLAG_END_STREAM, len(data)) + data


def _file_type_for_mode(mode: int) -> int:
    if stat.S_ISDIR(mode):
        return filesystem_pb2.FILE_TYPE_DIRECTORY
    return filesystem_pb2.FILE_TYPE_FILE


def _entry_proto(p: Path) -> Any:
    st = p.lstat()
    entry = filesystem_pb2.EntryInfo(
        name=p.name,
        type=_file_type_for_mode(st.st_mode),
        path=str(p),
        size=int(st.st_size),
        mode=int(st.st_mode & 0o777),
        permissions=oct(st.st_mode & 0o777),
    )
    entry.modified_time.FromSeconds(int(st.st_mtime))
    with contextlib.suppress(Exception):
        entry.owner = pwd.getpwuid(st.st_uid).pw_name
    with contextlib.suppress(Exception):
        entry.group = grp.getgrgid(st.st_gid).gr_name
    if stat.S_ISLNK(st.st_mode):
        with contextlib.suppress(Exception):
            entry.symlink_target = os.readlink(p)
    return entry


def _entry_dict(p: Path) -> Dict[str, Any]:
    entry = _entry_proto(p)
    ftype = "dir" if entry.type == filesystem_pb2.FILE_TYPE_DIRECTORY else "file"
    return {
        "name": entry.name,
        "type": ftype,
        "path": entry.path,
        "size": int(entry.size),
        "mode": int(entry.mode),
        "permissions": entry.permissions,
    }


def _iter_dir_entries(root: Path, depth: int) -> Iterable[Path]:
    if depth <= 1:
        yield from sorted(root.iterdir(), key=lambda x: x.name)
        return
    for child in sorted(root.iterdir(), key=lambda x: x.name):
        yield child
        if child.is_dir() and not child.is_symlink():
            yield from _iter_dir_entries(child, depth - 1)


def _snapshot_tree(path: Path, recursive: bool) -> Dict[str, tuple[int, int, int, bool]]:
    out: Dict[str, tuple[int, int, int, bool]] = {}
    if not path.exists():
        return out
    candidates: Iterable[Path]
    if path.is_dir():
        candidates = path.rglob("*") if recursive else path.iterdir()
    else:
        candidates = [path]
    for p in candidates:
        with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
            st = p.lstat()
            out[str(p)] = (
                int(st.st_mtime_ns),
                int(st.st_size),
                int(st.st_mode & 0o777),
                bool(stat.S_ISDIR(st.st_mode)),
            )
    return out


@dataclass
class _Watcher:
    watcher_id: str
    path: Path
    recursive: bool
    include_entry: bool
    snapshot: Dict[str, tuple[int, int, int, bool]]


_WATCHERS: Dict[str, _Watcher] = {}


def _poll_watcher(watcher: _Watcher) -> List[Any]:
    new_snapshot = _snapshot_tree(watcher.path, watcher.recursive)
    old_snapshot = watcher.snapshot
    watcher.snapshot = new_snapshot
    events: List[Any] = []
    for path in sorted(set(new_snapshot) - set(old_snapshot)):
        events.append(_fs_event(path, filesystem_pb2.EVENT_TYPE_CREATE, watcher.include_entry))
    for path in sorted(set(old_snapshot) - set(new_snapshot)):
        events.append(_fs_event(path, filesystem_pb2.EVENT_TYPE_REMOVE, False))
    for path in sorted(set(new_snapshot) & set(old_snapshot)):
        old = old_snapshot[path]
        new = new_snapshot[path]
        if old[2] != new[2]:
            events.append(_fs_event(path, filesystem_pb2.EVENT_TYPE_CHMOD, watcher.include_entry))
        elif old[0] != new[0] or old[1] != new[1]:
            events.append(_fs_event(path, filesystem_pb2.EVENT_TYPE_WRITE, watcher.include_entry))
    return events


def _fs_event(path: str, event_type: int, include_entry: bool) -> Any:
    ev = filesystem_pb2.FilesystemEvent(name=Path(path).name, type=event_type)
    if include_entry:
        with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
            ev.entry.CopyFrom(_entry_proto(Path(path)))
    return ev


@dataclass
class _ManagedProcess:
    pid: int
    config: Any
    proc: asyncio.subprocess.Process
    is_pty: bool
    master_fd: Optional[int] = None
    tag: Optional[str] = None
    stdin_enabled: bool = False
    history: List[Any] = field(default_factory=list)
    subscribers: set[asyncio.Queue] = field(default_factory=set)
    end_event: Optional[Any] = None
    pty_reader_done: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def start_event(self) -> Any:
        return process_pb2.ProcessEvent(start=process_pb2.ProcessEvent.StartEvent(pid=self.pid))

    def process_info(self) -> Any:
        info = process_pb2.ProcessInfo(config=self.config, pid=self.pid)
        if self.tag:
            info.tag = self.tag
        return info

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    def emit(self, event: Any) -> None:
        self.history.append(event)
        for q in list(self.subscribers):
            q.put_nowait(event)

    def emit_data(self, kind: str, data: bytes) -> None:
        if not data:
            return
        ev = process_pb2.ProcessEvent()
        if kind == "stdout":
            ev.data.stdout = data
        elif kind == "stderr":
            ev.data.stderr = data
        else:
            ev.data.pty = data
        self.emit(ev)

    def emit_end(self, code: int, error: str = "") -> None:
        if self.end_event is not None:
            return
        ev = process_pb2.ProcessEvent()
        ev.end.exit_code = int(code)
        ev.end.exited = True
        ev.end.status = "exited"
        if error:
            ev.end.error = error
        self.end_event = ev
        self.emit(ev)
        if self.master_fd is not None:
            with contextlib.suppress(OSError):
                os.close(self.master_fd)
            self.master_fd = None


_PROCESSES: Dict[int, _ManagedProcess] = {}


def _selector_pid(selector: Any) -> int:
    if selector.HasField("pid"):
        return int(selector.pid)
    if selector.HasField("tag"):
        for proc in _PROCESSES.values():
            if proc.tag == selector.tag:
                return proc.pid
    return 0


def _process_or_error(selector: Any) -> Optional[_ManagedProcess]:
    pid = _selector_pid(selector)
    return _PROCESSES.get(pid)


def _argv_from_config(config: Any) -> List[str]:
    cmd = str(config.cmd or "").strip() or "/bin/sh"
    return [cmd, *[str(arg) for arg in config.args]]


def _env_from_config(config: Any) -> Dict[str, str]:
    return {**os.environ, **{str(k): str(v) for k, v in dict(config.envs).items()}}


def _cwd_from_config(config: Any) -> str:
    if config.HasField("cwd") and str(config.cwd or "").strip():
        return _safe_abs_path(str(config.cwd))
    return "/"


async def _read_pipe(proc: _ManagedProcess, stream: Any, kind: str) -> None:
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            break
        proc.emit_data(kind, chunk)


async def _read_pty(proc: _ManagedProcess) -> None:
    assert proc.master_fd is not None
    os.set_blocking(proc.master_fd, False)
    try:
        while True:
            try:
                chunk = os.read(proc.master_fd, 65536)
                if chunk:
                    proc.emit_data("pty", chunk)
                    continue
            except BlockingIOError:
                pass
            except OSError as exc:
                if exc.errno not in (errno.EIO, errno.EBADF):
                    proc.emit_data("pty", str(exc).encode())
                break
            await asyncio.sleep(0.02)
    finally:
        proc.pty_reader_done.set()


async def _wait_process(proc: _ManagedProcess) -> None:
    code = await proc.proc.wait()
    if proc.is_pty:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.pty_reader_done.wait(), timeout=1.0)
    proc.emit_end(int(code or 0))


def _resize_pty_fd(fd: int, rows: int, cols: int) -> None:
    rows = max(1, int(rows or 24))
    cols = max(1, int(cols or 80))
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def _pty_preexec(slave_fd: int):
    def _setup_child_session() -> None:
        os.setsid()
        for fd in (0, slave_fd):
            with contextlib.suppress(OSError, AttributeError):
                fcntl.ioctl(fd, termios.TIOCSCTTY, 0)
                break

    return _setup_child_session


async def _start_process(req: Any) -> _ManagedProcess:
    config = req.process
    argv = _argv_from_config(config)
    env = _env_from_config(config)
    cwd = _cwd_from_config(config)
    tag = req.tag if req.HasField("tag") else None
    has_pty = req.HasField("pty")

    if has_pty:
        master_fd, slave_fd = pty.openpty()
        if req.pty.HasField("size"):
            _resize_pty_fd(master_fd, req.pty.size.rows, req.pty.size.cols)
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            env=env,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            preexec_fn=_pty_preexec(slave_fd),
        )
        with contextlib.suppress(OSError):
            os.close(slave_fd)
        managed = _ManagedProcess(
            pid=int(proc.pid),
            config=config,
            proc=proc,
            is_pty=True,
            master_fd=master_fd,
            tag=tag,
        )
        _PROCESSES[managed.pid] = managed
        asyncio.create_task(_read_pty(managed))
        asyncio.create_task(_wait_process(managed))
        return managed

    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        env=env,
        stdin=asyncio.subprocess.PIPE if req.stdin else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        preexec_fn=os.setsid,
    )
    managed = _ManagedProcess(
        pid=int(proc.pid),
        config=config,
        proc=proc,
        is_pty=False,
        tag=tag,
        stdin_enabled=bool(req.stdin),
    )
    _PROCESSES[managed.pid] = managed
    assert proc.stdout and proc.stderr
    asyncio.create_task(_read_pipe(managed, proc.stdout, "stdout"))
    asyncio.create_task(_read_pipe(managed, proc.stderr, "stderr"))
    asyncio.create_task(_wait_process(managed))
    return managed


async def _event_stream(
    request: Request,
    proc: _ManagedProcess,
    response_type: Any,
) -> AsyncIterator[bytes]:
    yield _encode_connect_envelope(request, response_type(event=proc.start_event))
    for event in list(proc.history):
        yield _encode_connect_envelope(request, response_type(event=event))
        if event.HasField("end"):
            yield _encode_connect_end()
            return
    q = proc.subscribe()
    try:
        while True:
            event = await q.get()
            yield _encode_connect_envelope(request, response_type(event=event))
            if event.HasField("end"):
                break
    finally:
        proc.unsubscribe(q)
    yield _encode_connect_end()


def _signal_process(proc: _ManagedProcess, sig: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        os.killpg(proc.pid, sig)
        return
    with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
        proc.proc.send_signal(sig)


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
    return JSONResponse({"ok": True, "service": "envd-guest", "phase": "connect-v1"})


async def fs_stat(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        path = _safe_abs_path(str(body.get("path") or ""))
        p = Path(path)
        if not p.exists() and not p.is_symlink():
            return JSONResponse({"detail": "not found"}, status_code=404)
        return JSONResponse({"entry": _entry_dict(p)})
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
        entries = []
        for ch in _iter_dir_entries(root, max(1, int(body.get("depth") or 1))):
            if len(entries) >= _MAX_LIST_ENTRIES:
                break
            entries.append(_entry_dict(ch))
        return JSONResponse({"entries": entries})
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def fs_mkdir(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        path = _safe_abs_path(str(body.get("path") or ""))
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        return JSONResponse({"entry": _entry_dict(p)})
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
            shutil.rmtree(p)
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
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        Path(src).rename(dst)
        return JSONResponse({"entry": _entry_dict(Path(dst))})
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
        return Response(content=p.read_bytes(), media_type="application/octet-stream")
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
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                p = Path(path)
                p.write_bytes(raw)
                wrote.append({"path": path, "bytes_written": len(raw), "entry": _entry_dict(p)})
        else:
            dest = request.query_params.get("path") or "/"
            raw = await request.body()
            if (request.headers.get("content-encoding") or "").lower() == "gzip":
                raw = gzip.decompress(raw)
            path = _safe_abs_path(dest)
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            p = Path(path)
            p.write_bytes(raw)
            wrote.append({"path": path, "bytes_written": len(raw), "entry": _entry_dict(p)})
        if "e2b" in (request.headers.get("user-agent") or "").lower() or "multipart/form-data" in ct:
            return JSONResponse([item["entry"] for item in wrote])
        if len(wrote) == 1:
            return JSONResponse({"path": wrote[0]["path"], "bytes_written": wrote[0]["bytes_written"]})
        return JSONResponse({"files": [{"path": item["path"], "bytes_written": item["bytes_written"]} for item in wrote]})
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def process_start_legacy(request: Request) -> Response:
    try:
        body = await request.json()
        config = process_pb2.ProcessConfig()
        config.cmd = "/bin/sh"
        config.args.extend(["-c", str(body.get("command") or "")])
        if body.get("cwd"):
            config.cwd = str(body.get("cwd"))
        env = body.get("env")
        if isinstance(env, dict):
            config.envs.update({str(k): str(v) for k, v in env.items()})
        req = process_pb2.StartRequest(process=config, stdin=False)
        proc = await _start_process(req)

        async def combined() -> AsyncIterator[bytes]:
            yield (json.dumps({"type": "start", "pid": proc.pid}) + "\n").encode()
            for event in proc.history:
                if event.HasField("data"):
                    if event.data.stdout:
                        yield (json.dumps({"type": "stdout", "b64": base64.b64encode(event.data.stdout).decode("ascii")}) + "\n").encode()
                    if event.data.stderr:
                        yield (json.dumps({"type": "stderr", "b64": base64.b64encode(event.data.stderr).decode("ascii")}) + "\n").encode()
                if event.HasField("end"):
                    yield (json.dumps({"type": "exit", "code": int(event.end.exit_code)}) + "\n").encode()
                    return
            q = proc.subscribe()
            try:
                while True:
                    event = await q.get()
                    if event.HasField("data"):
                        if event.data.stdout:
                            yield (json.dumps({"type": "stdout", "b64": base64.b64encode(event.data.stdout).decode("ascii")}) + "\n").encode()
                        if event.data.stderr:
                            yield (json.dumps({"type": "stderr", "b64": base64.b64encode(event.data.stderr).decode("ascii")}) + "\n").encode()
                    if event.HasField("end"):
                        yield (json.dumps({"type": "exit", "code": int(event.end.exit_code)}) + "\n").encode()
                        break
            finally:
                proc.unsubscribe(q)

        return StreamingResponse(combined(), media_type="application/x-ndjson")
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=400)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": str(e)}, status_code=500)


async def rpc_fs_stat(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.StatRequest)
        p = Path(_safe_abs_path(req.path))
        if not p.exists() and not p.is_symlink():
            return _connect_error(404, "not_found", f"not found: {req.path}")
        return _connect_unary(request, filesystem_pb2.StatResponse(entry=_entry_proto(p)))
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_list_dir(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.ListDirRequest)
        root = Path(_safe_abs_path(req.path or "/"))
        if not root.exists():
            return _connect_error(404, "not_found", f"not found: {req.path}")
        if not root.is_dir():
            return _connect_error(400, "invalid_argument", f"not a directory: {req.path}")
        resp = filesystem_pb2.ListDirResponse()
        for child in _iter_dir_entries(root, max(1, int(req.depth or 1))):
            if len(resp.entries) >= _MAX_LIST_ENTRIES:
                break
            with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
                resp.entries.append(_entry_proto(child))
        return _connect_unary(request, resp)
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_mkdir(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.MakeDirRequest)
        p = Path(_safe_abs_path(req.path))
        if p.exists():
            return _connect_error(409, "already_exists", f"exists: {req.path}")
        p.mkdir(parents=True, exist_ok=False)
        return _connect_unary(request, filesystem_pb2.MakeDirResponse(entry=_entry_proto(p)))
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_remove(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.RemoveRequest)
        p = Path(_safe_abs_path(req.path))
        if p.is_dir() and not p.is_symlink():
            shutil.rmtree(p)
        else:
            p.unlink(missing_ok=False)
        return _connect_unary(request, filesystem_pb2.RemoveResponse())
    except FileNotFoundError:
        return _connect_error(404, "not_found", "not found")
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_move(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.MoveRequest)
        src = Path(_safe_abs_path(req.source))
        dst = Path(_safe_abs_path(req.destination))
        dst.parent.mkdir(parents=True, exist_ok=True)
        src.rename(dst)
        return _connect_unary(request, filesystem_pb2.MoveResponse(entry=_entry_proto(dst)))
    except FileNotFoundError:
        return _connect_error(404, "not_found", "not found")
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_create_watcher(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.CreateWatcherRequest)
        p = Path(_safe_abs_path(req.path))
        if not p.exists():
            return _connect_error(404, "not_found", f"not found: {req.path}")
        watcher_id = uuid.uuid4().hex
        _WATCHERS[watcher_id] = _Watcher(
            watcher_id=watcher_id,
            path=p,
            recursive=bool(req.recursive),
            include_entry=bool(req.include_entry),
            snapshot=_snapshot_tree(p, bool(req.recursive)),
        )
        return _connect_unary(request, filesystem_pb2.CreateWatcherResponse(watcher_id=watcher_id))
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_get_watcher_events(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.GetWatcherEventsRequest)
        watcher = _WATCHERS.get(req.watcher_id)
        if watcher is None:
            return _connect_error(404, "not_found", f"unknown watcher: {req.watcher_id}")
        resp = filesystem_pb2.GetWatcherEventsResponse()
        resp.events.extend(_poll_watcher(watcher))
        return _connect_unary(request, resp)
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_remove_watcher(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, filesystem_pb2.RemoveWatcherRequest)
        _WATCHERS.pop(req.watcher_id, None)
        return _connect_unary(request, filesystem_pb2.RemoveWatcherResponse())
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_fs_watch_dir(request: Request) -> Response:
    try:
        req = await _read_stream_message(request, filesystem_pb2.WatchDirRequest)
        p = Path(_safe_abs_path(req.path))
        if not p.exists():
            return _connect_error(404, "not_found", f"not found: {req.path}")
        watcher = _Watcher(uuid.uuid4().hex, p, bool(req.recursive), bool(req.include_entry), _snapshot_tree(p, bool(req.recursive)))

        async def stream() -> AsyncIterator[bytes]:
            yield _encode_connect_envelope(request, filesystem_pb2.WatchDirResponse(start=filesystem_pb2.WatchDirResponse.StartEvent()))
            while True:
                for event in _poll_watcher(watcher):
                    yield _encode_connect_envelope(request, filesystem_pb2.WatchDirResponse(filesystem=event))
                await asyncio.sleep(0.5)

        return StreamingResponse(stream(), media_type=_connect_media_type(request, stream=True), headers={"connect-protocol-version": "1"})
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_list(request: Request) -> Response:
    resp = process_pb2.ListResponse()
    for proc in _PROCESSES.values():
        if proc.end_event is None:
            resp.processes.append(proc.process_info())
    return _connect_unary(request, resp)


async def rpc_process_start(request: Request) -> Response:
    try:
        req = await _read_stream_message(request, process_pb2.StartRequest)
        proc = await _start_process(req)
        return StreamingResponse(
            _event_stream(request, proc, process_pb2.StartResponse),
            media_type=_connect_media_type(request, stream=True),
            headers={"connect-protocol-version": "1"},
        )
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except FileNotFoundError as e:
        return _connect_error(404, "not_found", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_connect(request: Request) -> Response:
    try:
        req = await _read_stream_message(request, process_pb2.ConnectRequest)
        proc = _process_or_error(req.process)
        if proc is None:
            return _connect_error(404, "not_found", "process not found")
        return StreamingResponse(
            _event_stream(request, proc, process_pb2.ConnectResponse),
            media_type=_connect_media_type(request, stream=True),
            headers={"connect-protocol-version": "1"},
        )
    except ValueError as e:
        return _connect_error(400, "invalid_argument", str(e))
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_update(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, process_pb2.UpdateRequest)
        proc = _process_or_error(req.process)
        if proc is None:
            return _connect_error(404, "not_found", "process not found")
        if req.HasField("pty") and proc.master_fd is not None and req.pty.HasField("size"):
            _resize_pty_fd(proc.master_fd, req.pty.size.rows, req.pty.size.cols)
        return _connect_unary(request, process_pb2.UpdateResponse())
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_send_input(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, process_pb2.SendInputRequest)
        proc = _process_or_error(req.process)
        if proc is None:
            return _connect_error(404, "not_found", "process not found")
        if req.input.HasField("pty"):
            if proc.master_fd is None:
                return _connect_error(400, "invalid_argument", "process is not a PTY")
            os.write(proc.master_fd, bytes(req.input.pty))
        elif req.input.HasField("stdin"):
            if proc.proc.stdin is None:
                return _connect_error(400, "invalid_argument", "stdin is not open")
            proc.proc.stdin.write(bytes(req.input.stdin))
            await proc.proc.stdin.drain()
        return _connect_unary(request, process_pb2.SendInputResponse())
    except BrokenPipeError:
        return _connect_error(409, "failed_precondition", "stdin is closed")
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_send_signal(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, process_pb2.SendSignalRequest)
        proc = _process_or_error(req.process)
        if proc is None:
            return _connect_error(404, "not_found", "process not found")
        sig = signal.SIGKILL if int(req.signal) == process_pb2.SIGNAL_SIGKILL else signal.SIGTERM
        _signal_process(proc, sig)
        return _connect_unary(request, process_pb2.SendSignalResponse())
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_close_stdin(request: Request) -> Response:
    try:
        req = await _read_unary_message(request, process_pb2.CloseStdinRequest)
        proc = _process_or_error(req.process)
        if proc is None:
            return _connect_error(404, "not_found", "process not found")
        if proc.proc.stdin is not None:
            proc.proc.stdin.close()
        return _connect_unary(request, process_pb2.CloseStdinResponse())
    except Exception as e:  # noqa: BLE001
        return _connect_error(500, "internal", str(e))


async def rpc_process_stream_input(_: Request) -> JSONResponse:
    return _connect_error(501, "unimplemented", "client-stream process input is not implemented; use SendInput.")


routes = [
    Route("/health", health, methods=["GET"]),
    Route("/v1/fs/stat", fs_stat, methods=["POST"]),
    Route("/v1/fs/list_dir", fs_list_dir, methods=["POST"]),
    Route("/v1/fs/mkdir", fs_mkdir, methods=["POST"]),
    Route("/v1/fs/remove", fs_remove, methods=["POST"]),
    Route("/v1/fs/move", fs_move, methods=["POST"]),
    Route("/files", files_get, methods=["GET"]),
    Route("/files", files_post, methods=["POST"]),
    Route("/v1/process/start", process_start_legacy, methods=["POST"]),
    Route("/filesystem.Filesystem/Stat", rpc_fs_stat, methods=["POST"]),
    Route("/filesystem.Filesystem/ListDir", rpc_fs_list_dir, methods=["POST"]),
    Route("/filesystem.Filesystem/MakeDir", rpc_fs_mkdir, methods=["POST"]),
    Route("/filesystem.Filesystem/Remove", rpc_fs_remove, methods=["POST"]),
    Route("/filesystem.Filesystem/Move", rpc_fs_move, methods=["POST"]),
    Route("/filesystem.Filesystem/CreateWatcher", rpc_fs_create_watcher, methods=["POST"]),
    Route("/filesystem.Filesystem/GetWatcherEvents", rpc_fs_get_watcher_events, methods=["POST"]),
    Route("/filesystem.Filesystem/RemoveWatcher", rpc_fs_remove_watcher, methods=["POST"]),
    Route("/filesystem.Filesystem/WatchDir", rpc_fs_watch_dir, methods=["POST"]),
    Route("/process.Process/List", rpc_process_list, methods=["POST"]),
    Route("/process.Process/Start", rpc_process_start, methods=["POST"]),
    Route("/process.Process/Connect", rpc_process_connect, methods=["POST"]),
    Route("/process.Process/Update", rpc_process_update, methods=["POST"]),
    Route("/process.Process/SendInput", rpc_process_send_input, methods=["POST"]),
    Route("/process.Process/SendSignal", rpc_process_send_signal, methods=["POST"]),
    Route("/process.Process/CloseStdin", rpc_process_close_stdin, methods=["POST"]),
    Route("/process.Process/StreamInput", rpc_process_stream_input, methods=["POST"]),
]

app = Starlette(routes=routes, middleware=[Middleware(_AuthMiddleware)])
