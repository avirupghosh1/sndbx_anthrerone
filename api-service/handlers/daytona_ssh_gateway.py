"""Daytona SSH gateway backed by envd PTY sessions."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import logging
import secrets
import shlex
import struct
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlsplit

import httpx
from fastapi import HTTPException, Request

try:
    import asyncssh
except ImportError:  # pragma: no cover - local dev may not have optional gateway deps installed.
    asyncssh = None  # type: ignore[assignment]

from config import get_config
from envd_guest.proto import process_pb2
from middleware import ApiKeyPrincipal, SandboxNotFoundException
from orchestrator import SandboxManager

logger = logging.getLogger(__name__)

_ACCESS_METADATA_KEY = "daytona_ssh_access"
_ACCESS_TOKEN_PREFIX = "dssh_"
_CONNECT_HEADER = struct.Struct(">BI")
_CONNECT_FLAG_END_STREAM = 0b00000010
_PAGE_SIZE = 200

_server: Any = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _token_hash(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _metadata(row: Optional[dict[str, Any]]) -> dict[str, Any]:
    md = (row or {}).get("metadata")
    return dict(md) if isinstance(md, dict) else {}


def _access_records(row: Optional[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    records = _metadata(row).get(_ACCESS_METADATA_KEY)
    if not isinstance(records, dict):
        return {}
    return {str(k): dict(v) for k, v in records.items() if isinstance(v, dict)}


def _record_is_valid(record: dict[str, Any], *, now: Optional[datetime] = None) -> bool:
    expires_at = _parse_iso(record.get("expiresAt"))
    return bool(expires_at and expires_at > (now or _now()))


def _prune_access_records(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    current = _now()
    return {k: v for k, v in records.items() if _record_is_valid(v, now=current)}


def _save_access_records(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    records: dict[str, dict[str, Any]],
) -> None:
    row = sandbox_manager.get_sandbox(sandbox_id)
    if not row:
        raise SandboxNotFoundException(sandbox_id)
    if not sandbox_manager.db.merge_sandbox_metadata(sandbox_id, {_ACCESS_METADATA_KEY: records}):
        raise SandboxNotFoundException(sandbox_id)


def _public_ssh_endpoint(request: Request) -> tuple[str, int]:
    cfg = get_config()
    explicit_host = str(getattr(cfg, "DAYTONA_SSH_GATEWAY_PUBLIC_HOST", "") or "").strip()
    raw_host = explicit_host
    if not raw_host:
        raw_host = (
            request.headers.get("x-forwarded-host")
            or request.headers.get("host")
            or request.url.netloc
            or "localhost"
        )
    raw_host = raw_host.split(",", 1)[0].strip()
    if "://" in raw_host:
        netloc = urlsplit(raw_host).netloc
    else:
        netloc = raw_host

    parsed_port: Optional[int] = None
    if netloc.startswith("["):
        host = netloc[1:].split("]", 1)[0]
        rest = netloc.split("]", 1)[1] if "]" in netloc else ""
        if rest.startswith(":"):
            with contextlib.suppress(ValueError):
                parsed_port = int(rest[1:])
    else:
        host, sep, port = netloc.partition(":")
        if sep:
            with contextlib.suppress(ValueError):
                parsed_port = int(port)
    public_port_raw = getattr(cfg, "DAYTONA_SSH_GATEWAY_PUBLIC_PORT", None)
    try:
        public_port = int(public_port_raw or parsed_port or getattr(cfg, "DAYTONA_SSH_GATEWAY_PORT", 2222))
    except (TypeError, ValueError):
        public_port = 2222
    return (host or "localhost", max(1, min(65535, public_port)))


def _ssh_command(request: Request, token: str) -> str:
    host, port = _public_ssh_endpoint(request)
    opts = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
    return f"ssh {opts} -p {port} {token}@{host}"


def _access_dto(record: dict[str, Any], token: str, request: Request) -> dict[str, Any]:
    return {
        "id": str(record.get("id") or ""),
        "sandboxId": str(record.get("sandboxId") or ""),
        "token": token,
        "expiresAt": str(record.get("expiresAt") or _now_iso()),
        "createdAt": str(record.get("createdAt") or _now_iso()),
        "updatedAt": str(record.get("updatedAt") or _now_iso()),
        "sshCommand": _ssh_command(request, token),
    }


def _ttl_minutes(expires_in_minutes: Optional[float]) -> int:
    cfg = get_config()
    default_minutes = max(1, int(getattr(cfg, "DAYTONA_SSH_ACCESS_DEFAULT_TTL_MIN", 60) or 60))
    max_minutes = max(default_minutes, int(getattr(cfg, "DAYTONA_SSH_ACCESS_MAX_TTL_MIN", 1440) or 1440))
    if expires_in_minutes is None:
        return default_minutes
    try:
        minutes = int(float(expires_in_minutes))
    except (TypeError, ValueError):
        minutes = default_minutes
    return max(1, min(max_minutes, minutes))


def create_ssh_access_record(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    row: dict[str, Any],
    request: Request,
    *,
    expires_in_minutes: Optional[float],
) -> dict[str, Any]:
    sandbox_id = str(row.get("sandbox_id") or row.get("id") or "").strip()
    if not sandbox_id:
        raise HTTPException(status_code=404, detail="Sandbox not found")
    token = f"{_ACCESS_TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
    now = _now()
    expires_at = now + timedelta(minutes=_ttl_minutes(expires_in_minutes))
    record = {
        "id": f"ssh-{secrets.token_hex(8)}",
        "sandboxId": sandbox_id,
        "tokenHash": _token_hash(token),
        "ownerClientId": str(principal.client_id),
        "createdAt": now.isoformat().replace("+00:00", "Z"),
        "updatedAt": now.isoformat().replace("+00:00", "Z"),
        "expiresAt": expires_at.isoformat().replace("+00:00", "Z"),
    }
    records = _prune_access_records(_access_records(row))
    records[str(record["tokenHash"])] = record
    _save_access_records(sandbox_manager, sandbox_id, records)
    return _access_dto(record, token, request)


def revoke_ssh_access_record(
    sandbox_manager: SandboxManager,
    row: dict[str, Any],
    *,
    token: Optional[str],
) -> None:
    sandbox_id = str(row.get("sandbox_id") or row.get("id") or "").strip()
    if not sandbox_id:
        raise HTTPException(status_code=404, detail="Sandbox not found")
    if token:
        records = _prune_access_records(_access_records(row))
        records.pop(_token_hash(token), None)
    else:
        records = {}
    _save_access_records(sandbox_manager, sandbox_id, records)


def find_ssh_access_by_token(
    sandbox_manager: SandboxManager,
    token: str,
    *,
    owner_client_id: Optional[str] = None,
) -> Optional[tuple[dict[str, Any], dict[str, Any]]]:
    token = str(token or "").strip()
    if not token:
        return None
    wanted = _token_hash(token)
    offset = 0
    while True:
        rows = sandbox_manager.db.list_sandboxes(limit=_PAGE_SIZE, offset=offset)
        if not rows:
            return None
        for row in rows:
            record = _access_records(row).get(wanted)
            if not record:
                continue
            if owner_client_id and str(record.get("ownerClientId") or "") != str(owner_client_id):
                continue
            if not _record_is_valid(record):
                continue
            return row, record
        if len(rows) < _PAGE_SIZE:
            return None
        offset += len(rows)


def validate_ssh_access_token(
    sandbox_manager: SandboxManager,
    token: str,
    *,
    owner_client_id: Optional[str] = None,
) -> dict[str, Any]:
    found = find_ssh_access_by_token(sandbox_manager, token, owner_client_id=owner_client_id)
    if not found:
        return {"valid": False, "sandboxId": ""}
    row, record = found
    sandbox_id = str(record.get("sandboxId") or row.get("sandbox_id") or "")
    if str(row.get("state") or "").lower() not in {"running", "paused"}:
        return {"valid": False, "sandboxId": sandbox_id}
    return {"valid": True, "sandboxId": sandbox_id}


def _envd_connection_or_503(sandbox_manager: SandboxManager, sandbox_id: str) -> dict[str, Any]:
    info, reason = sandbox_manager.get_envd_connection_ex(sandbox_id)
    if not info:
        raise HTTPException(status_code=503, detail=f"envd unavailable: {reason or 'unknown'}")
    return info


def _envd_headers(info: dict[str, Any], *, stream: bool = False) -> dict[str, str]:
    headers = {
        "X-Access-Token": str(info.get("access_token") or ""),
        "Content-Type": "application/connect+proto" if stream else "application/proto",
        "Connect-Protocol-Version": "1",
    }
    traffic_token = str(info.get("traffic_access_token") or "").strip()
    if traffic_token:
        headers["e2b-traffic-access-token"] = traffic_token
    internal_route_headers = info.get("internal_route_headers")
    if isinstance(internal_route_headers, dict):
        for key, value in internal_route_headers.items():
            k = str(key or "").strip()
            v = str(value or "").strip()
            if k and v:
                headers[k] = v
    return headers


def _connect_envelope(message: Any) -> bytes:
    payload = message.SerializeToString()
    return _CONNECT_HEADER.pack(0, len(payload)) + payload


def _parse_connect_messages(buffer: bytearray, response_type: Any) -> list[Any]:
    messages: list[Any] = []
    while len(buffer) >= _CONNECT_HEADER.size:
        flags, size = _CONNECT_HEADER.unpack(bytes(buffer[: _CONNECT_HEADER.size]))
        total = _CONNECT_HEADER.size + int(size)
        if len(buffer) < total:
            break
        payload = bytes(buffer[_CONNECT_HEADER.size:total])
        del buffer[:total]
        if flags & _CONNECT_FLAG_END_STREAM:
            continue
        msg = response_type()
        msg.ParseFromString(payload)
        messages.append(msg)
    return messages


def _process_selector(session: dict[str, Any]) -> Any:
    selector = process_pb2.ProcessSelector()
    pid = session.get("pid")
    try:
        if pid is not None:
            selector.pid = int(pid)
            return selector
    except Exception:
        pass
    selector.tag = str(session.get("tag") or session.get("id") or "")
    return selector


async def _envd_unary(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    rpc_path: str,
    request_message: Any,
    response_type: Any,
    *,
    timeout: float = 30.0,
) -> Any:
    info = _envd_connection_or_503(sandbox_manager, sandbox_id)
    url = f"{str(info['http_base_url']).rstrip('/')}{rpc_path}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=5.0)) as client:
        response = await client.post(
            url,
            headers=_envd_headers(info),
            content=request_message.SerializeToString(),
        )
    if response.status_code >= 400:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    out = response_type()
    out.ParseFromString(response.content)
    return out


async def _start_envd_pty(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    *,
    tag: str,
    rows: int,
    cols: int,
) -> dict[str, Any]:
    info = _envd_connection_or_503(sandbox_manager, sandbox_id)
    url = f"{str(info['http_base_url']).rstrip('/')}/process.Process/Start"
    req = process_pb2.StartRequest()
    req.process.cmd = "/bin/sh"
    req.process.cwd = "/"
    req.pty.size.rows = max(1, int(rows or 24))
    req.pty.size.cols = max(1, int(cols or 80))
    req.tag = tag
    req.stdin = True
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0, read=30.0)) as client:
        async with client.stream(
            "POST",
            url,
            headers=_envd_headers(info, stream=True),
            content=_connect_envelope(req),
        ) as response:
            if response.status_code >= 400:
                detail = (await response.aread()).decode("utf-8", "replace")
                raise HTTPException(status_code=response.status_code, detail=detail)
            buffer = bytearray()
            async for chunk in response.aiter_raw():
                buffer.extend(chunk)
                for msg in _parse_connect_messages(buffer, process_pb2.StartResponse):
                    if msg.HasField("event") and msg.event.HasField("start"):
                        pid = int(msg.event.start.pid)
                        if pid > 0:
                            return {
                                "id": tag,
                                "tag": tag,
                                "pid": pid,
                                "rows": rows,
                                "cols": cols,
                            }
    raise HTTPException(status_code=502, detail="envd did not return an SSH PTY pid")


async def _envd_send_pty_input(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    payload: bytes,
) -> None:
    req = process_pb2.SendInputRequest(process=_process_selector(session))
    req.input.pty = payload
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/SendInput", req, process_pb2.SendInputResponse, timeout=15.0)


async def _envd_resize_pty(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
    *,
    rows: int,
    cols: int,
) -> None:
    req = process_pb2.UpdateRequest(process=_process_selector(session))
    req.pty.size.rows = max(1, int(rows or 24))
    req.pty.size.cols = max(1, int(cols or 80))
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/Update", req, process_pb2.UpdateResponse, timeout=15.0)


async def _envd_kill_process(
    sandbox_manager: SandboxManager,
    sandbox_id: str,
    session: dict[str, Any],
) -> None:
    req = process_pb2.SendSignalRequest(process=_process_selector(session), signal=process_pb2.SIGNAL_SIGKILL)
    await _envd_unary(sandbox_manager, sandbox_id, "/process.Process/SendSignal", req, process_pb2.SendSignalResponse, timeout=15.0)


def _term_size(term_size: Any) -> tuple[int, int]:
    width = getattr(term_size, "width", None)
    height = getattr(term_size, "height", None)
    if width is not None and height is not None:
        return max(1, int(height or 24)), max(1, int(width or 80))
    try:
        cols = int(term_size[0])
        rows = int(term_size[1])
    except Exception:
        cols, rows = 80, 24
    return max(1, rows), max(1, cols)


async def _suppress_gateway_error(awaitable: Any) -> None:
    with contextlib.suppress(Exception):
        await awaitable


if asyncssh is not None:

    class _DaytonaSSHServer(asyncssh.SSHServer):
        def __init__(self, sandbox_manager: SandboxManager):
            self._sandbox_manager = sandbox_manager
            self._sandbox_id = ""

        def begin_auth(self, username: str) -> bool:
            found = find_ssh_access_by_token(self._sandbox_manager, username)
            if not found:
                self._sandbox_id = ""
                return True
            row, record = found
            sandbox_id = str(record.get("sandboxId") or row.get("sandbox_id") or "")
            if not sandbox_id:
                self._sandbox_id = ""
                return True
            self._sandbox_id = sandbox_id
            return False

        def password_auth_supported(self) -> bool:
            return False

        def public_key_auth_supported(self) -> bool:
            return False

        def session_requested(self) -> Any:
            if not self._sandbox_id:
                return False
            return _DaytonaSSHSession(self._sandbox_manager, self._sandbox_id)


    class _DaytonaSSHSession(asyncssh.SSHServerSession):
        def __init__(self, sandbox_manager: SandboxManager, sandbox_id: str):
            self._sandbox_manager = sandbox_manager
            self._sandbox_id = sandbox_id
            self._chan: Any = None
            self._envd_session: Optional[dict[str, Any]] = None
            self._pending_input: list[bytes] = []
            self._task: Optional[asyncio.Task] = None
            self._rows = 24
            self._cols = 80
            self._command: Optional[str] = None
            self._closed = False

        def connection_made(self, chan: Any) -> None:
            self._chan = chan

        def pty_requested(self, term_type: str, term_size: Any, term_modes: Any) -> bool:
            _ = (term_type, term_modes)
            self._rows, self._cols = _term_size(term_size)
            return True

        def shell_requested(self) -> bool:
            self._command = None
            self._task = asyncio.create_task(self._run())
            return True

        def exec_requested(self, command: str) -> bool:
            self._command = str(command or "")
            self._task = asyncio.create_task(self._run())
            return True

        def data_received(self, data: Any, datatype: Any) -> None:
            _ = datatype
            payload = data if isinstance(data, bytes) else str(data).encode("utf-8")
            if not payload:
                return
            if self._envd_session:
                asyncio.create_task(
                    _suppress_gateway_error(
                        _envd_send_pty_input(self._sandbox_manager, self._sandbox_id, self._envd_session, payload)
                    )
                )
            else:
                self._pending_input.append(payload)

        def terminal_size_changed(self, width: int, height: int, pixwidth: int, pixheight: int) -> None:
            _ = (pixwidth, pixheight)
            self._cols = max(1, int(width or 80))
            self._rows = max(1, int(height or 24))
            if self._envd_session:
                asyncio.create_task(
                    _suppress_gateway_error(
                        _envd_resize_pty(
                            self._sandbox_manager,
                            self._sandbox_id,
                            self._envd_session,
                            rows=self._rows,
                            cols=self._cols,
                        )
                    )
                )

        def eof_received(self) -> bool:
            if self._envd_session:
                asyncio.create_task(
                    _suppress_gateway_error(
                        _envd_send_pty_input(self._sandbox_manager, self._sandbox_id, self._envd_session, b"\x04")
                    )
                )
            return False

        def connection_lost(self, exc: Optional[Exception]) -> None:
            _ = exc
            self._closed = True
            if self._task:
                self._task.cancel()
            if self._envd_session:
                asyncio.create_task(
                    _suppress_gateway_error(
                        _envd_kill_process(self._sandbox_manager, self._sandbox_id, self._envd_session)
                    )
                )

        async def _run(self) -> None:
            tag = f"ssh-{secrets.token_hex(8)}"
            try:
                self._envd_session = await _start_envd_pty(
                    self._sandbox_manager,
                    self._sandbox_id,
                    tag=tag,
                    rows=self._rows,
                    cols=self._cols,
                )
                for payload in self._pending_input:
                    await _envd_send_pty_input(self._sandbox_manager, self._sandbox_id, self._envd_session, payload)
                self._pending_input.clear()
                if self._command:
                    command = self._command.replace("\r", "").replace("\n", " ")
                    await _envd_send_pty_input(
                        self._sandbox_manager,
                        self._sandbox_id,
                        self._envd_session,
                        f"{command}\nexit\n".encode("utf-8"),
                    )
                await self._pump_envd_to_ssh()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("Daytona SSH session failed sandbox=%s: %s", self._sandbox_id, exc)
                if self._chan and not self._closed:
                    with contextlib.suppress(Exception):
                        self._chan.write(str(exc).encode("utf-8") + b"\r\n")
                        self._chan.exit(1)
                        self._chan.close()

        async def _pump_envd_to_ssh(self) -> None:
            assert self._envd_session is not None
            info = _envd_connection_or_503(self._sandbox_manager, self._sandbox_id)
            url = f"{str(info['http_base_url']).rstrip('/')}/process.Process/Connect"
            req = process_pb2.ConnectRequest(process=_process_selector(self._envd_session))
            async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=5.0)) as client:
                async with client.stream(
                    "POST",
                    url,
                    headers=_envd_headers(info, stream=True),
                    content=_connect_envelope(req),
                ) as response:
                    if response.status_code >= 400:
                        detail = (await response.aread()).decode("utf-8", "replace")
                        raise HTTPException(status_code=response.status_code, detail=detail)
                    buffer = bytearray()
                    async for chunk in response.aiter_raw():
                        if self._closed:
                            return
                        buffer.extend(chunk)
                        for msg in _parse_connect_messages(buffer, process_pb2.ConnectResponse):
                            if not msg.HasField("event"):
                                continue
                            event = msg.event
                            if event.HasField("data"):
                                payload = bytes(event.data.pty or event.data.stdout or event.data.stderr)
                                if payload and self._chan:
                                    self._chan.write(payload)
                            if event.HasField("end"):
                                if self._chan:
                                    self._chan.exit(int(event.end.exit_code))
                                    self._chan.close()
                                self._closed = True
                                return


def _host_key() -> Any:
    assert asyncssh is not None
    raw = str(getattr(get_config(), "DAYTONA_SSH_GATEWAY_HOST_KEY", "") or "").strip()
    if raw:
        return asyncssh.import_private_key(raw.replace("\\n", "\n"))
    return asyncssh.generate_private_key("ssh-rsa")


async def start_daytona_ssh_gateway(sandbox_manager: SandboxManager) -> None:
    global _server
    cfg = get_config()
    if not bool(getattr(cfg, "DAYTONA_SSH_GATEWAY_ENABLED", True)):
        logger.info("Daytona SSH gateway disabled")
        return
    if asyncssh is None:
        logger.warning("Daytona SSH gateway disabled: asyncssh is not installed")
        return
    if _server is not None:
        return
    host = str(getattr(cfg, "DAYTONA_SSH_GATEWAY_HOST", "0.0.0.0") or "0.0.0.0")
    port = int(getattr(cfg, "DAYTONA_SSH_GATEWAY_PORT", 2222) or 2222)
    _server = await asyncssh.create_server(
        lambda: _DaytonaSSHServer(sandbox_manager),
        host,
        port,
        server_host_keys=[_host_key()],
        encoding=None,
    )
    logger.info("Daytona SSH gateway listening on %s:%s", host, port)


async def stop_daytona_ssh_gateway() -> None:
    global _server
    if _server is None:
        return
    _server.close()
    await _server.wait_closed()
    _server = None
