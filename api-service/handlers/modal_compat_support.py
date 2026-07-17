"""Support helpers for the Modal gRPC compatibility gateway."""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import logging
import re
import secrets
import shlex
import tarfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, NamedTuple, Optional

from config import get_config
from handlers import templates as template_handlers
from handlers.modal_proto_wire import (
    RawMessage,
    bytes_value,
    f_double,
    f_int,
    f_message,
    f_string,
    fields,
    integer,
    join,
    messages,
    string,
    string_map,
    strings,
)
from middleware import ApiKeyPrincipal, ClientAuthError, authenticate_client_credential
from models import RegisterTemplateFromDockerfileRequest
from orchestrator import SandboxManager

try:  # pragma: no cover - optional dependency in local dev shells.
    from grpclib import GRPCError, Status
except ImportError:  # pragma: no cover
    GRPCError = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

GENERIC_STATUS_SUCCESS = 1

GENERIC_STATUS_FAILURE = 2

GENERIC_STATUS_TERMINATED = 3

GENERIC_STATUS_TIMEOUT = 4

FILE_DESCRIPTOR_STDOUT = 1

FILE_DESCRIPTOR_STDERR = 2

TASK_EXEC_FD_STDOUT = 0

TASK_EXEC_FD_STDERR = 1

_DEFAULT_ENVIRONMENT = "main"

_IMAGE_BUILDER_VERSION = "2025.06"

_FS_TOOLS_PATH = "/__modal/.bin/modal-sandbox-fs-tools"

class _CompatHandler(NamedTuple):
    func: Callable[..., Any]
    cardinality: Any
    request_type: type[RawMessage]
    reply_type: type[RawMessage]

def _handler(
    func: Callable[..., Any],
    cardinality: Any,
    request_type: type[RawMessage] = RawMessage,
    reply_type: type[RawMessage] = RawMessage,
) -> _CompatHandler:
    return _CompatHandler(func, cardinality, request_type, reply_type)

@dataclass
class ExecRecord:
    task_id: str
    sandbox_id: str
    exec_id: str
    stdout: bytes
    stderr: bytes
    exit_code: int

@dataclass
class FileHandle:
    sandbox_id: str
    path: str
    mode: str
    data: bytearray
    pos: int = 0

class ModalCompatState:
    def __init__(self) -> None:
        self.task_to_sandbox: dict[str, str] = {}
        self.execs: dict[tuple[str, str], ExecRecord] = {}
        self.terminated_results: dict[str, int] = {}
        self.snapshots: dict[str, str] = {}
        self.auth_tokens: dict[str, ApiKeyPrincipal] = {}
        self.router_tokens: dict[str, str] = {}
        self.sandbox_tags: dict[str, dict[str, str]] = {}
        self.file_handles: dict[str, FileHandle] = {}
        self.file_exec_outputs: dict[str, tuple[bytes, tuple[int, str] | None]] = {}

_state = ModalCompatState()

def _grpc_error(status: Any, message: str) -> Exception:
    if GRPCError is None:
        raise RuntimeError(message)
    return GRPCError(status, message)

def _unimplemented(name: str) -> Exception:
    return _grpc_error(Status.UNIMPLEMENTED, f"Modal {name} is not implemented in this sandbox API.")

def _metadata(stream: Any) -> dict[str, str]:
    raw = getattr(stream, "metadata", None) or {}
    if hasattr(raw, "items"):
        items = raw.items()
    else:
        items = raw
    out: dict[str, str] = {}
    for item in items:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        k, v = item
        key = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        if isinstance(v, bytes):
            val = v.decode("utf-8", "ignore")
        elif isinstance(v, (list, tuple)):
            val = str(v[-1]) if v else ""
        else:
            val = str(v)
        out[key.lower()] = val
    return out

def _principal_from_stream(stream: Any, sandbox_manager: SandboxManager) -> ApiKeyPrincipal:
    md = _metadata(stream)
    modal_auth_token = (md.get("x-modal-auth-token") or "").strip()
    if modal_auth_token:
        principal = _state.auth_tokens.get(modal_auth_token)
        if principal is not None:
            return principal

    api_key = (
        md.get("x-modal-token-secret")
        or md.get("x-api-key")
        or ""
    ).strip()
    auth = (md.get("authorization") or "").strip()
    if not api_key and auth.lower().startswith("bearer "):
        api_key = auth.split(" ", 1)[1].strip()
    if not api_key:
        raise _grpc_error(Status.UNAUTHENTICATED, "MODAL_TOKEN_SECRET must contain a valid sandbox API key.")

    try:
        return authenticate_client_credential(api_key)
    except ClientAuthError as ex:
        status = Status.PERMISSION_DENIED if ex.status_code == 403 else Status.UNAUTHENTICATED
        raise _grpc_error(status, ex.detail)

async def _recv(stream: Any) -> bytes:
    msg = await stream.recv_message()
    if isinstance(msg, RawMessage):
        return msg.data
    return bytes(msg or b"")

async def _send(stream: Any, data: bytes = b"") -> None:
    await stream.send_message(RawMessage(data))

def _generic_result(status: int, *, exit_code: int = 0, exception: str = "") -> bytes:
    return join(
        [
            f_int(1, status),
            f_string(2, exception),
            f_int(3, exit_code),
        ]
    )

def _environment_metadata(name: str) -> bytes:
    settings = join([f_string(1, _IMAGE_BUILDER_VERSION), f_string(2, "local")])
    return join([f_string(1, name or _DEFAULT_ENVIRONMENT), f_message(2, settings)])

def _environment_response(name: str) -> bytes:
    env = name or _DEFAULT_ENVIRONMENT
    return join([f_string(1, f"en-{env}"), f_message(2, _environment_metadata(env))])

def _image_metadata(*, workdir: str = "/", packages: Optional[dict[str, str]] = None) -> bytes:
    parts = [
        f_string(1, "Python 3"),
        f_string(3, workdir or "/"),
        f_string(5, _IMAGE_BUILDER_VERSION),
    ]
    for key, value in (packages or {}).items():
        parts.append(f_message(2, join([f_string(1, key), f_string(2, value)])))
    return join(parts)

def _task_logs(data: str, *, fd: int = FILE_DESCRIPTOR_STDOUT) -> bytes:
    return join([f_string(1, data), f_double(7, time.time()), f_int(8, fd)])

def _base_image_refs(image_msg: bytes) -> dict[str, str]:
    refs: dict[str, str] = {}
    for msg in messages(fields(image_msg), 5):
        item = fields(msg)
        image_id = string(item, 1)
        docker_tag = string(item, 2)
        if docker_tag and image_id:
            refs[docker_tag] = image_id
    return refs

def _context_tar_gzip_base64(image_msg: bytes) -> str:
    context_files = []
    for msg in messages(fields(image_msg), 7):
        item = fields(msg)
        filename = string(item, 1).lstrip("/") or "context-file"
        data = bytes_value(item, 2)
        if data:
            context_files.append((filename, data))
    if not context_files:
        return ""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        with tarfile.open(fileobj=gz, mode="w") as tf:
            for filename, data in context_files:
                info = tarfile.TarInfo(filename)
                info.size = len(data)
                info.mtime = int(time.time())
                tf.addfile(info, io.BytesIO(data))
    return base64.b64encode(buf.getvalue()).decode("ascii")

def _template_row_for_image(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, image_id: str) -> dict[str, Any] | None:
    if not image_id:
        return None
    row = sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, image_id)
    if row:
        return row
    return sandbox_manager.db.get_sandbox_template(image_id)

def _base_ref_for_image(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, image_id: str) -> str:
    row = _template_row_for_image(sandbox_manager, principal, image_id)
    if row:
        return str(row.get("warm_snapshot_image") or row.get("registry_image_ref") or row.get("base_image") or "").strip()
    return image_id

def _dockerfile_for_modal_image(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    image_msg: bytes,
) -> tuple[str, dict[str, str], str]:
    item = fields(image_msg)
    commands = strings(item, 6)
    base_refs = {
        name: _base_ref_for_image(sandbox_manager, principal, image_id)
        for name, image_id in _base_image_refs(image_msg).items()
    }
    rewritten: list[str] = []
    workdir = "/"
    for raw in commands:
        line = raw.strip()
        if not line:
            continue
        match = re.match(r"(?i)^FROM\s+([^\s]+)(.*)$", line)
        if match:
            source = match.group(1)
            suffix = match.group(2) or ""
            replacement = base_refs.get(source, source)
            line = f"FROM {replacement}{suffix}"
        workdir_match = re.match(r"(?i)^WORKDIR\s+(.+)$", line)
        if workdir_match:
            workdir = workdir_match.group(1).strip().strip("'\"") or workdir
        rewritten.append(line)
    if not any(line.upper().startswith("FROM ") for line in rewritten):
        rewritten.insert(0, "FROM python:3.11-slim")
    return "\n".join(rewritten) + "\n", string_map(item, 22), workdir

async def _register_image(
    sandbox_manager: SandboxManager,
    principal: ApiKeyPrincipal,
    image_msg: bytes,
) -> tuple[str, str]:
    dockerfile, build_args, workdir = _dockerfile_for_modal_image(sandbox_manager, principal, image_msg)
    context_b64 = _context_tar_gzip_base64(image_msg)
    digest = hashlib.sha256()
    digest.update(dockerfile.encode("utf-8"))
    digest.update(context_b64.encode("ascii"))
    for key, value in sorted(build_args.items()):
        digest.update(key.encode("utf-8") + b"=" + value.encode("utf-8"))
    image_id = f"im-{digest.hexdigest()[:24]}"
    existing = _template_row_for_image(sandbox_manager, principal, image_id)
    if existing and str(
        existing.get("warm_snapshot_image") or existing.get("registry_image_ref") or existing.get("base_image") or ""
    ).strip():
        return image_id, workdir
    req = RegisterTemplateFromDockerfileRequest(
        template_id=image_id,
        dockerfile=dockerfile,
        build_args=build_args,
        context_tar_gzip_base64=context_b64 or None,
        env={},
        start_cmd="",
        ready_cmd="",
        settle_seconds=0,
    )
    await template_handlers.register_template_from_dockerfile(req, principal, sandbox_manager)
    return image_id, workdir

def _sandbox_metadata(defn: bytes) -> tuple[dict[str, Any], str, str, int, Optional[str], Optional[str]]:
    item = fields(defn)
    args = strings(item, 1)
    image_id = string(item, 3) or "python:3.11"
    timeout = integer(item, 7, 3600) or 3600
    workdir = string(item, 8) or None
    name = string(item, 30) or None
    metadata: dict[str, Any] = {
        "modal": {
            "entrypoint_args": args,
            "image_id": image_id,
            "name": name,
        }
    }
    if args:
        metadata["start_cmd"] = shlex.join(args)
    ports = []
    for port_specs in messages(item, 20):
        p_fields = fields(port_specs)
        for port_msg in messages(p_fields, 1):
            p = integer(fields(port_msg), 1, 0)
            if p:
                ports.append(p)
    if ports:
        metadata["guest_ports"] = ports
    return metadata, image_id, name or "", int(timeout), workdir, shlex.join(args) if args else None

def _resources(defn: bytes) -> tuple[str, str]:
    item = fields(defn)
    resource_msgs = messages(item, 5)
    if not resource_msgs:
        return "1", "512m"
    res = fields(resource_msgs[-1])
    memory_mb = integer(res, 2, 512) or 512
    milli_cpu = integer(res, 3, 1000) or 1000
    cpu = max(0.001, milli_cpu / 1000.0)
    cpu_s = str(int(cpu)) if cpu.is_integer() else str(cpu)
    return cpu_s, f"{memory_mb}m"

def _task_id_for_sandbox(sandbox_id: str) -> str:
    return f"task-{sandbox_id}"

def _tags_from_messages(raw_tags: list[bytes]) -> dict[str, str]:
    tags: dict[str, str] = {}
    for raw in raw_tags:
        item = fields(raw)
        key = string(item, 1)
        value = string(item, 2)
        if key:
            tags[key] = value
    return tags

def _tag_messages(tags: dict[str, str]) -> list[bytes]:
    return [f_message(1, join([f_string(1, key), f_string(2, value)])) for key, value in tags.items()]

def _sandbox_info_tag_messages(tags: dict[str, str]) -> list[bytes]:
    return [f_message(6, join([f_string(1, key), f_string(2, value)])) for key, value in tags.items()]

def _modal_metadata(row: dict[str, Any]) -> dict[str, Any]:
    md = row.get("metadata") or {}
    modal_md = md.get("modal") if isinstance(md, dict) else {}
    return modal_md if isinstance(modal_md, dict) else {}

def _row_created_timestamp(row: dict[str, Any]) -> float:
    raw = row.get("created_at")
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, datetime):
        dt = raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    text = str(raw or "").strip()
    if text:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
        try:
            return time.mktime(time.strptime(text.split(".")[0], "%Y-%m-%dT%H:%M:%S"))
        except Exception:
            pass
    return time.time()

def _tags_for_sandbox(row: dict[str, Any]) -> dict[str, str]:
    sandbox_id = str(row.get("sandbox_id") or row.get("id") or "").strip()
    tags = dict(_modal_metadata(row).get("tags") or {})
    tags.update(_state.sandbox_tags.get(sandbox_id, {}))
    return {str(k): str(v) for k, v in tags.items()}

def _get_sandbox_row(sandbox_manager: SandboxManager, sandbox_id: str) -> Optional[dict[str, Any]]:
    sid = (sandbox_id or "").strip()
    if not sid:
        return None
    return sandbox_manager.get_sandbox(sid)

def _sandbox_is_terminal(row: dict[str, Any]) -> bool:
    state = str((row or {}).get("state") or "").strip().lower()
    return state in {"deleted", "expired", "exited", "failed", "killed", "stopped", "terminated"}

def _authorize_router(stream: Any, task_id: str = "") -> None:
    auth = (_metadata(stream).get("authorization") or "").strip()
    token = auth.split(" ", 1)[1].strip() if auth.lower().startswith("bearer ") else auth
    sandbox_id = _state.router_tokens.get(token)
    if not sandbox_id:
        raise _grpc_error(Status.UNAUTHENTICATED, "Missing or invalid Modal task router token.")
    if task_id and task_id != _task_id_for_sandbox(sandbox_id):
        raise _grpc_error(Status.PERMISSION_DENIED, "Modal task router token does not match this task.")

def _sandbox_for_task_id(task_id: str) -> str:
    sandbox_id = _state.task_to_sandbox.get(task_id)
    if sandbox_id:
        return sandbox_id
    if task_id.startswith("task-"):
        return task_id[5:]
    return ""

def _signed_int32(value: int) -> int:
    if value >= 1 << 63:
        value -= 1 << 64
    elif value >= 1 << 31:
        value -= 1 << 32
    return int(value)

def _fs_output(data: bytes = b"", error: tuple[int, str] | None = None) -> str:
    exec_id = f"fs-{secrets.token_hex(12)}"
    _state.file_exec_outputs[exec_id] = (bytes(data or b""), error)
    return exec_id

def _fs_error(code: int, message: str) -> str:
    return _fs_output(b"", (code, message))

def _fs_error_message(code: int, message: str) -> bytes:
    return join([f_int(1, code), f_string(2, message)])

def _sandbox_info(row: dict[str, Any]) -> bytes:
    modal_md = _modal_metadata(row)
    sandbox_id = str(row.get("sandbox_id") or row.get("id") or "")
    image_id = str((modal_md or {}).get("image_id") or row.get("template_id") or "")
    name = str((modal_md or {}).get("name") or "")
    app_id = str((modal_md or {}).get("app_id") or "ap-local")
    created_ts = _row_created_timestamp(row)
    task_info = join(
        [
            f_string(1, _task_id_for_sandbox(sandbox_id)),
            f_double(2, created_ts),
            f_string(7, sandbox_id),
        ]
    )
    return join(
        [
            f_string(1, sandbox_id),
            f_double(3, created_ts),
            f_message(4, task_info),
            f_string(5, app_id),
            join(_sandbox_info_tag_messages(_tags_for_sandbox(row))),
            f_string(7, name),
            f_string(8, image_id),
            f_int(11, int(row.get("timeout") or 3600)),
        ]
    )

def _public_modal_gateway_url() -> str:
    cfg = get_config()
    explicit = str(getattr(cfg, "MODAL_COMPAT_GATEWAY_PUBLIC_URL", "") or "").strip().rstrip("/")
    if explicit:
        return explicit
    host = str(getattr(cfg, "MODAL_COMPAT_GATEWAY_PUBLIC_HOST", "") or "").strip() or "127.0.0.1"
    port = int(getattr(cfg, "MODAL_COMPAT_GATEWAY_PORT", 50051) or 50051)
    return f"http://{host}:{port}"
