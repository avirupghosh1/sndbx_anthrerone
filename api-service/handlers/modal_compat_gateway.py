"""Modal Python SDK compatibility gateway.

Modal's Python SDK talks to ``ModalClient`` and ``TaskCommandRouter`` over
gRPC/HTTP2. This gateway exposes the subset of those RPCs that maps to the
generic sandbox/template architecture in this repo. It intentionally lives
outside the FastAPI routers because JSON HTTP routes cannot receive Modal's
gRPC requests.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import posixpath
import secrets
import shlex
import time
from typing import Any

from async_runner import run_io
from config import get_config
from handlers import templates as template_handlers
from handlers.modal_proto_wire import (
    RawMessage,
    boolean,
    bytes_value,
    double,
    f_bool,
    f_bytes,
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
from middleware import ApiKeyPrincipal
from models import CreateSandboxRequest, RegisterTemplateRequest
from orchestrator import SandboxManager
from orchestrator.guest_ports import ports_from_metadata
from orchestrator.sandbox_connections import data_plane_base_url
from handlers.modal_compat_support import (
    GENERIC_STATUS_SUCCESS,
    GENERIC_STATUS_TERMINATED,
    TASK_EXEC_FD_STDOUT,
    TASK_EXEC_FD_STDERR,
    _DEFAULT_ENVIRONMENT,
    _IMAGE_BUILDER_VERSION,
    _FS_TOOLS_PATH,
    _handler,
    ExecRecord,
    FileHandle,
    _state,
    _grpc_error,
    _unimplemented,
    _principal_from_stream,
    _recv,
    _send,
    _generic_result,
    _environment_response,
    _image_metadata,
    _task_logs,
    _template_row_for_image,
    _register_image,
    _sandbox_metadata,
    _resources,
    _task_id_for_sandbox,
    _tags_from_messages,
    _tag_messages,
    _modal_metadata,
    _row_created_timestamp,
    _tags_for_sandbox,
    _get_sandbox_row,
    _sandbox_is_terminal,
    _authorize_router,
    _sandbox_for_task_id,
    _signed_int32,
    _fs_output,
    _fs_error,
    _fs_error_message,
    _sandbox_info,
    _public_modal_gateway_url,
)

try:  # pragma: no cover - optional dependency in local dev shells.
    from grpclib import Status
    from grpclib.const import Cardinality
    from grpclib.server import Server
except ImportError:  # pragma: no cover
    Status = None  # type: ignore[assignment]
    Cardinality = None  # type: ignore[assignment]
    Server = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

_server: Any = None

class ModalClientCompatService:
    def __init__(self, sandbox_manager: SandboxManager) -> None:
        self.sandbox_manager = sandbox_manager

    def __mapping__(self) -> dict[str, Any]:
        h = _handler
        c = Cardinality
        return {
            "/modal.client.ModalClient/ClientHello": h(self.ClientHello, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/AuthTokenGet": h(self.AuthTokenGet, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/AppGetOrCreate": h(self.AppGetOrCreate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/AppCreate": h(self.AppCreate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ContainerFilesystemExec": h(self.ContainerFilesystemExec, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ContainerFilesystemExecGetOutput": h(self.ContainerFilesystemExecGetOutput, c.UNARY_STREAM, RawMessage, RawMessage),
            "/modal.client.ModalClient/EnvironmentGetOrCreate": h(self.EnvironmentGetOrCreate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/EnvironmentList": h(self.EnvironmentList, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ImageFromId": h(self.ImageFromId, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ImageGetOrCreate": h(self.ImageGetOrCreate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ImageJoinStreaming": h(self.ImageJoinStreaming, c.UNARY_STREAM, RawMessage, RawMessage),
            "/modal.client.ModalClient/ImageGetByTag": h(self.ImageGetByTag, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ImageListTags": h(self.ImageListTags, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/ImagePublish": h(self.ImagePublish, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxCreate": h(self.SandboxCreate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxCreateV2": h(self.SandboxCreateV2, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxWait": h(self.SandboxWait, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxWaitV2": h(self.SandboxWait, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxTerminate": h(self.SandboxTerminate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxTerminateV2": h(self.SandboxTerminate, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetTaskId": h(self.SandboxGetTaskId, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetTaskIdV2": h(self.SandboxGetTaskId, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/TaskGetCommandRouterAccess": h(self.TaskGetCommandRouterAccess, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetCommandRouterAccess": h(self.SandboxGetCommandRouterAccess, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxList": h(self.SandboxList, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxListV2": h(self.SandboxList, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetFromName": h(self.SandboxGetFromName, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetFromNameV2": h(self.SandboxGetFromName, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetTunnels": h(self.SandboxGetTunnels, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxGetTunnelsV2": h(self.SandboxGetTunnels, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxCreateConnectToken": h(self.SandboxCreateConnectToken, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxTagsGet": h(self.SandboxTagsGet, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxTagsGetV2": h(self.SandboxTagsGet, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxTagsSet": h(self.SandboxTagsSet, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxTagsSetV2": h(self.SandboxTagsSet, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxSnapshot": h(self.SandboxSnapshot, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxSnapshotWait": h(self.SandboxSnapshotWait, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxSnapshotGet": h(self.SandboxSnapshotGet, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxSnapshotFs": h(self.SandboxSnapshotFs, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/SandboxRestore": h(self.SandboxRestore, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/MountPutFile": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/BlobCreate": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.client.ModalClient/BlobGet": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
        }

    async def _principal(self, stream: Any) -> ApiKeyPrincipal:
        return _principal_from_stream(stream, self.sandbox_manager)

    async def ClientHello(self, stream: Any) -> None:
        await _recv(stream)
        await _send(stream, join([f_string(2, _IMAGE_BUILDER_VERSION)]))

    async def AuthTokenGet(self, stream: Any) -> None:
        principal = await self._principal(stream)
        await _recv(stream)
        token = f"modal-local-{principal.key_id}-{secrets.token_urlsafe(24)}"
        _state.auth_tokens[token] = principal
        await _send(stream, f_string(1, token))

    async def AppGetOrCreate(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        name = string(req, 1) or "sandbox"
        await _send(stream, f_string(1, f"ap-{hashlib.sha1(name.encode()).hexdigest()[:16]}"))

    async def AppCreate(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        name = string(req, 2) or "sandbox"
        await _send(stream, f_string(1, f"ap-{hashlib.sha1(name.encode()).hexdigest()[:16]}"))

    async def ContainerFilesystemExec(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = _sandbox_for_task_id(string(req, 10))
        if not sandbox_id or not _get_sandbox_row(self.sandbox_manager, sandbox_id):
            raise _grpc_error(Status.NOT_FOUND, "Sandbox task not found for filesystem operation.")

        if messages(req, 14):
            raise _unimplemented("sandbox file watch")

        file_descriptor = ""
        exec_id = ""
        if messages(req, 1):
            msg = fields(messages(req, 1)[-1])
            path = string(msg, 2)
            mode = string(msg, 3) or "r"
            raw_content = b""
            if "r" in mode or "a" in mode or "+" in mode:
                content = await run_io(self.sandbox_manager.read_file, sandbox_id, path)
                raw_content = (content or "").encode("utf-8")
                if content is None and "r" in mode and all(flag not in mode for flag in ("w", "a", "x", "+")):
                    exec_id = _fs_error(2, f"No such file: {path}")
            if not exec_id and ("w" in mode or "x" in mode):
                raw_content = b""
                ok = await run_io(self.sandbox_manager.write_file, sandbox_id, path, "")
                if not ok:
                    exec_id = _fs_error(5, f"Failed to open file for writing: {path}")
            if not exec_id:
                file_descriptor = f"fd-{secrets.token_hex(12)}"
                handle = FileHandle(sandbox_id=sandbox_id, path=path, mode=mode, data=bytearray(raw_content))
                if "a" in mode:
                    handle.pos = len(handle.data)
                _state.file_handles[file_descriptor] = handle
                exec_id = _fs_output()
            await _send(stream, join([f_string(1, exec_id), f_string(2, file_descriptor)]))
            return

        if messages(req, 2):
            msg = fields(messages(req, 2)[-1])
            fd = string(msg, 1)
            handle = _state.file_handles.get(fd)
            if not handle:
                await _send(stream, f_string(1, _fs_error(22, f"Invalid file descriptor: {fd}")))
                return
            data = bytes_value(msg, 2)
            end = handle.pos + len(data)
            if handle.pos > len(handle.data):
                handle.data.extend(b"\x00" * (handle.pos - len(handle.data)))
            handle.data[handle.pos:end] = data
            handle.pos = end
            await _send(stream, f_string(1, _fs_output()))
            return

        if messages(req, 3):
            msg = fields(messages(req, 3)[-1])
            fd = string(msg, 1)
            handle = _state.file_handles.get(fd)
            if not handle:
                await _send(stream, f_string(1, _fs_error(22, f"Invalid file descriptor: {fd}")))
                return
            if 2 in msg:
                n = max(0, integer(msg, 2, 0))
                out = bytes(handle.data[handle.pos : handle.pos + n])
            else:
                out = bytes(handle.data[handle.pos :])
            handle.pos += len(out)
            await _send(stream, f_string(1, _fs_output(out)))
            return

        if messages(req, 4):
            fd = string(fields(messages(req, 4)[-1]), 1)
            handle = _state.file_handles.get(fd)
            if not handle:
                await _send(stream, f_string(1, _fs_error(22, f"Invalid file descriptor: {fd}")))
                return
            ok = await run_io(
                self.sandbox_manager.write_file,
                handle.sandbox_id,
                handle.path,
                bytes(handle.data).decode("utf-8", "replace"),
            )
            await _send(stream, f_string(1, _fs_output() if ok else _fs_error(5, f"Failed to flush file: {handle.path}")))
            return

        if messages(req, 5):
            fd = string(fields(messages(req, 5)[-1]), 1)
            handle = _state.file_handles.get(fd)
            if not handle:
                await _send(stream, f_string(1, _fs_error(22, f"Invalid file descriptor: {fd}")))
                return
            data = bytes(handle.data)
            newline = data.find(b"\n", handle.pos)
            end = len(data) if newline < 0 else newline + 1
            out = data[handle.pos:end]
            handle.pos = end
            await _send(stream, f_string(1, _fs_output(out)))
            return

        if messages(req, 6):
            msg = fields(messages(req, 6)[-1])
            fd = string(msg, 1)
            handle = _state.file_handles.get(fd)
            if not handle:
                await _send(stream, f_string(1, _fs_error(22, f"Invalid file descriptor: {fd}")))
                return
            offset = _signed_int32(integer(msg, 2, 0))
            whence = integer(msg, 3, 0)
            if whence == 0:
                handle.pos = max(0, offset)
            elif whence == 1:
                handle.pos = max(0, handle.pos + offset)
            elif whence == 2:
                handle.pos = max(0, len(handle.data) + offset)
            await _send(stream, f_string(1, _fs_output()))
            return

        if messages(req, 9):
            fd = string(fields(messages(req, 9)[-1]), 1)
            handle = _state.file_handles.pop(fd, None)
            if not handle:
                await _send(stream, f_string(1, _fs_error(22, f"Invalid file descriptor: {fd}")))
                return
            ok = True
            if any(flag in handle.mode for flag in ("w", "a", "+", "x")):
                ok = await run_io(
                    self.sandbox_manager.write_file,
                    handle.sandbox_id,
                    handle.path,
                    bytes(handle.data).decode("utf-8", "replace"),
                )
            await _send(stream, f_string(1, _fs_output() if ok else _fs_error(5, f"Failed to close file: {handle.path}")))
            return

        if messages(req, 11):
            path = string(fields(messages(req, 11)[-1]), 1)
            entries = await run_io(self.sandbox_manager.list_files, sandbox_id, path)
            if entries is None:
                exec_id = _fs_error(2, f"No such directory: {path}")
            else:
                names = []
                for entry in entries:
                    name = str(entry.get("name") or "")
                    if not name:
                        name = posixpath.basename(str(entry.get("path") or "").rstrip("/"))
                    if name:
                        names.append(name)
                exec_id = _fs_output(json.dumps({"paths": names}).encode("utf-8"))
            await _send(stream, f_string(1, exec_id))
            return

        if messages(req, 12):
            msg = fields(messages(req, 12)[-1])
            path = string(msg, 1)
            ok = await run_io(self.sandbox_manager.create_directory, sandbox_id, path)
            await _send(stream, f_string(1, _fs_output() if ok else _fs_error(5, f"Failed to create directory: {path}")))
            return

        if messages(req, 13):
            msg = fields(messages(req, 13)[-1])
            path = string(msg, 1)
            recursive = boolean(msg, 2, False)
            ok = await run_io(self.sandbox_manager.delete_file, sandbox_id, path, recursive)
            await _send(stream, f_string(1, _fs_output() if ok else _fs_error(2, f"No such file or directory: {path}")))
            return

        await _send(stream, f_string(1, _fs_error(22, "Unsupported filesystem operation.")))

    async def ContainerFilesystemExecGetOutput(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        exec_id = string(req, 1)
        data, error = _state.file_exec_outputs.pop(exec_id, (b"", None))
        if error:
            await _send(stream, f_message(2, _fs_error_message(error[0], error[1])))
        elif data:
            await _send(stream, join([f_bytes(1, data), f_int(3, 0)]))
        await _send(stream, join([f_int(3, 1), f_bool(4, True)]))

    async def EnvironmentGetOrCreate(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        await _send(stream, _environment_response(string(req, 1) or _DEFAULT_ENVIRONMENT))

    async def EnvironmentList(self, stream: Any) -> None:
        await self._principal(stream)
        await _recv(stream)
        item = join(
            [
                f_string(1, _DEFAULT_ENVIRONMENT),
                f_string(2, "local"),
                f_double(3, time.time()),
                f_string(12, f"en-{_DEFAULT_ENVIRONMENT}"),
            ]
        )
        await _send(stream, f_message(2, item))

    async def ImageFromId(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        image_id = string(req, 1)
        row = _template_row_for_image(self.sandbox_manager, principal, image_id)
        if not row:
            raise _grpc_error(Status.NOT_FOUND, f"Modal image not found: {image_id}")
        await _send(stream, join([f_string(1, str(row.get("template_id") or image_id)), f_message(2, _image_metadata())]))

    async def ImageGetOrCreate(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        image_msg = bytes_value(req, 2)
        if not image_msg:
            raise _grpc_error(Status.INVALID_ARGUMENT, "ImageGetOrCreate requires image definition.")
        image_id, workdir = await _register_image(self.sandbox_manager, principal, image_msg)
        result = _generic_result(GENERIC_STATUS_SUCCESS)
        await _send(stream, join([f_string(1, image_id), f_message(2, result), f_message(3, _image_metadata(workdir=workdir))]))

    async def ImageJoinStreaming(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        image_id = string(req, 1)
        payload = join(
            [
                f_message(1, _generic_result(GENERIC_STATUS_SUCCESS)),
                f_message(2, _task_logs(f"Image {image_id} ready\n")),
                f_string(3, "1"),
                f_bool(4, True),
                f_message(5, _image_metadata()),
            ]
        )
        await _send(stream, payload)

    async def ImageGetByTag(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        tag = string(req, 1)
        row = self.sandbox_manager.db.get_sandbox_template_by_alias(principal.client_id, tag)
        if not row:
            raise _grpc_error(Status.NOT_FOUND, f"Modal image tag not found: {tag}")
        await _send(stream, f_string(1, str(row.get("template_id") or tag)))

    async def ImageListTags(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        tag_prefix = string(req, 2)
        rows = self.sandbox_manager.db.list_sandbox_templates(principal.client_id)
        parts = []
        for row in rows:
            tag = str(row.get("template_alias") or "")
            if tag_prefix and not tag.startswith(tag_prefix):
                continue
            parts.append(
                f_message(
                    1,
                    join(
                        [
                            f_string(1, tag),
                            f_string(2, str(row.get("template_id") or "")),
                            f_string(3, f"rev-{hashlib.sha1(str(row.get('template_id') or '').encode()).hexdigest()[:12]}"),
                            f_double(4, time.time()),
                            f_double(5, time.time()),
                        ]
                    ),
                )
            )
        await _send(stream, join(parts))

    async def ImagePublish(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        image_id = string(req, 1)
        tag = string(req, 2)
        row = _template_row_for_image(self.sandbox_manager, principal, image_id)
        if not row:
            raise _grpc_error(Status.NOT_FOUND, f"Modal image not found: {image_id}")
        warm_ref = str(row.get("warm_snapshot_image") or "").strip()
        registry_ref = str(row.get("registry_image_ref") or "").strip()
        image_ref = warm_ref or registry_ref or str(row.get("base_image") or image_id)
        storage_id = f"imtag-{hashlib.sha1((principal.client_id + ':' + tag).encode()).hexdigest()[:24]}"
        await run_io(
            self.sandbox_manager.db.upsert_sandbox_template,
            storage_id,
            image_ref,
            {},
            "",
            0,
            "",
            principal.client_id,
            principal.key_id,
            tag,
        )
        if str(row.get("source_kind") or "").strip():
            await run_io(
                self.sandbox_manager.db.set_template_build_source,
                storage_id,
                source_kind=str(row.get("source_kind") or ""),
                source_build_mode=str(row.get("source_build_mode") or "docker_cli"),
                dockerfile_text=str(row.get("dockerfile_text") or ""),
                build_args=dict(row.get("build_args") or {}),
                context_tar_gzip_base64=(row.get("context_tar_gzip_base64") or None),
            )
        await run_io(
            self.sandbox_manager.db.set_template_warm_snapshot,
            storage_id,
            warm_ref or image_ref,
            None,
            registry_image_ref=registry_ref or None,
            materialized_gateway_instance_id=str(row.get("materialized_gateway_instance_id") or "").strip() or None,
        )
        revision = f"rev-{hashlib.sha1((storage_id + ':' + tag).encode()).hexdigest()[:12]}"
        await _send(stream, join([f_string(1, storage_id), f_string(2, revision)]))

    async def _create_sandbox(self, stream: Any, *, v2: bool) -> None:
        principal = await self._principal(stream)
        req_raw = await _recv(stream)
        req_fields = fields(req_raw)
        definition = bytes_value(req_fields, 2)
        if not definition:
            raise _grpc_error(Status.INVALID_ARGUMENT, "SandboxCreate requires definition.")
        metadata, image_id, _name, timeout, workdir, _start_cmd = _sandbox_metadata(definition)
        tags = _tags_from_messages(messages(req_fields, 4))
        app_id = string(req_fields, 1)
        metadata["modal"]["app_id"] = app_id
        metadata["modal"]["tags"] = tags
        cpu_limit, memory_limit = _resources(definition)
        request = CreateSandboxRequest(
            template_id=image_id,
            metadata=metadata,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
        )
        sandbox_id = await run_io(
            self.sandbox_manager.create_sandbox,
            request.template_id,
            request.metadata,
            request.cpu_limit,
            request.memory_limit,
            request.timeout,
            None,
            principal.client_id,
            principal.key_id,
        )
        if not sandbox_id:
            raise _grpc_error(Status.UNAVAILABLE, "Failed to create sandbox.")
        task_id = _task_id_for_sandbox(sandbox_id)
        _state.task_to_sandbox[task_id] = sandbox_id
        _state.sandbox_tags[sandbox_id] = tags
        if v2:
            await _send(stream, join([f_string(1, sandbox_id), f_string(3, task_id)]))
        else:
            await _send(stream, f_string(1, sandbox_id))

    async def SandboxCreate(self, stream: Any) -> None:
        await self._create_sandbox(stream, v2=False)

    async def SandboxCreateV2(self, stream: Any) -> None:
        await self._create_sandbox(stream, v2=True)

    async def SandboxWait(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        if sandbox_id in _state.terminated_results:
            result = _generic_result(GENERIC_STATUS_TERMINATED, exit_code=_state.terminated_results[sandbox_id])
            await _send(stream, f_message(1, result))
            return
        row = _get_sandbox_row(self.sandbox_manager, sandbox_id)
        if not row:
            logger.info("Modal SandboxWait: sandbox not found sandbox_id=%s", sandbox_id)
            raise _grpc_error(Status.NOT_FOUND, f"Sandbox not found: {sandbox_id}")
        if _sandbox_is_terminal(row):
            result = _generic_result(GENERIC_STATUS_TERMINATED, exit_code=0)
            await _send(stream, f_message(1, result))
            return
        await _send(stream, b"")

    async def SandboxTerminate(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        ok = await run_io(self.sandbox_manager.kill_sandbox, sandbox_id)
        _state.terminated_results[sandbox_id] = 0 if ok else 1
        await _send(stream, f_message(1, _generic_result(GENERIC_STATUS_TERMINATED, exit_code=0 if ok else 1)))

    async def SandboxGetTaskId(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        if not _get_sandbox_row(self.sandbox_manager, sandbox_id):
            await _send(stream, f_message(2, _generic_result(GENERIC_STATUS_TERMINATED)))
            return
        task_id = _task_id_for_sandbox(sandbox_id)
        _state.task_to_sandbox[task_id] = sandbox_id
        await _send(stream, f_string(1, task_id))

    async def _send_command_router_access(self, stream: Any, target: str) -> None:
        if target and not _get_sandbox_row(self.sandbox_manager, target):
            raise _grpc_error(Status.NOT_FOUND, f"Sandbox not found: {target}")
        token = f"modal-router-{secrets.token_urlsafe(24)}"
        _state.router_tokens[token] = target
        await _send(stream, join([f_string(1, token), f_string(2, _public_modal_gateway_url())]))

    async def TaskGetCommandRouterAccess(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        task_id = string(req, 1)
        target = _sandbox_for_task_id(task_id)
        await self._send_command_router_access(stream, target)

    async def SandboxGetCommandRouterAccess(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        target = string(req, 1) or _state.task_to_sandbox.get(string(req, 2), "")
        await self._send_command_router_access(stream, target)

    async def SandboxList(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        app_id = string(req, 1)
        before_ts = double(req, 2, 0.0)
        include_finished = boolean(req, 4, False)
        tag_filter = _tags_from_messages(messages(req, 5))
        page_limit = 100
        fetch_limit = 500
        try:
            rows = self.sandbox_manager.db.list_sandboxes(
                limit=fetch_limit,
                offset=0,
                owner_client_id=principal.client_id,
            )
        except TypeError:
            rows = self.sandbox_manager.list_sandboxes(limit=fetch_limit, offset=0)
        rows = list(rows or [])

        filtered: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            if row.get("owner_client_id") and row.get("owner_client_id") != principal.client_id:
                continue
            if row.get("is_warm_pool"):
                continue
            modal_md = _modal_metadata(row)
            if not modal_md:
                continue
            if app_id and str(modal_md.get("app_id") or "") != app_id:
                continue
            if not include_finished and _sandbox_is_terminal(row):
                continue
            row_tags = _tags_for_sandbox(row)
            if any(row_tags.get(k) != v for k, v in tag_filter.items()):
                continue
            created_ts = _row_created_timestamp(row)
            if before_ts and created_ts >= before_ts:
                continue
            filtered.append((created_ts, row))

        filtered.sort(key=lambda item: item[0], reverse=True)
        await _send(stream, join(f_message(1, _sandbox_info(row)) for _, row in filtered[:page_limit]))

    async def SandboxGetFromName(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        name = string(req, 1)
        app_name = string(req, 3)
        app_id = f"ap-{hashlib.sha1(app_name.encode()).hexdigest()[:16]}" if app_name else ""
        rows = self.sandbox_manager.db.list_sandboxes(limit=200, offset=0)
        for row in rows:
            if row.get("owner_client_id") and row.get("owner_client_id") != principal.client_id:
                continue
            if _sandbox_is_terminal(row):
                continue
            modal_md = _modal_metadata(row)
            if app_id and str((modal_md or {}).get("app_id") or "") not in ("", app_id):
                continue
            if str((modal_md or {}).get("name") or "") == name:
                await _send(stream, f_string(1, str(row.get("sandbox_id") or "")))
                return
        raise _grpc_error(Status.NOT_FOUND, f"Sandbox name not found: {name}")

    async def SandboxGetTunnels(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        row = _get_sandbox_row(self.sandbox_manager, sandbox_id)
        if not row:
            raise _grpc_error(Status.NOT_FOUND, f"Sandbox not found: {sandbox_id}")
        parts = [f_message(1, _generic_result(GENERIC_STATUS_SUCCESS))]
        for port in ports_from_metadata(row.get("metadata") or {}):
            url = data_plane_base_url(get_config(), sandbox_id=sandbox_id, port=port, scheme="http")
            parts.append(
                f_message(
                    2,
                    join(
                        [
                            f_string(1, url),
                            f_int(2, 443),
                            f_string(3, url),
                            f_int(4, 80),
                            f_int(5, port),
                        ]
                    ),
                )
            )
        await _send(stream, join(parts))

    async def SandboxCreateConnectToken(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        port = integer(req, 3, 8080) or 8080
        url = data_plane_base_url(get_config(), sandbox_id=sandbox_id, port=port, scheme="http")
        await _send(stream, join([f_string(1, url), f_string(2, secrets.token_urlsafe(24))]))

    async def SandboxTagsGet(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        row = _get_sandbox_row(self.sandbox_manager, sandbox_id) or {}
        await _send(stream, join(_tag_messages(_tags_for_sandbox(row))))

    async def SandboxTagsSet(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 2) or string(req, 1)
        tags = _tags_from_messages(messages(req, 3))
        _state.sandbox_tags[sandbox_id] = tags
        row = _get_sandbox_row(self.sandbox_manager, sandbox_id)
        if row:
            modal_md = dict(_modal_metadata(row))
            modal_md["tags"] = tags
            try:
                await run_io(self.sandbox_manager.db.merge_sandbox_metadata, sandbox_id, {"modal": modal_md})
            except Exception:
                logger.debug("Modal SandboxTagsSet: metadata persist failed sandbox_id=%s", sandbox_id, exc_info=True)
        await _send(stream, b"")

    async def SandboxSnapshot(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        out = await run_io(self.sandbox_manager.create_filesystem_snapshot, sandbox_id, "modal-snapshot")
        if not out:
            raise _grpc_error(Status.INTERNAL, "Failed to create sandbox snapshot.")
        snapshot_id = str(out.get("snapshot_id") or f"sn-{secrets.token_hex(8)}")
        _state.snapshots[snapshot_id] = str(out.get("image_ref") or "")
        await _send(stream, f_string(1, snapshot_id))

    async def SandboxSnapshotWait(self, stream: Any) -> None:
        await self._principal(stream)
        await _recv(stream)
        await _send(stream, f_message(1, _generic_result(GENERIC_STATUS_SUCCESS)))

    async def SandboxSnapshotGet(self, stream: Any) -> None:
        await self._principal(stream)
        req = fields(await _recv(stream))
        snapshot_id = string(req, 1)
        if snapshot_id not in _state.snapshots and not self.sandbox_manager.db.get_sandbox_snapshot(snapshot_id):
            raise _grpc_error(Status.NOT_FOUND, f"Snapshot not found: {snapshot_id}")
        await _send(stream, f_string(1, snapshot_id))

    async def SandboxSnapshotFs(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        sandbox_id = string(req, 1)
        out = await run_io(self.sandbox_manager.create_filesystem_snapshot, sandbox_id, "modal-filesystem-snapshot")
        if not out:
            raise _grpc_error(Status.INTERNAL, "Failed to create filesystem snapshot.")
        image_id = f"im-{hashlib.sha256(str(out.get('image_ref') or '').encode()).hexdigest()[:24]}"
        await template_handlers.register_template(
            request=RegisterTemplateRequest(
                template_id=image_id,
                base_image=str(out.get("image_ref") or "python:3.11"),
                warm_snapshot_image=str(out.get("image_ref") or ""),
                settle_seconds=0,
            ),
            principal=principal,
            sandbox_manager=self.sandbox_manager,
        )
        await _send(stream, join([f_string(1, image_id), f_message(2, _generic_result(GENERIC_STATUS_SUCCESS)), f_message(3, _image_metadata())]))

    async def SandboxRestore(self, stream: Any) -> None:
        principal = await self._principal(stream)
        req = fields(await _recv(stream))
        snapshot_id = string(req, 1)
        row = self.sandbox_manager.db.get_sandbox_snapshot(snapshot_id, owner_client_id=principal.client_id)
        image_ref = str((row or {}).get("image_ref") or _state.snapshots.get(snapshot_id) or "").strip()
        if not image_ref:
            raise _grpc_error(Status.NOT_FOUND, f"Snapshot not found: {snapshot_id}")
        request = CreateSandboxRequest(
            template_id="python:3.11",
            metadata={"modal": {"restored_from_snapshot": snapshot_id}},
            from_snapshot_image=image_ref,
        )
        sandbox_id = await run_io(
            self.sandbox_manager.create_sandbox,
            request.template_id,
            request.metadata,
            request.cpu_limit,
            request.memory_limit,
            request.timeout,
            request.from_snapshot_image,
            principal.client_id,
            principal.key_id,
        )
        if not sandbox_id:
            raise _grpc_error(Status.UNAVAILABLE, "Failed to restore sandbox.")
        _state.task_to_sandbox[_task_id_for_sandbox(sandbox_id)] = sandbox_id
        await _send(stream, f_string(1, sandbox_id))

    async def Unsupported(self, stream: Any) -> None:
        await _recv(stream)
        raise _unimplemented("RPC")

class TaskCommandRouterCompatService:
    def __init__(self, sandbox_manager: SandboxManager) -> None:
        self.sandbox_manager = sandbox_manager

    def __mapping__(self) -> dict[str, Any]:
        h = _handler
        c = Cardinality
        return {
            "/modal.task_command_router.TaskCommandRouter/TaskExecStart": h(self.TaskExecStart, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskExecPoll": h(self.TaskExecPoll, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskExecWait": h(self.TaskExecWait, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskExecStdioRead": h(self.TaskExecStdioRead, c.UNARY_STREAM, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskExecStdinStatus": h(self.Empty, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskExecStdinWrite": h(self.Empty, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/SandboxWaitUntilReady": h(self.SandboxWaitUntilReady, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/SandboxStdinWriteV2": h(self.Empty, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/SandboxStdioReadV2": h(self.EmptyStream, c.UNARY_STREAM, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskReloadVolumes": h(self.Empty, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskContainerCreate": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskContainerGet": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskContainerList": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskContainerTerminate": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskContainerWait": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskMountDirectory": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskSnapshotDirectory": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskSnapshotFilesystem": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskSetNetworkAccess": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
            "/modal.task_command_router.TaskCommandRouter/TaskUnmountDirectory": h(self.Unsupported, c.UNARY_UNARY, RawMessage, RawMessage),
        }

    def _sandbox_for_task(self, task_id: str) -> str:
        sandbox_id = _state.task_to_sandbox.get(task_id)
        if sandbox_id:
            return sandbox_id
        if task_id.startswith("task-"):
            return task_id[5:]
        return ""

    async def TaskExecStart(self, stream: Any) -> None:
        req = fields(await _recv(stream))
        task_id = string(req, 1)
        _authorize_router(stream, task_id)
        exec_id = string(req, 2)
        args = strings(req, 3)
        workdir = string(req, 7) or None
        env = string_map(req, 12)
        sandbox_id = self._sandbox_for_task(task_id)
        if not sandbox_id or not _get_sandbox_row(self.sandbox_manager, sandbox_id):
            raise _grpc_error(Status.NOT_FOUND, f"Sandbox task not found: {task_id}")
        if not args:
            raise _grpc_error(Status.INVALID_ARGUMENT, "TaskExecStart requires command_args.")
        if args[0] == _FS_TOOLS_PATH:
            raise _unimplemented("sandbox filesystem tools")
        timeout = integer(req, 6, 0) or None
        result = await run_io(
            self.sandbox_manager.run_command,
            sandbox_id,
            shlex.join(args),
            workdir,
            env,
            timeout,
            None,
        )
        result = result or {"exit_code": -1, "stdout": "", "stderr": "exec failed"}
        _state.execs[(task_id, exec_id)] = ExecRecord(
            task_id=task_id,
            sandbox_id=sandbox_id,
            exec_id=exec_id,
            stdout=str(result.get("stdout") or "").encode("utf-8"),
            stderr=str(result.get("stderr") or "").encode("utf-8"),
            exit_code=int(result.get("exit_code") or 0),
        )
        await _send(stream, b"")

    async def TaskExecPoll(self, stream: Any) -> None:
        req = fields(await _recv(stream))
        task_id = string(req, 1)
        _authorize_router(stream, task_id)
        rec = _state.execs.get((task_id, string(req, 2)))
        await _send(stream, f_int(1, rec.exit_code if rec else 0))

    async def TaskExecWait(self, stream: Any) -> None:
        req = fields(await _recv(stream))
        task_id = string(req, 1)
        _authorize_router(stream, task_id)
        rec = _state.execs.get((task_id, string(req, 2)))
        await _send(stream, f_int(1, rec.exit_code if rec else 0))

    async def TaskExecStdioRead(self, stream: Any) -> None:
        req = fields(await _recv(stream))
        task_id = string(req, 1)
        _authorize_router(stream, task_id)
        rec = _state.execs.get((task_id, string(req, 2)))
        fd = integer(req, 4, TASK_EXEC_FD_STDOUT)
        if not rec:
            return
        data = rec.stderr if fd == TASK_EXEC_FD_STDERR else rec.stdout
        offset = integer(req, 3, 0)
        if offset < len(data):
            await _send(stream, f_bytes(1, data[offset:]))

    async def SandboxWaitUntilReady(self, stream: Any) -> None:
        req = fields(await _recv(stream))
        _authorize_router(stream, string(req, 1))
        await _send(stream, f_double(1, time.time()))

    async def Empty(self, stream: Any) -> None:
        await _recv(stream)
        _authorize_router(stream)
        await _send(stream, b"")

    async def EmptyStream(self, stream: Any) -> None:
        await _recv(stream)
        _authorize_router(stream)
        return

    async def Unsupported(self, stream: Any) -> None:
        await _recv(stream)
        _authorize_router(stream)
        raise _unimplemented("TaskCommandRouter RPC")

async def start_modal_compat_gateway(sandbox_manager: SandboxManager) -> None:
    global _server
    cfg = get_config()
    if not bool(getattr(cfg, "MODAL_COMPAT_GATEWAY_ENABLED", True)):
        logger.info("Modal compatibility gateway disabled")
        return
    if Server is None:
        logger.warning("Modal compatibility gateway unavailable: grpclib is not installed")
        return
    if _server is not None:
        return
    host = str(getattr(cfg, "MODAL_COMPAT_GATEWAY_HOST", "0.0.0.0") or "0.0.0.0")
    port = int(getattr(cfg, "MODAL_COMPAT_GATEWAY_PORT", 50051) or 50051)
    _server = Server([ModalClientCompatService(sandbox_manager), TaskCommandRouterCompatService(sandbox_manager)])
    await _server.start(host, port)
    logger.info("Modal compatibility gRPC gateway listening on %s:%s", host, port)

async def stop_modal_compat_gateway() -> None:
    global _server
    if _server is None:
        return
    _server.close()
    with contextlib.suppress(Exception):
        await _server.wait_closed()
    _server = None
