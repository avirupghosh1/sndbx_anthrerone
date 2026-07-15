#!/usr/bin/env python3
"""Full generic my_sandbox_sdk smoke test.

This script validates the local SDK copy in ./my_sandbox_sdk and exercises the
generic API routes only. It does not call provider compatibility paths such as
/sandbox, /toolbox, /v2, or /v3.

Required for live mode:
  API_URL=http://127.0.0.1:8000 API_KEY=... python generic_client_full_smoke.py

Useful toggles:
  DRY_RUN=1              import + method surface check only
  TEMPLATE_ID=python:3.11
  RUN_TEMPLATE_BUILD=1
  RUN_SNAPSHOT=1
  RUN_GIT=1
  RUN_AGENT=1
  KEEP_SANDBOX=0
"""

from __future__ import annotations

import asyncio
import inspect
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parent.parent
SDK_ROOT = ROOT / "my_sandbox_sdk"
sys.path.insert(0, str(SDK_ROOT))

from my_sdk import (  # noqa: E402
    AsyncGit,
    AsyncPty,
    AsyncProcess,
    AsyncSandbox,
    AsyncTemplate,
    Git,
    Pty,
    Process,
    Sandbox,
    Template,
    wait_for_port,
    wait_for_timeout,
)
from my_sdk.api import APIEndpoints  # noqa: E402
from my_sdk.async_sdk.commands import AsyncCommands  # noqa: E402
from my_sdk.async_sdk.filesystem import AsyncFilesystem  # noqa: E402
from my_sdk.models import BuildInfo, SandboxInfo, SandboxLifecycle  # noqa: E402
from my_sdk.sync.commands import Commands  # noqa: E402
from my_sdk.sync.filesystem import Filesystem  # noqa: E402


EXPECTED_METHODS: dict[type[Any], set[str]] = {
    Sandbox: {
        "health",
        "ready",
        "diagnostics",
        "create",
        "list",
        "attach",
        "connect",
        "get_host",
        "refresh_envd_connection",
        "refresh_guest_connection",
        "refresh_e2b_connection",
        "open_websocket",
        "open_agent_websocket",
        "set_timeout",
        "info",
        "lifecycle",
        "create_snapshot",
        "list_snapshots",
        "is_running",
        "kill",
        "pause",
        "resume",
        "metrics",
        "system_metrics",
        "ports",
        "port_in_use",
        "set_labels",
        "set_network_settings",
        "set_public",
        "preview_url",
        "signed_preview_url",
        "expire_signed_preview_url",
        "create_ssh_access",
        "revoke_ssh_access",
        "validate_ssh_access",
        "spawn_agent",
        "list_agents",
        "get_agent",
        "kill_agent",
        "send_agent_message",
        "get_agent_messages",
    },
    Commands: {"run", "run_stream", "run_python", "list", "kill"},
    Filesystem: {
        "list",
        "read",
        "read_bytes",
        "info",
        "write",
        "write_bytes",
        "delete",
        "create_directory",
        "upload",
        "download",
        "download_bytes",
        "bulk_download",
        "bulk_upload",
        "move",
        "set_permissions",
        "search",
        "find",
        "replace",
        "exists",
    },
    Process: {
        "execute",
        "code_run",
        "create_session",
        "list_sessions",
        "get_session",
        "delete_session",
        "execute_session_command",
        "get_session_command",
        "get_session_command_logs",
        "send_session_command_input",
        "entrypoint_session",
        "entrypoint_logs",
    },
    Pty: {"create_session", "list_sessions", "get_session", "resize_session", "delete_session", "connect_url"},
    Git: {
        "init",
        "clone",
        "status",
        "branches",
        "create_branch",
        "checkout_branch",
        "delete_branch",
        "add",
        "commit",
        "pull",
        "push",
        "reset",
        "restore",
        "remotes",
        "remote_add",
        "set_config",
        "get_config",
        "configure_user",
        "dangerously_authenticate",
        "history",
    },
    Template: {
        "set_file_context_path",
        "from_base_image",
        "from_python_image",
        "from_ubuntu_image",
        "from_docker_image",
        "set_workdir",
        "set_user",
        "set_envs",
        "set_env",
        "copy",
        "run_cmd",
        "apt_install",
        "pip_install",
        "npm_install",
        "set_start_cmd",
        "set_settle_seconds",
        "use_dockerfile",
        "with_build_context",
        "from_dockerfile",
        "from_dockerfile_file",
        "set_build_arg",
        "set_post_build_start_cmd",
        "build",
        "build_stream",
        "build_in_background",
        "get_build_status",
        "list_registered",
        "get_registered",
    },
    AsyncSandbox: {
        "health",
        "ready",
        "diagnostics",
        "create",
        "list",
        "beta_create",
        "attach",
        "connect",
        "get_host",
        "refresh_envd_connection",
        "refresh_guest_connection",
        "refresh_e2b_connection",
        "open_websocket",
        "open_agent_websocket",
        "set_timeout",
        "info",
        "lifecycle",
        "create_snapshot",
        "list_snapshots",
        "is_running",
        "kill",
        "pause",
        "resume",
        "metrics",
        "system_metrics",
        "ports",
        "port_in_use",
        "set_labels",
        "set_network_settings",
        "set_public",
        "preview_url",
        "signed_preview_url",
        "expire_signed_preview_url",
        "create_ssh_access",
        "revoke_ssh_access",
        "validate_ssh_access",
        "spawn_agent",
        "list_agents",
        "get_agent",
        "kill_agent",
        "send_agent_message",
        "get_agent_messages",
    },
    AsyncCommands: {"run", "run_stream", "run_python", "list", "kill"},
    AsyncFilesystem: {
        "list",
        "read",
        "read_bytes",
        "info",
        "write",
        "write_bytes",
        "delete",
        "create_directory",
        "upload",
        "download",
        "download_bytes",
        "bulk_download",
        "bulk_upload",
        "move",
        "set_permissions",
        "search",
        "find",
        "replace",
        "exists",
    },
    AsyncProcess: {
        "execute",
        "code_run",
        "create_session",
        "list_sessions",
        "get_session",
        "delete_session",
        "execute_session_command",
        "get_session_command",
        "get_session_command_logs",
        "send_session_command_input",
        "entrypoint_session",
        "entrypoint_logs",
    },
    AsyncPty: {"create_session", "list_sessions", "get_session", "resize_session", "delete_session", "connect_url"},
    AsyncGit: {
        "init",
        "clone",
        "status",
        "branches",
        "create_branch",
        "checkout_branch",
        "delete_branch",
        "add",
        "commit",
        "pull",
        "push",
        "reset",
        "restore",
        "remotes",
        "remote_add",
        "set_config",
        "get_config",
        "configure_user",
        "dangerously_authenticate",
        "history",
    },
}


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", ""}


def public_methods(cls: type[Any]) -> set[str]:
    methods: set[str] = set()
    for name, value in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        raw = inspect.getattr_static(cls, name)
        if isinstance(raw, (staticmethod, classmethod)) or inspect.isfunction(value) or inspect.ismethod(value):
            methods.add(name)
    return methods


def check_surface() -> None:
    for cls, expected in EXPECTED_METHODS.items():
        missing = sorted(expected - public_methods(cls))
        if missing:
            raise AssertionError(f"{cls.__name__} missing methods: {missing}")
    bad_prefixes = ("/sandbox/", "/toolbox/", "/v2/", "/v3/")
    bad_exact = {"/sandbox", "/toolbox", "/v2", "/v3"}
    bad = []
    for name in dir(APIEndpoints):
        if name.startswith("_"):
            continue
        value = getattr(APIEndpoints, name)
        if isinstance(value, str) and (value in bad_exact or value.startswith(bad_prefixes)):
            bad.append((name, value))
    if bad:
        raise AssertionError(f"provider compatibility endpoint leaked into generic client constants: {bad}")
    print("surface_check=pass")


def ok_no_exception(_: Any) -> bool:
    return True


def ok_not_none(value: Any) -> bool:
    return value is not None


def ok_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def ok_dict(value: Any) -> bool:
    return isinstance(value, dict)


def ok_list(value: Any) -> bool:
    return isinstance(value, list)


def ok_true(value: Any) -> bool:
    return value is True


def ok_false(value: Any) -> bool:
    return value is False


def ok_bytes(value: Any) -> bool:
    return isinstance(value, (bytes, bytearray))


def ok_build_info(value: Any) -> bool:
    return isinstance(value, BuildInfo) and bool(value.template_id)


def ok_sandbox(value: Any) -> bool:
    return isinstance(value, Sandbox) and bool(value.sandbox_id)


def ok_sandbox_info(value: Any) -> bool:
    return isinstance(value, SandboxInfo) and bool(value.sandbox_id)


def ok_sandbox_lifecycle(value: Any) -> bool:
    return isinstance(value, SandboxLifecycle) and bool(value.sandbox_id)


def ok_command_success(value: Any) -> bool:
    return getattr(value, "exit_code", 1) == 0


def step(
    name: str,
    fn: Callable[[], Any],
    validator: Callable[[Any], bool] | None = None,
    validation: str = "no_exception",
) -> Any:
    started = time.monotonic()
    print(f"RUN {name}", flush=True)
    result = fn()
    validator = validator or ok_no_exception
    if not validator(result):
        raise AssertionError(f"{name} validation failed ({validation}): {result!r}")
    elapsed = time.monotonic() - started
    print(f"VALID {name} validation={validation}", flush=True)
    print(f"PASS {name} {elapsed:.3f}s", flush=True)
    return result


async def async_step(
    name: str,
    fn: Callable[[], Any],
    validator: Callable[[Any], bool] | None = None,
    validation: str = "no_exception",
) -> Any:
    started = time.monotonic()
    print(f"RUN {name}", flush=True)
    result = await fn()
    validator = validator or ok_no_exception
    if not validator(result):
        raise AssertionError(f"{name} validation failed ({validation}): {result!r}")
    elapsed = time.monotonic() - started
    print(f"VALID {name} validation={validation}", flush=True)
    print(f"PASS {name} {elapsed:.3f}s", flush=True)
    return result


def expect_not_implemented(name: str, fn: Callable[[], Any]) -> None:
    print(f"RUN {name}", flush=True)
    try:
        fn()
    except NotImplementedError:
        print(f"PASS {name} raised NotImplementedError", flush=True)
        return
    raise AssertionError(f"{name} did not raise NotImplementedError")


def require_api() -> tuple[str, str | None, float]:
    api_url = (os.getenv("API_URL") or os.getenv("SANDBOX_API_URL") or "").strip().rstrip("/")
    api_key = (os.getenv("API_KEY") or os.getenv("SANDBOX_API_KEY") or "").strip() or None
    timeout = float(os.getenv("REQUEST_TIMEOUT", "900"))
    if not api_url:
        raise SystemExit("API_URL is required for live mode. Use DRY_RUN=1 for import/surface validation only.")
    return api_url, api_key, timeout


def exercise_template_methods(api_url: str, api_key: str | None, request_timeout: float) -> str:
    suffix = uuid.uuid4().hex[:8]
    template_name = os.getenv("SMOKE_TEMPLATE_NAME", f"generic-sdk-smoke-{suffix}")
    stream_name = f"{template_name}-stream"
    with tempfile.TemporaryDirectory(prefix="generic-sdk-template-") as tmp:
        tmpdir = Path(tmp)
        src_file = tmpdir / "copy.txt"
        src_file.write_text("copy-ok\n", encoding="utf-8")
        dockerfile = tmpdir / "Dockerfile"
        dockerfile.write_text("FROM python:3.11\n", encoding="utf-8")

        context_dsl = Template()
        step(
            "template.dsl.set_file_context_path",
            lambda: context_dsl.set_file_context_path(str(tmpdir)),
            lambda value: value is context_dsl,
            "same_builder",
        )

        dsl = Template()
        returns_builder = lambda value: value is dsl
        step("template.dsl.from_base_image", lambda: dsl.from_base_image(), returns_builder, "same_builder")
        step("template.dsl.from_python_image", lambda: dsl.from_python_image("3.11"), returns_builder, "same_builder")
        step("template.dsl.from_ubuntu_image", lambda: dsl.from_ubuntu_image("22.04"), returns_builder, "same_builder")
        step("template.dsl.from_docker_image", lambda: dsl.from_docker_image("python:3.11"), returns_builder, "same_builder")
        step("template.dsl.set_workdir", lambda: dsl.set_workdir("/tmp"), returns_builder, "same_builder")
        step("template.dsl.set_user", lambda: dsl.set_user("root"), returns_builder, "same_builder")
        step("template.dsl.set_envs", lambda: dsl.set_envs({"GENERIC_SDK_SMOKE": "1"}), returns_builder, "same_builder")
        step("template.dsl.set_env", lambda: dsl.set_env("GENERIC_SDK_ONE", "yes"), returns_builder, "same_builder")
        step("template.dsl.copy", lambda: dsl.copy(str(src_file), "/tmp/copy.txt"), returns_builder, "same_builder")
        step("template.dsl.run_cmd", lambda: dsl.run_cmd("true"), returns_builder, "same_builder")
        step("template.dsl.apt_install", lambda: dsl.apt_install(["git"]), returns_builder, "same_builder")
        step("template.dsl.pip_install", lambda: dsl.pip_install(["pytest"]), returns_builder, "same_builder")
        step("template.dsl.npm_install", lambda: dsl.npm_install("install"), returns_builder, "same_builder")
        step("template.dsl.set_start_cmd.timeout", lambda: dsl.set_start_cmd("python3 -V", readiness=wait_for_timeout(0)), returns_builder, "same_builder")
        step("template.dsl.set_start_cmd.port", lambda: dsl.set_start_cmd("python3 -V", readiness=wait_for_port(49983)), returns_builder, "same_builder")
        step("template.dsl.set_settle_seconds", lambda: dsl.set_settle_seconds(0), returns_builder, "same_builder")
        step("template.dsl.use_dockerfile", lambda: dsl.use_dockerfile("FROM python:3.11\n"), returns_builder, "same_builder")
        step("template.dsl.with_build_context", lambda: dsl.with_build_context(str(tmpdir)), returns_builder, "same_builder")
        step("template.dsl.from_dockerfile", lambda: dsl.from_dockerfile(str(dockerfile), context_dir=str(tmpdir)), returns_builder, "same_builder")
        step("template.dsl.from_dockerfile_file", lambda: dsl.from_dockerfile_file(str(dockerfile)), returns_builder, "same_builder")
        step("template.dsl.set_build_arg", lambda: dsl.set_build_arg("GENERIC_SDK_ARG", "1"), returns_builder, "same_builder")
        step("template.dsl.set_post_build_start_cmd", lambda: dsl.set_post_build_start_cmd("true", settle_seconds=0), returns_builder, "same_builder")

    logical_template = (
        Template()
        .from_python_image("3.11")
        .set_env("GENERIC_SDK_SMOKE_TEMPLATE", "1")
        .set_settle_seconds(0)
    )
    build_info = step(
        "Template.build",
        lambda: Template.build(
            logical_template,
            template_name,
            api_url=api_url,
            api_key=api_key,
            request_timeout=request_timeout,
        ),
        ok_build_info,
        "BuildInfo.template_id",
    )
    step(
        "Template.build_stream",
        lambda: Template.build_stream(
            Template().from_python_image("3.11").set_settle_seconds(0),
            stream_name,
            api_url=api_url,
            api_key=api_key,
            request_timeout=request_timeout,
        ),
        ok_build_info,
        "BuildInfo.template_id",
    )
    expect_not_implemented("Template.build_in_background", lambda: Template.build_in_background(logical_template, template_name))
    expect_not_implemented("Template.get_build_status", lambda: Template.get_build_status(build_info))
    step("Template.list_registered", lambda: Template.list_registered(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_list, "list")
    step("Template.get_registered", lambda: Template.get_registered(template_name, api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_not_none, "template_record")
    return template_name


async def exercise_async_surface(api_url: str, api_key: str | None, request_timeout: float, sandbox_id: str) -> None:
    async_sandbox = step(
        "AsyncSandbox.attach",
        lambda: AsyncSandbox.attach(sandbox_id, api_url, api_key, request_timeout=request_timeout),
        lambda value: isinstance(value, AsyncSandbox) and value.sandbox_id == sandbox_id,
        "AsyncSandbox.sandbox_id",
    )
    await async_step("AsyncSandbox.health", lambda: AsyncSandbox.health(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_dict, "dict")
    await async_step("AsyncSandbox.ready", lambda: AsyncSandbox.ready(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_dict, "dict")
    await async_step("AsyncSandbox.diagnostics", lambda: AsyncSandbox.diagnostics(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_dict, "dict")
    await async_step("AsyncSandbox.list", lambda: AsyncSandbox.list(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_list, "list")
    await async_step("AsyncSandbox.info", lambda: async_sandbox.info(), ok_sandbox_info, "SandboxInfo")
    await async_step("AsyncSandbox.lifecycle", lambda: async_sandbox.lifecycle(), ok_sandbox_lifecycle, "SandboxLifecycle")
    await async_step(
        "AsyncCommands.run",
        lambda: async_sandbox.commands.run("echo async-command-ok", timeout=30),
        lambda value: ok_command_success(value) and "async-command-ok" in value.stdout,
        "exit_code_0_stdout",
    )
    await async_step("AsyncFilesystem.create_directory", lambda: async_sandbox.files.create_directory("/tmp/generic-sdk-async"))
    await async_step("AsyncFilesystem.write", lambda: async_sandbox.files.write("/tmp/generic-sdk-async/hello.txt", "async file ok\n"))
    await async_step(
        "AsyncFilesystem.read",
        lambda: async_sandbox.files.read("/tmp/generic-sdk-async/hello.txt"),
        lambda value: value == "async file ok\n",
        "exact_text",
    )
    await async_step("AsyncFilesystem.write_bytes", lambda: async_sandbox.files.write_bytes("/tmp/generic-sdk-async/raw.bin", b"async-bytes"))
    await async_step(
        "AsyncFilesystem.read_bytes",
        lambda: async_sandbox.files.read_bytes("/tmp/generic-sdk-async/raw.bin"),
        lambda value: value == b"async-bytes",
        "exact_bytes",
    )
    await async_step(
        "AsyncProcess.execute",
        lambda: async_sandbox.process.execute("echo async-process-ok", timeout=30),
        lambda value: "async-process-ok" in str(value),
        "contains_stdout",
    )
    await async_step("AsyncPty.create_session", lambda: async_sandbox.pty.create_session("generic-sdk-async-pty"), ok_dict, "dict")
    await async_step("AsyncPty.get_session", lambda: async_sandbox.pty.get_session("generic-sdk-async-pty"), ok_dict, "dict")
    await async_step("AsyncPty.resize_session", lambda: async_sandbox.pty.resize_session("generic-sdk-async-pty", 30, 90))
    await async_step("AsyncPty.delete_session", lambda: async_sandbox.pty.delete_session("generic-sdk-async-pty"))
    await async_step("AsyncSandbox.system_metrics", lambda: async_sandbox.system_metrics(), ok_dict, "dict")
    await async_step("AsyncSandbox.ports", lambda: async_sandbox.ports(), ok_dict, "dict")
    step("AsyncPty.connect_url", lambda: async_sandbox.pty.connect_url("generic-sdk-async-pty"), ok_str, "url")


def exercise_files(sandbox: Sandbox) -> None:
    root = "/tmp/generic-sdk-smoke"
    step("files.create_directory", lambda: sandbox.files.create_directory(root), ok_true, "true")
    step("files.write", lambda: sandbox.files.write(f"{root}/hello.txt", "hello generic client\nneedle\n"), ok_not_none, "write_info")
    step("files.read", lambda: sandbox.files.read(f"{root}/hello.txt"), lambda value: value.startswith("hello generic"), "text_prefix")
    step("files.write_bytes", lambda: sandbox.files.write_bytes(f"{root}/raw.bin", b"\x00generic-bytes\xff"), ok_not_none, "write_info")
    step("files.read_bytes", lambda: sandbox.files.read_bytes(f"{root}/raw.bin"), lambda value: value == b"\x00generic-bytes\xff", "exact_bytes")
    step("files.info", lambda: sandbox.files.info(f"{root}/hello.txt"), ok_dict, "dict")
    step("files.list", lambda: sandbox.files.list(root), ok_list, "list")
    step("files.exists.true", lambda: sandbox.files.exists(f"{root}/hello.txt"), ok_true, "true")
    step("files.bulk_upload", lambda: sandbox.files.bulk_upload({f"{root}/bulk-a.txt": b"a", f"{root}/bulk-b.txt": b"b"}), ok_true, "true")
    step("files.bulk_download", lambda: sandbox.files.bulk_download([f"{root}/bulk-a.txt", f"{root}/bulk-b.txt"]), ok_bytes, "bytes")
    step("files.search", lambda: sandbox.files.search(root, "*.txt"), ok_list, "list")
    step("files.find", lambda: sandbox.files.find(root, "needle"), ok_list, "list")
    step("files.replace", lambda: sandbox.files.replace([f"{root}/hello.txt"], "needle", "replaced"), ok_list, "list")
    step("files.read.after_replace", lambda: sandbox.files.read(f"{root}/hello.txt"), lambda value: "replaced" in value, "contains_replacement")
    step("files.move", lambda: sandbox.files.move(f"{root}/bulk-a.txt", f"{root}/moved.txt"), ok_true, "true")
    step("files.set_permissions", lambda: sandbox.files.set_permissions(f"{root}/moved.txt", mode="644"), ok_true, "true")
    with tempfile.TemporaryDirectory(prefix="generic-sdk-files-") as tmp:
        local_in = Path(tmp) / "upload.txt"
        local_out = Path(tmp) / "download.txt"
        local_in.write_text("upload local ok\n", encoding="utf-8")
        step("files.upload", lambda: sandbox.files.upload(str(local_in), f"{root}/uploaded.txt"), ok_not_none, "write_info")
        step("files.download", lambda: sandbox.files.download(f"{root}/uploaded.txt", str(local_out)), lambda value: value == len("upload local ok\n"), "byte_count")
        assert local_out.read_text(encoding="utf-8") == "upload local ok\n"
        step("files.download_bytes", lambda: sandbox.files.download_bytes(f"{root}/uploaded.txt"), lambda value: value == b"upload local ok\n", "exact_bytes")
    step("files.delete.file", lambda: sandbox.files.delete(f"{root}/uploaded.txt"), ok_true, "true")
    step("files.delete.recursive", lambda: sandbox.files.delete(root, recursive=True), ok_true, "true")
    step("files.exists.false", lambda: sandbox.files.exists(f"{root}/hello.txt"), ok_false, "false")


def exercise_commands(sandbox: Sandbox) -> None:
    step(
        "commands.run",
        lambda: sandbox.commands.run("echo command-ok", timeout=30),
        lambda value: ok_command_success(value) and "command-ok" in value.stdout,
        "exit_code_0_stdout",
    )
    step(
        "commands.run_stream",
        lambda: list(sandbox.commands.run_stream("printf stream-ok", timeout=30)),
        lambda value: any("stream-ok" in str(ev.get("chunk", "")) for ev in value),
        "stream_contains_output",
    )
    step(
        "commands.run_python",
        lambda: sandbox.commands.run_python("print('python-ok')", timeout=30),
        lambda value: ok_command_success(value) and "python-ok" in value.stdout,
        "exit_code_0_stdout",
    )
    step("commands.list", lambda: sandbox.commands.list(), ok_list, "list")
    step("commands.kill.nonexistent", lambda: sandbox.commands.kill(999999))


def exercise_process(sandbox: Sandbox) -> None:
    step("process.execute", lambda: sandbox.process.execute("echo process-ok", timeout=30), lambda value: "process-ok" in str(value), "contains_output")
    step("process.code_run", lambda: sandbox.process.code_run("print('code-run-ok')", timeout=30), lambda value: "code-run-ok" in str(value), "contains_output")
    session_id = f"generic-sdk-session-{uuid.uuid4().hex[:8]}"
    step("process.create_session", lambda: sandbox.process.create_session(session_id, cwd="/tmp"), lambda value: value.get("sessionId") == session_id or value.get("id") == session_id, "session_id")
    step("process.list_sessions", lambda: sandbox.process.list_sessions(), ok_list, "list")
    step("process.get_session", lambda: sandbox.process.get_session(session_id), ok_dict, "dict")
    command = step("process.execute_session_command", lambda: sandbox.process.execute_session_command(session_id, "pwd && echo session-ok", timeout=30), ok_dict, "dict")
    command_id = str(command.get("cmdId") or command.get("id") or "")
    if not command_id:
        raise AssertionError(f"process command response missing command id: {command}")
    step("process.get_session_command", lambda: sandbox.process.get_session_command(session_id, command_id), ok_dict, "dict")
    step("process.get_session_command_logs", lambda: sandbox.process.get_session_command_logs(session_id, command_id), ok_dict, "dict")
    step("process.send_session_command_input", lambda: sandbox.process.send_session_command_input(session_id, command_id, "ignored-input\n"), ok_true, "true")
    step("process.entrypoint_session", lambda: sandbox.process.entrypoint_session(), ok_dict, "dict")
    step("process.entrypoint_logs", lambda: sandbox.process.entrypoint_logs(), ok_dict, "dict")
    step("process.delete_session", lambda: sandbox.process.delete_session(session_id), ok_true, "true")


def exercise_pty(sandbox: Sandbox) -> None:
    session_id = f"generic-sdk-pty-{uuid.uuid4().hex[:8]}"
    step("pty.create_session", lambda: sandbox.pty.create_session(session_id, rows=24, cols=80, cwd="/tmp"), ok_dict, "dict")
    step("pty.list_sessions", lambda: sandbox.pty.list_sessions(), ok_dict, "dict")
    step("pty.get_session", lambda: sandbox.pty.get_session(session_id), ok_dict, "dict")
    step("pty.resize_session", lambda: sandbox.pty.resize_session(session_id, 32, 100), ok_dict, "dict")
    step(
        "pty.connect_url",
        lambda: sandbox.pty.connect_url(session_id),
        lambda value: f"/sandboxes/{sandbox.sandbox_id}/pty/sessions/{session_id}/connect" in value,
        "generic_ws_url",
    )
    step("pty.delete_session", lambda: sandbox.pty.delete_session(session_id), ok_true, "true")


def exercise_git(sandbox: Sandbox) -> None:
    git_version = sandbox.commands.run("git --version", timeout=30)
    if git_version.exit_code != 0:
        raise AssertionError(f"git is not available in sandbox: {git_version.stderr or git_version.stdout}")
    base = f"/tmp/generic-sdk-git-{uuid.uuid4().hex[:8]}"
    origin = f"{base}/origin.git"
    repo = f"{base}/repo"
    clone = f"{base}/clone"
    step("git.cleanup", lambda: sandbox.commands.run(f"rm -rf {base} && mkdir -p {base}", timeout=30), ok_command_success, "exit_code_0")
    step("git.init.bare", lambda: sandbox.git.init(origin, bare=True), ok_true, "true")
    step("git.init", lambda: sandbox.git.init(repo, initial_branch="main"), ok_true, "true")
    step("git.configure_user", lambda: sandbox.git.configure_user("SDK Smoke", "sdk-smoke@example.com", scope="local", path=repo), ok_true, "true")
    step("git.set_config", lambda: sandbox.git.set_config("smoke.value", "ok", scope="local", path=repo), ok_true, "true")
    step("git.get_config", lambda: sandbox.git.get_config("smoke.value", scope="local", path=repo), lambda value: value.get("value") == "ok", "config_value")
    step("git.write_file", lambda: sandbox.commands.run(f"printf 'hello git\\n' > {repo}/README.md", timeout=30), ok_command_success, "exit_code_0")
    step("git.add", lambda: sandbox.git.add(repo, ["README.md"]), ok_true, "true")
    step("git.commit", lambda: sandbox.git.commit(repo, "initial", author="SDK Smoke", email="sdk-smoke@example.com"), ok_dict, "dict")
    step("git.status", lambda: sandbox.git.status(repo), ok_dict, "dict")
    step("git.branches", lambda: sandbox.git.branches(repo), ok_dict, "dict")
    step("git.remote_add", lambda: sandbox.git.remote_add(repo, "origin", origin, overwrite=True), ok_true, "true")
    step("git.remotes", lambda: sandbox.git.remotes(repo), ok_dict, "dict")
    step("git.push.main", lambda: sandbox.git.push(repo, remote="origin", branch="main", set_upstream=True), ok_true, "true")
    step("git.clone", lambda: sandbox.git.clone(origin, clone), ok_true, "true")
    step("git.clone.configure_user", lambda: sandbox.git.configure_user("SDK Smoke", "sdk-smoke@example.com", scope="local", path=clone), ok_true, "true")
    step("git.create_branch", lambda: sandbox.git.create_branch(clone, "feature"), ok_true, "true")
    step("git.checkout_branch", lambda: sandbox.git.checkout_branch(clone, "feature"), ok_true, "true")
    step("git.write_feature", lambda: sandbox.commands.run(f"printf 'feature\\n' > {clone}/feature.txt", timeout=30), ok_command_success, "exit_code_0")
    step("git.add.feature", lambda: sandbox.git.add(clone, ["feature.txt"]), ok_true, "true")
    step("git.commit.feature", lambda: sandbox.git.commit(clone, "feature", author="SDK Smoke", email="sdk-smoke@example.com"), ok_dict, "dict")
    step("git.history", lambda: sandbox.git.history(clone), ok_list, "list")
    step("git.reset", lambda: sandbox.git.reset(clone, target="HEAD"), ok_true, "true")
    step("git.restore", lambda: sandbox.git.restore(clone, files=["feature.txt"], worktree=True), ok_true, "true")
    step("git.push.feature", lambda: sandbox.git.push(clone, remote="origin", branch="feature"), ok_true, "true")
    step("git.pull", lambda: sandbox.git.pull(clone, remote="origin", branch="main"), ok_true, "true")
    step("git.checkout.main", lambda: sandbox.git.checkout_branch(clone, "main"), ok_true, "true")
    step("git.delete_branch", lambda: sandbox.git.delete_branch(clone, "feature"), ok_true, "true")
    step("git.dangerously_authenticate", lambda: sandbox.git.dangerously_authenticate("user", "pass", host="example.com"), ok_true, "true")


def exercise_agent(sandbox: Sandbox) -> None:
    agent = step(
        "agents.spawn_agent",
        lambda: sandbox.spawn_agent(
            "generic-sdk-agent",
            agent_code="print('generic sdk agent ran')\n",
            config={"single_run": True},
            auto_start=True,
        ),
        lambda value: bool(value.get("agent_id")),
        "agent_id",
    )
    agent_id = str(agent.get("agent_id") or "")
    if not agent_id:
        raise AssertionError(f"spawn_agent response missing agent_id: {agent}")
    step("agents.list_agents", lambda: sandbox.list_agents(), ok_dict, "dict")
    step("agents.get_agent", lambda: sandbox.get_agent(agent_id), ok_dict, "dict")
    step("agents.send_agent_message", lambda: sandbox.send_agent_message(agent_id, {"hello": "world"}), ok_dict, "dict")
    step("agents.get_agent_messages", lambda: sandbox.get_agent_messages(agent_id, limit=10), ok_dict, "dict")
    step("agents.kill_agent", lambda: sandbox.kill_agent(agent_id, force=True), ok_dict, "dict")


def exercise_sandbox(api_url: str, api_key: str | None, request_timeout: float, template_id: str) -> None:
    sandbox: Sandbox | None = None
    keep = env_bool("KEEP_SANDBOX", False)
    try:
        step("Sandbox.health", lambda: Sandbox.health(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_dict, "dict")
        step("Sandbox.ready", lambda: Sandbox.ready(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_dict, "dict")
        step("Sandbox.diagnostics", lambda: Sandbox.diagnostics(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_dict, "dict")
        step("Sandbox.list.before", lambda: Sandbox.list(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_list, "list")
        sandbox = step(
            "Sandbox.create",
            lambda: Sandbox.create(
                api_url=api_url,
                api_key=api_key,
                template_id=template_id,
                metadata={"guest_ports": [49983, 8765], "smoke": "generic-client"},
                timeout=900,
                warmpool_size=0,
                request_timeout=request_timeout,
            ),
            ok_sandbox,
            "Sandbox.sandbox_id",
        )
        print(f"sandbox_id={sandbox.sandbox_id}")
        step("Sandbox.attach", lambda: Sandbox.attach(sandbox.sandbox_id, api_url, api_key, request_timeout=request_timeout), ok_sandbox, "Sandbox.sandbox_id")
        step("Sandbox.connect.without_e2b", lambda: Sandbox.connect(sandbox.sandbox_id, api_url=api_url, api_key=api_key, with_e2b=False, request_timeout=request_timeout), ok_sandbox, "Sandbox.sandbox_id")
        step("Sandbox.info", lambda: sandbox.info(), ok_sandbox_info, "SandboxInfo")
        step("Sandbox.lifecycle", lambda: sandbox.lifecycle(), ok_sandbox_lifecycle, "SandboxLifecycle")
        step("Sandbox.is_running", lambda: sandbox.is_running(), ok_true, "true")
        step("Sandbox.set_timeout", lambda: sandbox.set_timeout(900), lambda value: value is None, "none")
        step("Sandbox.refresh_envd_connection", lambda: sandbox.refresh_envd_connection())
        step("Sandbox.envd_connection.property", lambda: sandbox.envd_connection, ok_not_none, "not_none")
        step("Sandbox.envd_http_base_url.property", lambda: sandbox.envd_http_base_url, ok_str, "url")
        step("Sandbox.envd_api_url.property", lambda: sandbox.envd_api_url, ok_str, "url")
        step("Sandbox.envd_access_token.property", lambda: sandbox.envd_access_token, ok_str, "token")
        step("Sandbox.sandbox_domain.property", lambda: sandbox.sandbox_domain, ok_str, "domain")
        step("Sandbox.get_host", lambda: sandbox.get_host(49983), ok_str, "host")
        step("Sandbox.refresh_guest_connection", lambda: sandbox.refresh_guest_connection(49983, scheme="http"))
        step("Sandbox.refresh_e2b_connection", lambda: sandbox.refresh_e2b_connection(8765))
        step("Sandbox.e2b_connection.property", lambda: sandbox.e2b_connection, ok_not_none, "not_none")
        step("Sandbox.ws_url.property", lambda: sandbox.ws_url, ok_str, "url")
        step("Sandbox.traffic_access_token.property", lambda: sandbox.traffic_access_token, ok_str, "token")
        step("Sandbox.e2b_style_host.property", lambda: sandbox.e2b_style_host, ok_str, "host")
        try:
            step("Sandbox.open_websocket.construct", lambda: sandbox.open_websocket(8765))
            step("Sandbox.open_agent_websocket.construct", lambda: sandbox.open_agent_websocket())
        except RuntimeError as exc:
            print(f"SKIP websocket construct optional_dependency_missing detail={exc}")
        exercise_commands(sandbox)
        exercise_files(sandbox)
        exercise_process(sandbox)
        exercise_pty(sandbox)
        if env_bool("RUN_GIT", True):
            exercise_git(sandbox)
        step("Sandbox.metrics", lambda: sandbox.metrics(), ok_not_none, "metrics_model")
        step("Sandbox.system_metrics", lambda: sandbox.system_metrics(), ok_dict, "dict")
        step("Sandbox.ports", lambda: sandbox.ports(), ok_dict, "dict")
        step("Sandbox.port_in_use", lambda: sandbox.port_in_use(49983), lambda value: isinstance(value, bool), "bool")
        step("Sandbox.set_labels", lambda: sandbox.set_labels({"generic-sdk-smoke": "true"}), ok_dict, "dict")
        step("Sandbox.set_network_settings", lambda: sandbox.set_network_settings(networkBlockAll=False, networkAllowList=[], domainAllowList=[]), ok_dict, "dict")
        step("Sandbox.set_public.false", lambda: sandbox.set_public(False), ok_dict, "dict")
        step("Sandbox.preview_url", lambda: sandbox.preview_url(49983), ok_dict, "dict")
        signed = step("Sandbox.signed_preview_url", lambda: sandbox.signed_preview_url(49983, expires_in_seconds=60), ok_dict, "dict")
        signed_token = str(signed.get("token") or "")
        if signed_token:
            step("Sandbox.expire_signed_preview_url", lambda: sandbox.expire_signed_preview_url(49983, signed_token), ok_true, "true")
        ssh = step("Sandbox.create_ssh_access", lambda: sandbox.create_ssh_access(expires_in_minutes=5), lambda value: bool(value.get("token")), "token")
        ssh_token = str(ssh.get("token") or "")
        if not ssh_token:
            raise AssertionError(f"create_ssh_access response missing token: {ssh}")
        step(
            "Sandbox.validate_ssh_access.valid",
            lambda: Sandbox.validate_ssh_access(ssh_token, api_url=api_url, api_key=api_key, request_timeout=request_timeout),
            lambda value: value.get("valid") is True,
            "valid_true",
        )
        step("Sandbox.revoke_ssh_access", lambda: sandbox.revoke_ssh_access(ssh_token), ok_dict, "dict")
        step(
            "Sandbox.validate_ssh_access.revoked",
            lambda: Sandbox.validate_ssh_access(ssh_token, api_url=api_url, api_key=api_key, request_timeout=request_timeout),
            lambda value: value.get("valid") is False,
            "valid_false",
        )
        if env_bool("RUN_AGENT", True):
            exercise_agent(sandbox)
        if env_bool("RUN_SNAPSHOT", True):
            snap = step(
                "Sandbox.create_snapshot",
                lambda: sandbox.create_snapshot(f"generic-sdk-smoke-{uuid.uuid4().hex[:8]}"),
                lambda value: bool(getattr(value, "snapshot_id", None)),
                "snapshot_id",
            )
            if not snap.snapshot_id:
                raise AssertionError(f"snapshot missing id: {snap}")
            step("Sandbox.list_snapshots", lambda: sandbox.list_snapshots(limit=10), ok_list, "list")
        step("Async generic surface live subset", lambda: asyncio.run(exercise_async_surface(api_url, api_key, request_timeout, sandbox.sandbox_id)))
        step("Sandbox.pause", lambda: sandbox.pause(), ok_true, "true")
        step("Sandbox.resume", lambda: sandbox.resume(), ok_true, "true")
    finally:
        if sandbox is not None and not keep:
            try:
                step("Sandbox.kill.cleanup", lambda: sandbox.kill(request_timeout=request_timeout), lambda value: isinstance(value, bool), "bool")
            except Exception as exc:
                print(f"WARN cleanup failed sandbox_id={sandbox.sandbox_id} detail={exc}", flush=True)
        elif sandbox is not None:
            print(f"KEEP_SANDBOX=1 sandbox_id={sandbox.sandbox_id}", flush=True)


async def exercise_async_template_methods(api_url: str, api_key: str | None, request_timeout: float) -> None:
    suffix = uuid.uuid4().hex[:8]
    name = f"generic-sdk-async-template-{suffix}"
    info = await async_step(
        "AsyncTemplate.build",
        lambda: AsyncTemplate.build(
            Template().from_python_image("3.11").set_settle_seconds(0),
            name,
            api_url=api_url,
            api_key=api_key,
            request_timeout=request_timeout,
        ),
        ok_build_info,
        "BuildInfo.template_id",
    )
    await async_step(
        "AsyncTemplate.build_stream",
        lambda: AsyncTemplate.build_stream(
            Template().from_python_image("3.11").set_settle_seconds(0),
            f"{name}-stream",
            api_url=api_url,
            api_key=api_key,
            request_timeout=request_timeout,
        ),
        ok_build_info,
        "BuildInfo.template_id",
    )
    try:
        await AsyncTemplate.build_in_background(Template(), name, api_url=api_url, api_key=api_key)
    except NotImplementedError:
        pass
    else:
        raise AssertionError("AsyncTemplate.build_in_background did not raise NotImplementedError")
    try:
        await AsyncTemplate.get_build_status(info)
    except NotImplementedError:
        pass
    else:
        raise AssertionError("AsyncTemplate.get_build_status did not raise NotImplementedError")
    await async_step("AsyncTemplate.list_registered", lambda: AsyncTemplate.list_registered(api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_list, "list")
    await async_step("AsyncTemplate.get_registered", lambda: AsyncTemplate.get_registered(name, api_url=api_url, api_key=api_key, request_timeout=request_timeout), ok_not_none, "template_record")


def main() -> None:
    check_surface()
    if env_bool("DRY_RUN", False):
        print("dry_run=pass")
        return

    api_url, api_key, request_timeout = require_api()
    if env_bool("RUN_TEMPLATE_BUILD", True):
        template_id = exercise_template_methods(api_url, api_key, request_timeout)
        step("AsyncTemplate methods", lambda: asyncio.run(exercise_async_template_methods(api_url, api_key, request_timeout)))
    else:
        template_id = os.getenv("TEMPLATE_ID", "python:3.11")
    template_id = os.getenv("TEMPLATE_ID", template_id)
    print(f"using_template_id={template_id}")
    exercise_sandbox(api_url, api_key, request_timeout, template_id)
    print("generic_client_full_smoke=pass")


if __name__ == "__main__":
    main()
