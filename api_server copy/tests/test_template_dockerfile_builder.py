from __future__ import annotations

import io
import sys
import tarfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import orchestrator.template_dockerfile_builder as builder


def _install_fake_parser(structure: list[dict[str, str]]) -> tuple[object, object]:
    class _FakeParser:
        def __init__(self) -> None:
            self.structure = []
            self._content = ""

        @property
        def content(self) -> str:
            return self._content

        @content.setter
        def content(self, value: str) -> None:
            self._content = value
            self.structure = structure

    old_parser = builder.DockerfileParser
    old_import_err = getattr(builder, "_IMPORT_ERR", None)
    builder.DockerfileParser = _FakeParser
    builder._IMPORT_ERR = None
    return old_parser, old_import_err


def _restore_parser(old_parser: object, old_import_err: object) -> None:
    builder.DockerfileParser = old_parser
    builder._IMPORT_ERR = old_import_err


def test_copy_directory_to_explicit_destination_copies_contents(tmp_path: Path) -> None:
    ctx = tmp_path / "ctx"
    src_dir = ctx / "agentlib"
    src_dir.mkdir(parents=True)
    (src_dir / "pyproject.toml").write_text("[project]\nname='agentlib'\n", encoding="utf-8")
    (src_dir / "README.md").write_text("hello\n", encoding="utf-8")

    archives: list[tuple[str, list[str]]] = []
    mkdir_cmds: list[str] = []

    def run_command(
        _container_id: str,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        user: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, object]:
        del cwd, env, user, timeout
        if command.startswith("mkdir -p "):
            mkdir_cmds.append(command)
        return {"exit_code": 0, "stdout": "", "stderr": "", "pid": 1}

    def put_archive_bytes(dest_root: str, data: bytes) -> bool:
        names: list[str] = []
        with tarfile.open(fileobj=io.BytesIO(data), mode="r") as tf:
            names = [m.name for m in tf.getmembers() if m.isfile()]
        archives.append((dest_root, names))
        return True

    old_parser, old_import_err = _install_fake_parser(
        [
            {"instruction": "FROM", "value": "python:3.12-slim"},
            {"instruction": "COPY", "value": "agentlib /app/agentlib"},
        ]
    )
    try:
        logs, env = builder.apply_dockerfile_inside_container(
            run_command=run_command,
            put_archive_bytes=put_archive_bytes,
            container_id="cid",
            dockerfile="FROM python:3.12-slim\nCOPY agentlib /app/agentlib\n",
            context_dir=ctx,
            build_args=None,
            run_timeout=30.0,
        )
    finally:
        _restore_parser(old_parser, old_import_err)

    assert env == {}
    assert any("mkdir -p /app/agentlib" in cmd for cmd in mkdir_cmds)
    assert archives == [("/app/agentlib", ["pyproject.toml", "README.md"])]
    assert any("COPY 'agentlib' -> '/app/agentlib' (dir contents)" in line for line in logs)


def test_run_preserves_shell_local_braced_variables() -> None:
    commands: list[str] = []
    run_value = (
        'REAL_BIN="$(readlink -f "$(which agent-browser)")" '
        '&& mv "$REAL_BIN" "${REAL_BIN}-real" '
        '&& printf \'#!/bin/bash\\nexec timeout "${AGENT_BROWSER_CMD_TIMEOUT:-20}" "%s" "$@"\\n\' '
        '"${REAL_BIN}-real" > "$REAL_BIN" '
        '&& chmod +x "$REAL_BIN"'
    )

    def run_command(
        _container_id: str,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        user: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, object]:
        del cwd, env, user, timeout
        commands.append(command)
        return {"exit_code": 0, "stdout": "", "stderr": "", "pid": 1}

    old_parser, old_import_err = _install_fake_parser(
        [
            {"instruction": "FROM", "value": "python:3.12-slim"},
            {"instruction": "RUN", "value": run_value},
        ]
    )
    try:
        builder.apply_dockerfile_inside_container(
            run_command=run_command,
            put_archive_bytes=lambda _dest_root, _data: True,
            container_id="cid",
            dockerfile=f"FROM python:3.12-slim\nRUN {run_value}\n",
            context_dir=None,
            build_args=None,
            run_timeout=30.0,
        )
    finally:
        _restore_parser(old_parser, old_import_err)

    assert commands == [run_value]
