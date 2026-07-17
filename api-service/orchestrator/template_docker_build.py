"""Build a local Docker image from a Dockerfile (E2B-style **self-hosted** counterpart).

E2B’s Python SDK (`e2b/template`) parses Dockerfiles and sends steps to **their** cloud builder.
This module runs ``docker build`` on the API host next to Docker Engine so ``POST /templates/from-dockerfile``
can register a ``template_id`` whose ``base_image`` is the built tag.
"""

from __future__ import annotations

import io
import re
import shutil
import tarfile
import tempfile
import uuid
from pathlib import Path
from typing import Callable

import docker

from config import get_config

from .envd_template_bake import (
    dockerfile_append_envd_layer,
    resolve_envd_restore_user_for_embed,
    write_envd_guest_build_context,
)


def _sanitize_for_tag(template_id: str) -> str:
    s = re.sub(r"[^a-z0-9._-]+", "-", template_id.strip().lower()).strip("-") or "tpl"
    return s[:48]


def _stringify_build_logs(entries: object) -> str:
    lines: list[str] = []
    for entry in entries if isinstance(entries, list) else list(entries or []):
        if isinstance(entry, dict):
            stream = str(entry.get("stream") or "")
            if stream:
                lines.append(stream)
                continue
            error = str(entry.get("error") or "")
            if error:
                lines.append(error)
                continue
            status = str(entry.get("status") or "")
            progress = str(entry.get("progress") or "")
            if status:
                lines.append(f"{status} {progress}".rstrip() + "\n")
        elif entry:
            lines.append(str(entry))
    return "".join(lines)


def build_image_from_dockerfile(
    *,
    dockerfile: str,
    image_tag: str | None,
    template_id: str,
    build_args: dict[str, str] | None,
    context_tar_gzip: bytes | None,
    build_timeout_sec: int,
    embed_envd: bool = False,
    log_callback: Callable[[str], None] | None = None,
) -> tuple[str, str]:
    """Build an image on the configured Docker Engine from a temp context directory.

    Uses Docker SDK so the API can drive a remote dockerd (for example the runtime-gateway DinD
    service) via ``DOCKER_HOST`` without requiring a Docker CLI binary in the API container.

    Returns ``(image_tag, combined_build_log)``. Raises ``RuntimeError`` on failure.
    """
    tag = (
        (image_tag or "").strip()
        or f"mysandbox-df-{_sanitize_for_tag(template_id)}:{uuid.uuid4().hex[:12]}"
    )
    tmp = Path(tempfile.mkdtemp(prefix="tpl-df-"))
    client = None
    try:
        if context_tar_gzip:
            buf = io.BytesIO(context_tar_gzip)
            with tarfile.open(fileobj=buf, mode="r:gz") as tf:
                tf.extractall(tmp)
        df_text = dockerfile
        if embed_envd:
            if write_envd_guest_build_context(tmp / "envd_guest"):
                cfg = get_config()
                ru = resolve_envd_restore_user_for_embed(
                    dockerfile,
                    str(getattr(cfg, "ENVD_DOCKERFILE_RESTORE_USER", "auto") or "auto"),
                )
                df_text = (
                    dockerfile.rstrip() + dockerfile_append_envd_layer(restore_user=ru)
                ).lstrip()
            # else: envd_guest missing on API host — build user Dockerfile only
        (tmp / "Dockerfile").write_text(df_text, encoding="utf-8")
        clean_build_args = {k.strip(): v for k, v in (build_args or {}).items() if k.strip()}
        cfg = get_config()
        client_timeout = max(
            60,
            int(max(build_timeout_sec, getattr(cfg, "TEMPLATE_DOCKER_CLIENT_TIMEOUT_SEC", 600) or 600)),
        )
        try:
            client = docker.from_env(timeout=client_timeout)
            build_logs = client.api.build(
                path=str(tmp),
                dockerfile="Dockerfile",
                tag=tag,
                rm=True,
                forcerm=True,
                pull=False,
                buildargs=clean_build_args or None,
                decode=True,
            )
            log_parts: list[str] = []
            for entry in build_logs:
                chunk = _stringify_build_logs([entry])
                if not chunk:
                    continue
                log_parts.append(chunk)
                if log_callback is not None:
                    log_callback(chunk)
                if isinstance(entry, dict) and entry.get("error"):
                    raise RuntimeError(f"docker build failed: {''.join(log_parts)[-12000:]}")
            log = "".join(log_parts)
        except docker.errors.BuildError as ex:
            log = _stringify_build_logs(getattr(ex, "build_log", None))
            detail = (str(ex) or log or "docker build failed").strip()
            raise RuntimeError(f"docker build failed: {(log or detail)[-12000:]}") from ex
        except docker.errors.APIError as ex:
            raise RuntimeError(f"Docker API build failed: {ex}") from ex
        return tag, log
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        shutil.rmtree(tmp, ignore_errors=True)
