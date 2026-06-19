"""Build a local Docker image from a Dockerfile (E2B-style **self-hosted** counterpart).

E2B’s Python SDK (`e2b/template`) parses Dockerfiles and sends steps to **their** cloud builder.
This module runs ``docker build`` on the API host next to Docker Engine so ``POST /templates/from-dockerfile``
can register a ``template_id`` whose ``base_image`` is the built tag.
"""

from __future__ import annotations

import io
import re
import shutil
import subprocess
import tarfile
import tempfile
import uuid
from pathlib import Path

from config import get_config

from .envd_template_bake import (
    dockerfile_append_envd_layer,
    resolve_envd_restore_user_for_embed,
    write_envd_guest_build_context,
)


def _sanitize_for_tag(template_id: str) -> str:
    s = re.sub(r"[^a-z0-9._-]+", "-", template_id.strip().lower()).strip("-") or "tpl"
    return s[:48]


def build_image_from_dockerfile(
    *,
    dockerfile: str,
    image_tag: str | None,
    template_id: str,
    build_args: dict[str, str] | None,
    context_tar_gzip: bytes | None,
    build_timeout_sec: int,
    embed_envd: bool = False,
) -> tuple[str, str]:
    """Run ``docker build`` in a temp directory.

    Returns ``(image_tag, combined_stdout_stderr)``. Raises ``RuntimeError`` on failure.
    """
    tag = (
        (image_tag or "").strip()
        or f"mysandbox-df-{_sanitize_for_tag(template_id)}:{uuid.uuid4().hex[:12]}"
    )
    tmp = Path(tempfile.mkdtemp(prefix="tpl-df-"))
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

        cmd: list[str] = ["docker", "build", "-t", tag]
        for k, v in (build_args or {}).items():
            if not k.strip():
                continue
            cmd.extend(["--build-arg", f"{k.strip()}={v}"])
        cmd.append(str(tmp))

        try:
            r = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max(60, int(build_timeout_sec)),
            )
        except subprocess.TimeoutExpired as ex:
            raise RuntimeError(f"docker build timed out after {build_timeout_sec}s") from ex
        log = f"{r.stdout or ''}\n{r.stderr or ''}"
        if r.returncode != 0:
            raise RuntimeError(f"docker build exit {r.returncode}: {log[-12000:]}")
        return tag, log
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
