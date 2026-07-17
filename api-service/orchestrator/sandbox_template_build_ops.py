"""Template image build operations for SandboxManager."""

from __future__ import annotations

import base64
import logging
import re
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from .container_manager import ContainerConfig
from .sandbox_constants import ENVD_TEMPLATE_BAKED_ENV
from .envd_template_bake import bake_envd_guest_into_container, should_embed_envd_at_template_build
from .runtime_gateway_templates import build_template_snapshot_via_gateway, gateway_template_build_enabled
from .runtime_utils import is_container_like_execution

logger = logging.getLogger(__name__)

class SandboxTemplateBuildMixin:
    def sync_warm_pool_default_segment(self, template_id: str, warm_ref: str) -> None:
        """Rebuild the **default** warm-pool segment to use ``warm_ref`` when it matches pool template_id.

        Without this, ``MultiWarmSandboxPool.start()`` provisions from ``from_snapshot_image=None``
        (raw ``python:3.11``) while ``POST /sandboxes`` uses ``warm_snapshot_image`` — different images.
        """
        pool = getattr(self, "warm_pool", None)
        if pool is None:
            return
        pid = (self._config.SANDBOX_WARM_POOL_TEMPLATE_ID or self._config.DEFAULT_TEMPLATE).strip()
        if (template_id or "").strip() != pid:
            return
        ref = (warm_ref or "").strip()
        if not ref:
            return
        key = self.warm_pool_key(
            pid,
            self._config.SANDBOX_WARM_POOL_CPU or self._config.DEFAULT_CPU_LIMIT,
            self._config.SANDBOX_WARM_POOL_MEMORY or self._config.DEFAULT_MEMORY_LIMIT,
            int(self._config.SANDBOX_WARM_POOL_TIMEOUT or self._config.DEFAULT_TIMEOUT),
        )
        segment = self.db.get_warm_pool_segment(key)
        desired_size = int(
            (segment or {}).get("desired_size")
            or getattr(self._config, "SANDBOX_WARM_POOL_SIZE", 0)
            or 0
        )
        if desired_size <= 0:
            return
        pool.ensure_pool_for(
            pid,
            self._config.SANDBOX_WARM_POOL_CPU or self._config.DEFAULT_CPU_LIMIT,
            self._config.SANDBOX_WARM_POOL_MEMORY or self._config.DEFAULT_MEMORY_LIMIT,
            int(self._config.SANDBOX_WARM_POOL_TIMEOUT or self._config.DEFAULT_TIMEOUT),
            ref,
            desired_size=desired_size,
        )
        logger.info(
            "Warm pool default segment now uses warm_snapshot_image for template_id=%r",
            pid,
        )

    def _build_registered_template_snapshot(self, template_id: str) -> bool:
        """One-time: base image + env + ``start_cmd`` + settle, then ``docker commit``."""
        lock = self._template_lock(template_id)
        with lock:
            row = self.db.get_sandbox_template(template_id)
            if not row:
                return False
            if row.get("warm_snapshot_image"):
                return True

            cfg = self._config
            if gateway_template_build_enabled(cfg):
                target = self._gateway_target_for_template_row(row)
                try:
                    result = build_template_snapshot_via_gateway(
                        cfg,
                        template_id=template_id,
                        base_image=str(row.get("base_image") or ""),
                        env=dict(row.get("env") or {}),
                        start_cmd=str(row.get("start_cmd") or ""),
                        settle_seconds=int(row.get("settle_seconds") or 20),
                        ready_cmd=str(row.get("ready_cmd") or ""),
                        embed_envd=bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
                        envd_pip_timeout_sec=float(
                            getattr(cfg, "ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", 300.0) or 300.0
                        ),
                        snapshot_repo=str(
                            getattr(cfg, "SANDBOX_SNAPSHOT_REPO", "mysandbox-snap") or "mysandbox-snap"
                        ),
                        gateway_api_base=(target.api_base if target else None),
                    )
                except RuntimeError as ex:
                    self.db.set_template_build_error(template_id, str(ex))
                    return False
                image_ref = str(result.get("image_ref") or "").strip()
                registry_ref = str(result.get("registry_image_ref") or "").strip()
                gateway_instance_id = str(result.get("gateway_instance_id") or "").strip()
                if not image_ref:
                    self.db.set_template_build_error(
                        template_id, "runtime-gateway template snapshot produced no image"
                    )
                    return False
                self.db.set_template_warm_snapshot(
                    template_id,
                    image_ref,
                    None,
                    registry_image_ref=registry_ref or None,
                    materialized_gateway_instance_id=gateway_instance_id or None,
                )
                if bool(getattr(cfg, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)):
                    self._mark_template_envd_baked(template_id)
                logger.info("Template %s warm snapshot (runtime-gateway): %s registry=%s", template_id, image_ref, registry_ref or "-")
                self.sync_warm_pool_default_segment(template_id, image_ref)
                return True

            name = f"tpl-build-{uuid.uuid4().hex[:10]}"
            env = dict(row.get("env") or {})
            build_timeout = max(int(row.get("settle_seconds") or 20) + 900, 1800)
            cfg_build = ContainerConfig(
                image=row["base_image"],
                cpu_limit=cfg.TEMPLATE_BUILD_CPU,
                memory_limit=cfg.TEMPLATE_BUILD_MEMORY,
                timeout=build_timeout,
                environment=env if env else None,
            )
            cid = self.execution.create_container(name, cfg_build)
            if not cid:
                self.db.set_template_build_error(template_id, "create_container failed for template build")
                return False
            try:
                sc = (row.get("start_cmd") or "").strip()
                if sc:
                    r = self.execution.run_command(cid, sc, timeout=3600.0)
                    ec = int(r.get("exit_code") or 0)
                    if ec != 0:
                        logger.warning(
                            "Template %s start_cmd non-zero exit=%s stderr=%s",
                            template_id,
                            ec,
                            (r.get("stderr") or "")[:2000],
                        )
                settle = max(0, min(int(row.get("settle_seconds") or 20), 600))
                time.sleep(settle)
                ready = (row.get("ready_cmd") or "").strip()
                if ready:
                    deadline = time.monotonic() + max(
                        30.0, float(getattr(cfg, "TEMPLATE_READY_TIMEOUT_SEC", 600) or 600)
                    )
                    poll_s = 2.0
                    ok_ready = False
                    while time.monotonic() < deadline:
                        rr = self.execution.run_command(cid, ready, timeout=120.0)
                        if int(rr.get("exit_code") or 0) == 0:
                            ok_ready = True
                            break
                        time.sleep(poll_s)
                    if not ok_ready:
                        self.db.set_template_build_error(
                            template_id,
                            "ready_cmd did not exit 0 before TEMPLATE_READY_TIMEOUT_SEC",
                        )
                        return False
                if should_embed_envd_at_template_build(cfg) and is_container_like_execution(self.execution):
                    pt = float(getattr(cfg, "ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", 300.0) or 300.0)
                    bake_envd_guest_into_container(
                        put_archive_to_container=self.execution.put_archive_to_container,
                        run_command=self.execution.run_command,
                        container_id=cid,
                        pip_timeout_sec=pt,
                    )
                repo = (cfg.SANDBOX_SNAPSHOT_REPO or "mysandbox-snap").strip().lower().replace("/", "-") or "mysandbox-snap"
                tag_raw = f"tpl-{template_id}-{uuid.uuid4().hex[:10]}"
                tag = re.sub(r"[^a-z0-9._-]", "-", tag_raw.lower())[:120] or "tpl"
                commit_fn = getattr(self.execution, "commit_filesystem_snapshot", None)
                if not callable(commit_fn):
                    self.db.set_template_build_error(template_id, "commit_filesystem_snapshot unavailable")
                    return False
                image_ref = commit_fn(cid, repo, tag)
                if not image_ref:
                    self.db.set_template_build_error(template_id, "docker commit failed")
                    return False
                self.db.set_template_warm_snapshot(template_id, image_ref, None)
                if should_embed_envd_at_template_build(cfg):
                    self._mark_template_envd_baked(template_id)
                logger.info("Template %s warm snapshot: %s", template_id, image_ref)
                self.sync_warm_pool_default_segment(template_id, image_ref)
                return True
            finally:
                self.execution.kill_container(cid, force=True)

    def build_template_from_dockerfile_parsed(
        self,
        template_id: str,
        dockerfile: str,
        env: Optional[Dict[str, Any]],
        start_cmd: str,
        settle_seconds: int,
        ready_cmd: str,
        build_args: Optional[Dict[str, str]],
        context_tar_gzip: Optional[bytes],
        image_tag: Optional[str],
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
        template_alias: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Parse Dockerfile, apply ``RUN``/``COPY``/… inside a build container, ``docker commit``, set warm snapshot.

        Same end state as ``_build_registered_template_snapshot`` (``warm_snapshot_image`` populated) so
        ``POST /sandboxes`` and the **warm pool** reuse this image without a second bake.
        """
        import io as _io
        import tarfile as _tarfile

        from .template_dockerfile_builder import (
            apply_dockerfile_inside_container,
            extract_base_image_from_dockerfile,
            extract_start_cmd_from_dockerfile,
        )

        plane: Any = self.execution
        if not hasattr(plane, "put_archive_to_container"):
            raise RuntimeError("Parsed Dockerfile builds require Docker ContainerManager.put_archive_to_container")

        base_image = extract_base_image_from_dockerfile(dockerfile)
        cfg = self._config
        tmp = Path(tempfile.mkdtemp(prefix="tpl-parse-"))
        cid: Optional[str] = None
        merge_env = dict(env or {})
        merged_template_env = dict(merge_env)
        image_ref: Optional[str] = None
        try:
            if context_tar_gzip:
                buf = _io.BytesIO(context_tar_gzip)
                with _tarfile.open(fileobj=buf, mode="r:gz") as tf:
                    try:
                        tf.extractall(tmp, filter="data")
                    except TypeError:
                        tf.extractall(tmp)

            name = f"tpl-parse-{uuid.uuid4().hex[:10]}"
            build_timeout = max(int(settle_seconds or 0) + 900, 1800)
            cfg_build = ContainerConfig(
                image=base_image,
                cpu_limit=cfg.TEMPLATE_BUILD_CPU,
                memory_limit=cfg.TEMPLATE_BUILD_MEMORY,
                timeout=build_timeout,
                environment=merge_env if merge_env else None,
            )
            cid = plane.create_container(name, cfg_build)
            if not cid:
                raise RuntimeError("create_container failed for Dockerfile template build")

            def _put(parent: str, data: bytes) -> bool:
                fn = getattr(plane, "put_archive_to_container", None)
                if not callable(fn):
                    return False
                return bool(fn(cid, parent, data))

            _, env_from_dockerfile = apply_dockerfile_inside_container(
                run_command=plane.run_command,
                put_archive_bytes=_put,
                container_id=cid,
                dockerfile=dockerfile,
                context_dir=tmp,
                build_args=build_args,
                run_timeout=float(getattr(cfg, "TEMPLATE_DOCKERFILE_RUN_TIMEOUT_SEC", 7200.0) or 7200.0),
            )
            merged_template_env.update(env_from_dockerfile)

            if should_embed_envd_at_template_build(cfg):
                pa = getattr(plane, "put_archive_to_container", None)
                if callable(pa):
                    pt = float(getattr(cfg, "ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", 300.0) or 300.0)
                    if bake_envd_guest_into_container(
                        put_archive_to_container=pa,
                        run_command=plane.run_command,
                        container_id=cid,
                        pip_timeout_sec=pt,
                    ):
                        merged_template_env[ENVD_TEMPLATE_BAKED_ENV] = "true"

            sc = (start_cmd or "").strip()
            if sc:
                r = plane.run_command(
                    cid,
                    sc,
                    timeout=3600.0,
                    env=merged_template_env if merged_template_env else None,
                )
                if int(r.get("exit_code") or 0) != 0:
                    logger.warning(
                        "Template %s post-Dockerfile start_cmd non-zero exit=%s",
                        template_id,
                        r.get("exit_code"),
                    )

            settle = max(0, min(int(settle_seconds or 20), 600))
            time.sleep(settle)
            ready = (ready_cmd or "").strip()
            if ready:
                deadline = time.monotonic() + max(
                    30.0, float(getattr(cfg, "TEMPLATE_READY_TIMEOUT_SEC", 600) or 600)
                )
                ok_ready = False
                while time.monotonic() < deadline:
                    rr = plane.run_command(
                        cid,
                        ready,
                        timeout=120.0,
                        env=merged_template_env if merged_template_env else None,
                    )
                    if int(rr.get("exit_code") or 0) == 0:
                        ok_ready = True
                        break
                    time.sleep(2.0)
                if not ok_ready:
                    raise RuntimeError("ready_cmd did not succeed within TEMPLATE_READY_TIMEOUT_SEC")

            repo_default = (cfg.SANDBOX_SNAPSHOT_REPO or "mysandbox-snap").strip().lower().replace("/", "-") or "mysandbox-snap"
            full_tag = (image_tag or "").strip()
            if full_tag and ":" in full_tag:
                rep, tg = full_tag.rsplit(":", 1)
                rep = rep.strip() or repo_default
                tg = re.sub(r"[^a-z0-9._-]", "-", (tg or "latest").lower())[:120] or "latest"
            elif full_tag:
                rep, tg = full_tag, "latest"
            else:
                rep = repo_default
                tg = re.sub(
                    r"[^a-z0-9._-]",
                    "-",
                    f"tpl-{template_id}-{uuid.uuid4().hex[:10]}".lower(),
                )[:120] or "tpl"

            commit_fn = getattr(plane, "commit_filesystem_snapshot", None)
            if not callable(commit_fn):
                raise RuntimeError("commit_filesystem_snapshot unavailable")
            image_ref = commit_fn(cid, rep, tg)
            if not image_ref:
                raise RuntimeError("docker commit failed after parsed Dockerfile build")
        finally:
            if cid:
                try:
                    plane.kill_container(cid, force=True)
                except Exception:
                    pass
            shutil.rmtree(tmp, ignore_errors=True)

        if not image_ref:
            raise RuntimeError("Dockerfile template build produced no image")

        self.db.upsert_sandbox_template(
            template_id,
            image_ref,
            merged_template_env,
            (start_cmd or "").strip() or extract_start_cmd_from_dockerfile(dockerfile),
            int(settle_seconds or 0),
            (ready_cmd or "").strip(),
            owner_client_id,
            owner_api_key_id,
            template_alias or template_id,
        )
        self.db.set_template_build_source(
            template_id,
            source_kind="dockerfile",
            source_build_mode="parsed",
            dockerfile_text=dockerfile,
            build_args=build_args or {},
            context_tar_gzip_base64=(
                base64.b64encode(context_tar_gzip).decode("ascii") if context_tar_gzip else None
            ),
        )
        if not self.db.set_template_warm_snapshot(template_id, image_ref, None):
            raise RuntimeError(
                f"set_template_warm_snapshot failed for template_id={template_id!r} "
                f"(image_ref={image_ref!r}); template row missing after upsert — check DATABASE_URL / DB."
            )
        if str(merged_template_env.get(ENVD_TEMPLATE_BAKED_ENV) or "").strip().lower() in ("1", "true", "yes", "on"):
            self._mark_template_envd_baked(template_id)
        logger.info("Template %s (parsed Dockerfile) warm snapshot: %s", template_id, image_ref)
        self.sync_warm_pool_default_segment(template_id, image_ref)
        return self.db.get_sandbox_template(template_id) or {}
