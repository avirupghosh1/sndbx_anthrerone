"""Sandbox management over Docker Engine, Firecracker microVMs, or Lima VMs."""

import json
import logging
import os
import re
import secrets
import shlex
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from .container_manager import ContainerManager, ContainerConfig
from orchestrator.runtime_utils import is_container_like_execution, is_k8s_execution
from orchestrator.guest_ports import resolve_guest_ports


def _create_env_from_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Merge SDK ``metadata.e2b_shim_envs`` / ``metadata.env`` into pod environment."""
    md = metadata or {}
    out: Dict[str, str] = {}
    for key in ("e2b_shim_envs", "env"):
        block = md.get(key)
        if not isinstance(block, dict):
            continue
        for k, v in block.items():
            if k and v is not None:
                out[str(k)] = str(v)
    return out


from .envd_template_bake import (
    ENVD_BAKE_MARKER,
    bake_envd_guest_into_container,
    container_has_baked_envd,
    guest_tcp_wait_loop_script,
    should_embed_envd_at_template_build,
    uvicorn_envd_start_background_script,
    uvicorn_envd_start_script,
)
from .firecracker_plane import FC_WARM_DOCKERLESS_MARKER
from .lima_plane import LIMA_WARM_DOCKERLESS_MARKER
from .template_image import resolve_sandbox_image
from database import Database

if TYPE_CHECKING:
    from .protocols import SandboxExecutionPlane

logger = logging.getLogger(__name__)

ENVD_TEMPLATE_BAKED_ENV = "MYSANDBOX_ENVD_BAKED"


def _resolve_sandbox_image(template_id: Optional[str]) -> str:
    return resolve_sandbox_image(template_id)


def _docker_engine_for_template_build(config: Any) -> Optional[ContainerManager]:
    """Docker ``runc`` client used to build Dockerfile templates when sandboxes run on Firecracker."""
    dh = (getattr(config, "DOCKER_HOST", None) or "").strip()
    if dh:
        os.environ["DOCKER_HOST"] = dh
    cm = ContainerManager(oci_runtime=None)
    return cm if cm.check_docker() else None


class SandboxManager:
    """Manages sandbox lifecycle."""

    instance: Optional["SandboxManager"] = None

    def __init__(
        self,
        db: Database,
        execution: Optional["SandboxExecutionPlane"] = None,
    ):
        self.db = db
        if execution is None:
            from config import get_config
            from .execution_backend import build_execution_backend

            execution = build_execution_backend(get_config())
        self.execution = execution
        from config import get_config

        self._config = get_config()
        # Serialize file + command I/O per sandbox so parallel agent tools (e.g. write_file ||
        # execute) cannot reorder at the API layer.
        self._sandbox_io_guard = threading.Lock()
        self._sandbox_io_locks: Dict[str, threading.Lock] = {}
        self._template_build_guard = threading.Lock()
        self._template_build_locks: Dict[str, threading.Lock] = {}
        self.warm_pool: Optional[Any] = None
        try:
            cfg = self._config
            kind = self.execution.get_backend_kind()
            if cfg.SANDBOX_WARM_POOL_SIZE > 0 and kind == "lima":
                logger.warning(
                    "SANDBOX_WARM_POOL_SIZE=%s is not supported with Lima per-VM sandboxes; warm pool disabled.",
                    cfg.SANDBOX_WARM_POOL_SIZE,
                )
            elif cfg.SANDBOX_WARM_POOL_SIZE > 0:
                from .warm_sandbox_pool import MultiWarmSandboxPool

                self.warm_pool = MultiWarmSandboxPool(self, cfg)
                self.warm_pool.start()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Warm sandbox pool not started: %s", ex)

    def _template_lock(self, template_id: str) -> threading.Lock:
        with self._template_build_guard:
            if template_id not in self._template_build_locks:
                self._template_build_locks[template_id] = threading.Lock()
            return self._template_build_locks[template_id]

    def discard_from_warm_pool(self, sandbox_id: str) -> None:
        """If ``sandbox_id`` was sitting in the warm deque, remove it (e.g. before kill)."""
        pool = getattr(self, "warm_pool", None)
        if pool is not None:
            pool.discard(sandbox_id)

    def _sandbox_io_lock(self, sandbox_id: str) -> threading.Lock:
        with self._sandbox_io_guard:
            if sandbox_id not in self._sandbox_io_locks:
                self._sandbox_io_locks[sandbox_id] = threading.Lock()
            return self._sandbox_io_locks[sandbox_id]

    @property
    def container_mgr(self) -> ContainerManager:
        """Docker Engine manager (``SANDBOX_ENGINE=docker`` only)."""
        if not is_container_like_execution(self.execution):
            raise TypeError(
                "SandboxManager.container_mgr is only valid when SANDBOX_ENGINE=docker; "
                "use self.execution for the active plane."
            )
        return self.execution

    def get_execution_kind(self) -> str:
        return self.execution.get_backend_kind()

    def describe_docker_workload_blocker(self) -> Optional[str]:
        """If the execution plane cannot run sandboxes, return a short diagnostic string."""
        from orchestrator.runtime_utils import workload_blocker_message

        return workload_blocker_message(self.execution)

    def get_e2b_agent_upstream_ws_uri(self, sandbox_id: str) -> Optional[str]:
        """Deprecated on control-plane — clients use proxy-service data-plane URLs."""
        return None

    def get_traffic_access_token(self, sandbox_id: str) -> Optional[str]:
        """Layer-3 token minted once at sandbox create and stored in metadata."""
        from orchestrator.sandbox_connections import traffic_access_token_for_row

        row = self.db.get_sandbox((sandbox_id or "").strip())
        if not row:
            return None
        return traffic_access_token_for_row(row)

    def get_envd_connection_ex(
        self, sandbox_id: str
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Like ``get_envd_connection`` but on failure returns ``(None, short_reason)`` for HTTP 503 hints.

        Reasons intentionally omit tokens and host ports.
        """
        sid = (sandbox_id or "").strip()
        kind = self.execution.get_backend_kind()
        if kind not in ("docker", "gvisor", "k8s"):
            return None, f"runtime {kind!r} does not support envd (need docker, gvisor, or k8s)"
        if not is_container_like_execution(self.execution):
            return None, "execution backend does not support in-guest envd"
        row = self.db.get_sandbox(sid)
        if not row:
            return None, "sandbox not found"
        cid = (row.get("container_id") or "").strip()
        if not cid or not self.execution.is_container_running(cid):
            return None, "container missing or not running"
        meta = row.get("metadata") or {}
        tok = (meta.get("envd_access_token") or "").strip()
        hp = meta.get("envd_host_tcp_port")
        if not tok:
            return (
                None,
                "no envd_access_token on this sandbox (recreate with ENVD_ALWAYS_ON=true on the API); "
                "existing sandboxes keep whatever was configured at create time",
            )
        port = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        from orchestrator.sandbox_connections import (
            allow_public_traffic_for_row,
            data_plane_base_url,
            data_plane_enabled_for_config,
            sandbox_domain_for_config,
        )

        if data_plane_enabled_for_config(self._config):
            out = {
                "sandbox_id": sid,
                "envd_port": port,
                "sandbox_domain": sandbox_domain_for_config(self._config),
                "http_base_url": data_plane_base_url(
                    self._config,
                    sandbox_id=sid,
                    port=port,
                    scheme="http",
                ),
                "access_token": tok,
            }
            if not allow_public_traffic_for_row(row, self._config):
                ttr = self.get_traffic_access_token(sid)
                if ttr:
                    out["traffic_access_token"] = ttr
            return out, None
        if hp is None:
            return None, "envd_host_tcp_port missing (set ENVD_PUBLISH_PORT=true or enable SANDBOX_DATA_PLANE_ENABLED)"
        try:
            hpi = int(hp)
        except (TypeError, ValueError):
            return None, "envd_host_tcp_port is not a valid integer"
        if not (1 <= hpi <= 65535):
            return None, "envd_host_tcp_port out of range"
        host = (getattr(self._config, "ENVD_UPSTREAM_HTTP_HOST", None) or "127.0.0.1").strip() or "127.0.0.1"
        return (
            {
                "sandbox_id": sid,
                "envd_port": port,
                "http_base_url": f"http://{host}:{hpi}",
                "access_token": tok,
            },
            None,
        )

    def get_envd_connection(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Return ``http_base_url``, ``access_token``, ``envd_port`` for Docker sandboxes with published envd."""
        info, _reason = self.get_envd_connection_ex(sandbox_id)
        return info

    def _guest_bootstrap_poll_seconds(self) -> float:
        return max(
            0.05,
            min(1.0, float(getattr(self._config, "GUEST_BOOTSTRAP_POLL_SEC", 0.1) or 0.1)),
        )

    def _guest_bootstrap_agent_wait_seconds(self) -> float:
        return max(
            1.0,
            float(getattr(self._config, "GUEST_BOOTSTRAP_AGENT_WAIT_SEC", 8.0) or 8.0),
        )

    def _resolve_template_start_spec(
        self, template_id: str
    ) -> tuple[str, str, Dict[str, str], int]:
        """Return ``(start_cmd, image_ref, template_env, guest_port)``."""
        tid = (template_id or "").strip()
        row = self.db.get_sandbox_template(tid) if tid else None
        sc = (row.get("start_cmd") or "").strip() if row else ""
        tpl_env = dict(row.get("env") or {}) if row else {}
        img_ref = ""
        if row:
            img_ref = (row.get("warm_snapshot_image") or row.get("base_image") or "").strip()
        if not sc and img_ref:
            sc = self.execution.image_start_cmd_shell(img_ref) or ""
        if not tpl_env and img_ref:
            tpl_env = self.execution.image_env_dict(img_ref)
        try:
            guest_port = int(str(tpl_env.get("PORT") or "0").strip() or "0")
        except ValueError:
            guest_port = 0
        return sc, img_ref, tpl_env, guest_port

    def _ensure_envd_baked(self, sandbox_id: str, container_id: str, *, pip_timeout: float) -> bool:
        """Ensure ``/opt/envd_guest`` and deps exist in the guest before startup."""
        with self._sandbox_io_lock(sandbox_id):
            if container_has_baked_envd(self.execution.run_command, container_id):
                return True
            return bake_envd_guest_into_container(
                put_archive_to_container=self.execution.put_archive_to_container,
                run_command=self.execution.run_command,
                container_id=container_id,
                pip_timeout_sec=pip_timeout,
            )

    def _template_declares_envd_baked(self, template_id: str) -> bool:
        tid = (template_id or "").strip()
        if not tid:
            return False
        row = self.db.get_sandbox_template(tid)
        if not row:
            return False
        raw = str((row.get("env") or {}).get(ENVD_TEMPLATE_BAKED_ENV) or "").strip().lower()
        return raw in ("1", "true", "yes", "on")

    def _k8s_in_pod_bootstrap_spec(
        self,
        template_id: str,
        *,
        start_envd: bool,
        envd_port: int,
    ) -> Optional[Dict[str, Any]]:
        if not is_k8s_execution(self.execution):
            return None
        if not bool(getattr(self._config, "K8S_TEMPLATE_BOOTSTRAP_IN_POD", True)):
            return None

        sc, _img_ref, tpl_env, guest_port = self._resolve_template_start_spec(template_id)
        if not sc and not start_envd:
            return None
        if start_envd and not self._template_declares_envd_baked(template_id):
            return None

        p = max(1, min(65535, int(envd_port)))
        parts = [
            "set -eu",
            ": > /tmp/template-start.log",
            ": > /tmp/envd.log",
        ]
        if start_envd:
            parts.append(uvicorn_envd_start_background_script(p))
        if sc:
            parts.append(
                "if command -v setsid >/dev/null 2>&1; then\n"
                f"  setsid -f /bin/sh -c {shlex.quote(sc)} >>/tmp/template-start.log 2>&1 &\n"
                "else\n"
                f"  nohup /bin/sh -c {shlex.quote(sc)} >>/tmp/template-start.log 2>&1 &\n"
                "fi"
            )
        parts.append("while true; do sleep 3600; done")
        try:
            gp = int(str(tpl_env.get("PORT") or "0").strip() or "0")
        except ValueError:
            gp = 0
        if gp <= 0:
            gp = guest_port
        return {
            "startup_command": ["/bin/sh", "-lc", "\n".join(parts)],
            "readiness_tcp_port": gp if 1 <= gp <= 65535 else None,
            "start_cmd": sc,
            "guest_port": gp if 1 <= gp <= 65535 else guest_port,
        }

    def _bootstrap_envd_daemon(
        self, sandbox_id: str, container_id: str, port: int, *, template_id: str = "", wait_for_listen: bool | None = None
    ) -> bool:
        """Start guest envd and optionally wait until localhost accepts on the envd port."""
        if not is_container_like_execution(self.execution):
            return True
        if wait_for_listen is None:
            wait_for_listen = bool(getattr(self._config, "ENVD_BOOTSTRAP_WAIT_ON_CREATE", False))
        p = max(1, min(65535, int(port)))
        pip_to = float(getattr(self._config, "ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", 300.0) or 300.0)
        declared_baked = self._template_declares_envd_baked(template_id)

        def _start_once() -> Dict[str, Any]:
            with self._sandbox_io_lock(sandbox_id):
                start = uvicorn_envd_start_script(p)
                if not wait_for_listen:
                    start = f"set -eu\n{uvicorn_envd_start_background_script(p)}"
                return self.execution.run_command(
                    container_id,
                    start,
                    timeout=120.0 if wait_for_listen else 30.0,
                )

        if not declared_baked:
            if not self._ensure_envd_baked(sandbox_id, container_id, pip_timeout=pip_to):
                logger.warning("envd auto-start: bake/bootstrap failed sandbox=%s", sandbox_id)
                return False

        st = _start_once()
        if int(st.get("exit_code") or 0) == 0:
            logger.info("envd auto-start: sandbox %s guest listening on tcp/%s", sandbox_id, p)
            return True

        if declared_baked:
            logger.warning(
                "envd auto-start: declared baked template failed first start sandbox=%s template=%s stderr=%s; retrying after bake probe",
                sandbox_id,
                template_id,
                (st.get("stderr") or "")[:1200],
            )
            if not self._ensure_envd_baked(sandbox_id, container_id, pip_timeout=pip_to):
                logger.warning("envd auto-start: fallback bake failed sandbox=%s", sandbox_id)
                return False
            st = _start_once()
            if int(st.get("exit_code") or 0) == 0:
                logger.info(
                    "envd auto-start: sandbox %s guest listening on tcp/%s after fallback bake",
                    sandbox_id,
                    p,
                )
                return True

        logger.warning(
            "envd auto-start: daemon did not listen on :%s sandbox=%s stderr=%s",
            p,
            sandbox_id,
            (st.get("stderr") or "")[:2500],
        )
        return False

    def _bootstrap_template_start_cmd(self, sandbox_id: str, container_id: str, template_id: str) -> None:
        """Background-start the template ``start_cmd`` (e.g. Dockerfile ``CMD``) — sandboxes boot with ``/bin/bash``."""
        if not is_container_like_execution(self.execution):
            return
        tid = (template_id or "").strip()
        if not tid:
            return
        row = self.db.get_sandbox_template(tid)
        if not row:
            return
        sc = (row.get("start_cmd") or "").strip()
        tpl_env = dict(row.get("env") or {})
        img_ref = (row.get("warm_snapshot_image") or row.get("base_image") or "").strip()
        if not sc and img_ref:
            sc = self.execution.image_start_cmd_shell(img_ref)
            if sc:
                logger.info(
                    "template start_cmd from image %r for %r: %s",
                    img_ref,
                    tid,
                    sc[:200],
                )
        if not tpl_env and img_ref:
            tpl_env = self.execution.image_env_dict(img_ref)
        try:
            guest_port = int(str(tpl_env.get("PORT") or "0").strip() or "0")
        except ValueError:
            guest_port = 0
        if not sc:
            logger.debug("template start_cmd empty for %r — skip bootstrap", tid)
            return
        exec_user = None if is_k8s_execution(self.execution) else "root"
        if img_ref and is_container_like_execution(self.execution) and not is_k8s_execution(self.execution):
            exec_user = self.execution.image_default_user(img_ref)
        script = (
            "set -eu\n"
            ": > /tmp/template-start.log\n"
            f"if command -v setsid >/dev/null 2>&1; then\n"
            f"  setsid -f /bin/sh -c {shlex.quote(sc)} >>/tmp/template-start.log 2>&1 &\n"
            "else\n"
            f"  nohup /bin/sh -c {shlex.quote(sc)} >>/tmp/template-start.log 2>&1 &\n"
            "fi\n"
            "exit 0\n"
        )
        with self._sandbox_io_lock(sandbox_id):
            # Inherit the container env (``ENV`` from image + create); do not pass a partial env dict
            # or ``PATH`` is lost and ``node``/``nodejs`` will not spawn.
            st = self.execution.run_command(container_id, script, timeout=60.0, user=exec_user)
            if int(st.get("exit_code") or 0) != 0:
                logger.warning(
                    "template start_cmd launcher failed sandbox=%s template=%s output=%s",
                    sandbox_id,
                    tid,
                    (st.get("stderr") or st.get("stdout") or "")[:2000],
                )
                return
            if 1 <= guest_port <= 65535:
                wait = guest_tcp_wait_loop_script(
                    guest_port,
                    max_seconds=self._guest_bootstrap_agent_wait_seconds(),
                    poll_seconds=self._guest_bootstrap_poll_seconds(),
                    log_path="/tmp/template-start.log",
                )
                wt = self.execution.run_command(
                    container_id,
                    wait,
                    timeout=self._guest_bootstrap_agent_wait_seconds() + 10.0,
                )
                if int(wt.get("exit_code") or 0) != 0:
                    logger.warning(
                        "template start_cmd did not listen on :%s sandbox=%s template=%s log=%s",
                        guest_port,
                        sandbox_id,
                        tid,
                        (wt.get("stderr") or wt.get("stdout") or "")[:2500],
                    )
                    return
        logger.info(
            "template start_cmd bootstrapped sandbox=%s template=%r cmd=%r port=%s",
            sandbox_id,
            tid,
            sc[:120],
            guest_port or "?",
        )

    def refresh_guest_routing_metadata(self, sandbox_id: str) -> None:
        """Store upstream targets for proxy-service (K8s Service DNS or Docker bridge)."""
        from orchestrator.sandbox_connections import build_guest_routing_record, k8s_pod_service_host

        sid = (sandbox_id or "").strip()
        md: Dict[str, Any] = {}
        if getattr(self._config, "is_k8s_runtime", None) and self._config.is_k8s_runtime():
            pod_ip = ""
            row = self.get_sandbox(sid)
            if row:
                cid = (row.get("container_id") or "").strip()
                if cid:
                    try:
                        pod_ip = (self.execution.get_container_internal_ipv4(cid) or "").strip()
                    except Exception:
                        pod_ip = ""
            md["k8s"] = {
                "namespace": (getattr(self._config, "K8S_NAMESPACE", None) or "sandboxes").strip(),
                "service_host": k8s_pod_service_host(self._config, sid),
                "pod_ip": pod_ip,
            }
        record = build_guest_routing_record(self, sid)
        if record:
            md["guest_routing"] = record
        if md:
            self.db.merge_sandbox_metadata(sid, md)

    def _bootstrap_guest_services_k8s_combined(
        self,
        sandbox_id: str,
        container_id: str,
        template_id: str,
        *,
        start_envd: bool,
        envd_port: int,
    ) -> bool:
        """Single kubectl exec fast path; if envd is missing from the image, bake it first."""
        sc, img_ref, _tpl_env, guest_port = self._resolve_template_start_spec(template_id)
        exec_user = None if is_k8s_execution(self.execution) else "root"
        if img_ref and is_container_like_execution(self.execution) and not is_k8s_execution(self.execution):
            exec_user = self.execution.image_default_user(img_ref)

        wait_sec = self._guest_bootstrap_agent_wait_seconds()
        poll_sec = self._guest_bootstrap_poll_seconds()
        declared_baked = self._template_declares_envd_baked(template_id)
        parts = ["set -eu"]

        if start_envd:
            p = max(1, min(65535, int(envd_port)))
            envd_wait = guest_tcp_wait_loop_script(
                p,
                max_seconds=min(wait_sec, 15.0),
                poll_seconds=poll_sec,
                log_path="/tmp/envd.log",
            )
            if not declared_baked:
                pip_to = float(getattr(self._config, "ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", 300.0) or 300.0)
                if not self._ensure_envd_baked(sandbox_id, container_id, pip_timeout=pip_to):
                    logger.warning("k8s combined guest bootstrap: envd bake failed sandbox=%s", sandbox_id)
                    return False
            # Subshell: wait loop uses ``exit 0`` on success — must not terminate the
            # combined script before ``start_cmd`` runs.
            parts.append(
                f"{uvicorn_envd_start_background_script(p)}\n(\n{envd_wait}\n)"
            )

        if sc:
            parts.append(": > /tmp/template-start.log")
            parts.append(
                "if command -v setsid >/dev/null 2>&1; then\n"
                f"  setsid -f /bin/sh -c {shlex.quote(sc)} >>/tmp/template-start.log 2>&1 &\n"
                "else\n"
                f"  nohup /bin/sh -c {shlex.quote(sc)} >>/tmp/template-start.log 2>&1 &\n"
                "fi"
            )

        if 1 <= guest_port <= 65535:
            agent_wait = guest_tcp_wait_loop_script(
                guest_port,
                max_seconds=wait_sec,
                poll_seconds=poll_sec,
                log_path="/tmp/template-start.log",
            )
            parts.append(f"(\n{agent_wait}\n)")
        elif sc:
            parts.append("sleep 0.5")

        if len(parts) == 1:
            return True

        script = "\n".join(parts)
        def _run_script() -> Dict[str, Any]:
            with self._sandbox_io_lock(sandbox_id):
                return self.execution.run_command(
                    container_id,
                    script,
                    timeout=wait_sec + 30.0,
                    user=exec_user,
                )

        st = _run_script()
        if int(st.get("exit_code") or 0) != 0 and start_envd and declared_baked:
            pip_to = float(getattr(self._config, "ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", 300.0) or 300.0)
            logger.warning(
                "k8s combined guest bootstrap: declared baked template failed first attempt sandbox=%s template=%s output=%s; retrying after bake probe",
                sandbox_id,
                template_id,
                (st.get("stderr") or st.get("stdout") or "")[:1500],
            )
            if not self._ensure_envd_baked(sandbox_id, container_id, pip_timeout=pip_to):
                logger.warning("k8s combined guest bootstrap: fallback bake failed sandbox=%s", sandbox_id)
                return False
            st = _run_script()

        if int(st.get("exit_code") or 0) != 0 and 1 <= guest_port <= 65535:
            logger.warning(
                "k8s combined guest bootstrap: agent :%s not ready sandbox=%s template=%s log=%s",
                guest_port,
                sandbox_id,
                template_id,
                (st.get("stderr") or st.get("stdout") or "")[:2500],
            )
            return False
        if int(st.get("exit_code") or 0) != 0:
            logger.warning(
                "k8s combined guest bootstrap failed sandbox=%s template=%s output=%s",
                sandbox_id,
                template_id,
                (st.get("stderr") or st.get("stdout") or "")[:2000],
            )
            return False

        if sc:
            logger.info(
                "template start_cmd bootstrapped sandbox=%s template=%r cmd=%r port=%s",
                sandbox_id,
                template_id,
                sc[:120],
                guest_port or "?",
            )
        if start_envd:
            logger.info(
                "envd auto-start: sandbox %s guest tcp/%s ready",
                sandbox_id,
                envd_port,
            )
        return True

    def _bootstrap_guest_services(self, sandbox_id: str, container_id: str, template_id: str) -> bool:
        """Idempotent guest daemons after the workload is running (control-plane responsibility)."""
        if not is_container_like_execution(self.execution):
            return True

        envd_port_cfg = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        envd_always = bool(getattr(self._config, "ENVD_ALWAYS_ON", True))
        publish_envd_legacy = bool(getattr(self._config, "ENVD_PUBLISH_PORT", False))
        start_envd = envd_always or publish_envd_legacy
        auto_start_envd = start_envd and getattr(self._config, "ENVD_AUTO_START", True)
        in_pod_boot = None
        if auto_start_envd or template_id:
            in_pod_boot = self._k8s_in_pod_bootstrap_spec(
                template_id,
                start_envd=auto_start_envd,
                envd_port=envd_port_cfg,
            )
        if in_pod_boot is not None:
            guest_port = int(in_pod_boot.get("guest_port") or 0)
            if 1 <= guest_port <= 65535:
                wt = self.execution.run_command(
                    container_id,
                    guest_tcp_wait_loop_script(
                        guest_port,
                        max_seconds=self._guest_bootstrap_agent_wait_seconds(),
                        poll_seconds=self._guest_bootstrap_poll_seconds(),
                        log_path="/tmp/template-start.log",
                    ),
                    timeout=self._guest_bootstrap_agent_wait_seconds() + 10.0,
                )
                if int(wt.get("exit_code") or 0) != 0:
                    logger.warning(
                        "k8s in-pod bootstrap: agent :%s not ready sandbox=%s template=%s log=%s",
                        guest_port,
                        sandbox_id,
                        template_id,
                        (wt.get("stderr") or wt.get("stdout") or "")[:2500],
                    )
                    return False
            if in_pod_boot.get("start_cmd"):
                logger.info(
                    "template start_cmd bootstrapped in-pod sandbox=%s template=%r cmd=%r port=%s",
                    sandbox_id,
                    template_id,
                    str(in_pod_boot.get("start_cmd"))[:120],
                    in_pod_boot.get("guest_port") or "?",
                )
            if auto_start_envd:
                logger.info("envd auto-start: sandbox %s guest tcp/%s ready (in-pod)", sandbox_id, envd_port_cfg)
            self.refresh_guest_routing_metadata(sandbox_id)
            return True

        if (
            is_k8s_execution(self.execution)
            and bool(getattr(self._config, "K8S_COMBINED_GUEST_BOOTSTRAP", True))
        ):
            ok = self._bootstrap_guest_services_k8s_combined(
                sandbox_id,
                container_id,
                template_id,
                start_envd=auto_start_envd,
                envd_port=envd_port_cfg,
            )
            self.refresh_guest_routing_metadata(sandbox_id)
            return ok

        if auto_start_envd:
            if not self._bootstrap_envd_daemon(
                sandbox_id,
                container_id,
                envd_port_cfg,
                template_id=template_id,
            ):
                self.refresh_guest_routing_metadata(sandbox_id)
                return False

        sc, _img_ref, _tpl_env, _guest_port = self._resolve_template_start_spec(template_id)
        if sc:
            self._bootstrap_template_start_cmd(sandbox_id, container_id, template_id)

        self.refresh_guest_routing_metadata(sandbox_id)
        return True

    def create_sandbox(
        self,
        template_id: str = "python:3.11",
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        from_snapshot_image: Optional[str] = None,
    ) -> Optional[str]:
        """Create new sandbox, optionally from a prior ``docker commit`` image or warm pool."""
        snap = (from_snapshot_image or "").strip()
        if snap:
            return self._create_sandbox_fresh(
                template_id=template_id,
                metadata=metadata,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=timeout,
                from_snapshot_image=snap,
            )

        tid = (template_id or "").strip()
        tpl = self.db.get_sandbox_template(tid) if tid else None

        pool = getattr(self, "warm_pool", None)

        # With warm pool enabled, bare Docker image refs (no prior POST /templates row) are
        # auto-registered so the first create can build warm_snapshot_image + ensure_pool_for.
        # Skip the configured default pool template_id so MultiWarm.start()'s base-image segment
        # is not replaced by a conflicting registered-template path for the same key.
        warm_pool_default_tid = (
            self._config.SANDBOX_WARM_POOL_TEMPLATE_ID or self._config.DEFAULT_TEMPLATE
        ).strip()
        if (
            tid
            and tpl is None
            and int(self._config.SANDBOX_WARM_POOL_SIZE) > 0
            and tid != warm_pool_default_tid
        ):
            base_image = _resolve_sandbox_image(tid)
            self.db.upsert_sandbox_template(
                tid,
                base_image,
                {},
                "",
                20,
                "",
            )
            tpl = self.db.get_sandbox_template(tid)
            logger.info(
                "Auto-registered logical template template_id=%r base_image=%r (warm pool)",
                tid,
                base_image,
            )

        # First use of the configured default template (e.g. python:3.11): register + warm snapshot so
        # ``ENVD_EMBED_AT_TEMPLATE_BUILD`` can bake envd into the committed image without a prior POST /templates.
        if (
            tid
            and tpl is None
            and should_embed_envd_at_template_build(self._config)
            and is_container_like_execution(self.execution)
            and self.execution.get_backend_kind() in ("docker", "gvisor")
            and tid == (self._config.DEFAULT_TEMPLATE or "").strip()
        ):
            base_image = _resolve_sandbox_image(tid)
            self.db.upsert_sandbox_template(
                tid,
                base_image,
                {},
                "",
                20,
                "",
            )
            tpl = self.db.get_sandbox_template(tid)
            logger.info(
                "Auto-registered default template_id=%r base_image=%r (envd template embed)",
                tid,
                base_image,
            )

        if tpl:
            if not tpl.get("warm_snapshot_image"):
                if not self._build_registered_template_snapshot(tid):
                    return None
                tpl = self.db.get_sandbox_template(tid) or tpl
            warm_img = (tpl.get("warm_snapshot_image") or "").strip()
            # Parsed Dockerfile builds store the committed OCI ref as ``base_image`` and should mirror
            # ``warm_snapshot_image``. If the snapshot column is still empty (e.g. failed UPDATE),
            # using ``_resolve_sandbox_image(template_id)`` would start ``newp1`` as a literal image
            # name while **env** still comes from the template row — looks like "right env, no /app".
            if not warm_img:
                bi = (tpl.get("base_image") or "").strip()
                if bi and bi != tid and (":" in bi or "/" in bi):
                    warm_img = bi
                    logger.warning(
                        "Template %r: warm_snapshot_image missing; using base_image %r for sandbox image",
                        tid,
                        bi,
                    )
            cfg = self._config
            if pool is not None and cfg.SANDBOX_WARM_POOL_SIZE > 0 and warm_img:
                pool.ensure_pool_for(tid, cpu_limit, memory_limit, int(timeout), warm_img)
            if pool is not None:
                sid = pool.try_acquire(tid, metadata, cpu_limit, memory_limit, int(timeout))
                if sid:
                    row = self.get_sandbox(sid)
                    if row:
                        if not self._bootstrap_guest_services(sid, row["container_id"], tid):
                            self.kill_sandbox(sid, force=True)
                            return None
                    return sid
            return self._create_sandbox_fresh(
                template_id=tid,
                metadata=metadata,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=timeout,
                from_snapshot_image=warm_img or None,
            )

        if pool is not None:
            sid = pool.try_acquire(tid, metadata, cpu_limit, memory_limit, int(timeout))
            if sid:
                row = self.get_sandbox(sid)
                if row:
                    if not self._bootstrap_guest_services(sid, row["container_id"], tid or template_id):
                        self.kill_sandbox(sid, force=True)
                        return None
                return sid
        return self._create_sandbox_fresh(
            template_id=template_id,
            metadata=metadata,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            from_snapshot_image=None,
        )

    def sync_warm_pool_default_segment(self, template_id: str, warm_ref: str) -> None:
        """Rebuild the **default** warm-pool segment to use ``warm_ref`` when it matches pool template_id.

        Without this, ``MultiWarmSandboxPool.start()`` provisions from ``from_snapshot_image=None``
        (raw ``python:3.11``) while ``POST /sandboxes`` uses ``warm_snapshot_image`` — different images.
        """
        pool = getattr(self, "warm_pool", None)
        if pool is None or int(getattr(self._config, "SANDBOX_WARM_POOL_SIZE", 0) or 0) <= 0:
            return
        pid = (self._config.SANDBOX_WARM_POOL_TEMPLATE_ID or self._config.DEFAULT_TEMPLATE).strip()
        if (template_id or "").strip() != pid:
            return
        ref = (warm_ref or "").strip()
        if not ref or ref in (FC_WARM_DOCKERLESS_MARKER, LIMA_WARM_DOCKERLESS_MARKER):
            return
        pool.ensure_pool_for(
            pid,
            self._config.SANDBOX_WARM_POOL_CPU or self._config.DEFAULT_CPU_LIMIT,
            self._config.SANDBOX_WARM_POOL_MEMORY or self._config.DEFAULT_MEMORY_LIMIT,
            int(self._config.SANDBOX_WARM_POOL_TIMEOUT or self._config.DEFAULT_TIMEOUT),
            ref,
        )
        logger.info(
            "Warm pool default segment now uses warm_snapshot_image for template_id=%r",
            pid,
        )

    def _build_registered_template_snapshot(self, template_id: str) -> bool:
        """One-time: base image + env + ``start_cmd`` + settle, then ``docker commit``."""
        if self.execution.get_backend_kind() == "firecracker":
            self.db.set_template_warm_snapshot(template_id, FC_WARM_DOCKERLESS_MARKER, None)
            logger.info(
                "Firecracker engine: skipping Docker-based template snapshot for %r (marker %s)",
                template_id,
                FC_WARM_DOCKERLESS_MARKER,
            )
            return True
        if self.execution.get_backend_kind() == "lima":
            self.db.set_template_warm_snapshot(template_id, LIMA_WARM_DOCKERLESS_MARKER, None)
            logger.info(
                "Lima isolation: skipping Docker-based template snapshot for %r (marker %s)",
                template_id,
                LIMA_WARM_DOCKERLESS_MARKER,
            )
            return True
        lock = self._template_lock(template_id)
        with lock:
            row = self.db.get_sandbox_template(template_id)
            if not row:
                return False
            if row.get("warm_snapshot_image"):
                return True

            # K8s + host-built image (``--host-docker``): skip tpl-build / docker commit.
            if self.execution.get_backend_kind() == "k8s":
                bi = (row.get("base_image") or "").strip()
                sc = (row.get("start_cmd") or "").strip()
                if bi and bi != template_id and (":" in bi or "/" in bi) and not sc:
                    self.db.set_template_warm_snapshot(template_id, bi, None)
                    logger.info(
                        "Template %s: using pre-built base_image as warm snapshot (k8s): %s",
                        template_id,
                        bi,
                    )
                    return True

            cfg = self._config
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

        kind = self.execution.get_backend_kind()
        if kind == "lima":
            raise RuntimeError(
                "Parsed Dockerfile template build requires Docker Engine (Lima VM isolation has no Docker build path)."
            )
        plane: Any = self.execution
        if kind == "firecracker":
            docker_cm = _docker_engine_for_template_build(self._config)
            if docker_cm is None:
                raise RuntimeError(
                    "Firecracker engine: Dockerfile template build needs Docker Engine on this host "
                    "(set DOCKER_HOST, ensure `docker info` works, and keep `docker` on PATH)."
                )
            plane = docker_cm
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
                    bake_envd_guest_into_container(
                        put_archive_to_container=pa,
                        run_command=plane.run_command,
                        container_id=cid,
                        pip_timeout_sec=pt,
                    )

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

        warm_ref = image_ref
        if self.execution.get_backend_kind() == "firecracker":
            from .fc_dockerfile_rootfs_export import materialize_firecracker_template_ext4

            warm_ref = materialize_firecracker_template_ext4(
                self._config, oci_image_ref=image_ref, template_id=template_id
            )

        self.db.upsert_sandbox_template(
            template_id,
            image_ref,
            merged_template_env,
            (start_cmd or "").strip() or extract_start_cmd_from_dockerfile(dockerfile),
            int(settle_seconds or 0),
            (ready_cmd or "").strip(),
        )
        if not self.db.set_template_warm_snapshot(template_id, warm_ref, None):
            raise RuntimeError(
                f"set_template_warm_snapshot failed for template_id={template_id!r} "
                f"(warm_ref={warm_ref!r}); SQLite row missing after upsert — check DATABASE_PATH / DB."
            )
        logger.info("Template %s (parsed Dockerfile) warm snapshot: %s", template_id, warm_ref)
        self.sync_warm_pool_default_segment(template_id, warm_ref)
        return self.db.get_sandbox_template(template_id) or {}

    def materialize_firecracker_rootfs_from_oci(self, oci_image_ref: str, template_id: str) -> str:
        """Export a built OCI tag to a host ``.ext4`` for Firecracker (see ``fc_dockerfile_rootfs_export``)."""
        from .fc_dockerfile_rootfs_export import materialize_firecracker_template_ext4

        return materialize_firecracker_template_ext4(
            self._config, oci_image_ref=oci_image_ref, template_id=template_id
        )

    def _create_sandbox_fresh(
        self,
        template_id: str = "python:3.11",
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        from_snapshot_image: Optional[str] = None,
    ) -> Optional[str]:
        """Create a brand-new sandbox (never taken from the warm pool)."""
        sandbox_id = f"sb-{uuid.uuid4().hex[:16]}"
        container_name = f"sandbox-{sandbox_id}"

        meta = dict(metadata or {})
        allow_pt = meta.pop("allow_public_traffic", None)
        network = meta.pop("network", None)
        if allow_pt is None and isinstance(network, dict) and "allow_public_traffic" in network:
            allow_pt = network.get("allow_public_traffic")
        if allow_pt is None:
            allow_pt = getattr(self._config, "SANDBOX_DEFAULT_ALLOW_PUBLIC_TRAFFIC", False)
        meta["allow_public_traffic"] = bool(allow_pt)
        metadata = meta

        snap = (from_snapshot_image or "").strip()
        root_override: Optional[str] = None
        fc_bundle_ref: Optional[str] = None
        if self.execution.get_backend_kind() == "firecracker":
            image = (template_id or "").strip() or "firecracker"
            if snap:
                if snap == FC_WARM_DOCKERLESS_MARKER:
                    snap = ""
                elif snap.startswith("fc-bundle:"):
                    fc_bundle_ref = snap
                    snap = ""
                elif snap.endswith(".ext4") or os.path.isfile(snap):
                    root_override = snap
                else:
                    logger.warning(
                        "Firecracker: ignoring docker image / snapshot ref %r (use host .ext4 path or fc-bundle:…)",
                        snap,
                    )
        elif self.execution.get_backend_kind() == "lima":
            image = (template_id or "").strip() or "lima"
            if snap and snap != LIMA_WARM_DOCKERLESS_MARKER:
                logger.warning(
                    "Lima: ignoring Docker image / snapshot ref %r (use LIMA_SANDBOX_TEMPLATE / template_id as template://…)",
                    snap,
                )
            snap = ""
        elif snap:
            image = snap
        else:
            image = _resolve_sandbox_image(template_id)
        runtime = self.execution.get_backend_kind()
        logger.info(
            "Creating sandbox %s runtime=%s template_id=%r image=%r (from_snapshot=%s)",
            sandbox_id,
            runtime,
            template_id,
            image,
            bool(snap),
        )

        tpl = self.db.get_sandbox_template((template_id or "").strip()) if template_id else None
        env_for_create: Optional[Dict[str, str]] = None
        if tpl:
            ev = dict(tpl.get("env") or {})
            if ev:
                env_for_create = ev

        meta_env = _create_env_from_metadata(metadata)
        if meta_env:
            env_for_create = {**(env_for_create or {}), **meta_env}

        if is_k8s_execution(self.execution):
            cpu_limit = (getattr(self._config, "K8S_SANDBOX_CPU_LIMIT", None) or cpu_limit).strip()
            memory_limit = (
                getattr(self._config, "K8S_SANDBOX_MEMORY_LIMIT", None) or memory_limit
            ).strip()

        envd_port_cfg = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        is_container_backend = is_container_like_execution(self.execution)
        envd_always = is_container_backend and bool(getattr(self._config, "ENVD_ALWAYS_ON", True))
        publish_envd_legacy = (
            is_container_backend
            and not is_k8s_execution(self.execution)
            and bool(getattr(self._config, "ENVD_PUBLISH_PORT", False))
        )
        publish_envd = publish_envd_legacy
        start_envd = envd_always or publish_envd_legacy

        guest_ports = resolve_guest_ports(
            metadata=metadata,
            template_row=tpl,
            config=self._config,
            include_envd=start_envd,
        )
        metadata = {**(metadata or {}), "guest_ports": guest_ports}

        envd_token: Optional[str] = None
        traffic_token: Optional[str] = None
        if start_envd:
            envd_token = secrets.token_urlsafe(32)
            if env_for_create is None:
                env_for_create = {}
            env_for_create = {**env_for_create, "ENVD_ACCESS_TOKEN": envd_token}
        from orchestrator.sandbox_connections import data_plane_enabled_for_config

        if is_container_backend and data_plane_enabled_for_config(self._config):
            traffic_token = secrets.token_urlsafe(32)

        k8s_boot = None
        if is_k8s_execution(self.execution):
            k8s_boot = self._k8s_in_pod_bootstrap_spec(
                template_id,
                start_envd=start_envd,
                envd_port=envd_port_cfg,
            )

        config = ContainerConfig(
            image=image,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            environment=env_for_create,
            rootfs_path=root_override,
            fc_bundle_ref=fc_bundle_ref,
            guest_ports=guest_ports,
            publish_envd_port=publish_envd,
            envd_port=envd_port_cfg,
            startup_command=list(k8s_boot.get("startup_command") or []) if k8s_boot else None,
            readiness_tcp_port=(k8s_boot.get("readiness_tcp_port") if k8s_boot else None),
        )

        container_id = self.execution.create_container(container_name, config)
        if not container_id:
            logger.error("Failed to create workload for sandbox %s", sandbox_id)
            return None

        self.db.create_sandbox(
            sandbox_id=sandbox_id,
            container_id=container_id,
            template_id=template_id,
            metadata=metadata,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            runtime=runtime,
        )

        if start_envd and is_container_backend and envd_token:
            md_envd: Dict[str, Any] = {"envd_access_token": envd_token}
            if traffic_token:
                md_envd["traffic_access_token"] = traffic_token
            if publish_envd:
                ehp = self.execution.get_container_tcp_host_port(container_id, envd_port_cfg)
                if ehp:
                    md_envd["envd_host_tcp_port"] = int(ehp)
                    hhost = (getattr(self._config, "ENVD_UPSTREAM_HTTP_HOST", None) or "127.0.0.1").strip()
                    logger.info(
                        "Sandbox %s envd legacy publish http://%s:%s/ (host → container :%s)",
                        sandbox_id,
                        hhost or "127.0.0.1",
                        ehp,
                        envd_port_cfg,
                    )
                else:
                    logger.warning(
                        "Sandbox %s: envd port publish requested but no host binding for tcp/%s",
                        sandbox_id,
                        envd_port_cfg,
                    )
            self.db.merge_sandbox_metadata(sandbox_id, md_envd)
        elif traffic_token:
            self.db.merge_sandbox_metadata(sandbox_id, {"traffic_access_token": traffic_token})

        if is_container_backend:
            if not self._bootstrap_guest_services(sandbox_id, container_id, template_id):
                logger.error("Sandbox %s bootstrap failed; tearing down workload", sandbox_id)
                self.kill_sandbox(sandbox_id, force=True)
                return None
        else:
            self.refresh_guest_routing_metadata(sandbox_id)

        logger.info("Sandbox created: %s", sandbox_id)
        return sandbox_id

    def get_sandbox(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox info."""
        return self.db.get_sandbox(sandbox_id)

    def get_sandbox_by_container(self, container_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox by container / instance ID."""
        return self.db.get_sandbox_by_container(container_id)

    def list_sandboxes(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """List all sandboxes."""
        return self.db.list_sandboxes(limit=limit, offset=offset)

    def is_running(self, sandbox_id: str) -> bool:
        """Check if sandbox is running."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        return self.execution.is_container_running(sandbox["container_id"])

    def create_filesystem_snapshot(
        self,
        sandbox_id: str,
        label: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Docker: ``docker commit`` into a new image, or Firecracker: full VM snapshot (``fc-bundle:`` ref)."""
        commit_fn = getattr(self.execution, "commit_filesystem_snapshot", None)
        if not callable(commit_fn):
            return None
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            logger.error("create_filesystem_snapshot: unknown sandbox %s", sandbox_id)
            return None
        cfg = self._config
        repo = (cfg.SANDBOX_SNAPSHOT_REPO or "mysandbox-snap").strip().lower().replace("/", "-")
        if not repo:
            repo = "mysandbox-snap"
        snap_uuid = uuid.uuid4().hex[:12]
        raw_tag = f"{sandbox_id}-{snap_uuid}"
        tag = re.sub(r"[^a-z0-9._-]", "-", raw_tag.lower())[:120] or "snap"
        image_ref = commit_fn(sandbox["container_id"], repo, tag)
        if not image_ref:
            return None
        snapshot_id = f"snap-{snap_uuid}"
        row = self.db.insert_sandbox_snapshot(snapshot_id, sandbox_id, image_ref, label)
        meta = dict(sandbox.get("metadata") or {})
        meta["last_snapshot_image"] = image_ref
        meta["last_snapshot_id"] = snapshot_id
        self.db.merge_sandbox_metadata(sandbox_id, meta)
        return row

    def list_filesystem_snapshots(self, sandbox_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        if not self.get_sandbox(sandbox_id):
            return []
        return self.db.list_sandbox_snapshots(sandbox_id, limit)

    def run_command(
        self,
        sandbox_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Run command in sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            logger.error("Sandbox not found: %s", sandbox_id)
            return None

        container_id = sandbox["container_id"]

        with self._sandbox_io_lock(sandbox_id):
            result = self.execution.run_command(
                container_id=container_id,
                command=command,
                cwd=cwd,
                env=env,
                timeout=timeout,
                user=user,
            )

        cmd_id = f"cmd-{uuid.uuid4().hex[:16]}"
        self.db.add_command_history(
            command_id=cmd_id,
            sandbox_id=sandbox_id,
            command=command,
            exit_code=result["exit_code"],
            stdout=result["stdout"],
            stderr=result["stderr"],
            pid=result["pid"],
            execution_time=0.0,
        )

        return result

    def iter_run_command_sse(
        self,
        sandbox_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Iterator[str]:
        """Server-Sent Events lines: ``data: <json>\\n\\n`` with stdout/stderr chunks and a final exit."""
        cmd_id = f"cmd-{uuid.uuid4().hex[:16]}"
        stdout_buf: List[str] = []
        stderr_buf: List[str] = []
        exit_code = -1
        started = time.time()

        def jline(obj: Dict[str, Any]) -> str:
            return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            yield jline({"type": "error", "message": "Sandbox not found"})
            yield jline({"type": "exit", "exit_code": -1})
            return

        try:
            with self._sandbox_io_lock(sandbox_id):
                stream_fn = getattr(self.execution, "run_command_stream", None)
                if callable(stream_fn):
                    for ev in stream_fn(
                        sandbox["container_id"],
                        command,
                        cwd=cwd,
                        env=env,
                        timeout=timeout,
                        user=user,
                    ):
                        t = ev.get("type")
                        if t == "stdout":
                            stdout_buf.append(ev.get("chunk") or "")
                            yield jline({"type": "stdout", "chunk": ev.get("chunk") or ""})
                        elif t == "stderr":
                            stderr_buf.append(ev.get("chunk") or "")
                            yield jline({"type": "stderr", "chunk": ev.get("chunk") or ""})
                        elif t == "error":
                            yield jline(ev)
                        elif t == "exit":
                            exit_code = int(ev.get("exit_code", -1))
                            yield jline({"type": "exit", "exit_code": exit_code})
                else:
                    r = self.execution.run_command(
                        sandbox["container_id"],
                        command,
                        cwd=cwd,
                        env=env,
                        timeout=timeout,
                        user=user,
                    )
                    r = r or {}
                    if r.get("stdout"):
                        s = str(r["stdout"])
                        stdout_buf.append(s)
                        yield jline({"type": "stdout", "chunk": s})
                    if r.get("stderr"):
                        s = str(r["stderr"])
                        stderr_buf.append(s)
                        yield jline({"type": "stderr", "chunk": s})
                    exit_code = int(r.get("exit_code", -1))
                    yield jline({"type": "exit", "exit_code": exit_code})
        except Exception as e:  # noqa: BLE001
            logger.exception("iter_run_command_sse: %s", e)
            yield jline({"type": "error", "message": str(e)})
            yield jline({"type": "exit", "exit_code": -1})
            exit_code = -1
        finally:
            elapsed = time.time() - started
            self.db.add_command_history(
                cmd_id,
                sandbox_id,
                command,
                exit_code,
                "".join(stdout_buf),
                "".join(stderr_buf),
                -1,
                elapsed,
            )

    def read_file(self, sandbox_id: str, path: str) -> Optional[str]:
        """Read file from sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return None

        with self._sandbox_io_lock(sandbox_id):
            return self.execution.read_file(sandbox["container_id"], path)

    def write_file(self, sandbox_id: str, path: str, content: str) -> bool:
        """Write file to sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        with self._sandbox_io_lock(sandbox_id):
            return self.execution.write_file(sandbox["container_id"], path, content)

    def list_files(self, sandbox_id: str, path: str = "/") -> Optional[list]:
        """List files in sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return None

        with self._sandbox_io_lock(sandbox_id):
            return self.execution.list_files(sandbox["container_id"], path)

    def delete_file(self, sandbox_id: str, path: str, recursive: bool = False) -> bool:
        """Delete file from sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        with self._sandbox_io_lock(sandbox_id):
            return self.execution.delete_file(
                sandbox["container_id"], path, recursive=recursive
            )

    def create_directory(self, sandbox_id: str, path: str, mode: int = 0o755) -> bool:
        """Create directory in sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        with self._sandbox_io_lock(sandbox_id):
            return self.execution.create_directory(sandbox["container_id"], path, mode)

    def get_metrics(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox metrics."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return None

        stats = self.execution.get_container_stats(sandbox["container_id"])
        if not stats:
            return None

        return {
            "sandbox_id": sandbox_id,
            "memory_usage": stats["memory_usage"],
            "memory_limit": stats["memory_limit"],
            "cpu_percent": stats["cpu_percent"],
            "uptime": stats["uptime"],
        }

    def get_sandbox_lifecycle(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """DB state plus whether the workload process is still running."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return None
        alive = self.execution.is_container_running(sandbox["container_id"])
        return {
            "sandbox_id": sandbox_id,
            "state": sandbox.get("state", "unknown"),
            "running": bool(alive),
            "timeout_seconds": int(sandbox["timeout"])
            if sandbox.get("timeout") is not None
            else None,
        }

    def refresh_sandbox_timeout(self, sandbox_id: str, timeout_seconds: int) -> bool:
        """Update stored lease timeout (E2B ``set_timeout``). Requires a running sandbox row."""
        sid = (sandbox_id or "").strip()
        if not sid:
            return False
        if not self.get_sandbox(sid):
            return False
        if not self.is_running(sid):
            return False
        ts = max(60, min(int(timeout_seconds), 604800))
        return bool(self.db.update_sandbox_timeout(sid, ts))

    def kill_sandbox(self, sandbox_id: str, force: bool = True) -> bool:
        """Kill sandbox."""
        self.discard_from_warm_pool(sandbox_id)
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            logger.error("Sandbox not found: %s", sandbox_id)
            return False

        container_id = sandbox["container_id"]

        if not self.execution.kill_container(container_id, force=force):
            logger.error("Failed to kill workload for sandbox %s", sandbox_id)
            return False

        self.db.update_sandbox_state(sandbox_id, "killed")
        self.db.delete_sandbox(sandbox_id)

        logger.info("Sandbox killed: %s", sandbox_id)
        return True

    def pause_sandbox(self, sandbox_id: str) -> bool:
        """Pause sandbox (Docker: freeze cgroup / ``docker pause``)."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        if self.execution.pause_instance(sandbox["container_id"]):
            self.db.update_sandbox_state(sandbox_id, "paused")
            logger.info("Sandbox paused: %s", sandbox_id)
            return True
        logger.warning("Pause not applied for sandbox %s (unsupported or failed)", sandbox_id)
        return False

    def resume_sandbox(self, sandbox_id: str) -> bool:
        """Resume paused sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        if self.execution.resume_instance(sandbox["container_id"]):
            self.db.update_sandbox_state(sandbox_id, "running")
            self.refresh_guest_routing_metadata(sandbox_id)
            logger.info("Sandbox resumed: %s", sandbox_id)
            return True
        logger.warning("Resume not applied for sandbox %s (unsupported or failed)", sandbox_id)
        return False
