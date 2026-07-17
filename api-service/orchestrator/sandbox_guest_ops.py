"""Guest service bootstrap, envd routing, and data-plane connection helpers."""

from __future__ import annotations

import logging
import shlex
import threading
import time
from typing import Any, Dict, List, Optional

import httpx

from .envd_template_bake import (
    ENVD_BAKE_MARKER,
    bake_envd_guest_into_container,
    container_has_baked_envd,
    envd_health_wait_loop_script,
    guest_tcp_wait_loop_script,
    should_embed_envd_at_template_build,
    uvicorn_envd_start_background_script,
    uvicorn_envd_start_script,
)
from .runtime_utils import is_container_like_execution
from .sandbox_constants import ENVD_TEMPLATE_BAKED_ENV

logger = logging.getLogger(__name__)

class SandboxGuestOpsMixin:
    def get_e2b_agent_upstream_ws_uri(self, sandbox_id: str) -> Optional[str]:
        """Deprecated on control-plane — clients use runtime-gateway data-plane URLs."""
        return None

    def get_traffic_access_token(self, sandbox_id: str) -> Optional[str]:
        """Layer-3 token minted once at sandbox create and stored in metadata."""
        from orchestrator.sandbox_connections import traffic_access_token_for_row

        row = self.db.get_sandbox((sandbox_id or "").strip())
        if not row:
            return None
        return traffic_access_token_for_row(row)

    def get_envd_connection_ex(
        self, sandbox_id: str, *, internal: bool = True
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Like ``get_envd_connection`` but on failure returns ``(None, short_reason)`` for HTTP 503 hints.

        Reasons intentionally omit tokens and host ports.
        """
        sid = (sandbox_id or "").strip()
        kind = self.execution.get_backend_kind()
        if kind not in ("docker", "gvisor"):
            return None, f"runtime {kind!r} does not support envd (need docker or gvisor)"
        if not is_container_like_execution(self.execution):
            return None, "execution backend does not support in-guest envd"
        row = self.db.get_sandbox(sid)
        if not row:
            return None, "sandbox not found"
        execution = self._execution_for_row(row)
        runtime_failure = self.get_sandbox_runtime_failure(sid)
        if runtime_failure:
            return None, runtime_failure
        cid = (row.get("container_id") or "").strip()
        if not cid or not execution.is_container_running(cid):
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
            resolve_guest_upstream_http,
            sandbox_domain_for_config,
        )

        if data_plane_enabled_for_config(self._config):
            external_http_base_url = data_plane_base_url(
                self._config,
                sandbox_id=sid,
                port=port,
                scheme="http",
            )
            internal_http_base_url = ""
            internal_headers: Dict[str, str] = {}
            if internal:
                gateway_api_base = str(row.get("gateway_api_base") or "").strip().rstrip("/")
                if gateway_api_base:
                    internal_http_base_url = gateway_api_base
                    internal_headers = {
                        "x-runtime-gateway-forwarded": "1",
                        "x-sandbox-id": sid,
                        "x-guest-port": str(port),
                    }
                else:
                    internal_http_base_url = resolve_guest_upstream_http(self, sid, port) or ""
            if internal and not internal_http_base_url:
                return None, f"envd internal upstream unavailable for guest port {port}"
            out = {
                "sandbox_id": sid,
                "envd_port": port,
                "sandbox_domain": sandbox_domain_for_config(self._config),
                "http_base_url": internal_http_base_url or external_http_base_url,
                "access_token": tok,
            }
            if internal_headers:
                out["internal_route_headers"] = internal_headers
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
        self,
        template_id: str,
        *,
        execution: Optional[Any] = None,
    ) -> tuple[str, str, Dict[str, str], int]:
        """Return ``(start_cmd, image_ref, template_env, guest_port)``."""
        tid = (template_id or "").strip()
        row = self.db.get_sandbox_template(tid) if tid else None
        sc = (row.get("start_cmd") or "").strip() if row else ""
        tpl_env = dict(row.get("env") or {}) if row else {}
        img_ref = ""
        if row:
            img_ref = (row.get("warm_snapshot_image") or row.get("base_image") or "").strip()
        execution = execution or self.execution
        if not sc and img_ref:
            sc = execution.image_start_cmd_shell(img_ref) or ""
        if not tpl_env and img_ref:
            tpl_env = execution.image_env_dict(img_ref)
        try:
            guest_port = int(str(tpl_env.get("PORT") or "0").strip() or "0")
        except ValueError:
            guest_port = 0
        return sc, img_ref, tpl_env, guest_port

    def _ensure_envd_baked(self, sandbox_id: str, container_id: str, *, pip_timeout: float) -> bool:
        """Ensure ``/opt/envd_guest`` and deps exist in the guest before startup."""
        execution = self._execution_for_row(self.get_sandbox(sandbox_id) or {})
        with self._sandbox_io_lock(sandbox_id):
            if container_has_baked_envd(execution.run_command, container_id):
                return True
            return bake_envd_guest_into_container(
                put_archive_to_container=execution.put_archive_to_container,
                run_command=execution.run_command,
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

    def _mark_template_envd_baked(self, template_id: str) -> None:
        tid = (template_id or "").strip()
        if not tid or not should_embed_envd_at_template_build(self._config):
            return
        merge = getattr(self.db, "merge_template_env", None)
        if not callable(merge):
            return
        try:
            merge(tid, {ENVD_TEMPLATE_BAKED_ENV: "true"})
        except Exception:
            logger.debug("Template %s envd-baked marker update failed", tid, exc_info=True)

    def _startup_managed_bootstrap_spec(
        self,
        template_id: str,
        *,
        start_envd: bool,
        envd_port: int,
        execution: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        if not is_container_like_execution(self.execution):
            return None

        sc, _img_ref, tpl_env, guest_port = self._resolve_template_start_spec(
            template_id,
            execution=execution,
        )
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
            "start_cmd": sc,
            "guest_port": gp if 1 <= gp <= 65535 else guest_port,
            "envd_port": p if start_envd else 0,
        }

    def _wait_for_startup_managed_service(
        self,
        sandbox_id: str,
        container_id: str,
        port: int,
        *,
        template_id: str,
        label: str,
        log_path: str,
        timeout_seconds: float,
    ) -> bool:
        p = int(port or 0)
        if not (1 <= p <= 65535):
            return True
        execution = self._execution_for_row(self.get_sandbox(sandbox_id) or {})
        wait_script = (
            envd_health_wait_loop_script
            if str(label or "").strip().lower() == "envd"
            else guest_tcp_wait_loop_script
        )
        wt = execution.run_command(
            container_id,
            wait_script(
                p,
                max_seconds=timeout_seconds,
                poll_seconds=self._guest_bootstrap_poll_seconds(),
                log_path=log_path,
            ),
            timeout=timeout_seconds + 10.0,
        )
        if int(wt.get("exit_code") or 0) == 0:
            return True
        logger.warning(
            "startup-managed bootstrap: %s :%s not ready sandbox=%s template=%s log=%s",
            label,
            p,
            sandbox_id,
            template_id,
            (wt.get("stderr") or wt.get("stdout") or "")[:2500],
        )
        return False

    def _wait_for_gateway_guest_readiness(
        self,
        sandbox_id: str,
        container_id: str,
        probes: List[Dict[str, Any]],
        *,
        timeout_seconds: float,
        warn_on_failure: bool = True,
        update_routing_on_success: bool = False,
    ) -> Optional[bool]:
        """Validate guest listeners from the owning runtime-gateway shard.

        Docker/gVisor sandboxes run behind a per-shard Docker bridge. The API pod cannot
        reliably dial that bridge directly, and Docker exec-based polling is slow. The
        owning runtime-gateway can dial the bridge immediately, so use it as the readiness
        vantage point before returning a ready-to-use sandbox.
        """
        if not probes:
            return True
        row = self.get_sandbox(sandbox_id) or {}
        api_base = str(row.get("gateway_api_base") or row.get("gateway_route_base") or "").strip().rstrip("/")
        if not api_base:
            return None
        execution = self._execution_for_row(row)
        host = ""
        try:
            host = (execution.get_container_internal_ipv4(container_id) or "").strip()
        except Exception as ex:  # noqa: BLE001
            logger.debug("gateway readiness: could not inspect container ip sandbox=%s: %s", sandbox_id, ex)
        if not host:
            return None

        targets: List[Dict[str, Any]] = []
        for probe in probes:
            try:
                port = max(1, min(65535, int(probe.get("port") or 0)))
            except (TypeError, ValueError):
                continue
            if not (1 <= port <= 65535):
                continue
            targets.append(
                {
                    "label": str(probe.get("label") or port),
                    "host": host,
                    "port": port,
                    "mode": str(probe.get("mode") or "tcp"),
                    "path": str(probe.get("path") or "/"),
                }
            )
        if not targets:
            return True

        timeout = max(0.5, float(timeout_seconds or 0.0))
        payload = {
            "targets": targets,
            "timeout_seconds": timeout,
            "poll_seconds": self._guest_bootstrap_poll_seconds(),
        }
        started = time.monotonic()
        try:
            with httpx.Client(timeout=httpx.Timeout(timeout + 5.0, connect=2.0)) as client:
                resp = client.post(
                    f"{api_base}/internal/runtime/probe",
                    headers=self._gateway_headers(),
                    json=payload,
                )
            if resp.status_code == 404:
                return None
            elapsed = max(0.0, time.monotonic() - started)
            data = resp.json() if resp.content else {}
            results = data.get("results") if isinstance(data, dict) else None
            if resp.status_code < 400 and bool(data.get("ok")):
                if update_routing_on_success:
                    self._merge_gateway_probe_routing_metadata(sandbox_id, targets)
                logger.info(
                    "gateway readiness probe complete sandbox=%s gateway=%s seconds=%.3f results=%s",
                    sandbox_id,
                    row.get("gateway_instance_id") or "-",
                    elapsed,
                    results or [],
                )
                return True
            log = logger.warning if warn_on_failure else logger.info
            log(
                "gateway readiness probe not ready sandbox=%s gateway=%s status=%s seconds=%.3f results=%s body=%s",
                sandbox_id,
                row.get("gateway_instance_id") or "-",
                resp.status_code,
                elapsed,
                results or [],
                resp.text[:1000],
            )
            return False
        except Exception as ex:  # noqa: BLE001
            log = logger.warning if warn_on_failure else logger.info
            log("gateway readiness probe error sandbox=%s: %s", sandbox_id, ex)
            return None

    def _merge_gateway_probe_routing_metadata(self, sandbox_id: str, targets: List[Dict[str, Any]]) -> None:
        sid = (sandbox_id or "").strip()
        if not sid:
            return
        routing: Dict[str, Any] = {}
        for target in targets:
            host = str(target.get("host") or "").strip()
            if not host:
                continue
            try:
                port = max(1, min(65535, int(target.get("port") or 0)))
            except (TypeError, ValueError):
                continue
            if not (1 <= port <= 65535):
                continue
            routing[str(port)] = {
                "scheme": "http",
                "host": host,
                "port": port,
                "guest_port": port,
                "kind": "bridge",
                "upstream_http": f"http://{host}:{port}",
            }
        if routing:
            self.db.merge_sandbox_metadata(sid, {"guest_routing": routing})

    def _startup_managed_readiness_probes(
        self,
        startup_boot: Optional[Dict[str, Any]],
        *,
        auto_start_envd: bool,
        envd_port_cfg: int,
    ) -> List[Dict[str, Any]]:
        if startup_boot is None:
            return []
        probes: List[Dict[str, Any]] = []
        if auto_start_envd:
            probes.append(
                {
                    "label": "envd",
                    "port": int(startup_boot.get("envd_port") or envd_port_cfg),
                    "mode": "http",
                    "path": "/health",
                }
            )
        guest_port = int(startup_boot.get("guest_port") or 0)
        if guest_port > 0:
            probes.append(
                {
                    "label": "agent",
                    "port": guest_port,
                    "mode": "tcp",
                }
            )
        return probes

    def ensure_guest_port_ready(self, sandbox_id: str, guest_port: int, *, timeout_seconds: Optional[float] = None) -> bool:
        """Wait until a requested guest port is reachable from its owning gateway shard."""
        sid = (sandbox_id or "").strip()
        row = self.get_sandbox(sid)
        if not row:
            return False
        cid = (row.get("container_id") or "").strip()
        if not cid:
            return False
        try:
            port = max(1, min(65535, int(guest_port)))
        except (TypeError, ValueError):
            return False
        envd_port = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        probe = {
            "label": "envd" if port == envd_port else "guest",
            "port": port,
            "mode": "http" if port == envd_port else "tcp",
            "path": "/health" if port == envd_port else "/",
        }
        wait = (
            float(timeout_seconds)
            if timeout_seconds is not None
            else float(getattr(self._config, "SANDBOX_ROUTE_READY_WAIT_SEC", 12.0) or 12.0)
        )
        ready = self._wait_for_gateway_guest_readiness(
            sid,
            cid,
            [probe],
            timeout_seconds=wait,
            warn_on_failure=True,
            update_routing_on_success=True,
        )
        # If no gateway probe is available for this runtime, preserve the existing route behavior.
        return True if ready is None else bool(ready)

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

        execution = self._execution_for_row(self.get_sandbox(sandbox_id) or {})

        def _start_once() -> Dict[str, Any]:
            with self._sandbox_io_lock(sandbox_id):
                start = uvicorn_envd_start_script(p)
                if not wait_for_listen:
                    start = f"set -eu\n{uvicorn_envd_start_background_script(p)}"
                return execution.run_command(
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
                "envd auto-start: declared baked template failed first start sandbox=%s template=%s output=%s; retrying after bake probe",
                sandbox_id,
                template_id,
                (st.get("stderr") or st.get("stdout") or "")[:1200],
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

        diag = execution.run_command(
            container_id,
            (
                "printf '%s\\n' '--- envd marker ---'; "
                f"cat {shlex.quote(ENVD_BAKE_MARKER)} 2>&1 || true; "
                "printf '%s\\n' '--- envd health ---'; "
                "python3 - <<'PY' 2>&1 || true\n"
                "import urllib.request\n"
                f"print(urllib.request.urlopen('http://127.0.0.1:{p}/health', timeout=2).read().decode())\n"
                "PY\n"
                "printf '%s\\n' '--- envd log ---'; "
                "cat /tmp/envd.log 2>&1 || true"
            ),
            timeout=10.0,
        )
        logger.warning(
            "envd auto-start: daemon did not listen on :%s sandbox=%s output=%s diagnostic=%s",
            p,
            sandbox_id,
            (st.get("stderr") or st.get("stdout") or "")[:2500],
            (diag.get("stderr") or diag.get("stdout") or "")[:2500],
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
        sandbox_row = self.get_sandbox(sandbox_id) or {}
        execution = self._execution_for_row(sandbox_row)
        sc = (row.get("start_cmd") or "").strip()
        tpl_env = dict(row.get("env") or {})
        img_ref = (row.get("warm_snapshot_image") or row.get("base_image") or "").strip()
        if not sc and img_ref:
            sc = execution.image_start_cmd_shell(img_ref)
            if sc:
                logger.info(
                    "template start_cmd from image %r for %r: %s",
                    img_ref,
                    tid,
                    sc[:200],
                )
        if not tpl_env and img_ref:
            tpl_env = execution.image_env_dict(img_ref)
        try:
            guest_port = int(str(tpl_env.get("PORT") or "0").strip() or "0")
        except ValueError:
            guest_port = 0
        if not sc:
            logger.debug("template start_cmd empty for %r — skip bootstrap", tid)
            return
        exec_user = "root"
        if img_ref and is_container_like_execution(self.execution):
            exec_user = execution.image_default_user(img_ref)
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
            st = execution.run_command(container_id, script, timeout=60.0, user=exec_user)
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
                wt = execution.run_command(
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
        """Store Docker bridge upstream targets for runtime-gateway."""
        from orchestrator.sandbox_connections import build_guest_routing_record

        sid = (sandbox_id or "").strip()
        md: Dict[str, Any] = {}
        record = build_guest_routing_record(self, sid)
        if record:
            md["guest_routing"] = record
        if md:
            self.db.merge_sandbox_metadata(sid, md)

    def _bootstrap_guest_services(self, sandbox_id: str, container_id: str, template_id: str) -> bool:
        """Idempotent guest daemons after the workload is running (control-plane responsibility)."""
        if not is_container_like_execution(self.execution):
            return True
        envd_port_cfg = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        envd_always = bool(getattr(self._config, "ENVD_ALWAYS_ON", True))
        publish_envd_legacy = bool(getattr(self._config, "ENVD_PUBLISH_PORT", False))
        start_envd = envd_always or publish_envd_legacy
        auto_start_envd = start_envd and getattr(self._config, "ENVD_AUTO_START", True)
        execution = self._execution_for_row(self.get_sandbox(sandbox_id) or {})
        startup_boot = None
        if auto_start_envd or template_id:
            startup_boot = self._startup_managed_bootstrap_spec(
                template_id,
                start_envd=auto_start_envd,
                envd_port=envd_port_cfg,
                execution=execution,
            )
        if startup_boot is not None:
            probes = self._startup_managed_readiness_probes(
                startup_boot,
                auto_start_envd=auto_start_envd,
                envd_port_cfg=envd_port_cfg,
            )
            guest_port = int(startup_boot.get("guest_port") or 0)
            gateway_ready = self._wait_for_gateway_guest_readiness(
                sandbox_id,
                container_id,
                probes,
                timeout_seconds=max(self._guest_bootstrap_agent_wait_seconds(), 15.0 if auto_start_envd else 0.0),
                update_routing_on_success=True,
            )
            if gateway_ready is not None:
                if not gateway_ready:
                    if not auto_start_envd:
                        return False
                    logger.warning(
                        "startup-managed bootstrap: gateway saw incompatible envd, refreshing sandbox=%s template=%s",
                        sandbox_id,
                        template_id,
                    )
                    if not self._bootstrap_envd_daemon(
                        sandbox_id,
                        container_id,
                        envd_port_cfg,
                        template_id=template_id,
                        wait_for_listen=True,
                    ):
                        return False
                    gateway_ready = self._wait_for_gateway_guest_readiness(
                        sandbox_id,
                        container_id,
                        probes,
                        timeout_seconds=max(self._guest_bootstrap_agent_wait_seconds(), 15.0),
                        update_routing_on_success=True,
                    )
                    if gateway_ready is False:
                        return False
                if gateway_ready is not None:
                    if startup_boot.get("start_cmd"):
                        logger.info(
                            "template start_cmd bootstrapped at startup sandbox=%s template=%r cmd=%r port=%s",
                            sandbox_id,
                            template_id,
                            str(startup_boot.get("start_cmd"))[:120],
                            startup_boot.get("guest_port") or "?",
                        )
                    if auto_start_envd:
                        logger.info("envd auto-start: sandbox %s guest tcp/%s ready (gateway)", sandbox_id, envd_port_cfg)
                    return True
            if auto_start_envd:
                envd_wait = min(self._guest_bootstrap_agent_wait_seconds(), 15.0)
                if not self._wait_for_startup_managed_service(
                    sandbox_id,
                    container_id,
                    int(startup_boot.get("envd_port") or 0),
                    template_id=template_id,
                    label="envd",
                    log_path="/tmp/envd.log",
                    timeout_seconds=envd_wait,
                ):
                    if not self._bootstrap_envd_daemon(
                        sandbox_id,
                        container_id,
                        envd_port_cfg,
                        template_id=template_id,
                        wait_for_listen=True,
                    ):
                        return False
            if guest_port > 0 and not self._wait_for_startup_managed_service(
                sandbox_id,
                container_id,
                guest_port,
                template_id=template_id,
                label="agent",
                log_path="/tmp/template-start.log",
                timeout_seconds=self._guest_bootstrap_agent_wait_seconds(),
            ):
                return False
            if startup_boot.get("start_cmd"):
                logger.info(
                    "template start_cmd bootstrapped at startup sandbox=%s template=%r cmd=%r port=%s",
                    sandbox_id,
                    template_id,
                    str(startup_boot.get("start_cmd"))[:120],
                    startup_boot.get("guest_port") or "?",
                )
            if auto_start_envd:
                logger.info("envd auto-start: sandbox %s guest tcp/%s ready (startup)", sandbox_id, envd_port_cfg)
            self.refresh_guest_routing_metadata(sandbox_id)
            return True

        if auto_start_envd:
            if not self._bootstrap_envd_daemon(
                sandbox_id,
                container_id,
                envd_port_cfg,
                template_id=template_id,
            ):
                self.refresh_guest_routing_metadata(sandbox_id)
                return False

        sc, _img_ref, _tpl_env, _guest_port = self._resolve_template_start_spec(
            template_id,
            execution=execution,
        )
        if sc:
            self._bootstrap_template_start_cmd(sandbox_id, container_id, template_id)

        self.refresh_guest_routing_metadata(sandbox_id)
        return True

    def _start_guest_bootstrap_background(self, sandbox_id: str, container_id: str, template_id: str) -> None:
        """Finish guest envd/template startup after a bounded cold-create response."""
        sid = (sandbox_id or "").strip()
        cid = (container_id or "").strip()
        if not sid or not cid:
            return

        def _run() -> None:
            started = time.monotonic()
            try:
                ok = self._bootstrap_guest_services(sid, cid, template_id)
                elapsed = round(max(0.0, time.monotonic() - started), 3)
                updates: Dict[str, Any] = {
                    "sandbox_bootstrap_pending": False,
                    "sandbox_bootstrap_seconds": elapsed,
                }
                if ok:
                    updates["sandbox_bootstrap_error"] = ""
                    logger.info("async guest bootstrap complete sandbox=%s seconds=%s", sid, elapsed)
                else:
                    updates["sandbox_bootstrap_error"] = "guest bootstrap failed"
                    logger.warning("async guest bootstrap failed sandbox=%s seconds=%s", sid, elapsed)
                self.db.merge_sandbox_metadata(sid, updates)
            except Exception as ex:  # noqa: BLE001
                elapsed = round(max(0.0, time.monotonic() - started), 3)
                logger.warning("async guest bootstrap error sandbox=%s seconds=%s: %s", sid, elapsed, ex)
                try:
                    self.db.merge_sandbox_metadata(
                        sid,
                        {
                            "sandbox_bootstrap_pending": False,
                            "sandbox_bootstrap_seconds": elapsed,
                            "sandbox_bootstrap_error": f"{type(ex).__name__}: {ex}",
                        },
                    )
                except Exception:
                    logger.debug("async guest bootstrap: metadata update failed sandbox=%s", sid, exc_info=True)

        threading.Thread(
            target=_run,
            name=f"guest-bootstrap-{sid[:16]}",
            daemon=True,
        ).start()
