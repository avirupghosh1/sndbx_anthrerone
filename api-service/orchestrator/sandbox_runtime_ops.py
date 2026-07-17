"""Runtime and lifecycle operations for SandboxManager."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class SandboxRuntimeOpsMixin:
    def get_sandbox(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox info."""
        return self.db.get_sandbox(sandbox_id)

    def _remember_recent_created_row(self, row: Optional[Dict[str, Any]]) -> None:
        sid = str((row or {}).get("sandbox_id") or "").strip()
        if not sid:
            return
        with self._recent_created_rows_lock:
            self._recent_created_rows[sid] = dict(row or {})
            while len(self._recent_created_rows) > 128:
                self._recent_created_rows.pop(next(iter(self._recent_created_rows)), None)

    def get_sandbox_for_create_response(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        sid = (sandbox_id or "").strip()
        if not sid:
            return None
        with self._recent_created_rows_lock:
            row = self._recent_created_rows.pop(sid, None)
        if row:
            return row
        return self.get_sandbox(sid)

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
        if str(sandbox.get("state") or "").strip().lower() != "running":
            return False
        if self.get_sandbox_runtime_failure(sandbox_id) is not None:
            return False
        cid = str(sandbox.get("container_id") or "").strip()
        return self._container_runtime_state_for_row(sandbox, cid) == "running"

    def create_filesystem_snapshot(
        self,
        sandbox_id: str,
        label: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Persist the Docker writable layer as a new image."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            logger.error("create_filesystem_snapshot: unknown sandbox %s", sandbox_id)
            return None
        commit_fn = getattr(self._execution_for_row(sandbox), "commit_filesystem_snapshot", None)
        if not callable(commit_fn):
            self._record_observability_event(
                severity="error",
                category="sandbox",
                action="snapshot_failed",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                template_id=str(sandbox.get("template_id") or ""),
                message="Filesystem snapshot is unavailable for this execution backend",
                metadata={"label": label or ""},
            )
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
            self._record_observability_event(
                severity="error",
                category="sandbox",
                action="snapshot_failed",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                template_id=str(sandbox.get("template_id") or ""),
                message="Filesystem snapshot commit returned no image ref",
                metadata={"label": label or "", "repo": repo},
            )
            return None
        snapshot_id = f"snap-{snap_uuid}"
        row = self.db.insert_sandbox_snapshot(
            snapshot_id,
            sandbox_id,
            image_ref,
            label,
            owner_client_id=sandbox.get("owner_client_id"),
        )
        meta = dict(sandbox.get("metadata") or {})
        meta["last_snapshot_image"] = image_ref
        meta["last_snapshot_id"] = snapshot_id
        self.db.merge_sandbox_metadata(sandbox_id, meta)
        self._record_observability_event(
            severity="info",
            category="sandbox",
            action="snapshot_created",
            entity_type="snapshot",
            entity_id=snapshot_id,
            sandbox_id=sandbox_id,
            gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
            template_id=str(sandbox.get("template_id") or ""),
            message="Created filesystem snapshot",
            metadata={"snapshot_id": snapshot_id, "image_ref": image_ref, "label": label or ""},
        )
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
            result = self._execution_for_row(sandbox).run_command(
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
                stream_fn = getattr(self._execution_for_row(sandbox), "run_command_stream", None)
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
                    r = self._execution_for_row(sandbox).run_command(
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
            return self._execution_for_row(sandbox).read_file(sandbox["container_id"], path)

    def write_file(self, sandbox_id: str, path: str, content: str) -> bool:
        """Write file to sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        with self._sandbox_io_lock(sandbox_id):
            return self._execution_for_row(sandbox).write_file(sandbox["container_id"], path, content)

    def list_files(self, sandbox_id: str, path: str = "/") -> Optional[list]:
        """List files in sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return None

        with self._sandbox_io_lock(sandbox_id):
            return self._execution_for_row(sandbox).list_files(sandbox["container_id"], path)

    def delete_file(self, sandbox_id: str, path: str, recursive: bool = False) -> bool:
        """Delete file from sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        with self._sandbox_io_lock(sandbox_id):
            return self._execution_for_row(sandbox).delete_file(
                sandbox["container_id"], path, recursive=recursive
            )

    def create_directory(self, sandbox_id: str, path: str, mode: int = 0o755) -> bool:
        """Create directory in sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        with self._sandbox_io_lock(sandbox_id):
            return self._execution_for_row(sandbox).create_directory(sandbox["container_id"], path, mode)

    def get_metrics(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox metrics."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return None

        stats = self._execution_for_row(sandbox).get_container_stats(sandbox["container_id"])
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
        alive = self.get_sandbox_runtime_failure(sandbox_id) is None
        return {
            "sandbox_id": sandbox_id,
            "state": sandbox.get("state", "unknown"),
            "running": bool(alive),
            "timeout_seconds": int(sandbox["timeout"])
            if sandbox.get("timeout") is not None
            else None,
            "lease_expires_at": sandbox.get("lease_expires_at"),
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

    def _mark_sandbox_lost(self, sandbox_id: str, *, detail: Optional[str] = None) -> bool:
        sid = (sandbox_id or "").strip()
        if not sid:
            return False
        row = self.db.get_sandbox(sid)
        if not row:
            return False
        msg = (
            detail
            or "Previous sandbox container died after runtime-gateway restart; recreate the sandbox."
        )
        meta = dict(row.get("metadata") or {})
        meta["runtime_error"] = msg
        meta["runtime_error_code"] = "container_died"
        meta["runtime_error_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.db.update_sandbox_state(sid, "lost")
        self.db.merge_sandbox_metadata(sid, meta)
        self.discard_from_warm_pool(sid)
        self._record_observability_event(
            severity="warning",
            category="reconcile",
            action="sandbox_lost",
            entity_type="sandbox",
            entity_id=sid,
            sandbox_id=sid,
            gateway_instance_id=str(row.get("gateway_instance_id") or ""),
            template_id=str(row.get("template_id") or ""),
            message=msg,
            metadata={
                "previous_state": row.get("state") or "",
                "container_id": row.get("container_id") or "",
                "is_warm_pool": bool(row.get("is_warm_pool")),
            },
        )
        return True

    def _clear_sandbox_runtime_error(self, sandbox_id: str) -> None:
        sid = (sandbox_id or "").strip()
        if not sid:
            return
        row = self.db.get_sandbox(sid)
        if not row:
            return
        meta = dict(row.get("metadata") or {})
        changed = False
        for key in ("runtime_error", "runtime_error_code", "runtime_error_at"):
            if key in meta:
                meta.pop(key, None)
                changed = True
        if changed:
            self.db.merge_sandbox_metadata(sid, meta)

    def _container_runtime_state_for_row(self, row: dict, container_id: str) -> str:
        cid = str(container_id or "").strip()
        if not cid:
            return "missing"
        execution = self._execution_for_row(row)
        state_fn = getattr(execution, "get_container_state", None)
        if callable(state_fn):
            return str(state_fn(cid) or "unknown").strip().lower()
        try:
            return "running" if execution.is_container_running(cid) else "stopped"
        except Exception:
            return "unknown"

    @staticmethod
    def _runtime_state_matches_db_state(db_state: str, runtime_state: str) -> bool:
        row_state = str(db_state or "").strip().lower()
        state = str(runtime_state or "").strip().lower()
        if state == "running":
            return row_state in {"running", "starting", "resuming", "pausing"}
        if state == "paused":
            return row_state in {"paused", "pausing"}
        if state == "stopped":
            # Older runtime-gateway images collapsed Docker's "paused" status to
            # "stopped". Keep paused rows readable during rolling/local upgrades;
            # resume/kill still verifies the workload by acting on the container.
            return row_state in {"paused", "pausing"}
        return False

    def get_sandbox_runtime_failure(self, sandbox_id: str) -> Optional[str]:
        sid = (sandbox_id or "").strip()
        if not sid:
            return None
        row = self.db.get_sandbox(sid)
        if not row:
            return None
        meta = dict(row.get("metadata") or {})
        msg = str(
            meta.get("runtime_error")
            or "Previous sandbox container died after runtime-gateway restart; recreate the sandbox."
        )
        if str(row.get("state") or "").strip().lower() == "lost":
            return msg
        cid = str(row.get("container_id") or "").strip()
        if not cid:
            self._mark_sandbox_lost(sid, detail=msg)
            return msg
        row_state = str(row.get("state") or "").strip().lower()
        try:
            runtime_state = self._container_runtime_state_for_row(row, cid)
            if runtime_state == "unknown":
                return None
            if self._runtime_state_matches_db_state(row_state, runtime_state):
                return None
        except Exception:
            return None
        self._mark_sandbox_lost(sid, detail=msg)
        return msg

    def _delete_sandbox_record(self, sandbox_id: str, *, mark_state: Optional[str] = None) -> bool:
        sid = (sandbox_id or "").strip()
        if not sid:
            return False
        self.discard_from_warm_pool(sid)
        if mark_state:
            try:
                self.db.update_sandbox_state(sid, mark_state)
            except Exception:
                pass
        deleted = self.db.delete_sandbox(sid)
        if deleted:
            logger.info("Sandbox record deleted: %s", sid)
        return deleted

    def kill_sandbox(self, sandbox_id: str, force: bool = True) -> bool:
        """Kill sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            logger.error("Sandbox not found: %s", sandbox_id)
            return False

        container_id = str(sandbox.get("container_id") or "").strip()
        if not container_id:
            logger.warning("Sandbox %s has no workload id; deleting stale DB row", sandbox_id)
            deleted = self._delete_sandbox_record(sandbox_id, mark_state="killed")
            if deleted:
                self._record_observability_event(
                    severity="warning",
                    category="sandbox",
                    action="killed",
                    entity_type="sandbox",
                    entity_id=sandbox_id,
                    sandbox_id=sandbox_id,
                    gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                    template_id=str(sandbox.get("template_id") or ""),
                    message="Deleted stale sandbox row with no workload id",
                    metadata={"force": bool(force), "workload_missing": True},
                )
            return deleted

        killed = False
        try:
            killed = bool(self._execution_for_row(sandbox).kill_container(container_id, force=force))
        except Exception as ex:  # noqa: BLE001
            logger.warning("Kill workload raised for sandbox %s: %s", sandbox_id, ex)
            killed = False
        if not killed:
            try:
                if not self._execution_for_row(sandbox).is_container_running(container_id):
                    logger.warning(
                        "Sandbox %s workload already absent; deleting stale DB row",
                        sandbox_id,
                    )
                    deleted = self._delete_sandbox_record(sandbox_id, mark_state="killed")
                    if deleted:
                        self._record_observability_event(
                            severity="warning",
                            category="sandbox",
                            action="killed",
                            entity_type="sandbox",
                            entity_id=sandbox_id,
                            sandbox_id=sandbox_id,
                            gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                            template_id=str(sandbox.get("template_id") or ""),
                            message="Deleted sandbox row after workload was already absent",
                            metadata={"force": bool(force), "container_id": container_id, "workload_missing": True},
                        )
                    return deleted
            except Exception:
                logger.warning(
                    "Sandbox %s workload liveness unknown after kill failure; keeping DB row",
                    sandbox_id,
                )
                return False
            logger.error("Failed to kill workload for sandbox %s", sandbox_id)
            return False

        self._delete_sandbox_record(sandbox_id, mark_state="killed")
        self._record_observability_event(
            severity="info",
            category="sandbox",
            action="killed",
            entity_type="sandbox",
            entity_id=sandbox_id,
            sandbox_id=sandbox_id,
            gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
            template_id=str(sandbox.get("template_id") or ""),
            message="Killed sandbox",
            metadata={"force": bool(force), "container_id": container_id},
        )

        logger.info("Sandbox killed: %s", sandbox_id)
        return True

    def pause_sandbox(self, sandbox_id: str) -> bool:
        """Pause sandbox (Docker: freeze cgroup / ``docker pause``)."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False
        previous_state = str(sandbox.get("state") or "running").strip().lower() or "running"
        container_id = str(sandbox.get("container_id") or "").strip()
        if not container_id:
            return False

        self.db.update_sandbox_state(sandbox_id, "pausing")
        if self._execution_for_row(sandbox).pause_instance(container_id):
            self.db.update_sandbox_state(sandbox_id, "paused")
            self._clear_sandbox_runtime_error(sandbox_id)
            self._record_observability_event(
                severity="info",
                category="sandbox",
                action="paused",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                template_id=str(sandbox.get("template_id") or ""),
                message="Paused sandbox",
                metadata={"previous_state": previous_state, "container_id": container_id},
            )
            logger.info("Sandbox paused: %s", sandbox_id)
            return True
        runtime_state = self._container_runtime_state_for_row(sandbox, container_id)
        if runtime_state == "paused":
            self.db.update_sandbox_state(sandbox_id, "paused")
            self._clear_sandbox_runtime_error(sandbox_id)
            self._record_observability_event(
                severity="info",
                category="sandbox",
                action="paused",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                template_id=str(sandbox.get("template_id") or ""),
                message="Observed sandbox already paused",
                metadata={"previous_state": previous_state, "container_id": container_id},
            )
            logger.info("Sandbox paused: %s", sandbox_id)
            return True
        self.db.update_sandbox_state(sandbox_id, previous_state)
        self._record_observability_event(
            severity="warning",
            category="sandbox",
            action="pause_failed",
            entity_type="sandbox",
            entity_id=sandbox_id,
            sandbox_id=sandbox_id,
            gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
            template_id=str(sandbox.get("template_id") or ""),
            message="Pause was not applied",
            metadata={"previous_state": previous_state, "container_id": container_id, "runtime_state": runtime_state},
        )
        logger.warning("Pause not applied for sandbox %s (unsupported or failed)", sandbox_id)
        return False

    def resume_sandbox(self, sandbox_id: str) -> bool:
        """Resume paused sandbox."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False
        previous_state = str(sandbox.get("state") or "paused").strip().lower() or "paused"
        container_id = str(sandbox.get("container_id") or "").strip()
        if not container_id:
            return False

        self.db.update_sandbox_state(sandbox_id, "resuming")
        if self._execution_for_row(sandbox).resume_instance(container_id):
            self.db.update_sandbox_state(sandbox_id, "running")
            self._clear_sandbox_runtime_error(sandbox_id)
            self.refresh_guest_routing_metadata(sandbox_id)
            self._record_observability_event(
                severity="info",
                category="sandbox",
                action="resumed",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                template_id=str(sandbox.get("template_id") or ""),
                message="Resumed sandbox",
                metadata={"previous_state": previous_state, "container_id": container_id},
            )
            logger.info("Sandbox resumed: %s", sandbox_id)
            return True
        runtime_state = self._container_runtime_state_for_row(sandbox, container_id)
        if runtime_state == "running":
            self.db.update_sandbox_state(sandbox_id, "running")
            self._clear_sandbox_runtime_error(sandbox_id)
            self.refresh_guest_routing_metadata(sandbox_id)
            self._record_observability_event(
                severity="info",
                category="sandbox",
                action="resumed",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
                template_id=str(sandbox.get("template_id") or ""),
                message="Observed sandbox already running",
                metadata={"previous_state": previous_state, "container_id": container_id},
            )
            logger.info("Sandbox resumed: %s", sandbox_id)
            return True
        self.db.update_sandbox_state(sandbox_id, previous_state)
        self._record_observability_event(
            severity="warning",
            category="sandbox",
            action="resume_failed",
            entity_type="sandbox",
            entity_id=sandbox_id,
            sandbox_id=sandbox_id,
            gateway_instance_id=str(sandbox.get("gateway_instance_id") or ""),
            template_id=str(sandbox.get("template_id") or ""),
            message="Resume was not applied",
            metadata={"previous_state": previous_state, "container_id": container_id, "runtime_state": runtime_state},
        )
        logger.warning("Resume not applied for sandbox %s (unsupported or failed)", sandbox_id)
        return False
