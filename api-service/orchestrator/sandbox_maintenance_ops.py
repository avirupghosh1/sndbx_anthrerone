"""Background maintenance and persisted-state reconciliation for SandboxManager."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional

from .container_manager import ContainerManager
from .gateway_targets import GatewayTarget

logger = logging.getLogger(__name__)


class SandboxMaintenanceOpsMixin:
    def _start_lease_reaper(self) -> None:
        if self._lease_reaper_thread and self._lease_reaper_thread.is_alive():
            return
        self._lease_reaper_stop.clear()
        self._lease_reaper_thread = threading.Thread(
            target=self._lease_reaper_loop,
            name="sandbox-lease-reaper",
            daemon=True,
        )
        self._lease_reaper_thread.start()

    def stop_background_work(self, timeout: float = 5.0) -> None:
        self._lease_reaper_stop.set()
        if self._lease_reaper_thread and self._lease_reaper_thread.is_alive():
            self._lease_reaper_thread.join(timeout=timeout)
        self._gateway_deletion_cost_stop.set()
        if self._gateway_deletion_cost_thread and self._gateway_deletion_cost_thread.is_alive():
            self._gateway_deletion_cost_thread.join(timeout=timeout)
        sampler_stop = getattr(self, "_observability_sampler_stop", None)
        if sampler_stop is not None:
            sampler_stop.set()
        sampler_thread = getattr(self, "_observability_sampler_thread", None)
        if sampler_thread is not None and sampler_thread.is_alive():
            sampler_thread.join(timeout=timeout)

    def _lease_reaper_loop(self) -> None:
        interval = max(
            1.0,
            float(getattr(self._config, "SANDBOX_LEASE_REAPER_INTERVAL_SEC", 5.0) or 5.0),
        )
        reconcile_interval = max(
            interval,
            float(getattr(self._config, "SANDBOX_STATE_RECONCILE_INTERVAL_SEC", 10.0) or 10.0),
        )
        template_image_reconcile_interval = max(
            reconcile_interval,
            float(getattr(self._config, "TEMPLATE_IMAGE_RECONCILE_INTERVAL_SEC", 60.0) or 60.0),
        )
        last_reconcile_at = 0.0
        while not self._lease_reaper_stop.wait(interval):
            now = time.monotonic()
            if now - last_reconcile_at >= reconcile_interval:
                try:
                    stats = self.reconcile_persisted_state(limit=5000)
                    if int(stats.get("stale_marked_lost") or 0) or int(stats.get("warm_pool_revived") or 0):
                        logger.info("Sandbox state reconcile: %s", stats)
                except Exception as ex:  # noqa: BLE001
                    self._record_observability_event(
                        severity="error",
                        category="reconcile",
                        action="state_reconcile_failed",
                        entity_type="control_plane",
                        message="Sandbox state reconcile cycle failed",
                        metadata={"error": str(ex)},
                    )
                    logger.warning("Sandbox state reconcile cycle failed: %s", ex)
                last_reconcile_at = now
            if now - self._last_template_image_reconcile_at >= template_image_reconcile_interval:
                try:
                    stats = self.reconcile_template_image_availability(limit=500)
                    if int(stats.get("changed") or 0) or int(stats.get("errors") or 0):
                        logger.info("Template image availability reconcile: %s", stats)
                except Exception as ex:  # noqa: BLE001
                    self._record_observability_event(
                        severity="error",
                        category="reconcile",
                        action="template_image_reconcile_failed",
                        entity_type="control_plane",
                        message="Template image availability reconcile cycle failed",
                        metadata={"error": str(ex)},
                    )
                    logger.warning("Template image availability reconcile cycle failed: %s", ex)
                self._last_template_image_reconcile_at = now
            try:
                self.reap_expired_sandboxes(limit=100)
            except Exception as ex:  # noqa: BLE001
                logger.warning("Sandbox lease reaper cycle failed: %s", ex)
            try:
                self.purge_lost_sandboxes(limit=100)
            except Exception as ex:  # noqa: BLE001
                logger.warning("Lost sandbox purge cycle failed: %s", ex)
            try:
                self.prune_runtime_artifacts()
            except Exception as ex:  # noqa: BLE001
                logger.warning("Runtime artifact prune cycle failed: %s", ex)

    def reap_expired_sandboxes(self, limit: int = 100) -> int:
        expired = self.db.list_expired_sandboxes(limit=limit)
        reaped = 0
        for row in expired:
            sid = str(row.get("sandbox_id") or "").strip()
            if not sid:
                continue
            logger.info(
                "Sandbox lease expired: sandbox_id=%s lease_expires_at=%s",
                sid,
                row.get("lease_expires_at"),
            )
            if self.kill_sandbox(sid, force=True):
                reaped += 1
        return reaped

    def purge_lost_sandboxes(self, limit: int = 100) -> int:
        retention = max(60, int(getattr(self._config, "SANDBOX_LOST_RETENTION_SEC", 86400) or 86400))
        purged = int(self.db.purge_lost_sandboxes(retention, limit=limit))
        if purged:
            logger.info(
                "Purged lost sandboxes from DB: count=%s retention_seconds=%s",
                purged,
                retention,
            )
        return purged

    def prune_runtime_artifacts(self) -> Dict[str, int]:
        results = {"containers": 0, "images": 0}
        keep_refs: set[str] = set()
        list_templates = getattr(self.db, "list_all_sandbox_templates", None)
        template_rows = list_templates() if callable(list_templates) else self.db.list_sandbox_templates()
        for row in template_rows:
            for key in ("warm_snapshot_image", "registry_image_ref", "base_image"):
                ref = str(row.get(key) or "").strip()
                if ref:
                    keep_refs.add(ref)
        for ref in self.db.list_all_snapshot_image_refs():
            if ref:
                keep_refs.add(ref)
        repo_default = (
            str(getattr(self._config, "SANDBOX_SNAPSHOT_REPO", "mysandbox-snap") or "mysandbox-snap")
            .strip()
            .lower()
            .replace("/", "-")
        ) or "mysandbox-snap"
        prefixes = [repo_default, "mysandbox-df-"]

        seen: set[int] = set()
        executions: List[Any] = []
        base_execution = self.execution
        executions.append(base_execution)
        kind = base_execution.get_backend_kind()
        if kind in ("docker", "gvisor"):
            executions = []
            for target in self._gateway_targets():
                try:
                    status = self._gateway_runtime_status(target)
                    if not status.get("reachable"):
                        continue
                    executions.append(self._execution_for_gateway_target(target))
                except Exception:
                    continue
            if not executions:
                executions.append(base_execution)

        for execution in executions:
            if id(execution) in seen:
                continue
            seen.add(id(execution))
            prune_exited = getattr(execution, "prune_exited_containers", None)
            prune_images = getattr(execution, "prune_generated_images", None)
            if callable(prune_exited):
                results["containers"] += int(
                    prune_exited(int(getattr(self._config, "RUNTIME_EXITED_CONTAINER_RETENTION_SEC", 1800) or 1800))
                )
            if callable(prune_images):
                results["images"] += int(
                    prune_images(
                        keep_refs=keep_refs,
                        older_than_seconds=int(getattr(self._config, "TEMPLATE_IMAGE_RETENTION_SEC", 172800) or 172800),
                        repo_prefixes=prefixes,
                    )
                )
        if results["containers"] or results["images"]:
            logger.info(
                "Runtime artifact prune: removed exited_containers=%s images=%s",
                results["containers"],
                results["images"],
            )
        return results

    def reconcile_template_image_availability(self, limit: int = 200) -> Dict[str, int]:
        return self._template_images.reconcile(limit)

    def _image_exists(self, image_ref: str) -> bool:
        ref = (image_ref or "").strip()
        if not ref:
            return False
        fn = getattr(self.execution, "image_exists", None)
        if callable(fn):
            try:
                return bool(fn(ref))
            except Exception:
                return False
        return True

    def _image_exists_for_row(self, row: Optional[Dict[str, Any]], image_ref: str) -> bool:
        return self._template_images.image_exists_for_row(row, image_ref)

    def _repair_missing_template_image(self, template_id: str, row: Dict[str, Any]) -> Optional[str]:
        return self._template_images.repair_missing_image(template_id, row)

    def _ensure_template_runtime_image(self, template_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._template_images.ensure(template_id, row)

    def _image_for_gateway_target(
        self,
        *,
        template_id: str,
        row: Optional[Dict[str, Any]],
        requested_image: str,
        target: Optional[GatewayTarget],
    ) -> str:
        return self._template_images.image_for_target(
            template_id=template_id,
            row=row,
            requested_image=requested_image,
            target=target,
        )

    def _resolve_template_alias_for_create(
        self,
        requested_template_id: str,
        row: Optional[Dict[str, Any]],
        owner_client_id: Optional[str],
    ) -> tuple[str, Optional[Dict[str, Any]]]:
        """Resolve friendly template aliases to the tenant-scoped materialized row."""
        requested = (requested_template_id or "").strip()
        if not requested:
            return requested, row

        def usable(src: Optional[Dict[str, Any]]) -> bool:
            if not src:
                return False
            return bool((src.get("warm_snapshot_image") or src.get("registry_image_ref") or "").strip())

        candidate = self.db.get_best_sandbox_template_by_alias(
            requested,
            owner_client_id=owner_client_id,
            exclude_template_id=requested,
        )
        if not candidate or usable(row):
            return requested, row

        resolved = (candidate.get("template_id") or "").strip()
        if not resolved or resolved == requested:
            return requested, row

        logger.info(
            "Resolved sandbox template alias requested=%r resolved_template_id=%r owner_client_id=%r",
            requested,
            resolved,
            owner_client_id,
        )
        return resolved, candidate

    def reconcile_persisted_state(self, limit: int = 5000) -> Dict[str, int]:
        """Reconcile DB sandbox rows against the live execution plane after a restart."""
        from orchestrator.runtime_utils import workload_blocker_message

        blocker = workload_blocker_message(self.execution)
        if blocker:
            logger.warning("Sandbox reconcile skipped because execution plane is not ready: %s", blocker)
            return {
                "checked": 0,
                "stale_marked_lost": 0,
                "gateway_missing_marked_lost": 0,
                "expired_reaped": 0,
                "routing_refreshed": 0,
            }
        rows = self.db.list_sandboxes(limit=max(1, int(limit)), offset=0)
        stats = {
            "checked": 0,
            "stale_marked_lost": 0,
            "gateway_missing_marked_lost": 0,
            "liveness_unknown": 0,
            "warm_pool_revived": 0,
            "expired_reaped": 0,
            "routing_refreshed": 0,
        }
        live_gateway_ids, gateway_discovery_authoritative = self._live_runtime_gateway_instance_ids(force_refresh=True)
        active_states = {"running", "starting", "pausing", "resuming"}
        for row in rows:
            sid = str(row.get("sandbox_id") or "").strip()
            cid = str(row.get("container_id") or "").strip()
            if not sid:
                continue
            row_state = str(row.get("state") or "").strip().lower()
            gateway_id = str(row.get("gateway_instance_id") or "").strip()
            if (
                gateway_discovery_authoritative
                and row_state in active_states
                and gateway_id
                and gateway_id not in live_gateway_ids
            ):
                if self._mark_sandbox_lost(
                    sid,
                    detail=(
                        f"Runtime gateway pod {gateway_id} is no longer live; "
                        "the sandbox workload was lost and must be recreated."
                    ),
                ):
                    stats["gateway_missing_marked_lost"] += 1
                    stats["stale_marked_lost"] += 1
                logger.warning(
                    "Sandbox reconcile: marked sandbox lost because gateway pod is gone sandbox=%s gateway=%s state=%s warm_pool=%s",
                    sid,
                    gateway_id,
                    row_state,
                    bool(row.get("is_warm_pool")),
                )
                continue
            if row_state != "running":
                if row_state == "starting" and cid:
                    try:
                        state = self._container_runtime_state_for_row(row, cid)
                        if state == "running":
                            self.refresh_guest_routing_metadata(sid)
                            if self.db.update_sandbox_state(sid, "running"):
                                stats["routing_refreshed"] += 1
                            continue
                        if state != "unknown" and self._mark_sandbox_lost(
                            sid,
                            detail="Sandbox was still starting when the control plane restarted and the workload no longer exists.",
                        ):
                            stats["stale_marked_lost"] += 1
                            continue
                    except Exception as ex:  # noqa: BLE001
                        logger.debug("Sandbox reconcile: starting-state probe failed sandbox=%s: %s", sid, ex)
                if row_state == "lost" and bool(row.get("is_warm_pool")) and cid:
                    current_gateways = (
                        live_gateway_ids
                        if gateway_discovery_authoritative
                        else self._current_gateway_instance_ids()
                    )
                    if gateway_id and current_gateways and gateway_id not in current_gateways:
                        continue
                    try:
                        state = self._container_runtime_state_for_row(row, cid)
                        if state == "running" and self.db.update_sandbox_state(sid, "running"):
                            stats["warm_pool_revived"] += 1
                            logger.warning(
                                "Sandbox reconcile: revived live warm-pool sandbox previously marked lost sandbox=%s gateway=%s container=%s",
                                sid,
                                str(row.get("gateway_instance_id") or "").strip() or "-",
                                cid[:12],
                            )
                    except Exception as ex:  # noqa: BLE001
                        logger.debug("Sandbox reconcile: lost warm-pool revive check failed sandbox=%s: %s", sid, ex)
                continue
            stats["checked"] += 1
            alive = False
            state = "missing" if cid else "missing"
            if cid:
                try:
                    state = self._container_runtime_state_for_row(row, cid)
                    alive = self._runtime_state_matches_db_state(row_state, state)
                except Exception as ex:  # noqa: BLE001
                    logger.warning(
                        "Sandbox reconcile: liveness check failed sandbox=%s container=%s detail=%s",
                        sid,
                        cid[:12],
                        ex,
                    )
                    state = "unknown"
            if state == "unknown":
                stats["liveness_unknown"] += 1
                logger.warning(
                    "Sandbox reconcile: skipped sandbox=%s container=%s because runtime liveness is unknown",
                    sid,
                    cid[:12] if cid else "",
                )
                continue
            if not alive:
                if self._mark_sandbox_lost(
                    sid,
                    detail="Previous sandbox container died after runtime-gateway restart; recreate the sandbox.",
                ):
                    stats["stale_marked_lost"] += 1
                logger.warning(
                    "Sandbox reconcile: marked sandbox lost sandbox=%s container=%s",
                    sid,
                    cid[:12] if cid else "",
                )
                continue
            try:
                self.refresh_guest_routing_metadata(sid)
                stats["routing_refreshed"] += 1
            except Exception as ex:  # noqa: BLE001
                logger.warning("Sandbox reconcile: routing refresh failed sandbox=%s: %s", sid, ex)

        try:
            stats["expired_reaped"] = int(self.reap_expired_sandboxes(limit=max(1, int(limit))))
        except Exception as ex:  # noqa: BLE001
            logger.warning("Sandbox reconcile: expired sandbox reap failed: %s", ex)
        changed = {
            key: value
            for key, value in stats.items()
            if key != "checked" and int(value or 0) > 0
        }
        if changed:
            self._record_observability_event(
                severity="warning" if int(stats.get("stale_marked_lost") or 0) else "info",
                category="reconcile",
                action="state_reconciled",
                entity_type="control_plane",
                message="Sandbox persisted-state reconciliation changed runtime state",
                metadata={"stats": dict(stats)},
            )
        return stats

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
        if not isinstance(self.execution, ContainerManager):
            raise TypeError(
                "SandboxManager.container_mgr is only valid for direct Docker execution; "
                "runtime-gateway mode does not expose a Docker client to the API."
            )
        return self.execution

    def get_execution_kind(self) -> str:
        return self.execution.get_backend_kind()

    def describe_docker_workload_blocker(self) -> Optional[str]:
        """If the execution plane cannot run sandboxes, return a short diagnostic string."""
        from orchestrator.runtime_utils import workload_blocker_message

        blocker = workload_blocker_message(self.execution)
        if blocker:
            return blocker
        if (self._last_create_error or "").strip():
            return self._last_create_error.strip()
        return None
