"""Sandbox management over Docker Engine runtime-gateway shards."""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional

from database import Database

from .gateway_targets import GatewayTargetSelector
from .runtime_gateway_execution import RuntimeGatewayExecution
from .sandbox_creation_ops import SandboxCreationOpsMixin
from .sandbox_gateway_ops import SandboxGatewayOpsMixin
from .sandbox_guest_ops import SandboxGuestOpsMixin
from .sandbox_maintenance_ops import SandboxMaintenanceOpsMixin
from .sandbox_runtime_ops import SandboxRuntimeOpsMixin
from .sandbox_template_build_ops import SandboxTemplateBuildMixin
from .template_image_lifecycle import TemplateImageLifecycle

if TYPE_CHECKING:
    from .protocols import SandboxExecutionPlane

logger = logging.getLogger(__name__)


class SandboxManager(
    SandboxCreationOpsMixin,
    SandboxGuestOpsMixin,
    SandboxMaintenanceOpsMixin,
    SandboxGatewayOpsMixin,
    SandboxTemplateBuildMixin,
    SandboxRuntimeOpsMixin,
):
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
        self._gateway_execution_guard = threading.Lock()
        self._gateway_execution_cache: Dict[str, RuntimeGatewayExecution] = {}
        self._gateway_selector = GatewayTargetSelector()
        self._gateway_status_cache: Dict[str, Dict[str, Any]] = {}
        self._gateway_status_lock = threading.Lock()
        self._gateway_image_cache: Dict[tuple[str, str], tuple[float, bool]] = {}
        self._gateway_image_cache_lock = threading.Lock()
        self._warm_pool_leader_lock = threading.Lock()
        self._warm_pool_leader_value = False
        self._warm_pool_leader_next_check_at = 0.0
        self._warm_pool_lease_client: Optional[Any] = None
        self._recent_created_rows: Dict[str, Dict[str, Any]] = {}
        self._recent_created_rows_lock = threading.Lock()
        self._lease_reaper_stop = threading.Event()
        self._lease_reaper_thread: Optional[threading.Thread] = None
        self._gateway_deletion_cost_stop = threading.Event()
        self._gateway_deletion_cost_thread: Optional[threading.Thread] = None
        self._observability_sampler_stop = threading.Event()
        self._observability_sampler_thread: Optional[threading.Thread] = None
        self._last_template_image_reconcile_at = 0.0
        self._last_create_error: str = ""
        self._template_images = TemplateImageLifecycle(self)
        self.warm_pool: Optional[Any] = None
        try:
            cfg = self._config
            from .warm_sandbox_pool import MultiWarmSandboxPool

            self.warm_pool = MultiWarmSandboxPool(self, cfg)
            self.warm_pool.start()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Warm sandbox pool not started: %s", ex)
        try:
            self._start_lease_reaper()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Sandbox lease reaper not started: %s", ex)
        try:
            self._start_runtime_gateway_deletion_cost_loop()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Runtime gateway deletion-cost updater not started: %s", ex)
        try:
            self._start_observability_sampler()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Observability sampler not started: %s", ex)

    def _record_observability_event(
        self,
        *,
        severity: str,
        category: str,
        action: str,
        entity_type: str = "",
        entity_id: str = "",
        gateway_instance_id: Optional[str] = None,
        template_id: Optional[str] = None,
        sandbox_id: Optional[str] = None,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        record = getattr(self.db, "record_observability_event", None)
        if not callable(record):
            return
        try:
            record(
                severity=severity,
                category=category,
                action=action,
                entity_type=entity_type,
                entity_id=entity_id,
                gateway_instance_id=gateway_instance_id,
                template_id=template_id,
                sandbox_id=sandbox_id,
                message=message,
                metadata=metadata or {},
            )
        except Exception as ex:  # noqa: BLE001
            logger.debug("observability event write failed: %s", ex, exc_info=True)

    def _start_observability_sampler(self) -> None:
        if not bool(getattr(self._config, "OBSERVABILITY_SAMPLER_ENABLED", True)):
            return
        if self._observability_sampler_thread and self._observability_sampler_thread.is_alive():
            return
        self._observability_sampler_stop.clear()
        self._observability_sampler_thread = threading.Thread(
            target=self._observability_sampler_loop,
            name="observability-sampler",
            daemon=True,
        )
        self._observability_sampler_thread.start()

    def _observability_sampler_loop(self) -> None:
        interval = max(5.0, float(getattr(self._config, "OBSERVABILITY_SAMPLE_INTERVAL_SEC", 30.0) or 30.0))
        while not self._observability_sampler_stop.wait(interval):
            try:
                self._sample_observability_once()
            except Exception as ex:  # noqa: BLE001
                logger.debug("observability sampler cycle failed: %s", ex, exc_info=True)

    @staticmethod
    def _metric_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _metric_gateway_ordinal(instance_id: str) -> int:
        try:
            return int(str(instance_id or "").rsplit("-", 1)[-1])
        except Exception:
            return 0

    def _sample_observability_once(self) -> None:
        record = getattr(self.db, "record_observability_metric_sample", None)
        if not callable(record):
            return
        ts = self._metric_timestamp()
        pod_metrics: Dict[str, Dict[str, Any]] = {}
        try:
            from .k8s_runtime_gateways import list_runtime_gateway_pods

            pod_metrics = {
                pod.name: {
                    "ordinal": int(pod.ordinal),
                    "ready": bool(pod.ready),
                    "cpu_millicores": int(pod.cpu_millicores),
                    "memory_bytes": int(pod.memory_bytes),
                }
                for pod in list_runtime_gateway_pods(self._config, force_refresh=False)
            }
        except Exception:
            pod_metrics = {}
        try:
            gateways = self.runtime_gateway_diagnostics()
        except Exception:
            gateways = []
        for row in gateways if isinstance(gateways, list) else []:
            gateway_id = str(row.get("gateway_instance_id") or "").strip()
            if not gateway_id:
                continue
            pod = pod_metrics.get(gateway_id, {})
            running = int(row.get("running_sandbox_count") or 0)
            warm = int(row.get("warm_sandbox_count") or 0)
            cpu_millicores = int(pod.get("cpu_millicores") or row.get("cpu_millicores") or 0)
            memory_bytes = int(pod.get("memory_bytes") or row.get("memory_bytes") or 0)
            memory_mib = int(max(0, memory_bytes) / (1024 * 1024))
            ordinal = int(pod.get("ordinal") or self._metric_gateway_ordinal(gateway_id))
            deletion_cost = (
                running * 100_000
                + warm * 50_000
                + int(max(0, cpu_millicores))
                + memory_mib
                - int(max(0, ordinal))
            )
            record(
                sample_type="gateway",
                gateway_instance_id=gateway_id,
                timestamp=ts,
                metrics={
                    "reachable": bool(row.get("reachable")),
                    "ready": bool(pod.get("ready")) if pod else bool(row.get("reachable")),
                    "running_sandbox_count": running,
                    "warm_sandbox_count": warm,
                    "cpu_millicores": cpu_millicores,
                    "memory_bytes": memory_bytes,
                    "deletion_cost": deletion_cost,
                    "disk_used_ratio": float(row.get("disk_used_ratio") or 0.0),
                    "disk_used_bytes": int(row.get("disk_used_bytes") or 0),
                    "disk_total_bytes": int(row.get("disk_total_bytes") or 0),
                },
            )
        try:
            pools = self.warm_pool_segment_diagnostics()
        except Exception:
            pools = []
        for row in pools if isinstance(pools, list) else []:
            key = str(row.get("warm_pool_key") or "").strip()
            if not key:
                continue
            desired = int(row.get("desired_size") or 0)
            ready = int(row.get("ready_count") or 0)
            inflight = int(row.get("inflight_count") or 0)
            record(
                sample_type="warm_pool",
                warm_pool_key=key,
                timestamp=ts,
                metrics={
                    "desired_size": desired,
                    "ready_count": ready,
                    "inflight_count": inflight,
                    "deficit": max(0, desired - ready - inflight),
                },
            )
        purge = getattr(self.db, "purge_observability_before", None)
        if callable(purge):
            retention_hours = max(1, int(getattr(self._config, "OBSERVABILITY_RETENTION_HOURS", 24) or 24))
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=retention_hours)).isoformat().replace("+00:00", "Z")
            purge(cutoff)
