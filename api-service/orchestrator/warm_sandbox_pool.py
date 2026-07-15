"""Pre-provisioned sandboxes to hide cold-start latency.

When ``SANDBOX_WARM_POOL_SIZE > 0``, one or more **pool segments** run in the background.
Each segment is keyed by ``(logical template_id, cpu, memory)``
and may provision from a **warm snapshot image** (Docker custom templates) or from
the base image (default ``SANDBOX_WARM_POOL_TEMPLATE_ID`` profile).

See ``docs/CUSTOM_TEMPLATES.md`` for custom templates + snapshot-backed warm pools (Docker).
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

if TYPE_CHECKING:
    from config import Config
    from .sandbox_manager import SandboxManager

logger = logging.getLogger(__name__)

PoolKey = Tuple[str, str, str]


def _compatible_pool_shape(
    template_id: str,
    cpu_limit: str,
    memory_limit: str,
) -> tuple[str, str, str]:
    return (template_id.strip(), str(cpu_limit), str(memory_limit))


def warm_pool_key_string(
    template_id: str,
    cpu_limit: str,
    memory_limit: str,
    timeout: int,
) -> str:
    return "|".join(
        [
            template_id.strip(),
            str(cpu_limit),
            str(memory_limit),
        ]
    )


class WarmSandboxPool:
    """Maintains ``pool_size`` idle sandboxes for one (template_id, cpu, mem) profile."""

    def __init__(
        self,
        manager: "SandboxManager",
        *,
        logical_template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        pool_size: int,
        from_snapshot_image: Optional[str] = None,
        provision_concurrency: int = 1,
    ):
        self._manager = manager
        self._logical_template_id = logical_template_id.strip()
        self._cpu = str(cpu_limit)
        self._mem = str(memory_limit)
        self._timeout = int(timeout)
        self._size = max(0, int(pool_size))
        self._from_snapshot = (from_snapshot_image or "").strip() or None
        self._provision_concurrency = max(1, int(provision_concurrency))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._pending_futures: set[Future[tuple[Optional[str], str, int]]] = set()
        self._pending_futures_lock = threading.Lock()
        self._pending_gateway_bytes: Dict[str, int] = {}
        self._pending_gateway_counts: Dict[str, int] = {}
        self._wake = threading.Event()
        self._last_inventory_reconcile_at = 0.0

    @property
    def from_snapshot_image(self) -> Optional[str]:
        """Snapshot image ref this segment uses when provisioning (``None`` = base image only)."""
        return self._from_snapshot

    @property
    def target_size(self) -> int:
        return self._size

    @property
    def pool_key(self) -> PoolKey:
        return (self._logical_template_id, self._cpu, self._mem)

    @property
    def pool_key_string(self) -> str:
        return warm_pool_key_string(self._logical_template_id, self._cpu, self._mem, self._timeout)

    def start(self) -> None:
        if self._size <= 0:
            return
        self._stop.clear()
        self._executor = ThreadPoolExecutor(
            max_workers=self._provision_concurrency,
            thread_name_prefix=f"warm-pool-{self._logical_template_id[:12]}",
        )
        self._thread = threading.Thread(
            target=self._run,
            name=f"warm-pool-{self._logical_template_id[:16]}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Warm pool segment started: target=%s template_id=%r snap=%r cpu=%s mem=%s timeout=%s concurrency=%s",
            self._size,
            self._logical_template_id,
            self._from_snapshot,
            self._cpu,
            self._mem,
            self._timeout,
            self._provision_concurrency,
        )

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        executor = self._executor
        self._executor = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def discard(self, sandbox_id: str) -> None:
        return None

    def resize(self, pool_size: int) -> None:
        self._size = max(0, int(pool_size))
        self._wake.set()

    def stats(self) -> dict[str, Any]:
        return {
            "template_id": self._logical_template_id,
            "target_size": self._size,
            "ready": self._manager.warm_pool_ready_count(self.pool_key_string),
            "from_snapshot_image": self._from_snapshot,
            "cpu_limit": self._cpu,
            "memory_limit": self._mem,
            "timeout": self._timeout,
            "provision_concurrency": self._provision_concurrency,
        }

    def try_acquire(
        self,
        template_id: str,
        metadata: Optional[Dict[str, Any]],
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
    ) -> Optional[str]:
        if self._size <= 0:
            return None
        if template_id.strip() != self._logical_template_id:
            return None
        if str(cpu_limit) != self._cpu or str(memory_limit) != self._mem:
            return None
        wait_sec = float(getattr(self._manager._config, "SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC", 0.0) or 0.0)
        deadline = time.monotonic() + wait_sec
        claimed = None
        waited_for_ready = False
        acquire_started = time.monotonic()
        while not self._stop.is_set():
            claim_metadata = dict(metadata or {})
            claim_metadata.pop("_warm_pool", None)
            claim_metadata["sandbox_allocation_source"] = (
                "cold_create" if waited_for_ready else "warm_pool_acquire"
            )
            claim_metadata["sandbox_allocation_pool_key"] = self.pool_key_string
            claim_metadata["sandbox_allocation_acquire_wait_seconds"] = round(
                max(0.0, time.monotonic() - acquire_started),
                3,
            )
            claimed = self._manager.acquire_warm_pool_sandbox(
                template_id=template_id,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=self._timeout,
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
                handoff_metadata=claim_metadata,
                handoff_timeout=int(timeout),
            )
            if claimed:
                break
            waited_for_ready = True
            if time.monotonic() >= deadline:
                break
            time.sleep(0.15)
        if not claimed:
            return None
        sid = str(claimed.get("sandbox_id") or "").strip()
        self._wake.set()
        md = claimed.get("metadata") if isinstance(claimed, dict) else {}
        md = md if isinstance(md, dict) else {}
        logger.info(
            "Warm handoff latency: sandbox=%s template=%s source=%s gateway=%s acquire_wait_seconds=%s",
            sid,
            self._logical_template_id,
            str(md.get("sandbox_allocation_source") or "warm_pool_acquire"),
            str(claimed.get("gateway_instance_id") or "-"),
            md.get("sandbox_allocation_acquire_wait_seconds"),
        )
        return sid

    def _run(self) -> None:
        idle_poll_seconds = max(
            0.1,
            min(
                5.0,
                float(getattr(self._manager._config, "SANDBOX_WARM_POOL_IDLE_POLL_SEC", 0.25) or 0.25),
            ),
        )
        while not self._stop.is_set():
            try:
                self._top_up()
            except Exception as ex:  # noqa: BLE001
                logger.exception("Warm pool top-up error: %s", ex)
            self._wake.wait(idle_poll_seconds)
            self._wake.clear()

    @contextmanager
    def _leader_keepalive(self):
        stop = threading.Event()
        ttl = max(
            5.0,
            float(getattr(self._manager._config, "WARM_POOL_COORDINATOR_LEASE_TTL_SEC", 15) or 15),
        )
        interval = max(1.0, min(ttl / 3.0, ttl - 1.0))

        def _refresh_loop() -> None:
            while not stop.wait(interval):
                try:
                    self._manager.is_warm_pool_leader()
                except Exception:
                    logger.debug("Warm pool leader keepalive refresh failed", exc_info=True)

        th = threading.Thread(
            target=_refresh_loop,
            name=f"warm-pool-lease-{self._logical_template_id[:16]}",
            daemon=True,
        )
        th.start()
        try:
            yield
        finally:
            stop.set()
            th.join(timeout=2.0)

    def _provision_one(self, gateway_instance_id: str, reserved_bytes: int) -> tuple[Optional[str], str, int]:
        try:
            row = self._manager.db.get_sandbox_template(self._logical_template_id)
            if row:
                row = self._manager._ensure_template_runtime_image(self._logical_template_id, row)
                fresh = (row.get("warm_snapshot_image") or row.get("registry_image_ref") or "").strip() or None
                if fresh != self._from_snapshot:
                    self._from_snapshot = fresh
        except Exception:
            logger.debug(
                "Warm pool: template image refresh failed for %r",
                self._logical_template_id,
                exc_info=True,
            )
        sid = self._manager._create_sandbox_fresh(
            template_id=self._logical_template_id,
            metadata={
                "_warm_pool": True,
                "warm_pool_snapshot_image": self._from_snapshot or "",
            },
            cpu_limit=self._cpu,
            memory_limit=self._mem,
            timeout=self._timeout,
            from_snapshot_image=self._from_snapshot,
            is_warm_pool=True,
            warm_pool_key=self.pool_key_string,
            forced_gateway_instance_id=gateway_instance_id,
        )
        return sid, gateway_instance_id, reserved_bytes

    def _provision_batch_slots_available(self) -> int:
        with self._pending_futures_lock:
            pending = len(self._pending_futures)
        return max(0, self._provision_concurrency - pending)

    def _planned_target(self) -> tuple[Optional[str], int]:
        target = self._manager.select_gateway_target_for_pool_create(
            template_id=self._logical_template_id,
            cpu_limit=self._cpu,
            memory_limit=self._mem,
            timeout=self._timeout,
            extra_used_bytes_by_gateway=dict(self._pending_gateway_bytes),
            extra_warm_counts_by_gateway=dict(self._pending_gateway_counts),
            force_refresh=False,
        )
        if target is None:
            return None, 0
        image_ref = self._from_snapshot or self._logical_template_id
        reserved = self._manager._estimated_sandbox_reservation_bytes(
            target,
            template_id=self._logical_template_id,
            image_ref=image_ref,
            force_refresh=False,
        )
        return target.instance_id, reserved

    def _submit_provision(self, gateway_instance_id: str, reserved_bytes: int) -> bool:
        executor = self._executor
        if executor is None:
            return False
        future = executor.submit(self._provision_one, gateway_instance_id, reserved_bytes)
        with self._pending_futures_lock:
            self._pending_futures.add(future)
            self._pending_gateway_bytes[gateway_instance_id] = (
                int(self._pending_gateway_bytes.get(gateway_instance_id) or 0) + max(0, int(reserved_bytes))
            )
            self._pending_gateway_counts[gateway_instance_id] = (
                int(self._pending_gateway_counts.get(gateway_instance_id) or 0) + 1
            )
        return True

    def _drain_completed_provisions(self) -> None:
        completed: list[Future[tuple[Optional[str], str, int]]] = []
        with self._pending_futures_lock:
            for future in list(self._pending_futures):
                if future.done():
                    self._pending_futures.remove(future)
                    completed.append(future)
        for future in completed:
            sid: Optional[str] = None
            gateway_instance_id = ""
            reserved_bytes = 0
            success = False
            try:
                sid, gateway_instance_id, reserved_bytes = future.result()
                if not sid:
                    self._manager.db.set_warm_pool_segment_error(
                        self.pool_key_string,
                        self._manager.get_last_create_error() or "warm pool provisioning failed",
                    )
                    logger.warning("Warm pool: failed to provision (template=%s)", self._logical_template_id)
                    continue
                row = self._manager.get_sandbox(sid)
                gateway_instance_id = str((row or {}).get("gateway_instance_id") or "").strip()
                if gateway_instance_id:
                    self._manager.db.set_warm_pool_segment_preferred_gateway(
                        self.pool_key_string,
                        gateway_instance_id,
                        clear_error=True,
                    )
                if row and bool(row.get("is_warm_pool")):
                    nready = self._manager.warm_pool_ready_count(self.pool_key_string)
                    logger.info(
                        "Warm pool: provisioned %s for template=%s (ready=%s)",
                        sid,
                        self._logical_template_id,
                        nready,
                    )
                success = True
            except Exception as ex:  # noqa: BLE001
                self._manager.db.set_warm_pool_segment_error(
                    self.pool_key_string,
                    str(ex) or self._manager.get_last_create_error() or "warm pool provisioning failed",
                )
                logger.warning("Warm pool: failed to provision (template=%s)", self._logical_template_id)
            finally:
                with self._pending_futures_lock:
                    if gateway_instance_id:
                        self._pending_gateway_bytes[gateway_instance_id] = max(
                            0,
                            int(self._pending_gateway_bytes.get(gateway_instance_id) or 0) - max(0, int(reserved_bytes)),
                        )
                        self._pending_gateway_counts[gateway_instance_id] = max(
                            0,
                            int(self._pending_gateway_counts.get(gateway_instance_id) or 0) - 1,
                        )
                self._manager.db.complete_warm_pool_slots(
                    warm_pool_key=self.pool_key_string,
                    count=1,
                    success=success,
                )

    def _reconcile_inventory_if_due(self) -> None:
        interval = float(
            getattr(self._manager._config, "WARM_POOL_INVENTORY_RECONCILE_SEC", 10.0) or 10.0
        )
        if interval <= 0:
            return
        now = time.monotonic()
        if now - self._last_inventory_reconcile_at < interval:
            return
        self._last_inventory_reconcile_at = now
        try:
            self._manager._live_warm_pool_rows(self.pool_key_string)
        except Exception:
            logger.debug(
                "Warm pool inventory reconcile failed key=%s",
                self.pool_key_string,
                exc_info=True,
            )

    def _top_up(self) -> None:
        if not self._manager.is_warm_pool_leader():
            return
        leader_ctx = self._leader_keepalive()
        with leader_ctx:
            while not self._stop.is_set():
                self._drain_completed_provisions()
                self._reconcile_inventory_if_due()
                ready_count = self._manager.warm_pool_ready_count(self.pool_key_string)
                if ready_count > self._size:
                    self._manager.trim_warm_pool_to_size(self.pool_key_string, self._size)
                    ready_count = self._manager.warm_pool_ready_count(self.pool_key_string)
                if self._manager.db.reset_warm_pool_inflight(
                    warm_pool_key=self.pool_key_string,
                    stale_after_seconds=float(
                        getattr(self._manager._config, "SANDBOX_WARM_POOL_INFLIGHT_STALE_SEC", 300.0) or 300.0
                    ),
                ):
                    logger.warning(
                        "Warm pool: cleared stale in-flight reservations key=%s ready=%s target=%s",
                        self.pool_key_string,
                        ready_count,
                        self._size,
                    )
                slots_available = self._provision_batch_slots_available()
                if slots_available <= 0:
                    return
                # Ramp brand-new segments with a single first provision. A custom template's
                # first client cold boot and a full warm-pool fan-out on the same shard can
                # overload gVisor/Docker startup and make readiness checks fail spuriously.
                if ready_count <= 0:
                    slots_available = min(slots_available, 1)
                reserve = self._manager.db.reserve_warm_pool_slots(
                    warm_pool_key=self.pool_key_string,
                    ready_count=ready_count,
                    batch_max=slots_available,
                )
                if reserve <= 0:
                    return
                submitted = 0
                for _ in range(reserve):
                    gateway_instance_id, reserved_bytes = self._planned_target()
                    if not gateway_instance_id:
                        break
                    if not self._submit_provision(gateway_instance_id, reserved_bytes):
                        break
                    submitted += 1
                if submitted < reserve:
                    self._manager.db.release_warm_pool_slots(
                        warm_pool_key=self.pool_key_string,
                        count=(reserve - submitted),
                    )
                    return


class MultiWarmSandboxPool:
    """One ``WarmSandboxPool`` segment per distinct (template_id, cpu, mem) profile."""

    def __init__(self, manager: "SandboxManager", config: "Config"):
        self._manager = manager
        self._cfg = config
        self._size = max(0, int(config.SANDBOX_WARM_POOL_SIZE))
        self._pools: Dict[PoolKey, WarmSandboxPool] = {}
        self._pools_lock = threading.Lock()
        self._ensure_key_locks: Dict[PoolKey, threading.Lock] = {}
        self._sync_stop = threading.Event()
        self._sync_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._sync_stop.clear()
        if self._size > 0:
            tid = (self._cfg.SANDBOX_WARM_POOL_TEMPLATE_ID or self._cfg.DEFAULT_TEMPLATE).strip()
            self.ensure_pool_for(
                tid,
                self._cfg.SANDBOX_WARM_POOL_CPU or self._cfg.DEFAULT_CPU_LIMIT,
                self._cfg.SANDBOX_WARM_POOL_MEMORY or self._cfg.DEFAULT_MEMORY_LIMIT,
                int(self._cfg.SANDBOX_WARM_POOL_TIMEOUT or self._cfg.DEFAULT_TIMEOUT),
                self._current_template_image_ref(tid),
            )
        self._sync_persisted_segments()
        self._sync_thread = threading.Thread(
            target=self._sync_loop,
            name="warm-pool-sync",
            daemon=True,
        )
        self._sync_thread.start()

    def _current_template_image_ref(self, template_id: str) -> Optional[str]:
        tid = (template_id or "").strip()
        if not tid:
            return None
        try:
            row = self._manager.db.get_sandbox_template(tid)
            if row:
                row = self._manager._ensure_template_runtime_image(tid, row)
                ref = (row.get("warm_snapshot_image") or row.get("registry_image_ref") or "").strip()
                return ref or None
        except Exception:
            logger.debug("warm pool: could not read template image for %r", tid, exc_info=True)
        return None

    def ensure_pool_for(
        self,
        logical_template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        from_snapshot_image: Optional[str],
        desired_size: Optional[int] = None,
    ) -> None:
        effective_size = self._size if desired_size is None else max(0, int(desired_size))
        key: PoolKey = (
            logical_template_id.strip(),
            str(cpu_limit),
            str(memory_limit),
        )
        key_string = warm_pool_key_string(key[0], key[1], key[2], int(timeout))
        snap = (from_snapshot_image or "").strip() or None

        # Serialize per pool key so two callers do not each ``start()`` a segment for the same key.
        with self._ensure_key_lock(key):
            old: Optional[WarmSandboxPool] = None
            old_snap: Optional[str] = None
            with self._pools_lock:
                cur = self._pools.get(key)
                if cur is not None and cur.from_snapshot_image == snap:
                    if effective_size <= 0:
                        del self._pools[key]
                        old = cur
                        old_snap = cur.from_snapshot_image
                    else:
                        self._manager.note_warm_pool_segment(
                            template_id=key[0],
                            cpu_limit=key[1],
                            memory_limit=key[2],
                            timeout=int(timeout),
                            desired_size=effective_size,
                        )
                        cur.resize(effective_size)
                        self._manager.trim_warm_pool_to_size(cur.pool_key_string, effective_size)
                        return
                elif cur is not None:
                    del self._pools[key]
                    old = cur
                    old_snap = cur.from_snapshot_image
                elif effective_size <= 0:
                    self._manager.note_warm_pool_segment(
                        template_id=key[0],
                        cpu_limit=key[1],
                        memory_limit=key[2],
                        timeout=int(timeout),
                        desired_size=0,
                    )
                    self._manager.trim_warm_pool_to_size(key_string, 0)
                    return

            if old is not None:
                old.stop(timeout=20.0)
                removed = self._manager.trim_warm_pool_to_size(old.pool_key_string, 0)
                if removed:
                    logger.info(
                        "Warm pool segment drained: removed %s sandbox(es) key=%s old_snap=%r new_snap=%r desired=%s",
                        removed,
                        old.pool_key_string,
                        old_snap,
                        snap,
                        effective_size,
                    )

            self._manager.note_warm_pool_segment(
                template_id=key[0],
                cpu_limit=key[1],
                memory_limit=key[2],
                timeout=int(timeout),
                desired_size=effective_size,
            )

            if effective_size <= 0:
                self._manager.trim_warm_pool_to_size(key_string, 0)
                return

            pool = WarmSandboxPool(
                self._manager,
                logical_template_id=key[0],
                cpu_limit=key[1],
                memory_limit=key[2],
                timeout=int(timeout),
                pool_size=effective_size,
                from_snapshot_image=snap,
                provision_concurrency=int(getattr(self._cfg, "SANDBOX_WARM_POOL_PROVISION_CONCURRENCY", 1) or 1),
            )
            with self._pools_lock:
                self._pools[key] = pool
            pool.start()

    def try_acquire(
        self,
        template_id: str,
        metadata: Optional[Dict[str, Any]],
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
    ) -> Optional[str]:
        key: PoolKey = (
            template_id.strip(),
            str(cpu_limit),
            str(memory_limit),
        )
        with self._pools_lock:
            pool = self._pools.get(key)
            if pool is None:
                want_shape = _compatible_pool_shape(template_id, cpu_limit, memory_limit)
                for existing_key, existing_pool in self._pools.items():
                    if _compatible_pool_shape(*existing_key[:3]) == want_shape:
                        pool = existing_pool
                        break
        if pool is None:
            claim_metadata = dict(metadata or {})
            claim_metadata.pop("_warm_pool", None)
            claim_metadata["sandbox_allocation_source"] = "warm_pool_acquire"
            claim_metadata["sandbox_allocation_pool_key"] = self._manager.warm_pool_key(
                template_id,
                cpu_limit,
                memory_limit,
                int(timeout),
            )
            claim_metadata["sandbox_allocation_acquire_wait_seconds"] = 0.0
            claimed = self._manager.acquire_warm_pool_sandbox(
                template_id=template_id,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
                handoff_metadata=claim_metadata,
                handoff_timeout=int(timeout),
            )
            if not claimed:
                return None
            sid = str(claimed.get("sandbox_id") or "").strip()
            return sid
        return pool.try_acquire(
            template_id,
            metadata,
            cpu_limit,
            memory_limit,
            timeout,
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
        )

    def discard(self, sandbox_id: str) -> None:
        with self._pools_lock:
            pools = list(self._pools.values())
        for p in pools:
            p.discard(sandbox_id)

    def stop(self, timeout: float = 5.0) -> None:
        self._sync_stop.set()
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=timeout)
        self._sync_thread = None
        with self._pools_lock:
            pools = list(self._pools.values())
            self._pools.clear()
            self._ensure_key_locks.clear()
        for p in pools:
            p.stop(timeout=timeout)

    def _sync_loop(self) -> None:
        while not self._sync_stop.wait(1.5):
            try:
                self._sync_persisted_segments()
            except Exception:
                logger.debug("warm pool: persisted segment sync failed", exc_info=True)

    def _sync_persisted_segments(self) -> None:
        try:
            persisted = self._manager.db.list_warm_pool_segments()
        except Exception:
            logger.debug("warm pool: could not list persisted segments", exc_info=True)
            return

        selected: Dict[tuple[str, str, str], Dict[str, Any]] = {}
        selected_rank: Dict[tuple[str, str, str], tuple[int, str]] = {}
        for segment in persisted:
            if int(segment.get("desired_size") or 0) <= 0:
                continue
            seg_tid = str(segment.get("template_id") or "").strip()
            if not seg_tid:
                continue
            seg_cpu = str(segment.get("cpu_limit") or self._cfg.DEFAULT_CPU_LIMIT)
            seg_mem = str(segment.get("memory_limit") or self._cfg.DEFAULT_MEMORY_LIMIT)
            shape = _compatible_pool_shape(seg_tid, seg_cpu, seg_mem)

            canonical_key = warm_pool_key_string(
                seg_tid,
                seg_cpu,
                seg_mem,
                int(segment.get("timeout") or self._cfg.DEFAULT_TIMEOUT),
            )
            rank = (
                1 if str(segment.get("warm_pool_key") or "").strip() == canonical_key else 0,
                str(segment.get("updated_at") or ""),
            )
            if shape not in selected_rank or rank > selected_rank[shape]:
                selected[shape] = segment
                selected_rank[shape] = rank

        for segment in selected.values():
            self.ensure_pool_for(
                str(segment.get("template_id") or "").strip(),
                str(segment.get("cpu_limit") or self._cfg.DEFAULT_CPU_LIMIT),
                str(segment.get("memory_limit") or self._cfg.DEFAULT_MEMORY_LIMIT),
                int(segment.get("timeout") or self._cfg.DEFAULT_TIMEOUT),
                self._current_template_image_ref(str(segment.get("template_id") or "").strip()),
                desired_size=int(segment.get("desired_size") or 0),
            )

    def _ensure_key_lock(self, key: PoolKey) -> threading.Lock:
        with self._pools_lock:
            lk = self._ensure_key_locks.get(key)
            if lk is None:
                lk = threading.Lock()
                self._ensure_key_locks[key] = lk
            return lk

    def stats(self) -> dict[str, Any]:
        with self._pools_lock:
            pools = list(self._pools.values())
        return {
            "enabled": self._size > 0,
            "target_per_pool": self._size,
            "provision_concurrency": int(getattr(self._cfg, "SANDBOX_WARM_POOL_PROVISION_CONCURRENCY", 1) or 1),
            "segments": [p.stats() for p in pools],
        }
