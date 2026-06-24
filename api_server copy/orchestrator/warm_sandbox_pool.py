"""Pre-provisioned sandboxes to hide cold-start latency.

When ``SANDBOX_WARM_POOL_SIZE > 0``, one or more **pool segments** run in the background.
Each segment is keyed by ``(logical template_id, cpu, memory, timeout)``
and may provision from a **warm snapshot image** (Docker custom templates), from an
``fc-bundle:`` ref (Firecracker), or from the base image (default ``SANDBOX_WARM_POOL_TEMPLATE_ID``
profile). **Lima VM sandboxes:** warm pool is not started (see ``SandboxManager``).

See ``docs/CUSTOM_TEMPLATES.md`` for custom templates + snapshot-backed warm pools (Docker).
``docs/FIRECRACKER.md`` covers Firecracker + optional ``SANDBOX_WARM_POOL_PROVISION_CONCURRENCY``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Deque, Dict, Optional, Set, Tuple

if TYPE_CHECKING:
    from config import Config
    from .sandbox_manager import SandboxManager

logger = logging.getLogger(__name__)

PoolKey = Tuple[str, str, str, int]


def _compatible_pool_shape(
    template_id: str,
    cpu_limit: str,
    memory_limit: str,
) -> tuple[str, str, str]:
    return (template_id.strip(), str(cpu_limit), str(memory_limit))


class WarmSandboxPool:
    """Maintains ``pool_size`` idle sandboxes for one (template_id, cpu, mem, timeout) profile."""

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
        self._available: Deque[str] = deque()
        self._warm_ids: Set[str] = set()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def from_snapshot_image(self) -> Optional[str]:
        """Snapshot image ref this segment uses when provisioning (``None`` = base image only)."""
        return self._from_snapshot

    @property
    def pool_key(self) -> PoolKey:
        return (self._logical_template_id, self._cpu, self._mem, self._timeout)

    def start(self) -> None:
        if self._size <= 0:
            return
        self._stop.clear()
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
        with self._lock:
            ids = list(self._available)
            self._available.clear()
            self._warm_ids.clear()
        for sid in ids:
            try:
                self._manager.kill_sandbox(sid)
            except Exception as ex:  # noqa: BLE001
                logger.warning("Warm pool shutdown: failed to kill %s: %s", sid, ex)

    def discard(self, sandbox_id: str) -> None:
        with self._lock:
            try:
                self._available.remove(sandbox_id)
            except ValueError:
                pass
            self._warm_ids.discard(sandbox_id)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "template_id": self._logical_template_id,
                "target_size": self._size,
                "ready": len(self._available),
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
    ) -> Optional[str]:
        if self._size <= 0:
            return None
        if template_id.strip() != self._logical_template_id:
            return None
        if str(cpu_limit) != self._cpu or str(memory_limit) != self._mem:
            return None
        with self._lock:
            if not self._available:
                return None
            sid = self._available.popleft()
            self._warm_ids.discard(sid)
        merged = dict(metadata or {})
        merged.pop("_warm_pool", None)
        base = self._manager.get_sandbox(sid)
        if base:
            prev = dict(base.get("metadata") or {})
            prev.pop("_warm_pool", None)
            merged = {**prev, **merged}
        self._manager.db.merge_sandbox_metadata(sid, merged)
        self._manager.db.update_sandbox_timeout(sid, int(timeout))
        logger.info("Warm pool: handed sandbox %s (template=%s)", sid, self._logical_template_id)
        return sid

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._top_up()
            except Exception as ex:  # noqa: BLE001
                logger.exception("Warm pool top-up error: %s", ex)
            time.sleep(1.5)

    def _provision_one(self) -> Optional[str]:
        return self._manager._create_sandbox_fresh(
            template_id=self._logical_template_id,
            metadata={"_warm_pool": True},
            cpu_limit=self._cpu,
            memory_limit=self._mem,
            timeout=self._timeout,
            from_snapshot_image=self._from_snapshot,
        )

    def _top_up(self) -> None:
        with self._lock:
            need = self._size - len(self._available)
        if need <= 0:
            return

        conc = max(1, min(self._provision_concurrency, need))

        if conc == 1:
            for _ in range(need):
                if self._stop.is_set():
                    break
                sid = self._provision_one()
                if not sid:
                    logger.warning("Warm pool: failed to provision (template=%s)", self._logical_template_id)
                    break
                with self._lock:
                    self._available.append(sid)
                    self._warm_ids.add(sid)
                with self._lock:
                    nready = len(self._available)
                logger.info(
                    "Warm pool: provisioned %s for template=%s (ready=%s)",
                    sid,
                    self._logical_template_id,
                    nready,
                )
            return

        remaining = need
        while remaining > 0 and not self._stop.is_set():
            batch = min(remaining, conc)
            with ThreadPoolExecutor(max_workers=batch) as ex:
                futures = [ex.submit(self._provision_one) for _ in range(batch)]
                results = [f.result() for f in futures]

            any_fail = any(not sid for sid in results)
            for sid in results:
                if not sid:
                    continue
                with self._lock:
                    self._available.append(sid)
                    self._warm_ids.add(sid)
                with self._lock:
                    nready = len(self._available)
                logger.info(
                    "Warm pool: provisioned %s for template=%s (ready=%s)",
                    sid,
                    self._logical_template_id,
                    nready,
                )

            remaining -= batch
            if any_fail:
                if any(results):
                    logger.warning(
                        "Warm pool: partial batch failure (template=%s); will retry on next cycle",
                        self._logical_template_id,
                    )
                else:
                    logger.warning("Warm pool: failed to provision (template=%s)", self._logical_template_id)
                break


class MultiWarmSandboxPool:
    """One ``WarmSandboxPool`` segment per distinct (template_id, cpu, mem, timeout) profile."""

    def __init__(self, manager: "SandboxManager", config: "Config"):
        self._manager = manager
        self._cfg = config
        self._size = max(0, int(config.SANDBOX_WARM_POOL_SIZE))
        self._pools: Dict[PoolKey, WarmSandboxPool] = {}
        self._pools_lock = threading.Lock()
        self._ensure_key_locks: Dict[PoolKey, threading.Lock] = {}

    def start(self) -> None:
        if self._size <= 0:
            return
        tid = (self._cfg.SANDBOX_WARM_POOL_TEMPLATE_ID or self._cfg.DEFAULT_TEMPLATE).strip()
        snap: Optional[str] = None
        try:
            row = self._manager.db.get_sandbox_template(tid)
            if row:
                wi = (row.get("warm_snapshot_image") or "").strip()
                # Skip Firecracker / Lima markers — pool still uses base OCI ref for those engines elsewhere.
                if wi and wi not in ("__fc_rootfs__", "__lima_vm__"):
                    snap = wi
        except Exception:
            logger.debug("warm pool: could not read warm_snapshot for %r", tid, exc_info=True)
        self.ensure_pool_for(
            tid,
            self._cfg.SANDBOX_WARM_POOL_CPU or self._cfg.DEFAULT_CPU_LIMIT,
            self._cfg.SANDBOX_WARM_POOL_MEMORY or self._cfg.DEFAULT_MEMORY_LIMIT,
            int(self._cfg.SANDBOX_WARM_POOL_TIMEOUT or self._cfg.DEFAULT_TIMEOUT),
            snap,
        )

    def ensure_pool_for(
        self,
        logical_template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        from_snapshot_image: Optional[str],
    ) -> None:
        if self._size <= 0:
            return
        wanted_shape = _compatible_pool_shape(logical_template_id, cpu_limit, memory_limit)
        key: PoolKey = (
            logical_template_id.strip(),
            str(cpu_limit),
            str(memory_limit),
            int(timeout),
        )
        snap = (from_snapshot_image or "").strip() or None

        # Serialize per pool key so two callers do not each ``start()`` a segment for the same key.
        with self._ensure_key_lock(key):
            old: Optional[WarmSandboxPool] = None
            with self._pools_lock:
                cur = self._pools.get(key)
                if cur is not None and cur.from_snapshot_image == snap:
                    return
                for existing_key, existing_pool in self._pools.items():
                    if _compatible_pool_shape(*existing_key[:3]) != wanted_shape:
                        continue
                    if existing_pool.from_snapshot_image == snap:
                        return
                if cur is not None:
                    del self._pools[key]
                    old = cur

            if old is not None:
                old.stop(timeout=20.0)

            pool = WarmSandboxPool(
                self._manager,
                logical_template_id=key[0],
                cpu_limit=key[1],
                memory_limit=key[2],
                timeout=key[3],
                pool_size=self._size,
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
    ) -> Optional[str]:
        key: PoolKey = (
            template_id.strip(),
            str(cpu_limit),
            str(memory_limit),
            int(timeout),
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
            return None
        return pool.try_acquire(template_id, metadata, cpu_limit, memory_limit, timeout)

    def discard(self, sandbox_id: str) -> None:
        with self._pools_lock:
            pools = list(self._pools.values())
        for p in pools:
            p.discard(sandbox_id)

    def stop(self, timeout: float = 5.0) -> None:
        with self._pools_lock:
            pools = list(self._pools.values())
            self._pools.clear()
            self._ensure_key_locks.clear()
        for p in pools:
            p.stop(timeout=timeout)

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
