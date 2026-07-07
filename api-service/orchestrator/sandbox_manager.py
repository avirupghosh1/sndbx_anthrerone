"""Sandbox management over Docker Engine runtime-gateway shards."""

import base64
import json
import logging
import re
import secrets
import shlex
import shutil
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional
import httpx

from .container_manager import ContainerManager, ContainerConfig
from orchestrator.runtime_utils import is_container_like_execution
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
from .template_image import resolve_sandbox_image
from .runtime_gateway_templates import (
    build_dockerfile_template_via_gateway,
    build_template_snapshot_via_gateway,
    gateway_template_build_enabled,
)
from .runtime_gateway_execution import RuntimeGatewayExecution
from .gateway_targets import GatewayTarget, GatewayTargetSelector, build_gateway_targets, target_for_instance
from .warm_sandbox_pool import warm_pool_key_string
from database import Database

if TYPE_CHECKING:
    from .protocols import SandboxExecutionPlane

logger = logging.getLogger(__name__)

ENVD_TEMPLATE_BAKED_ENV = "MYSANDBOX_ENVD_BAKED"


def _resolve_sandbox_image(template_id: Optional[str]) -> str:
    return resolve_sandbox_image(template_id)


def _looks_like_explicit_image_ref(template_id: Optional[str]) -> bool:
    """True for raw image refs; false for friendly aliases that must be registered."""
    raw = (template_id or "").strip()
    if not raw:
        return True
    resolved = _resolve_sandbox_image(raw)
    if resolved != raw:
        return True
    last = resolved.rsplit("/", 1)[-1]
    return "/" in resolved or ":" in last


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
        self._gateway_execution_guard = threading.Lock()
        self._gateway_execution_cache: Dict[str, RuntimeGatewayExecution] = {}
        self._gateway_selector = GatewayTargetSelector()
        self._gateway_status_cache: Dict[str, Dict[str, Any]] = {}
        self._gateway_status_lock = threading.Lock()
        self._gateway_image_cache: Dict[tuple[str, str], tuple[float, bool]] = {}
        self._gateway_image_cache_lock = threading.Lock()
        self._image_prefetch_inflight: set[tuple[str, str]] = set()
        self._image_prefetch_lock = threading.Lock()
        self._recent_created_rows: Dict[str, Dict[str, Any]] = {}
        self._recent_created_rows_lock = threading.Lock()
        self._lease_reaper_stop = threading.Event()
        self._lease_reaper_thread: Optional[threading.Thread] = None
        self._last_create_error: str = ""
        self.warm_pool: Optional[Any] = None
        try:
            cfg = self._config
            kind = self.execution.get_backend_kind()
            if cfg.SANDBOX_WARM_POOL_SIZE > 0:
                from .warm_sandbox_pool import MultiWarmSandboxPool

                self.warm_pool = MultiWarmSandboxPool(self, cfg)
                self.warm_pool.start()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Warm sandbox pool not started: %s", ex)
        try:
            self._start_lease_reaper()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Sandbox lease reaper not started: %s", ex)

    def _gateway_targets(self) -> List[GatewayTarget]:
        try:
            return build_gateway_targets(self._config)
        except Exception as ex:  # noqa: BLE001
            logger.warning("Runtime gateway target build failed: %s", ex)
            return []

    def _current_gateway_instance_ids(self) -> set[str]:
        return {str(t.instance_id or "").strip() for t in self._gateway_targets() if str(t.instance_id or "").strip()}

    def _choose_gateway_target(self) -> Optional[GatewayTarget]:
        targets = self._gateway_targets()
        if not targets:
            return None
        try:
            return self._gateway_selector.choose(
                targets,
                scheduler=str(getattr(self._config, "RUNTIME_GATEWAY_SCHEDULER", "round_robin") or "round_robin"),
            )
        except Exception:
            return targets[0]

    def warm_pool_key(self, template_id: str, cpu_limit: str, memory_limit: str, timeout: int) -> str:
        return warm_pool_key_string(template_id, cpu_limit, memory_limit, int(timeout))

    def is_warm_pool_leader(self) -> bool:
        cfg = self._config
        lease_name = str(getattr(cfg, "WARM_POOL_COORDINATOR_LEASE_NAME", "warm-pool-coordinator"))
        return self.db.acquire_postgres_advisory_lock(lease_name)

    def _gateway_headers(self) -> Dict[str, str]:
        key = (getattr(self._config, "RUNTIME_GATEWAY_API_KEY", None) or "").strip()
        if not key:
            return {}
        return {"X-API-Key": key}

    def _gateway_runtime_status(self, target: GatewayTarget, *, force_refresh: bool = False) -> Dict[str, Any]:
        ttl = float(getattr(self._config, "RUNTIME_GATEWAY_STATUS_CACHE_TTL_SEC", 2.0) or 2.0)
        now = time.time()
        if not force_refresh:
            with self._gateway_status_lock:
                cached = self._gateway_status_cache.get(target.instance_id)
                if cached and (now - float(cached.get("_ts") or 0.0)) <= ttl:
                    return dict(cached)
        data: Dict[str, Any] = {
            "gateway_instance_id": target.instance_id,
            "disk_total_bytes": 0,
            "disk_used_bytes": 0,
            "disk_free_bytes": 0,
            "disk_used_ratio": 0.0,
            "reachable": False,
        }
        try:
            with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
                resp = client.get(
                    f"{target.api_base}/internal/runtime/status",
                    headers=self._gateway_headers(),
                )
            if resp.status_code < 400:
                payload = resp.json()
                data.update(payload if isinstance(payload, dict) else {})
                data["reachable"] = True
        except Exception as ex:  # noqa: BLE001
            data["error"] = str(ex)
        data["_ts"] = now
        with self._gateway_status_lock:
            self._gateway_status_cache[target.instance_id] = dict(data)
        return data

    def warm_pool_segment_diagnostics(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for segment in self.db.list_warm_pool_segments():
            key = str(segment.get("warm_pool_key") or "")
            rows = self.db.list_warm_pool_sandboxes(warm_pool_key=key)
            by_gateway: Dict[str, int] = {}
            for row in rows:
                gid = str(row.get("gateway_instance_id") or "").strip() or "unassigned"
                by_gateway[gid] = by_gateway.get(gid, 0) + 1
            item = dict(segment)
            item["ready_count"] = len(rows)
            item["ready_by_gateway"] = by_gateway
            out.append(item)
        return out

    def runtime_gateway_diagnostics(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        warm_rows = self.db.list_warm_pool_sandboxes()
        for target in self._gateway_targets():
            status = self._gateway_runtime_status(target, force_refresh=True)
            warm_by_key: Dict[str, int] = {}
            for row in warm_rows:
                if str(row.get("gateway_instance_id") or "").strip() != target.instance_id:
                    continue
                key = str(row.get("warm_pool_key") or "").strip() or "unknown"
                warm_by_key[key] = warm_by_key.get(key, 0) + 1
            item = {
                k: v
                for k, v in status.items()
                if not str(k).startswith("_")
            }
            item["api_base"] = target.api_base
            item["route_base"] = target.route_base
            item["running_sandbox_count"] = self.db.count_running_sandboxes(
                gateway_instance_id=target.instance_id
            )
            item["warm_sandbox_count"] = sum(warm_by_key.values())
            item["warm_by_pool_key"] = warm_by_key
            out.append(item)
        return out

    def _estimated_sandbox_reservation_bytes(
        self,
        target: GatewayTarget,
        *,
        template_id: str,
        image_ref: str,
        force_refresh: bool = False,
    ) -> int:
        status = self._gateway_runtime_status(target, force_refresh=force_refresh)
        total = int(status.get("disk_total_bytes") or 0)
        used = int(status.get("disk_used_bytes") or 0)
        running = self.db.count_running_sandboxes(gateway_instance_id=target.instance_id)
        if running > 0 and used > 0:
            return max(512 * 1024 * 1024, used // running)
        if image_ref and self._gateway_has_image(target, image_ref):
            return 1024 * 1024 * 1024
        if total > 0:
            return max(1024 * 1024 * 1024, int(total * 0.08))
        return 2 * 1024 * 1024 * 1024

    def _gateway_can_accept_new_usage(
        self,
        target: GatewayTarget,
        *,
        force_refresh: bool = False,
        extra_used_bytes: int = 0,
    ) -> bool:
        status = self._gateway_runtime_status(target, force_refresh=force_refresh)
        if not status.get("reachable"):
            return False
        used = int(status.get("disk_used_bytes") or 0) + max(0, int(extra_used_bytes))
        total = int(status.get("disk_total_bytes") or 0)
        ratio = (float(used) / float(total)) if total > 0 else float(status.get("disk_used_ratio") or 0.0)
        limit_ratio = float(getattr(self._config, "RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO", 0.80) or 0.80)
        ok = ratio < limit_ratio
        if not ok:
            logger.info(
                "Runtime-gateway shard full: gateway=%s used=%.1f%% limit=%.1f%% used_bytes=%s total_bytes=%s source=%s",
                target.instance_id,
                ratio * 100.0,
                limit_ratio * 100.0,
                used,
                total,
                str(status.get("disk_metric_source") or ""),
            )
        return ok

    def _warm_pool_rows_by_gateway(self, warm_pool_key: str) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key):
            gid = str(row.get("gateway_instance_id") or "").strip()
            out.setdefault(gid, []).append(row)
        return out

    def _live_warm_pool_rows(self, warm_pool_key: str) -> List[Dict[str, Any]]:
        rows = self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key)
        kind = self.execution.get_backend_kind()
        if kind not in ("docker", "gvisor"):
            return rows

        targets_by_instance = {target.instance_id: target for target in self._gateway_targets()}
        live: List[Dict[str, Any]] = []
        for row in rows:
            sid = str(row.get("sandbox_id") or "").strip()
            cid = str(row.get("container_id") or "").strip()
            gid = str(row.get("gateway_instance_id") or "").strip()
            if not sid or not cid:
                if sid:
                    self._mark_sandbox_lost(
                        sid,
                        detail="Warm-pool sandbox has no runtime container; recreate the sandbox.",
                    )
                continue

            target = targets_by_instance.get(gid) if gid else None
            if gid and target is None:
                logger.warning(
                    "Warm pool inventory: sandbox=%s references unknown runtime-gateway shard %r",
                    sid,
                    gid,
                )
                continue
            if target is not None:
                status = self._gateway_runtime_status(target)
                if not status.get("reachable"):
                    logger.debug(
                        "Warm pool inventory: shard %s unreachable while checking sandbox=%s",
                        gid,
                        sid,
                    )
                    continue

            try:
                execution = self._execution_for_row(row)
                state_fn = getattr(execution, "get_container_state", None)
                if callable(state_fn):
                    state = str(state_fn(cid) or "unknown").strip().lower()
                    if state == "running":
                        live.append(row)
                        continue
                    if state == "unknown":
                        logger.debug(
                            "Warm pool inventory: container state unknown sandbox=%s gateway=%s container=%s",
                            sid,
                            gid or "-",
                            cid[:12],
                        )
                        continue
                elif execution.is_container_running(cid):
                    live.append(row)
                    continue
                else:
                    state = "stopped"
            except Exception as ex:  # noqa: BLE001
                logger.debug(
                    "Warm pool inventory: liveness check failed sandbox=%s container=%s: %s",
                    sid,
                    cid[:12],
                    ex,
                )
                continue

            if self._mark_sandbox_lost(
                sid,
                detail="Previous warm-pool container died after runtime-gateway restart; recreate the sandbox.",
            ):
                logger.warning(
                    "Warm pool inventory: marked stale warm sandbox lost sandbox=%s gateway=%s container=%s",
                    sid,
                    gid or "-",
                    cid[:12],
                )
        return live

    def warm_pool_ready_count(self, warm_pool_key: str) -> int:
        return len(self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key))

    def trim_warm_pool_to_size(self, warm_pool_key: str, desired_size: int) -> int:
        desired = max(0, int(desired_size))
        rows = self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key)
        extra = len(rows) - desired
        if extra <= 0:
            return 0
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        removed = 0
        for row in rows[:extra]:
            sid = str(row.get("sandbox_id") or "").strip()
            if not sid:
                continue
            try:
                if self.kill_sandbox(sid, force=True):
                    removed += 1
            except Exception as ex:  # noqa: BLE001
                logger.warning("Warm pool trim failed sandbox=%s: %s", sid, ex)
        if removed:
            logger.info(
                "Warm pool trim: removed %s excess sandbox(es) key=%s desired=%s",
                removed,
                warm_pool_key,
                desired,
            )
        return removed

    def note_warm_pool_segment(
        self,
        *,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        desired_size: int,
        ready_image_ref: Optional[str],
        preferred_gateway_instance_id: Optional[str] = None,
        last_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = self.warm_pool_key(template_id, cpu_limit, memory_limit, int(timeout))
        row = self.db.upsert_warm_pool_segment(
            warm_pool_key=key,
            template_id=template_id,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=int(timeout),
            desired_size=int(desired_size),
            ready_image_ref=ready_image_ref,
            preferred_gateway_instance_id=preferred_gateway_instance_id,
            last_error=last_error,
        )
        ref = (ready_image_ref or "").strip()
        if ref:
            self._schedule_gateway_image_prefetch(ref)
        return row

    def get_last_create_error(self) -> str:
        return str(self._last_create_error or "")

    def _gateway_has_image(
        self,
        target: GatewayTarget,
        image_ref: str,
        *,
        force_refresh: bool = False,
    ) -> bool:
        ref = (image_ref or "").strip()
        if not ref:
            return False
        ttl = 10.0
        cache_key = (target.instance_id, ref)
        now = time.time()
        if not force_refresh:
            with self._gateway_image_cache_lock:
                cached = self._gateway_image_cache.get(cache_key)
                if cached and (now - float(cached[0])) <= ttl:
                    return bool(cached[1])
        exists = False
        try:
            execution = self._execution_for_gateway_target(target)
            exists = bool(getattr(execution, "image_exists", lambda _ref: False)(ref))
        except Exception:
            exists = False
        with self._gateway_image_cache_lock:
            self._gateway_image_cache[cache_key] = (now, exists)
        return exists

    def _schedule_gateway_image_prefetch(self, image_ref: str) -> None:
        ref = (image_ref or "").strip()
        if not ref or not bool(getattr(self._config, "WARM_POOL_IMAGE_PREFETCH_ENABLED", True)):
            return
        if self.execution.get_backend_kind() not in ("docker", "gvisor"):
            return
        for target in self._gateway_targets():
            key = (target.instance_id, ref)
            with self._image_prefetch_lock:
                if key in self._image_prefetch_inflight:
                    continue
                self._image_prefetch_inflight.add(key)
            thread = threading.Thread(
                target=self._prefetch_image_to_gateway,
                args=(target, ref),
                name=f"prefetch-{target.instance_id}",
                daemon=True,
            )
            thread.start()

    def _prefetch_image_to_gateway(self, target: GatewayTarget, image_ref: str) -> None:
        key = (target.instance_id, image_ref)
        started = time.time()
        try:
            execution = self._execution_for_gateway_target(target)
            image_exists = getattr(execution, "image_exists", None)
            if callable(image_exists) and image_exists(image_ref):
                with self._gateway_image_cache_lock:
                    self._gateway_image_cache[key] = (time.time(), True)
                return
            pull_image = getattr(execution, "pull_image", None)
            if callable(pull_image) and pull_image(image_ref):
                with self._gateway_image_cache_lock:
                    self._gateway_image_cache[key] = (time.time(), True)
                logger.info(
                    "Warm pool image prefetched gateway=%s image=%s elapsed_ms=%s",
                    target.instance_id,
                    image_ref,
                    int((time.time() - started) * 1000),
                )
                return
            logger.warning(
                "Warm pool image prefetch failed gateway=%s image=%s",
                target.instance_id,
                image_ref,
            )
        except Exception as ex:  # noqa: BLE001
            logger.warning(
                "Warm pool image prefetch error gateway=%s image=%s: %s",
                target.instance_id,
                image_ref,
                ex,
            )
        finally:
            with self._image_prefetch_lock:
                self._image_prefetch_inflight.discard(key)

    def _best_gateway_by_free_disk(
        self,
        candidates: List[GatewayTarget],
        *,
        force_refresh: bool = False,
        preferred_image_ref: Optional[str] = None,
        extra_used_bytes_by_gateway: Optional[Dict[str, int]] = None,
    ) -> Optional[GatewayTarget]:
        best: Optional[GatewayTarget] = None
        best_score: tuple[float, int, int, str] | None = None
        image_ref = (preferred_image_ref or "").strip()
        statuses: Dict[str, Dict[str, Any]] = {}
        if len(candidates) > 1:
            with ThreadPoolExecutor(max_workers=min(8, len(candidates))) as pool:
                future_by_instance = {
                    pool.submit(self._gateway_runtime_status, target, force_refresh=force_refresh): target
                    for target in candidates
                }
                for future in as_completed(future_by_instance):
                    target = future_by_instance[future]
                    try:
                        statuses[target.instance_id] = future.result()
                    except Exception as ex:  # noqa: BLE001
                        statuses[target.instance_id] = {
                            "gateway_instance_id": target.instance_id,
                            "reachable": False,
                            "error": str(ex),
                        }
        for target in candidates:
            status = statuses.get(target.instance_id) or self._gateway_runtime_status(
                target,
                force_refresh=force_refresh,
            )
            if not status.get("reachable"):
                continue
            extra_used = int((extra_used_bytes_by_gateway or {}).get(target.instance_id) or 0)
            used = int(status.get("disk_used_bytes") or 0) + max(0, extra_used)
            total = int(status.get("disk_total_bytes") or 0)
            ratio = (float(used) / float(total)) if total > 0 else float(status.get("disk_used_ratio") or 0.0)
            limit_ratio = float(getattr(self._config, "RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO", 0.80) or 0.80)
            if ratio >= limit_ratio:
                logger.info(
                    "Runtime-gateway shard full: gateway=%s used=%.1f%% limit=%.1f%% used_bytes=%s total_bytes=%s source=%s",
                    target.instance_id,
                    ratio * 100.0,
                    limit_ratio * 100.0,
                    used,
                    total,
                    str(status.get("disk_metric_source") or ""),
                )
                continue
            has_image_rank = 1
            if image_ref:
                with self._gateway_image_cache_lock:
                    cached = self._gateway_image_cache.get((target.instance_id, image_ref))
                if cached and (time.time() - float(cached[0])) <= 10.0 and bool(cached[1]):
                    has_image_rank = 0
            score = (ratio, used, has_image_rank, target.instance_id)
            if best_score is None or score < best_score:
                best = target
                best_score = score
        if best is not None and best_score is not None:
            logger.info(
                "Scheduler decision: reason=free_disk gateway=%s disk_used_ratio=%.4f disk_used_bytes=%s image_ref=%s image_cached_rank=%s",
                best.instance_id,
                float(best_score[0]),
                int(best_score[1]),
                image_ref or "-",
                int(best_score[2]),
            )
        elif candidates:
            logger.warning(
                "Scheduler decision: no_gateway reason=free_disk candidates=%s image_ref=%s",
                ",".join(t.instance_id for t in candidates),
                image_ref or "-",
            )
        return best

    def _select_gateway_target_for_pool(
        self,
        *,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        template_row: Optional[Dict[str, Any]],
        extra_used_bytes_by_gateway: Optional[Dict[str, int]] = None,
        extra_warm_counts_by_gateway: Optional[Dict[str, int]] = None,
        force_refresh: bool = True,
    ) -> Optional[GatewayTarget]:
        targets = self._gateway_targets()
        if not targets:
            return None
        row = template_row or {}
        registry_ref = str(row.get("registry_image_ref") or "").strip()
        owner_instance = str(row.get("materialized_gateway_instance_id") or "").strip()
        if owner_instance and not registry_ref:
            pinned = target_for_instance(targets, owner_instance)
            if pinned is not None and self._gateway_can_accept_new_usage(pinned, force_refresh=force_refresh):
                logger.info(
                    "Scheduler decision: reason=template_owner gateway=%s template=%s",
                    pinned.instance_id,
                    template_id,
                )
                return pinned
            logger.warning(
                "Template %r is local-only on %s, but that runtime-gateway shard has no disk headroom",
                template_id,
                owner_instance,
            )
            return None
        warm_key = self.warm_pool_key(template_id, cpu_limit, memory_limit, int(timeout))
        segment = self.db.get_warm_pool_segment(warm_key)
        preferred_instance = str((segment or {}).get("preferred_gateway_instance_id") or "").strip()
        inventory = self._warm_pool_rows_by_gateway(warm_key)
        inventory_targets: list[tuple[float, int, int, str, GatewayTarget]] = []
        for instance_id, rows in inventory.items():
            target = target_for_instance(targets, instance_id)
            if target is not None:
                status = self._gateway_runtime_status(target, force_refresh=force_refresh)
                extra_used = int((extra_used_bytes_by_gateway or {}).get(target.instance_id) or 0)
                used = int(status.get("disk_used_bytes") or 0) + max(0, extra_used)
                total = int(status.get("disk_total_bytes") or 0)
                ratio = (float(used) / float(total)) if total > 0 else float(status.get("disk_used_ratio") or 0.0)
                warm_count = len(rows) + int((extra_warm_counts_by_gateway or {}).get(instance_id) or 0)
                inventory_targets.append((-float(warm_count), ratio, used, target.instance_id, target))
        inventory_targets.sort(key=lambda item: item[:4])
        for _count_score, _ratio, _used, _instance_id, target in inventory_targets:
            if self._gateway_can_accept_new_usage(
                target,
                force_refresh=force_refresh,
                extra_used_bytes=int((extra_used_bytes_by_gateway or {}).get(target.instance_id) or 0),
            ):
                logger.info(
                    "Scheduler decision: reason=max_warm_count gateway=%s template=%s warm_count=%s disk_used_ratio=%.4f",
                    target.instance_id,
                    template_id,
                    int(-_count_score),
                    float(_ratio),
                )
                return target
        if preferred_instance:
            preferred = target_for_instance(targets, preferred_instance)
            if preferred is not None and self._gateway_can_accept_new_usage(
                preferred,
                force_refresh=force_refresh,
                extra_used_bytes=int((extra_used_bytes_by_gateway or {}).get(preferred.instance_id) or 0),
            ):
                logger.info(
                    "Scheduler decision: reason=preferred_segment gateway=%s template=%s",
                    preferred.instance_id,
                    template_id,
                )
                return preferred
        candidate_targets = targets if registry_ref or not owner_instance else []
        preferred_image_ref = (
            str((segment or {}).get("ready_image_ref") or "").strip()
            or registry_ref
        )
        return self._best_gateway_by_free_disk(
            candidate_targets,
            force_refresh=force_refresh,
            preferred_image_ref=preferred_image_ref or None,
            extra_used_bytes_by_gateway=extra_used_bytes_by_gateway,
        )

    def acquire_warm_pool_sandbox(
        self,
        *,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        owner_client_id: Optional[str],
        owner_api_key_id: Optional[str],
        handoff_metadata: Optional[Dict[str, Any]] = None,
        handoff_timeout: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        key = self.warm_pool_key(template_id, cpu_limit, memory_limit, int(timeout))
        inventory: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.db.list_warm_pool_sandboxes(warm_pool_key=key):
            instance_id = str(row.get("gateway_instance_id") or "").strip()
            if not instance_id:
                continue
            inventory.setdefault(instance_id, []).append(row)

        # Hot acquisition must stay DB-only. These rows are inserted after successful
        # bootstrap, so runtime/container probing here only adds seconds to an
        # otherwise atomic warm handoff.
        preferred_instances = [
            instance_id
            for instance_id, rows in sorted(
                inventory.items(),
                key=lambda item: (-len(item[1]), item[0]),
            )
        ]
        for instance_id in preferred_instances:
            claimed = self.db.claim_warm_pool_sandbox(
                warm_pool_key=key,
                gateway_instance_id=instance_id,
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
                metadata_updates=handoff_metadata,
                timeout_seconds=handoff_timeout,
            )
            if claimed:
                self._remember_recent_created_row(claimed)
                return claimed
        claimed = self.db.claim_warm_pool_sandbox(
            warm_pool_key=key,
            gateway_instance_id=None,
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
            metadata_updates=handoff_metadata,
            timeout_seconds=handoff_timeout,
        )
        if claimed:
            self._remember_recent_created_row(claimed)
            return claimed
        return None

    def select_gateway_target_for_pool_create(
        self,
        *,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        extra_used_bytes_by_gateway: Optional[Dict[str, int]] = None,
        extra_warm_counts_by_gateway: Optional[Dict[str, int]] = None,
        force_refresh: bool = True,
    ) -> Optional[GatewayTarget]:
        tpl = self.db.get_sandbox_template((template_id or "").strip()) if template_id else None
        return self._select_gateway_target_for_pool(
            template_id=(template_id or "").strip(),
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=int(timeout),
            template_row=tpl,
            extra_used_bytes_by_gateway=extra_used_bytes_by_gateway,
            extra_warm_counts_by_gateway=extra_warm_counts_by_gateway,
            force_refresh=force_refresh,
        )

    def _gateway_target_for_template_row(self, row: Optional[Dict[str, Any]]) -> Optional[GatewayTarget]:
        targets = self._gateway_targets()
        if not targets:
            return None
        registry_ref = str((row or {}).get("registry_image_ref") or "").strip()
        owner_instance = str((row or {}).get("materialized_gateway_instance_id") or "").strip()
        if not registry_ref and owner_instance:
            pinned = target_for_instance(targets, owner_instance)
            if pinned is not None and self._gateway_can_accept_new_usage(pinned, force_refresh=True):
                return pinned
            return None
        return self._best_gateway_by_free_disk(
            targets,
            force_refresh=True,
            preferred_image_ref=registry_ref or None,
        )

    def _execution_for_gateway_target(self, target: GatewayTarget):
        kind = self.execution.get_backend_kind()
        if kind not in ("docker", "gvisor"):
            return self.execution
        key = (target.api_base or "").strip().rstrip("/")
        if not key:
            return self.execution
        with self._gateway_execution_guard:
            cached = self._gateway_execution_cache.get(key)
            if cached is not None:
                return cached
            execution = RuntimeGatewayExecution(
                api_base=key,
                api_key=getattr(self._config, "RUNTIME_GATEWAY_API_KEY", ""),
                backend_kind=kind,
            )
            self._gateway_execution_cache[key] = execution
            return execution

    def _execution_for_row(self, row: Optional[Dict[str, Any]]):
        if not row:
            return self.execution
        kind = self.execution.get_backend_kind()
        if kind not in ("docker", "gvisor"):
            return self.execution
        api_base = str(row.get("gateway_api_base") or row.get("gateway_route_base") or "").strip().rstrip("/")
        if not api_base:
            target = target_for_instance(self._gateway_targets(), str(row.get("gateway_instance_id") or ""))
            api_base = (target.api_base if target else "").strip().rstrip("/")
        if not api_base:
            return self.execution
        with self._gateway_execution_guard:
            cached = self._gateway_execution_cache.get(api_base)
            if cached is not None:
                return cached
            execution = RuntimeGatewayExecution(
                api_base=api_base,
                api_key=getattr(self._config, "RUNTIME_GATEWAY_API_KEY", ""),
                backend_kind=kind,
            )
            self._gateway_execution_cache[api_base] = execution
            return execution

    def _template_lock(self, template_id: str) -> threading.Lock:
        with self._template_build_guard:
            if template_id not in self._template_build_locks:
                self._template_build_locks[template_id] = threading.Lock()
            return self._template_build_locks[template_id]

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

    def _lease_reaper_loop(self) -> None:
        interval = max(
            1.0,
            float(getattr(self._config, "SANDBOX_LEASE_REAPER_INTERVAL_SEC", 5.0) or 5.0),
        )
        while not self._lease_reaper_stop.wait(interval):
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
        for row in self.db.list_sandbox_templates():
            for key in ("warm_snapshot_image", "base_image"):
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
        ref = (image_ref or "").strip()
        if not ref:
            return False
        registry_ref = str((row or {}).get("registry_image_ref") or "").strip()
        if registry_ref and ref == registry_ref:
            return True
        execution = self.execution
        owner_target = self._gateway_target_for_template_row(row)
        if owner_target is not None:
            execution = self._execution_for_gateway_target(owner_target)
        fn = getattr(execution, "image_exists", None)
        if callable(fn):
            try:
                return bool(fn(ref))
            except Exception:
                return False
        return True

    def _repair_missing_template_image(self, template_id: str, row: Dict[str, Any]) -> Optional[str]:
        source_kind = (row.get("source_kind") or "").strip().lower()
        warm_ref = (row.get("warm_snapshot_image") or "").strip()
        base_image = (row.get("base_image") or "").strip()
        if source_kind != "dockerfile":
            return None
        dockerfile = str(row.get("dockerfile_text") or "")
        if not dockerfile:
            self.db.set_template_build_error(
                template_id,
                f"Template image missing from runtime and rebuild source is unavailable for {warm_ref or base_image}. Rebuild the template.",
            )
            return None
        build_mode = (row.get("source_build_mode") or "docker_cli").strip() or "docker_cli"
        image_tag = warm_ref or base_image or None
        target = self._gateway_target_for_template_row(row)
        try:
            result = build_dockerfile_template_via_gateway(
                self._config,
                template_id=template_id,
                dockerfile=dockerfile,
                image_tag=image_tag,
                build_args=dict(row.get("build_args") or {}),
                context_tar_gzip_base64=(row.get("context_tar_gzip_base64") or None),
                build_mode=build_mode,
                embed_envd=bool(getattr(self._config, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
                gateway_api_base=(target.api_base if target else None),
            )
        except RuntimeError as ex:
            self.db.set_template_build_error(template_id, str(ex))
            return None
        rebuilt = str(result.get("image_tag") or image_tag or "").strip()
        registry_ref = str(result.get("registry_image_ref") or "").strip()
        gateway_instance_id = str(result.get("gateway_instance_id") or "").strip()
        effective_ref = registry_ref or rebuilt
        if not effective_ref:
            self.db.set_template_build_error(template_id, "runtime-gateway rebuild produced no image tag")
            return None
        self.db.set_template_warm_snapshot(
            template_id,
            effective_ref,
            None,
            registry_image_ref=registry_ref or None,
            materialized_gateway_instance_id=gateway_instance_id or None,
        )
        logger.info("Template %s rebuilt missing runtime image: %s", template_id, effective_ref)
        self.sync_warm_pool_default_segment(template_id, effective_ref)
        return effective_ref

    def _ensure_template_runtime_image(self, template_id: str, row: Dict[str, Any]) -> Dict[str, Any]:
        warm_ref = (row.get("warm_snapshot_image") or "").strip()
        if warm_ref and self._image_exists_for_row(row, warm_ref):
            return row
        base_image = (row.get("base_image") or "").strip()
        if warm_ref:
            logger.warning("Template %s references missing warm image %s", template_id, warm_ref)
        rebuilt = self._repair_missing_template_image(template_id, row)
        if rebuilt:
            return self.db.get_sandbox_template(template_id) or row
        if base_image and base_image != warm_ref and self._image_exists_for_row(row, base_image):
            self.db.set_template_warm_snapshot(
                template_id,
                base_image,
                None,
                registry_image_ref=(str(row.get("registry_image_ref") or "").strip() or None),
                materialized_gateway_instance_id=(
                    str(row.get("materialized_gateway_instance_id") or "").strip() or None
                ),
            )
            logger.warning(
                "Template %s missing warm image %s; falling back to existing base_image %s",
                template_id,
                warm_ref,
                base_image,
            )
            return self.db.get_sandbox_template(template_id) or row
        if not warm_ref and base_image and self._image_exists_for_row(row, base_image):
            return row
        if not warm_ref and not self._image_exists_for_row(row, base_image):
            rebuilt = self._repair_missing_template_image(template_id, row)
            if rebuilt:
                return self.db.get_sandbox_template(template_id) or row
        if warm_ref:
            self.db.set_template_build_error(
                template_id,
                f"Template image missing from runtime: {warm_ref}. Rebuild the template.",
            )
        elif base_image:
            self.db.set_template_build_error(
                template_id,
                f"Template base image missing from runtime: {base_image}. Rebuild the template.",
            )
        return self.db.get_sandbox_template(template_id) or row

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
        blocker = self.describe_docker_workload_blocker()
        if blocker:
            logger.warning("Sandbox reconcile skipped because execution plane is not ready: %s", blocker)
            return {
                "checked": 0,
                "stale_marked_lost": 0,
                "expired_reaped": 0,
                "routing_refreshed": 0,
            }
        rows = self.db.list_sandboxes(limit=max(1, int(limit)), offset=0)
        stats = {
            "checked": 0,
            "stale_marked_lost": 0,
            "liveness_unknown": 0,
            "warm_pool_revived": 0,
            "expired_reaped": 0,
            "routing_refreshed": 0,
        }
        for row in rows:
            sid = str(row.get("sandbox_id") or "").strip()
            cid = str(row.get("container_id") or "").strip()
            if not sid:
                continue
            row_state = str(row.get("state") or "").strip().lower()
            if row_state != "running":
                if row_state == "starting" and cid:
                    try:
                        execution = self._execution_for_row(row)
                        state_fn = getattr(execution, "get_container_state", None)
                        state = (
                            str(state_fn(cid) or "unknown").strip().lower()
                            if callable(state_fn)
                            else ("running" if execution.is_container_running(cid) else "stopped")
                        )
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
                    gateway_id = str(row.get("gateway_instance_id") or "").strip()
                    current_gateways = self._current_gateway_instance_ids()
                    if gateway_id and current_gateways and gateway_id not in current_gateways:
                        continue
                    try:
                        execution = self._execution_for_row(row)
                        state_fn = getattr(execution, "get_container_state", None)
                        state = (
                            str(state_fn(cid) or "unknown").strip().lower()
                            if callable(state_fn)
                            else ("running" if execution.is_container_running(cid) else "stopped")
                        )
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
                    execution = self._execution_for_row(row)
                    state_fn = getattr(execution, "get_container_state", None)
                    if callable(state_fn):
                        state = str(state_fn(cid) or "unknown").strip().lower()
                        alive = state == "running"
                    else:
                        alive = bool(execution.is_container_running(cid))
                        state = "running" if alive else "stopped"
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
        self, sandbox_id: str
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
        execution = self.execution
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

    def _startup_managed_bootstrap_spec(
        self,
        template_id: str,
        *,
        start_envd: bool,
        envd_port: int,
    ) -> Optional[Dict[str, Any]]:
        if not is_container_like_execution(self.execution):
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
        wt = execution.run_command(
            container_id,
            guest_tcp_wait_loop_script(
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
        startup_boot = None
        if auto_start_envd or template_id:
            startup_boot = self._startup_managed_bootstrap_spec(
                template_id,
                start_envd=auto_start_envd,
                envd_port=envd_port_cfg,
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

        sc, _img_ref, _tpl_env, _guest_port = self._resolve_template_start_spec(template_id)
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

    def create_sandbox(
        self,
        template_id: str = "python:3.11",
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        from_snapshot_image: Optional[str] = None,
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
    ) -> Optional[str]:
        """Create new sandbox, optionally from a prior ``docker commit`` image or warm pool."""
        self._last_create_error = ""
        snap = (from_snapshot_image or "").strip()
        if snap:
            return self._create_sandbox_fresh(
                template_id=template_id,
                metadata=metadata,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=timeout,
                from_snapshot_image=snap,
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
            )

        tid = (template_id or "").strip()
        tpl = self.db.get_sandbox_template(tid) if tid else None
        tid, tpl = self._resolve_template_alias_for_create(tid, tpl, owner_client_id)

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
            if not _looks_like_explicit_image_ref(tid):
                self._last_create_error = (
                    f"Unknown template alias {tid!r}. Build/register this template first, "
                    "or pass an explicit Docker image ref such as 'python:3.11' or 'registry/repo:tag'."
                )
                logger.warning(self._last_create_error)
                return None
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
            warm_ref_existing = (tpl.get("warm_snapshot_image") or tpl.get("registry_image_ref") or "").strip()
            base_image_existing = (tpl.get("base_image") or "").strip()
            if (
                not warm_ref_existing
                and base_image_existing
                and base_image_existing == tid
                and not _looks_like_explicit_image_ref(base_image_existing)
            ):
                self._last_create_error = (
                    f"Template alias {tid!r} is registered without a materialized image and has invalid "
                    f"base_image {base_image_existing!r}. Rebuild the template with a valid base image, "
                    "or create using the correct materialized template alias."
                )
                self.db.set_template_build_error(tid, self._last_create_error)
                logger.warning(self._last_create_error)
                return None
            tpl = self._ensure_template_runtime_image(tid, tpl)
            if not tpl.get("warm_snapshot_image"):
                if not self._build_registered_template_snapshot(tid):
                    self._last_create_error = str(
                        (self.db.get_sandbox_template(tid) or {}).get("build_error")
                        or f"Template {tid} could not be materialized"
                    )
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
            first_pool_request = False
            seed_target: Optional[GatewayTarget] = None
            pool_managed_template = bool(pool is not None and cfg.SANDBOX_WARM_POOL_SIZE > 0 and warm_img)
            warm_key = ""
            if pool_managed_template:
                warm_key = self.warm_pool_key(tid, cpu_limit, memory_limit, int(timeout))
                segment_before = self.db.get_warm_pool_segment(warm_key)
                segment_desired = int((segment_before or {}).get("desired_size") or 0)
                first_pool_request = (
                    (segment_before is None or segment_desired <= 0)
                    and self.warm_pool_ready_count(warm_key) <= 0
                )
                if first_pool_request and self.execution.get_backend_kind() in ("docker", "gvisor"):
                    seed_target = self._select_gateway_target_for_pool(
                        template_id=tid,
                        cpu_limit=cpu_limit,
                        memory_limit=memory_limit,
                        timeout=int(timeout),
                        template_row=tpl,
                    )
                    if seed_target is not None:
                        self.note_warm_pool_segment(
                            template_id=tid,
                            cpu_limit=cpu_limit,
                            memory_limit=memory_limit,
                            timeout=int(timeout),
                            desired_size=int(cfg.SANDBOX_WARM_POOL_SIZE),
                            ready_image_ref=warm_img,
                            preferred_gateway_instance_id=seed_target.instance_id,
                        )
                if not first_pool_request:
                    pool.ensure_pool_for(tid, cpu_limit, memory_limit, int(timeout), warm_img)
            if pool_managed_template and not first_pool_request:
                sid = pool.try_acquire(
                    tid,
                    metadata,
                    cpu_limit,
                    memory_limit,
                    int(timeout),
                    owner_client_id=owner_client_id,
                    owner_api_key_id=owner_api_key_id,
                )
                if sid:
                    return sid
                wait_sec = float(getattr(cfg, "SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC", 0.0) or 0.0)
                segment_after = self.db.get_warm_pool_segment(warm_key) if warm_key else None
                err = str((segment_after or {}).get("last_error") or "").strip()
                self._last_create_error = (
                    f"Timed out after {wait_sec:.1f}s waiting for a warm sandbox for template {tid!r}"
                    + (f": {err}" if err else "")
                )
                logger.warning(self._last_create_error)
                return None
            sid = self._create_sandbox_fresh(
                template_id=tid,
                metadata=metadata,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=timeout,
                from_snapshot_image=warm_img or None,
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
                forced_gateway_instance_id=seed_target.instance_id if seed_target is not None else None,
            )
            if sid and pool_managed_template and first_pool_request:
                pool.ensure_pool_for(tid, cpu_limit, memory_limit, int(timeout), warm_img)
            return sid

        if pool is not None:
            sid = pool.try_acquire(
                tid,
                metadata,
                cpu_limit,
                memory_limit,
                int(timeout),
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
            )
            if sid:
                return sid
        return self._create_sandbox_fresh(
            template_id=tid,
            metadata=metadata,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            from_snapshot_image=None,
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
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
        if not ref:
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
                effective_ref = registry_ref or image_ref
                if not effective_ref:
                    self.db.set_template_build_error(
                        template_id, "runtime-gateway template snapshot produced no image"
                    )
                    return False
                self.db.set_template_warm_snapshot(
                    template_id,
                    effective_ref,
                    None,
                    registry_image_ref=registry_ref or None,
                    materialized_gateway_instance_id=gateway_instance_id or None,
                )
                logger.info("Template %s warm snapshot (runtime-gateway): %s", template_id, effective_ref)
                self.sync_warm_pool_default_segment(template_id, effective_ref)
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
        logger.info("Template %s (parsed Dockerfile) warm snapshot: %s", template_id, image_ref)
        self.sync_warm_pool_default_segment(template_id, image_ref)
        return self.db.get_sandbox_template(template_id) or {}

    def _create_sandbox_fresh(
        self,
        template_id: str = "python:3.11",
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        from_snapshot_image: Optional[str] = None,
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
        is_warm_pool: bool = False,
        warm_pool_key: Optional[str] = None,
        forced_gateway_instance_id: Optional[str] = None,
    ) -> Optional[str]:
        """Create a brand-new sandbox (never taken from the warm pool)."""
        create_started = time.monotonic()
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
        meta["sandbox_allocation_source"] = "warm_pool_provision" if is_warm_pool else "cold_create"
        metadata = meta

        snap = (from_snapshot_image or "").strip()
        if snap:
            image = snap
        else:
            image = _resolve_sandbox_image(template_id)
        tpl = self.db.get_sandbox_template((template_id or "").strip()) if template_id else None
        runtime = self.execution.get_backend_kind()
        forced_target = None
        if runtime in ("docker", "gvisor") and forced_gateway_instance_id:
            forced_target = target_for_instance(self._gateway_targets(), forced_gateway_instance_id)
        if runtime in ("docker", "gvisor") and forced_target is not None:
            chosen_target = forced_target
        elif runtime in ("docker", "gvisor") and is_warm_pool:
            chosen_target = self._select_gateway_target_for_pool(
                template_id=(template_id or "").strip(),
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                template_row=tpl,
            )
        elif runtime in ("docker", "gvisor") and int(getattr(self._config, "SANDBOX_WARM_POOL_SIZE", 0) or 0) > 0:
            chosen_target = self._select_gateway_target_for_pool(
                template_id=(template_id or "").strip(),
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                template_row=tpl,
            )
        elif runtime in ("docker", "gvisor") and tpl:
            chosen_target = self._gateway_target_for_template_row(tpl)
        elif runtime in ("docker", "gvisor"):
            chosen_target = self._best_gateway_by_free_disk(
                self._gateway_targets(),
                force_refresh=True,
                preferred_image_ref=image,
            )
        else:
            chosen_target = None
        if runtime in ("docker", "gvisor") and chosen_target is None:
            self._last_create_error = (
                "No runtime-gateway shard has enough disk headroom under "
                f"RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO={getattr(self._config, 'RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO', 0.80)}"
            )
            logger.error(self._last_create_error)
            return None
        execution = self._execution_for_gateway_target(chosen_target) if chosen_target else self.execution
        logger.info(
            "Creating sandbox %s runtime=%s template_id=%r image=%r (from_snapshot=%s gateway=%s)",
            sandbox_id,
            runtime,
            template_id,
            image,
            bool(snap),
            chosen_target.instance_id if chosen_target else "-",
        )

        env_for_create: Optional[Dict[str, str]] = None
        if tpl:
            ev = dict(tpl.get("env") or {})
            if ev:
                env_for_create = ev

        meta_env = _create_env_from_metadata(metadata)
        if meta_env:
            env_for_create = {**(env_for_create or {}), **meta_env}

        envd_port_cfg = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        is_container_backend = is_container_like_execution(self.execution)
        envd_always = is_container_backend and bool(getattr(self._config, "ENVD_ALWAYS_ON", True))
        publish_envd_legacy = (
            is_container_backend
            and bool(getattr(self._config, "ENVD_PUBLISH_PORT", False))
        )
        publish_envd = publish_envd_legacy
        start_envd = envd_always or publish_envd_legacy
        auto_start_envd = start_envd and bool(getattr(self._config, "ENVD_AUTO_START", True))

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

        startup_boot = None
        if is_container_like_execution(self.execution):
            startup_boot = self._startup_managed_bootstrap_spec(
                template_id,
                start_envd=auto_start_envd,
                envd_port=envd_port_cfg,
            )

        config = ContainerConfig(
            image=image,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            environment=env_for_create,
            guest_ports=guest_ports,
            publish_envd_port=publish_envd,
            envd_port=envd_port_cfg,
            startup_command=list(startup_boot.get("startup_command") or []) if startup_boot else None,
        )

        container_id = execution.create_container(container_name, config)
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
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
            is_warm_pool=bool(is_warm_pool),
            warm_pool_key=warm_pool_key,
            gateway_instance_id=chosen_target.instance_id if chosen_target else None,
            gateway_route_base=chosen_target.route_base if chosen_target else None,
            gateway_api_base=chosen_target.api_base if chosen_target else None,
            gateway_docker_host=None,
            state="starting",
        )

        if start_envd and is_container_backend and envd_token:
            md_envd: Dict[str, Any] = {"envd_access_token": envd_token}
            if traffic_token:
                md_envd["traffic_access_token"] = traffic_token
            if publish_envd:
                ehp = execution.get_container_tcp_host_port(container_id, envd_port_cfg)
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

        bounded_client_bootstrap = (
            is_container_backend
            and not bool(is_warm_pool)
            and runtime in ("docker", "gvisor")
            and startup_boot is not None
        )
        if bounded_client_bootstrap:
            probes = self._startup_managed_readiness_probes(
                startup_boot,
                auto_start_envd=bool(auto_start_envd),
                envd_port_cfg=envd_port_cfg,
            )
            self.refresh_guest_routing_metadata(sandbox_id)
            wait_budget = float(getattr(self._config, "SANDBOX_COLD_CREATE_READY_WAIT_SEC", 1.0) or 0.0)
            ready = False
            started = time.monotonic()
            if probes and wait_budget > 0.0:
                ready = bool(
                    self._wait_for_gateway_guest_readiness(
                        sandbox_id,
                        container_id,
                        probes,
                        timeout_seconds=wait_budget,
                        warn_on_failure=False,
                    )
                )
            elapsed = round(max(0.0, time.monotonic() - started), 3)
            self.db.merge_sandbox_metadata(
                sandbox_id,
                {
                    "sandbox_bootstrap_pending": not ready,
                    "sandbox_bootstrap_error": "" if ready else "guest bootstrap still starting",
                    "sandbox_bootstrap_seconds": elapsed if ready else None,
                    "sandbox_create_seconds": round(max(0.0, time.monotonic() - create_started), 3),
                },
            )
            self.db.update_sandbox_state(sandbox_id, "running")
            logger.info(
                "Create latency: sandbox=%s source=%s gateway=%s create_seconds=%.3f bootstrap_ready=%s",
                sandbox_id,
                metadata.get("sandbox_allocation_source") or "-",
                chosen_target.instance_id if chosen_target else "-",
                max(0.0, time.monotonic() - create_started),
                ready,
            )
            if not ready:
                self._start_guest_bootstrap_background(sandbox_id, container_id, template_id)
                logger.info(
                    "Sandbox created: %s (guest bootstrap continuing, readiness_budget_seconds=%.3f)",
                    sandbox_id,
                    wait_budget,
                )
            else:
                logger.info("Sandbox created: %s (guest ready within cold-create budget)", sandbox_id)
            return sandbox_id

        if is_container_backend:
            if not self._bootstrap_guest_services(sandbox_id, container_id, template_id):
                logger.error("Sandbox %s bootstrap failed; tearing down workload", sandbox_id)
                self.kill_sandbox(sandbox_id, force=True)
                return None
            if startup_boot is None:
                self.refresh_guest_routing_metadata(sandbox_id)
        else:
            self.refresh_guest_routing_metadata(sandbox_id)

        create_seconds = round(max(0.0, time.monotonic() - create_started), 3)
        self.db.merge_sandbox_metadata(sandbox_id, {"sandbox_create_seconds": create_seconds})
        self.db.update_sandbox_state(sandbox_id, "running")
        logger.info(
            "Create latency: sandbox=%s source=%s gateway=%s create_seconds=%.3f",
            sandbox_id,
            metadata.get("sandbox_allocation_source") or "-",
            chosen_target.instance_id if chosen_target else "-",
            create_seconds,
        )
        logger.info("Sandbox created: %s", sandbox_id)
        return sandbox_id

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
        return self.get_sandbox_runtime_failure(sandbox_id) is None

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
        return True

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
        try:
            if self._execution_for_row(row).is_container_running(cid):
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
            return self._delete_sandbox_record(sandbox_id, mark_state="killed")

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
                    return self._delete_sandbox_record(sandbox_id, mark_state="killed")
            except Exception:
                logger.warning(
                    "Sandbox %s workload liveness unknown after kill failure; keeping DB row",
                    sandbox_id,
                )
                return False
            logger.error("Failed to kill workload for sandbox %s", sandbox_id)
            return False

        self._delete_sandbox_record(sandbox_id, mark_state="killed")

        logger.info("Sandbox killed: %s", sandbox_id)
        return True

    def pause_sandbox(self, sandbox_id: str) -> bool:
        """Pause sandbox (Docker: freeze cgroup / ``docker pause``)."""
        sandbox = self.get_sandbox(sandbox_id)
        if not sandbox:
            return False

        if self._execution_for_row(sandbox).pause_instance(sandbox["container_id"]):
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

        if self._execution_for_row(sandbox).resume_instance(sandbox["container_id"]):
            self.db.update_sandbox_state(sandbox_id, "running")
            self.refresh_guest_routing_metadata(sandbox_id)
            logger.info("Sandbox resumed: %s", sandbox_id)
            return True
        logger.warning("Resume not applied for sandbox %s (unsupported or failed)", sandbox_id)
        return False
