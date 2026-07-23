"""Runtime-gateway selection, diagnostics, and warm-pool scheduling."""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any, Dict, List, Optional

import httpx

from .gateway_targets import GatewayTarget, build_gateway_targets, target_for_instance
from .runtime_gateway_execution import RuntimeGatewayExecution
from .warm_sandbox_pool import warm_pool_key_string

logger = logging.getLogger(__name__)


def _gateway_ordinal(instance_id: str) -> int:
    match = re.search(r"-(\d+)$", instance_id or "")
    return int(match.group(1)) if match else 0


def _clamp_pod_deletion_cost(value: int) -> int:
    return max(-(2**31), min((2**31) - 1, int(value)))


class SandboxGatewayOpsMixin:
    def _runtime_gateway_targets_authoritative(self) -> bool:
        mode = str(
            getattr(self._config, "RUNTIME_GATEWAY_TARGET_DISCOVERY_MODE", "static") or "static"
        ).strip().lower()
        return mode in ("kubernetes", "k8s")

    def _statefulset_scale_down_aware(self) -> bool:
        return bool(getattr(self._config, "RUNTIME_GATEWAY_STATEFULSET_SCALE_DOWN_AWARE", False))

    def _statefulset_packing_target(self) -> int:
        return max(1, int(getattr(self._config, "RUNTIME_GATEWAY_STATEFULSET_PACKING_TARGET", 4) or 4))

    def _start_runtime_gateway_deletion_cost_loop(self) -> None:
        if not bool(getattr(self._config, "RUNTIME_GATEWAY_POD_DELETION_COST_ENABLED", False)):
            return
        if self._gateway_deletion_cost_thread and self._gateway_deletion_cost_thread.is_alive():
            return
        self._gateway_deletion_cost_thread = threading.Thread(
            target=self._runtime_gateway_deletion_cost_loop,
            name="runtime-gateway-deletion-cost",
            daemon=True,
        )
        self._gateway_deletion_cost_thread.start()

    def _runtime_gateway_deletion_cost_loop(self) -> None:
        interval = max(
            2.0,
            float(getattr(self._config, "RUNTIME_GATEWAY_POD_DELETION_COST_INTERVAL_SEC", 15.0) or 15.0),
        )
        while not self._gateway_deletion_cost_stop.wait(interval):
            try:
                self.update_runtime_gateway_deletion_costs()
            except Exception as ex:  # noqa: BLE001
                logger.debug("runtime-gateway deletion-cost update failed: %s", ex, exc_info=True)

    def update_runtime_gateway_deletion_costs(self) -> Dict[str, int]:
        """Annotate runtime pods with their current load.

        StatefulSets still scale down by ordinal, so scheduler placement is the
        real protection. This annotation is useful for visibility and for any
        controller path that does honor pod deletion cost.
        """
        try:
            from .k8s_runtime_gateways import list_runtime_gateway_pods, patch_runtime_gateway_deletion_cost
        except Exception as ex:  # noqa: BLE001
            logger.debug("runtime-gateway Kubernetes helpers unavailable: %s", ex)
            return {}

        pods = [
            pod
            for pod in list_runtime_gateway_pods(self._config, force_refresh=True)
            if pod.ready and not pod.deletion_timestamp
        ]
        if not pods:
            return {}

        warm_counts: Dict[str, int] = {}
        try:
            for row in self.db.list_warm_pool_sandboxes():
                gid = str(row.get("gateway_instance_id") or "").strip()
                if gid:
                    warm_counts[gid] = warm_counts.get(gid, 0) + 1
        except Exception:
            logger.debug("runtime-gateway deletion-cost: warm inventory unavailable", exc_info=True)

        costs: Dict[str, int] = {}
        for pod in pods:
            running = int(self.db.count_running_sandboxes(gateway_instance_id=pod.name))
            warm = int(warm_counts.get(pod.name, 0))
            memory_mib = int(max(0, pod.memory_bytes) / (1024 * 1024))
            cost = _clamp_pod_deletion_cost(
                running * 100_000
                + warm * 50_000
                + int(max(0, pod.cpu_millicores))
                + memory_mib
                - int(max(0, pod.ordinal))
            )
            if patch_runtime_gateway_deletion_cost(self._config, pod.name, cost):
                costs[pod.name] = cost

        if costs:
            logger.debug("runtime-gateway deletion costs updated: %s", costs)
        return costs

    def _gateway_targets(self) -> List[GatewayTarget]:
        try:
            return build_gateway_targets(self._config)
        except Exception as ex:  # noqa: BLE001
            logger.warning("Runtime gateway target build failed: %s", ex)
            return []

    def _current_gateway_instance_ids(self) -> set[str]:
        return {str(t.instance_id or "").strip() for t in self._gateway_targets() if str(t.instance_id or "").strip()}

    def _live_runtime_gateway_instance_ids(self, *, force_refresh: bool = False) -> tuple[set[str], bool]:
        """Return live runtime-gateway pod ids when Kubernetes discovery is authoritative.

        The boolean tells callers whether the set came from live pod discovery.
        If Kubernetes discovery is disabled or temporarily unavailable, callers
        must avoid treating absent ids as dead pods.
        """
        mode = str(
            getattr(self._config, "RUNTIME_GATEWAY_TARGET_DISCOVERY_MODE", "static") or "static"
        ).strip().lower()
        if mode not in ("kubernetes", "k8s"):
            return self._current_gateway_instance_ids(), False
        try:
            from .k8s_runtime_gateways import list_runtime_gateway_pods

            pods = list_runtime_gateway_pods(self._config, force_refresh=force_refresh)
        except Exception as ex:  # noqa: BLE001
            logger.warning("Runtime gateway pod discovery failed during reconcile: %s", ex)
            return set(), False
        live = {
            str(pod.name or "").strip()
            for pod in pods
            if pod.ready and not pod.deletion_timestamp
        }
        return {item for item in live if item}, True

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

    def is_warm_pool_leader(self, *, force_refresh: bool = False) -> bool:
        cfg = self._config
        lease_name = str(getattr(cfg, "WARM_POOL_COORDINATOR_LEASE_NAME", "warm-pool-coordinator"))
        ttl = max(5.0, float(getattr(cfg, "WARM_POOL_COORDINATOR_LEASE_TTL_SEC", 15) or 15))
        leader_refresh = max(1.0, min(ttl / 3.0, ttl - 1.0))
        follower_refresh = max(1.0, min(leader_refresh, 2.0))
        now = time.monotonic()
        with self._warm_pool_leader_lock:
            if not force_refresh and now < self._warm_pool_leader_next_check_at:
                return bool(self._warm_pool_leader_value)

            is_leader = False
            next_check = now + follower_refresh
            if bool(getattr(cfg, "WARM_POOL_USE_K8S_LEASE", True)):
                try:
                    from .k8s_leader_election import KubernetesLeaseClient

                    client = self._warm_pool_lease_client
                    if client is None:
                        client = KubernetesLeaseClient(cfg)
                        self._warm_pool_lease_client = client
                    if client.available():
                        is_leader = bool(client.try_acquire_or_renew(lease_name))
                        next_check = now + (leader_refresh if is_leader else follower_refresh)
                    else:
                        is_leader = bool(self.db.acquire_advisory_lock(lease_name))
                        next_check = now + (leader_refresh if is_leader else follower_refresh)
                except Exception:
                    logger.debug("Kubernetes Lease warm-pool leadership failed; falling back to DB lock", exc_info=True)
                    is_leader = bool(self.db.acquire_advisory_lock(lease_name))
                    next_check = now + (leader_refresh if is_leader else follower_refresh)
            else:
                is_leader = bool(self.db.acquire_advisory_lock(lease_name))
                next_check = now + (leader_refresh if is_leader else follower_refresh)

            self._warm_pool_leader_value = is_leader
            self._warm_pool_leader_next_check_at = next_check
            return is_leader

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
            image_refs = self._warm_pool_current_image_refs(key)
            rows = [
                row
                for row in self.db.list_warm_pool_sandboxes(warm_pool_key=key)
                if self._warm_pool_row_matches_image(row, image_refs)
            ]
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

    def _gateway_can_accept_new_usage(
        self,
        target: GatewayTarget,
        *,
        force_refresh: bool = False,
    ) -> bool:
        status = self._gateway_runtime_status(target, force_refresh=force_refresh)
        return bool(status.get("reachable"))

    def _warm_pool_rows_by_gateway(self, warm_pool_key: str) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key):
            gid = str(row.get("gateway_instance_id") or "").strip()
            out.setdefault(gid, []).append(row)
        return out

    def _warm_pool_row_gateway_available(
        self,
        row: Dict[str, Any],
        targets_by_instance: Optional[Dict[str, GatewayTarget]],
    ) -> bool:
        if targets_by_instance is None:
            return True
        sid = str(row.get("sandbox_id") or "").strip()
        gid = str(row.get("gateway_instance_id") or "").strip()
        if not gid:
            return True
        target = targets_by_instance.get(gid)
        if target is None:
            if sid:
                self._mark_sandbox_lost(
                    sid,
                    detail=(
                        f"Runtime gateway pod {gid} is no longer live; "
                        "the warm-pool sandbox workload was lost and must be recreated."
                    ),
                )
            logger.warning("Warm pool handoff skipped dead gateway sandbox=%s gateway=%s", sid or "-", gid)
            return False
        # Keep warm handoff DB-first. These rows are created only after bootstrap;
        # live container checks run in the inventory reconciler, not per request.
        return True

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

            if not self._warm_pool_row_gateway_available(row, targets_by_instance):
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

    def _warm_pool_current_image_refs(self, warm_pool_key: str) -> tuple[str, ...]:
        segment = self.db.get_warm_pool_segment((warm_pool_key or "").strip())
        template_id = str((segment or {}).get("template_id") or "").strip()
        if not template_id:
            template_id = str(warm_pool_key or "").split("|", 1)[0].strip()
        if not template_id:
            return ()
        row = self.db.get_sandbox_template(template_id)
        if not row:
            return ()
        refs: list[str] = []
        for value in (row.get("warm_snapshot_image"), row.get("registry_image_ref")):
            ref = str(value or "").strip()
            if ref and ref not in refs:
                refs.append(ref)
        return tuple(refs)

    def _warm_pool_current_image_ref(self, warm_pool_key: str) -> str:
        refs = self._warm_pool_current_image_refs(warm_pool_key)
        return refs[0] if refs else ""

    @staticmethod
    def _warm_pool_row_matches_image(row: Dict[str, Any], image_refs: tuple[str, ...]) -> bool:
        refs = tuple(ref for ref in image_refs if ref)
        if not refs:
            return True
        md = row.get("metadata") if isinstance(row, dict) else {}
        md = md if isinstance(md, dict) else {}
        return str(md.get("warm_pool_snapshot_image") or "").strip() in refs

    @staticmethod
    def _warm_pool_row_has_runtime_error(row: Dict[str, Any]) -> bool:
        md = row.get("metadata") if isinstance(row, dict) else {}
        md = md if isinstance(md, dict) else {}
        return bool(str(md.get("runtime_error") or "").strip())

    def _discard_stale_warm_pool_rows(self, warm_pool_key: str, image_refs: tuple[str, ...]) -> int:
        refs = tuple(ref for ref in image_refs if ref)
        if not refs:
            return 0
        removed = 0
        for row in self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key):
            if self._warm_pool_row_matches_image(row, refs):
                continue
            sid = str(row.get("sandbox_id") or "").strip()
            if not sid:
                continue
            try:
                if self.kill_sandbox(sid, force=True):
                    removed += 1
            except Exception as ex:  # noqa: BLE001
                logger.warning("Warm pool stale row discard failed sandbox=%s: %s", sid, ex)
        if removed:
            logger.info(
                "Warm pool stale image drain: removed %s sandbox(es) key=%s image_refs=%s",
                removed,
                warm_pool_key,
                ",".join(refs),
            )
            self._record_observability_event(
                severity="warning",
                category="warm_pool",
                action="stale_image_discard",
                entity_type="warm_pool",
                entity_id=warm_pool_key,
                message=f"Removed {removed} warm-pool sandbox(es) with stale template images",
                metadata={"warm_pool_key": warm_pool_key, "removed": removed, "image_refs": list(refs)},
            )
        return removed

    def warm_pool_ready_count(self, warm_pool_key: str) -> int:
        image_refs = self._warm_pool_current_image_refs(warm_pool_key)
        return len(
            [
                row
                for row in self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key)
                if self._warm_pool_row_matches_image(row, image_refs)
                and not self._warm_pool_row_has_runtime_error(row)
            ]
        )

    def trim_warm_pool_to_size(self, warm_pool_key: str, desired_size: int) -> int:
        desired = max(0, int(desired_size))
        rows = self.db.list_warm_pool_sandboxes(warm_pool_key=warm_pool_key)
        extra = len(rows) - desired
        if extra <= 0:
            return 0
        rows.sort(key=lambda row: str(row.get("created_at") or ""))
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
            self._record_observability_event(
                severity="info",
                category="warm_pool",
                action="trim",
                entity_type="warm_pool",
                entity_id=warm_pool_key,
                message=f"Trimmed {removed} excess warm-pool sandbox(es)",
                metadata={"warm_pool_key": warm_pool_key, "removed": removed, "desired_size": desired},
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
            preferred_gateway_instance_id=preferred_gateway_instance_id,
            last_error=last_error,
        )
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

    def _find_gateway_with_image(
        self,
        image_ref: str,
        *,
        preferred_instance_id: Optional[str] = None,
        force_refresh: bool = True,
    ) -> Optional[GatewayTarget]:
        return self._template_images.find_gateway_with_image(
            image_ref,
            preferred_instance_id=preferred_instance_id,
            force_refresh=force_refresh,
        )

    def _registry_image_exists_from_gateway(
        self,
        target: Optional[GatewayTarget],
        image_ref: str,
    ) -> bool:
        return self._template_images.registry_image_exists(target, image_ref)

    def _push_gateway_image_to_registry(
        self,
        *,
        template_id: str,
        image_ref: str,
        source_target: GatewayTarget,
    ) -> Optional[str]:
        return self._template_images.push_gateway_image_to_registry(
            template_id=template_id,
            image_ref=image_ref,
            source_target=source_target,
        )

    def _best_gateway_by_load(
        self,
        candidates: List[GatewayTarget],
        *,
        force_refresh: bool = False,
        preferred_image_ref: Optional[str] = None,
    ) -> Optional[GatewayTarget]:
        best: Optional[GatewayTarget] = None
        best_score: tuple[Any, ...] | None = None
        image_ref = (preferred_image_ref or "").strip()
        candidate_details: list[dict[str, Any]] = []
        for target in candidates:
            status = self._gateway_runtime_status(
                target,
                force_refresh=force_refresh,
            )
            if not status.get("reachable"):
                candidate_details.append(
                    {
                        "gateway_instance_id": target.instance_id,
                        "reachable": False,
                        "reason": status.get("error") or "unreachable",
                    }
                )
                continue
            has_image_rank = 1
            if image_ref:
                with self._gateway_image_cache_lock:
                    cached = self._gateway_image_cache.get((target.instance_id, image_ref))
                if cached and (time.time() - float(cached[0])) <= 10.0 and bool(cached[1]):
                    has_image_rank = 0
            running_count = self.db.count_running_sandboxes(gateway_instance_id=target.instance_id)
            if self._statefulset_scale_down_aware():
                packing_target = self._statefulset_packing_target()
                load_bucket = int(running_count) // packing_target
                score = (
                    load_bucket,
                    _gateway_ordinal(target.instance_id),
                    int(running_count),
                    has_image_rank,
                    target.instance_id,
                )
            else:
                score = (int(running_count), has_image_rank, _gateway_ordinal(target.instance_id), target.instance_id)
            candidate_details.append(
                {
                    "gateway_instance_id": target.instance_id,
                    "reachable": True,
                    "running_count": int(running_count),
                    "image_cached_rank": int(has_image_rank),
                    "ordinal": _gateway_ordinal(target.instance_id),
                    "score": list(score),
                }
            )
            if best_score is None or score < best_score:
                best = target
                best_score = score
        if best is not None and best_score is not None:
            if self._statefulset_scale_down_aware():
                running_count = int(best_score[2])
                image_rank = int(best_score[3])
                ordinal = int(best_score[1])
            else:
                running_count = int(best_score[0])
                image_rank = int(best_score[1])
                ordinal = _gateway_ordinal(best.instance_id)
            logger.info(
                "Scheduler decision: reason=load gateway=%s ordinal=%s running_count=%s image_ref=%s image_cached_rank=%s",
                best.instance_id,
                ordinal,
                running_count,
                image_ref or "-",
                image_rank,
            )
            self._record_observability_event(
                severity="info",
                category="scheduler",
                action="gateway_selected",
                entity_type="gateway",
                entity_id=best.instance_id,
                gateway_instance_id=best.instance_id,
                message=f"Selected runtime gateway {best.instance_id} by load",
                metadata={
                    "reason": "load",
                    "image_ref": image_ref,
                    "score": list(best_score),
                    "running_count": running_count,
                    "image_cached_rank": image_rank,
                    "ordinal": ordinal,
                    "candidates": candidate_details,
                },
            )
        elif candidates:
            logger.warning(
                "Scheduler decision: no_gateway reason=unreachable candidates=%s image_ref=%s",
                ",".join(t.instance_id for t in candidates),
                image_ref or "-",
            )
            self._record_observability_event(
                severity="warning",
                category="scheduler",
                action="no_gateway",
                entity_type="gateway",
                message="No reachable runtime-gateway shard is available",
                metadata={
                    "reason": "unreachable",
                    "image_ref": image_ref,
                    "candidates": candidate_details,
                },
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
        extra_warm_counts_by_gateway: Optional[Dict[str, int]] = None,
        force_refresh: bool = True,
        require_reachable: bool = True,
    ) -> Optional[GatewayTarget]:
        targets = self._gateway_targets()
        if not targets:
            return None
        row = template_row or {}
        warm_ref = str(row.get("warm_snapshot_image") or "").strip()
        registry_ref = str(row.get("registry_image_ref") or "").strip()
        owner_instance = str(row.get("materialized_gateway_instance_id") or "").strip()
        if owner_instance and warm_ref:
            pinned = target_for_instance(targets, owner_instance)
            if pinned is not None and (
                not require_reachable
                or self._gateway_can_accept_new_usage(pinned, force_refresh=force_refresh)
            ):
                logger.info(
                    "Scheduler decision: reason=template_owner_cached gateway=%s template=%s",
                    pinned.instance_id,
                    template_id,
                )
                self._record_observability_event(
                    severity="info",
                    category="scheduler",
                    action="gateway_selected",
                    entity_type="gateway",
                    entity_id=pinned.instance_id,
                    gateway_instance_id=pinned.instance_id,
                    template_id=template_id,
                    message=f"Selected runtime gateway {pinned.instance_id} because it has the materialized template image",
                    metadata={
                        "reason": "template_owner_cached",
                        "template_id": template_id,
                        "owner_instance": owner_instance,
                        "image_ref": warm_ref,
                    },
                )
                return pinned
            if registry_ref:
                logger.info(
                    "Template %r owner gateway %s is not available; falling back to registry image %s",
                    template_id,
                    owner_instance,
                    registry_ref,
                )
            else:
                logger.warning(
                    "Template %r is local-only on %s, but that runtime-gateway shard is not reachable",
                    template_id,
                    owner_instance,
                )
                self._record_observability_event(
                    severity="warning",
                    category="scheduler",
                    action="no_gateway",
                    entity_type="template",
                    entity_id=template_id,
                    gateway_instance_id=owner_instance,
                    template_id=template_id,
                    message=f"Template {template_id} is local-only on an unreachable runtime gateway",
                    metadata={"reason": "template_owner_unreachable", "owner_instance": owner_instance},
                )
                return None
        if owner_instance and not registry_ref:
            logger.warning(
                "Template %r is local-only on %s, but that runtime-gateway shard is not reachable",
                template_id,
                owner_instance,
            )
            self._record_observability_event(
                severity="warning",
                category="scheduler",
                action="no_gateway",
                entity_type="template",
                entity_id=template_id,
                gateway_instance_id=owner_instance,
                template_id=template_id,
                message=f"Template {template_id} is local-only on an unreachable runtime gateway",
                metadata={"reason": "template_owner_unreachable", "owner_instance": owner_instance},
            )
            return None
        warm_key = self.warm_pool_key(template_id, cpu_limit, memory_limit, int(timeout))
        segment = self.db.get_warm_pool_segment(warm_key)
        preferred_instance = str((segment or {}).get("preferred_gateway_instance_id") or "").strip()
        inventory = self._warm_pool_rows_by_gateway(warm_key)
        candidate_targets = targets if registry_ref or not owner_instance else []
        preferred_image_ref = warm_ref or registry_ref
        candidates: list[tuple[Any, ...]] = []
        candidate_details: list[dict[str, Any]] = []
        scale_down_aware = self._statefulset_scale_down_aware()
        for target in candidate_targets:
            if require_reachable:
                status = self._gateway_runtime_status(target, force_refresh=force_refresh)
                if not status.get("reachable"):
                    candidate_details.append(
                        {
                            "gateway_instance_id": target.instance_id,
                            "reachable": False,
                            "reason": status.get("error") or "unreachable",
                        }
                    )
                    continue
            warm_count = len(inventory.get(target.instance_id, [])) + int(
                (extra_warm_counts_by_gateway or {}).get(target.instance_id) or 0
            )
            running_count = self.db.count_running_sandboxes(gateway_instance_id=target.instance_id) + int(
                (extra_warm_counts_by_gateway or {}).get(target.instance_id) or 0
            )
            preferred_rank = 0 if preferred_instance and target.instance_id == preferred_instance else 1
            ordinal_rank = _gateway_ordinal(target.instance_id) if scale_down_aware else 0
            load_count = int(warm_count) + int(running_count)
            load_bucket = load_count // self._statefulset_packing_target() if scale_down_aware else 0
            candidates.append(
                (
                    int(load_bucket),
                    int(preferred_rank),
                    int(ordinal_rank),
                    int(warm_count),
                    int(running_count),
                    target.instance_id,
                    target,
                )
            )
            candidate_details.append(
                {
                    "gateway_instance_id": target.instance_id,
                    "reachable": True,
                    "warm_count": int(warm_count),
                    "running_count": int(running_count),
                    "preferred_rank": int(preferred_rank),
                    "ordinal": int(ordinal_rank),
                    "load_bucket": int(load_bucket),
                }
            )
        candidates.sort(key=lambda item: item[:-1])
        if candidates:
            (
                _load_bucket,
                _preferred_rank,
                ordinal,
                warm_count,
                running_count,
                _instance_id,
                target,
            ) = candidates[0]
            logger.info(
                "Scheduler decision: reason=balanced_warm_pool gateway=%s ordinal=%s template=%s warm_count=%s running_count=%s image_ref=%s",
                target.instance_id,
                ordinal,
                template_id,
                warm_count,
                running_count,
                preferred_image_ref or "-",
            )
            self._record_observability_event(
                severity="info",
                category="scheduler",
                action="gateway_selected",
                entity_type="gateway",
                entity_id=target.instance_id,
                gateway_instance_id=target.instance_id,
                template_id=template_id,
                message=f"Selected runtime gateway {target.instance_id} for warm-pool placement",
                metadata={
                    "reason": "balanced_warm_pool",
                    "template_id": template_id,
                    "warm_pool_key": warm_key,
                    "image_ref": preferred_image_ref,
                    "warm_count": int(warm_count),
                    "running_count": int(running_count),
                    "ordinal": int(ordinal),
                    "candidates": candidate_details,
                },
            )
            return target
        self._record_observability_event(
            severity="warning",
            category="scheduler",
            action="no_gateway",
            entity_type="template",
            entity_id=template_id,
            template_id=template_id,
            message=f"No runtime gateway is available for warm-pool placement for {template_id}",
            metadata={
                "reason": "warm_pool_no_candidate",
                "warm_pool_key": warm_key,
                "image_ref": preferred_image_ref,
                "candidates": candidate_details,
            },
        )
        return None

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
        image_refs = self._warm_pool_current_image_refs(key)
        self._discard_stale_warm_pool_rows(key, image_refs)
        targets_by_instance: Optional[Dict[str, GatewayTarget]] = None
        if self.execution.get_backend_kind() in ("docker", "gvisor"):
            targets_by_instance = {target.instance_id: target for target in self._gateway_targets()}
        inventory: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.db.list_warm_pool_sandboxes(warm_pool_key=key):
            if self._warm_pool_row_has_runtime_error(row):
                continue
            if not self._warm_pool_row_matches_image(row, image_refs):
                continue
            if not self._warm_pool_row_gateway_available(row, targets_by_instance):
                continue
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
                key=(
                    (lambda item: (_gateway_ordinal(item[0]), -len(item[1]), item[0]))
                    if self._statefulset_scale_down_aware()
                    else (lambda item: (-len(item[1]), item[0]))
                ),
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
                if not self._warm_pool_row_matches_image(claimed, image_refs):
                    sid = str(claimed.get("sandbox_id") or "").strip()
                    logger.warning(
                        "Warm pool claim returned stale image sandbox=%s key=%s image_refs=%s",
                        sid,
                        key,
                        ",".join(image_refs),
                    )
                    if sid:
                        self.kill_sandbox(sid, force=True)
                    continue
                self._remember_recent_created_row(claimed)
                self._record_observability_event(
                    severity="info",
                    category="warm_pool",
                    action="handoff",
                    entity_type="sandbox",
                    entity_id=str(claimed.get("sandbox_id") or ""),
                    sandbox_id=str(claimed.get("sandbox_id") or ""),
                    gateway_instance_id=instance_id,
                    template_id=template_id,
                    message="Handed off warm-pool sandbox",
                    metadata={
                        "warm_pool_key": key,
                        "preferred_gateway_instance_id": instance_id,
                        "image_refs": list(image_refs),
                        "owner_client_id": owner_client_id or "",
                    },
                )
                return claimed
        if targets_by_instance is not None:
            return None
        claimed = self.db.claim_warm_pool_sandbox(
            warm_pool_key=key,
            gateway_instance_id=None,
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
            metadata_updates=handoff_metadata,
            timeout_seconds=handoff_timeout,
        )
        if claimed:
            if not self._warm_pool_row_gateway_available(claimed, targets_by_instance):
                return None
            if not self._warm_pool_row_matches_image(claimed, image_refs):
                sid = str(claimed.get("sandbox_id") or "").strip()
                logger.warning(
                    "Warm pool fallback claim returned stale image sandbox=%s key=%s image_refs=%s",
                    sid,
                    key,
                    ",".join(image_refs),
                )
                if sid:
                    self.kill_sandbox(sid, force=True)
                return None
            self._remember_recent_created_row(claimed)
            self._record_observability_event(
                severity="info",
                category="warm_pool",
                action="handoff",
                entity_type="sandbox",
                entity_id=str(claimed.get("sandbox_id") or ""),
                sandbox_id=str(claimed.get("sandbox_id") or ""),
                gateway_instance_id=str(claimed.get("gateway_instance_id") or ""),
                template_id=template_id,
                message="Handed off warm-pool sandbox",
                metadata={
                    "warm_pool_key": key,
                    "preferred_gateway_instance_id": "",
                    "image_refs": list(image_refs),
                    "owner_client_id": owner_client_id or "",
                },
            )
            return claimed
        return None

    def select_gateway_target_for_pool_create(
        self,
        *,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        extra_warm_counts_by_gateway: Optional[Dict[str, int]] = None,
        force_refresh: bool = True,
    ) -> Optional[GatewayTarget]:
        tpl = self.db.get_sandbox_template((template_id or "").strip()) if template_id else None
        if tpl:
            tpl = self._ensure_template_runtime_image((template_id or "").strip(), tpl)
        return self._select_gateway_target_for_pool(
            template_id=(template_id or "").strip(),
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=int(timeout),
            template_row=tpl,
            extra_warm_counts_by_gateway=extra_warm_counts_by_gateway,
            force_refresh=force_refresh,
        )

    def _gateway_target_for_template_row(self, row: Optional[Dict[str, Any]]) -> Optional[GatewayTarget]:
        targets = self._gateway_targets()
        if not targets:
            return None
        warm_ref = str((row or {}).get("warm_snapshot_image") or "").strip()
        registry_ref = str((row or {}).get("registry_image_ref") or "").strip()
        owner_instance = str((row or {}).get("materialized_gateway_instance_id") or "").strip()
        if owner_instance and warm_ref:
            pinned = target_for_instance(targets, owner_instance)
            if pinned is not None and self._gateway_can_accept_new_usage(pinned, force_refresh=True):
                return pinned
            if registry_ref:
                return self._best_gateway_by_load(
                    targets,
                    force_refresh=True,
                    preferred_image_ref=registry_ref,
                )
            return None
        return self._best_gateway_by_load(
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
