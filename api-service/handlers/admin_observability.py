"""Read-only admin observability APIs for the control-plane portal."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from config import get_config
from database import Database
from middleware import validate_admin_api_key
from orchestrator import SandboxManager

router = APIRouter(prefix="/admin/observability", tags=["admin-observability"])


def _db() -> Database:
    sm = getattr(SandboxManager, "instance", None)
    if sm is not None:
        return sm.db
    cfg = get_config()
    return Database(
        cfg.DATABASE_URL,
        database_type=getattr(cfg, "DATABASE_TYPE", ""),
        database_username=getattr(cfg, "DATABASE_USERNAME", ""),
        database_password=getattr(cfg, "DATABASE_PASSWORD", ""),
    )


def _manager() -> SandboxManager:
    sm = getattr(SandboxManager, "instance", None)
    if sm is None:
        raise HTTPException(status_code=503, detail="Sandbox manager is not initialized")
    return sm


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _hours_ago(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


def _gateway_ordinal(instance_id: str) -> int:
    match = re.search(r"-(\d+)$", instance_id or "")
    return int(match.group(1)) if match else 0


def _clamp_pod_deletion_cost(value: int) -> int:
    return max(-(2**31), min((2**31) - 1, int(value)))


def _as_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _safe_list(fn, default: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    try:
        value = fn()
        return value if isinstance(value, list) else (default or [])
    except Exception:
        return default or []


def _pod_metrics_by_gateway() -> Dict[str, Dict[str, Any]]:
    try:
        from orchestrator.k8s_runtime_gateways import list_runtime_gateway_pods

        return {
            pod.name: {
                "ordinal": int(pod.ordinal),
                "ready": bool(pod.ready),
                "phase": pod.phase,
                "deletion_timestamp": pod.deletion_timestamp,
                "pod_ip": pod.pod_ip,
                "cpu_millicores": int(pod.cpu_millicores),
                "memory_bytes": int(pod.memory_bytes),
            }
            for pod in list_runtime_gateway_pods(get_config(), force_refresh=False)
        }
    except Exception:
        return {}


def _metric_samples_by(key_name: str, *, sample_type: str, since_hours: float = 6.0) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sample in _db().list_observability_metric_samples(
        sample_type=sample_type,
        since=_hours_ago(since_hours),
        limit=5000,
        ascending=True,
    ):
        key = str(sample.get(key_name) or "").strip()
        if key:
            out.setdefault(key, []).append(sample)
    return out


def _gateway_deletion_cost(row: Dict[str, Any]) -> int:
    running = _as_int(row.get("running_sandbox_count"))
    warm = _as_int(row.get("warm_sandbox_count"))
    memory_mib = int(max(0, _as_int(row.get("memory_bytes"))) / (1024 * 1024))
    return _clamp_pod_deletion_cost(
        running * 100_000
        + warm * 50_000
        + int(max(0, _as_int(row.get("cpu_millicores"))))
        + memory_mib
        - int(max(0, _as_int(row.get("ordinal"))))
    )


def get_gateways_payload() -> Dict[str, Any]:
    sm = _manager()
    db = _db()
    pod_metrics = _pod_metrics_by_gateway()
    histories = _metric_samples_by("gateway_instance_id", sample_type="gateway")
    rows: List[Dict[str, Any]] = []
    for raw in _safe_list(sm.runtime_gateway_diagnostics):
        gateway_id = str(raw.get("gateway_instance_id") or "").strip()
        pod = pod_metrics.get(gateway_id, {})
        row = dict(raw)
        row["gateway_instance_id"] = gateway_id
        row["ordinal"] = _as_int(pod.get("ordinal") if pod else _gateway_ordinal(gateway_id))
        row["ready"] = bool(pod.get("ready")) if pod else bool(row.get("reachable"))
        row["phase"] = str(pod.get("phase") or ("Running" if row.get("reachable") else "Unknown"))
        row["pod_ip"] = str(pod.get("pod_ip") or "")
        row["deletion_timestamp"] = str(pod.get("deletion_timestamp") or "")
        row["cpu_millicores"] = _as_int(pod.get("cpu_millicores") if pod else row.get("cpu_millicores"))
        row["memory_bytes"] = _as_int(pod.get("memory_bytes") if pod else row.get("memory_bytes"))
        row["disk_total_bytes"] = _as_int(row.get("disk_total_bytes"))
        row["disk_used_bytes"] = _as_int(row.get("disk_used_bytes"))
        row["disk_free_bytes"] = _as_int(row.get("disk_free_bytes"))
        row["disk_used_ratio"] = _as_float(row.get("disk_used_ratio"))
        row["running_sandbox_count"] = _as_int(row.get("running_sandbox_count"))
        row["warm_sandbox_count"] = _as_int(row.get("warm_sandbox_count"))
        row["deletion_cost"] = _gateway_deletion_cost(row)
        row["status"] = "healthy" if row.get("reachable") and row.get("ready") else "degraded"
        row["sandboxes"] = db.list_sandboxes_for_gateway(gateway_id, limit=1000) if gateway_id else []
        history = histories.get(gateway_id, [])
        if not history:
            history = [
                {
                    "timestamp": _now_iso(),
                    "metrics": {
                        "cpu_millicores": row["cpu_millicores"],
                        "memory_bytes": row["memory_bytes"],
                        "running_sandbox_count": row["running_sandbox_count"],
                        "warm_sandbox_count": row["warm_sandbox_count"],
                        "deletion_cost": row["deletion_cost"],
                    },
                }
            ]
        row["history"] = history
        rows.append(row)
    rows.sort(key=lambda item: (_as_int(item.get("ordinal")), str(item.get("gateway_instance_id") or "")))
    return {
        "generated_at": _now_iso(),
        "gateways": rows,
        "count": len(rows),
        "reachable_count": sum(1 for row in rows if row.get("reachable")),
    }


def get_warm_pools_payload() -> Dict[str, Any]:
    sm = _manager()
    db = _db()
    histories = _metric_samples_by("warm_pool_key", sample_type="warm_pool")
    warm_rows = db.list_warm_pool_sandboxes()
    oldest_by_key: Dict[str, str] = {}
    for row in warm_rows:
        key = str(row.get("warm_pool_key") or "").strip()
        if not key:
            continue
        value = str(row.get("lease_expires_at") or row.get("created_at") or "").strip()
        if value and (key not in oldest_by_key or value < oldest_by_key[key]):
            oldest_by_key[key] = value

    rows: List[Dict[str, Any]] = []
    for raw in _safe_list(sm.warm_pool_segment_diagnostics):
        row = dict(raw)
        key = str(row.get("warm_pool_key") or "").strip()
        desired = _as_int(row.get("desired_size"))
        ready = _as_int(row.get("ready_count"))
        inflight = _as_int(row.get("inflight_count"))
        row["warm_pool_key"] = key
        row["desired_size"] = desired
        row["ready_count"] = ready
        row["inflight_count"] = inflight
        row["deficit"] = max(0, desired - ready - inflight)
        row["oldest_warm_lease"] = oldest_by_key.get(key, "")
        row["status"] = "healthy" if row["deficit"] <= 0 and not row.get("last_error") else "degraded"
        history = histories.get(key, [])
        if not history:
            history = [
                {
                    "timestamp": _now_iso(),
                    "metrics": {
                        "desired_size": desired,
                        "ready_count": ready,
                        "inflight_count": inflight,
                        "deficit": row["deficit"],
                    },
                }
            ]
        row["history"] = history
        rows.append(row)
    rows.sort(key=lambda item: str(item.get("warm_pool_key") or ""))
    return {
        "generated_at": _now_iso(),
        "warm_pools": rows,
        "count": len(rows),
        "deficit_count": sum(1 for row in rows if _as_int(row.get("deficit")) > 0),
        "total_deficit": sum(_as_int(row.get("deficit")) for row in rows),
    }


def get_templates_images_payload() -> Dict[str, Any]:
    sm = _manager()
    db = _db()
    rows: List[Dict[str, Any]] = []
    targets = []
    if sm.get_execution_kind() in ("docker", "gvisor"):
        try:
            targets = sm._gateway_targets()
        except Exception:
            targets = []
    for raw in db.list_all_sandbox_templates(limit=1000):
        row = dict(raw)
        template_id = str(row.get("template_id") or "").strip()
        warm_ref = str(row.get("warm_snapshot_image") or "").strip()
        registry_ref = str(row.get("registry_image_ref") or "").strip()
        live_gateways: List[str] = []
        if warm_ref and targets:
            for target in targets:
                try:
                    if sm._gateway_has_image(target, warm_ref, force_refresh=False):
                        live_gateways.append(target.instance_id)
                except Exception:
                    continue
        registry_available = False
        if registry_ref:
            try:
                registry_available = bool(sm._registry_image_exists_from_gateway(None, registry_ref))
            except Exception:
                registry_available = False
        build_error = str(row.get("build_error") or "").strip()
        has_any_ref = bool(warm_ref or registry_ref)
        missing = bool(has_any_ref and not live_gateways and not registry_available)
        rebuild_needed = bool(missing or ("rebuild" in build_error.lower()) or (not has_any_ref and row.get("source_kind") == "dockerfile"))
        if build_error and rebuild_needed:
            status = "rebuilding"
        elif missing or rebuild_needed:
            status = "missing"
        elif build_error:
            status = "degraded"
        else:
            status = "healthy"
        rows.append(
            {
                "template_id": template_id,
                "template_alias": row.get("template_alias") or template_id,
                "base_image": row.get("base_image") or "",
                "warm_snapshot_image": warm_ref,
                "registry_image_ref": registry_ref,
                "registry_available": registry_available,
                "materialized_gateway_instance_id": row.get("materialized_gateway_instance_id") or "",
                "live_gateways": live_gateways,
                "missing": missing,
                "rebuild_needed": rebuild_needed,
                "build_error": build_error,
                "status": status,
                "updated_at": row.get("updated_at") or "",
            }
        )
    return {
        "generated_at": _now_iso(),
        "templates": rows,
        "count": len(rows),
        "missing_count": sum(1 for row in rows if row["missing"]),
        "rebuild_needed_count": sum(1 for row in rows if row["rebuild_needed"]),
        "registry_unavailable_count": sum(1 for row in rows if row["registry_image_ref"] and not row["registry_available"]),
    }


def get_summary_payload() -> Dict[str, Any]:
    sm = _manager()
    db = _db()
    gateways = get_gateways_payload()
    warm_pools = get_warm_pools_payload()
    templates = get_templates_images_payload()
    sandboxes = db.list_sandboxes(limit=10000, offset=0)
    active_states = {"running", "starting", "pausing", "resuming", "paused"}
    active_count = sum(1 for row in sandboxes if str(row.get("state") or "").lower() in active_states)
    lost_count = sum(1 for row in sandboxes if str(row.get("state") or "").lower() == "lost")
    warm_count = sum(1 for row in sandboxes if bool(row.get("is_warm_pool")))
    recent_errors = db.list_observability_events(
        severity="error",
        since=_hours_ago(1.0),
        limit=20,
    )
    blocker = sm.describe_docker_workload_blocker()
    degraded = bool(
        blocker
        or (gateways["count"] > 0 and gateways["reachable_count"] <= 0)
        or warm_pools["total_deficit"] > 0
        or templates["missing_count"] > 0
        or recent_errors
    )
    return {
        "generated_at": _now_iso(),
        "status": "degraded" if degraded else "healthy",
        "control_plane": {
            "status": "degraded" if blocker else "healthy",
            "version": getattr(get_config(), "API_VERSION", ""),
            "api_service_role": getattr(get_config(), "API_SERVICE_ROLE", "control"),
            "instance_id": getattr(get_config(), "API_SERVICE_INSTANCE_ID", ""),
            "execution_kind": sm.get_execution_kind(),
            "blocker": blocker or "",
        },
        "gateways": {
            "total": gateways["count"],
            "reachable": gateways["reachable_count"],
            "degraded": gateways["count"] - gateways["reachable_count"],
        },
        "registry": {
            "status": "degraded" if templates["registry_unavailable_count"] else "healthy",
            "templates_with_registry_ref": sum(1 for row in templates["templates"] if row["registry_image_ref"]),
            "unavailable": templates["registry_unavailable_count"],
        },
        "sandboxes": {
            "total": len(sandboxes),
            "active": active_count,
            "lost": lost_count,
            "warm": warm_count,
        },
        "warm_pools": {
            "total": warm_pools["count"],
            "deficit_count": warm_pools["deficit_count"],
            "total_deficit": warm_pools["total_deficit"],
        },
        "image_repair": {
            "missing": templates["missing_count"],
            "rebuild_needed": templates["rebuild_needed_count"],
        },
        "recent_errors": recent_errors,
    }


def get_events_payload(
    *,
    limit: int = 100,
    offset: int = 0,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    action: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    gateway_instance_id: Optional[str] = None,
    template_id: Optional[str] = None,
    sandbox_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> Dict[str, Any]:
    events = _db().list_observability_events(
        limit=limit,
        offset=offset,
        severity=severity,
        category=category,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        gateway_instance_id=gateway_instance_id,
        template_id=template_id,
        sandbox_id=sandbox_id,
        since=since,
        until=until,
    )
    return {
        "generated_at": _now_iso(),
        "limit": max(1, min(int(limit), 1000)),
        "offset": max(0, int(offset)),
        "events": events,
    }


def get_sandbox_timeline_payload(sandbox_id: str, *, limit: int = 200) -> Dict[str, Any]:
    sid = (sandbox_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="sandbox_id is required")
    db = _db()
    row = db.get_sandbox(sid)
    events = db.list_observability_events(sandbox_id=sid, limit=limit)
    commands = []
    if row:
        try:
            commands = db.get_command_history(sid, limit=50)
        except Exception:
            commands = []
    return {
        "generated_at": _now_iso(),
        "sandbox_id": sid,
        "sandbox": row,
        "events": events,
        "commands": commands,
    }


@router.get("/summary")
async def summary(_: str = Depends(validate_admin_api_key)):
    return get_summary_payload()


@router.get("/gateways")
async def gateways(_: str = Depends(validate_admin_api_key)):
    return get_gateways_payload()


@router.get("/warm-pools")
async def warm_pools(_: str = Depends(validate_admin_api_key)):
    return get_warm_pools_payload()


@router.get("/templates/images")
async def templates_images(_: str = Depends(validate_admin_api_key)):
    return get_templates_images_payload()


@router.get("/events")
async def events(
    _: str = Depends(validate_admin_api_key),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    severity: Optional[str] = None,
    category: Optional[str] = None,
    action: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    gateway_instance_id: Optional[str] = None,
    template_id: Optional[str] = None,
    sandbox_id: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
):
    return get_events_payload(
        limit=limit,
        offset=offset,
        severity=severity,
        category=category,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        gateway_instance_id=gateway_instance_id,
        template_id=template_id,
        sandbox_id=sandbox_id,
        since=since,
        until=until,
    )


@router.get("/sandboxes/{sandbox_id}/timeline")
async def sandbox_timeline(
    sandbox_id: str,
    _: str = Depends(validate_admin_api_key),
    limit: int = Query(200, ge=1, le=1000),
):
    return get_sandbox_timeline_payload(sandbox_id, limit=limit)
