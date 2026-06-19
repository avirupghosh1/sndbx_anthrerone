"""Resolve dialable upstream for sandbox guest ports (Kubernetes pod DNS)."""

from __future__ import annotations

from typing import Any, Optional


def k8s_pod_service_host(config: Any, sandbox_id: str) -> str:
    sid = (sandbox_id or "").strip()
    ns = (getattr(config, "K8S_NAMESPACE", None) or "sandboxes").strip()
    tpl = (
        getattr(config, "K8S_POD_SERVICE_TEMPLATE", None)
        or "sandbox-{sandbox_id}.{namespace}.svc.cluster.local"
    ).strip()
    return tpl.format(sandbox_id=sid, namespace=ns)


def k8s_guest_upstream_http(config: Any, sandbox_id: str, guest_port: int) -> str:
    """Linux K8s: guest listens on ``guest_port`` inside the pod; Service exposes the same port."""
    host = k8s_pod_service_host(config, sandbox_id)
    p = max(1, min(65535, int(guest_port)))
    return f"http://{host}:{p}"


def resolve_upstream_http(
    config: Any,
    *,
    sandbox_id: str,
    guest_port: int,
    route_upstream: Optional[str] = None,
) -> Optional[str]:
    mode = (getattr(config, "UPSTREAM_RESOLVE_MODE", None) or "k8s_dns").strip().lower()
    if mode == "control_plane" and route_upstream:
        return route_upstream.rstrip("/")
    if mode in ("k8s_dns", "k8s", "kubernetes"):
        return k8s_guest_upstream_http(config, sandbox_id, guest_port)
    if route_upstream:
        return route_upstream.rstrip("/")
    return k8s_guest_upstream_http(config, sandbox_id, guest_port)
