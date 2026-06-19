"""Resolve dialable upstream for sandbox guest ports (Kubernetes pod DNS)."""

from __future__ import annotations

from typing import Any, Optional

from shared.k8s_utils import k8s_guest_upstream_http, k8s_pod_service_host


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
