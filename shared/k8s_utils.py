"""Kubernetes pod/service DNS resolution helpers shared across services."""

from __future__ import annotations

from typing import Any


def k8s_pod_service_host(config: Any, sandbox_id: str) -> str:
    """Resolve the in-cluster DNS name for a sandbox pod's headless Service."""
    sid = (sandbox_id or "").strip()
    ns = (getattr(config, "K8S_NAMESPACE", None) or "sandboxes").strip()
    tpl = (
        getattr(config, "K8S_POD_SERVICE_TEMPLATE", None)
        or "sandbox-{sandbox_id}.{namespace}.svc.cluster.local"
    ).strip()
    return tpl.format(sandbox_id=sid, namespace=ns)


def k8s_guest_upstream_http(config: Any, sandbox_id: str, guest_port: int) -> str:
    """Build an HTTP upstream URL for a sandbox guest port via K8s Service DNS."""
    host = k8s_pod_service_host(config, sandbox_id)
    p = max(1, min(65535, int(guest_port)))
    return f"http://{host}:{p}"
