"""Sandbox data-plane URL helpers and guest upstream resolution for proxy-service."""

from __future__ import annotations

import hmac
from typing import Any, Dict, Optional, TYPE_CHECKING

from routing.host_parse import format_sandbox_base_url, format_sandbox_host
from orchestrator.runtime_utils import supports_data_plane_routing
from orchestrator.guest_ports import ports_from_metadata

if TYPE_CHECKING:
    from orchestrator.sandbox_manager import SandboxManager


def sandbox_domain_for_config(config: Any) -> str:
    dom = getattr(config, "SANDBOX_DATA_PLANE_DOMAIN", None) or "sndbx.com"
    return (dom or "sndbx.com").strip().lstrip(".")


def data_plane_enabled_for_config(config: Any) -> bool:
    return bool(getattr(config, "SANDBOX_DATA_PLANE_ENABLED", True))


def data_plane_debug_for_config(config: Any) -> bool:
    return bool(getattr(config, "SANDBOX_DATA_PLANE_DEBUG", False))


def ingress_debug_for_config(config: Any) -> bool:
    return data_plane_debug_for_config(config)


def data_plane_listen_port(config: Any, scheme: str) -> int:
    explicit = getattr(config, "SANDBOX_DATA_PLANE_LISTEN_PORT", None)
    if explicit is not None:
        return int(explicit)
    sch = (scheme or "http").rstrip(":/").lower()
    if sch in ("https", "wss"):
        return 443
    return int(getattr(config, "SANDBOX_DATA_PLANE_HTTP_PORT", 443))


def allow_public_traffic_for_row(row: Dict[str, Any], config: Any) -> bool:
    md = row.get("metadata") or {}
    if "allow_public_traffic" in md:
        return bool(md.get("allow_public_traffic"))
    net = md.get("network")
    if isinstance(net, dict) and "allow_public_traffic" in net:
        return bool(net.get("allow_public_traffic"))
    return bool(getattr(config, "SANDBOX_DEFAULT_ALLOW_PUBLIC_TRAFFIC", False))


def traffic_access_token_for_row(row: Dict[str, Any]) -> Optional[str]:
    tok = (row.get("metadata") or {}).get("traffic_access_token")
    s = str(tok).strip() if tok else ""
    return s or None


def verify_traffic_access_token(row: Dict[str, Any], token: str) -> bool:
    expected = traffic_access_token_for_row(row)
    provided = (token or "").strip()
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


def enrich_sandbox_response(
    row: Dict[str, Any],
    config: Any,
    *,
    include_secrets: bool = False,
) -> Dict[str, Any]:
    out = dict(row)
    out["sandbox_domain"] = sandbox_domain_for_config(config)
    out["envd_port"] = max(1, min(65535, int(getattr(config, "ENVD_PORT", 49983))))
    out["allow_public_traffic"] = allow_public_traffic_for_row(row, config)
    if include_secrets:
        tok = (row.get("metadata") or {}).get("envd_access_token")
        if tok:
            out["envd_access_token"] = str(tok)
        ttr = traffic_access_token_for_row(row)
        if ttr:
            out["traffic_access_token"] = ttr
    return out


def data_plane_base_url(
    config: Any,
    *,
    sandbox_id: str,
    port: int,
    scheme: str = "http",
) -> str:
    listen_port = data_plane_listen_port(config, scheme)
    base_scheme = getattr(config, "SANDBOX_DATA_PLANE_SCHEME", None) or scheme
    if scheme in ("ws", "wss"):
        use_scheme = "wss" if base_scheme == "https" else "ws"
    else:
        use_scheme = base_scheme.rstrip(":/")
    return format_sandbox_base_url(
        port=int(port),
        sandbox_id=sandbox_id,
        sandbox_domain=sandbox_domain_for_config(config),
        debug=data_plane_debug_for_config(config),
        scheme=use_scheme,
        listen_port=listen_port,
    )


def get_host_for_sandbox(config: Any, *, sandbox_id: str, port: int) -> str:
    return format_sandbox_host(
        port=int(port),
        sandbox_id=sandbox_id,
        sandbox_domain=sandbox_domain_for_config(config),
        debug=data_plane_debug_for_config(config),
    )


def k8s_pod_service_host(config: Any, sandbox_id: str) -> str:
    sid = (sandbox_id or "").strip()
    ns = (getattr(config, "K8S_NAMESPACE", None) or "sandboxes").strip()
    tpl = (
        getattr(config, "K8S_POD_SERVICE_TEMPLATE", None)
        or "sandbox-{sandbox_id}.{namespace}.svc.cluster.local"
    ).strip()
    return tpl.format(sandbox_id=sid, namespace=ns)


def k8s_guest_upstream_target(config: Any, sandbox_id: str, guest_port: int) -> Dict[str, Any]:
    host = k8s_pod_service_host(config, sandbox_id)
    p = max(1, min(65535, int(guest_port)))
    return {
        "scheme": "http",
        "host": host,
        "port": p,
        "guest_port": p,
        "kind": "k8s_service",
        "upstream_http": f"http://{host}:{p}",
    }


def is_k8s_runtime_config(config: Any) -> bool:
    fn = getattr(config, "is_k8s_runtime", None)
    if callable(fn):
        return bool(fn())
    return (getattr(config, "SANDBOX_RUNTIME", "") or "").strip().lower() in ("k8s", "kubernetes")


def _guest_port_upstream_target_docker(
    execution: Any,
    cfg: Any,
    *,
    container_id: str,
    guest_port: int,
    meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    from orchestrator.container_manager import ContainerManager

    if not isinstance(execution, ContainerManager):
        return None
    p = max(1, min(65535, int(guest_port)))
    ip = execution.get_container_internal_ipv4(container_id)
    if not ip:
        return None
    return {
        "scheme": "http",
        "host": ip,
        "port": p,
        "guest_port": p,
        "kind": "bridge",
        "upstream_http": f"http://{ip}:{p}",
    }


def build_guest_routing_record(
    manager: "SandboxManager",
    sandbox_id: str,
    *,
    guest_ports: Optional[list[int]] = None,
) -> Optional[Dict[str, Any]]:
    sid = (sandbox_id or "").strip()
    row = manager.get_sandbox(sid)
    if not row or not manager.is_running(sid):
        return None
    cfg = manager._config
    md = row.get("metadata") or {}
    ports = guest_ports or ports_from_metadata(md)
    if not ports:
        envd_p = max(1, min(65535, int(getattr(cfg, "ENVD_PORT", 49983))))
        if bool(getattr(cfg, "ENVD_ALWAYS_ON", True)):
            ports = [envd_p]

    if is_k8s_runtime_config(cfg):
        return {str(p): k8s_guest_upstream_target(cfg, sid, p) for p in ports}

    execution = manager.execution
    kind = execution.get_backend_kind()
    if not supports_data_plane_routing(kind):
        return None
    cid = (row.get("container_id") or "").strip()
    if not cid:
        return None
    meta = row.get("metadata") or {}
    out: Dict[str, Any] = {}
    for p in ports:
        target = _guest_port_upstream_target_docker(
            execution, cfg, container_id=cid, guest_port=p, meta=meta
        )
        if target:
            out[str(p)] = target
    return out or None


def resolve_guest_upstream_http(manager: "SandboxManager", sandbox_id: str, guest_port: int) -> Optional[str]:
    sid = (sandbox_id or "").strip()
    row = manager.get_sandbox(sid)
    if not row or not manager.is_running(sid):
        return None
    cfg = manager._config
    p = max(1, min(65535, int(guest_port)))

    if is_k8s_runtime_config(cfg):
        meta = row.get("metadata") or {}
        k8s = meta.get("k8s") if isinstance(meta.get("k8s"), dict) else {}
        host = (k8s.get("service_host") or "").strip() or k8s_pod_service_host(cfg, sid)
        return f"http://{host}:{p}"

    execution = manager.execution
    kind = execution.get_backend_kind()
    if not supports_data_plane_routing(kind):
        return None
    cid = (row.get("container_id") or "").strip()
    if not cid:
        return None
    meta = row.get("metadata") or {}
    target = _guest_port_upstream_target_docker(
        execution, cfg, container_id=cid, guest_port=p, meta=meta
    )
    if target:
        return str(target["upstream_http"])
    return None
