"""Sandbox data-plane URL helpers and guest upstream resolution for runtime-gateway."""

from __future__ import annotations

import hmac
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

from routing.host_parse import format_sandbox_base_url, format_sandbox_host
from orchestrator.runtime_utils import supports_data_plane_routing
from orchestrator.guest_ports import ports_from_metadata

if TYPE_CHECKING:
    from orchestrator.sandbox_manager import SandboxManager

logger = logging.getLogger(__name__)


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
    url = format_sandbox_base_url(
        port=int(port),
        sandbox_id=sandbox_id,
        sandbox_domain=sandbox_domain_for_config(config),
        debug=data_plane_debug_for_config(config),
        scheme=use_scheme,
        listen_port=listen_port,
    )
    logger.info(
        "SDK data-plane URL generated sandbox_id=%s guest_port=%s scheme=%s listen_port=%s url=%s",
        sandbox_id,
        int(port),
        use_scheme,
        listen_port,
        url,
    )
    return url


def get_host_for_sandbox(config: Any, *, sandbox_id: str, port: int) -> str:
    return format_sandbox_host(
        port=int(port),
        sandbox_id=sandbox_id,
        sandbox_domain=sandbox_domain_for_config(config),
        debug=data_plane_debug_for_config(config),
    )


def _guest_port_upstream_target_docker(
    execution: Any,
    cfg: Any,
    *,
    container_id: str,
    guest_port: int,
    meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    ip_fn = getattr(execution, "get_container_internal_ipv4", None)
    if not callable(ip_fn):
        return None
    p = max(1, min(65535, int(guest_port)))
    try:
        ip = ip_fn(container_id)
    except Exception as exc:  # noqa: BLE001
        logger.debug("guest upstream docker inspect failed container=%s port=%s: %s", container_id[:12], p, exc)
        return None
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


def _metadata_guest_upstream_http(row: Dict[str, Any], guest_port: int) -> Optional[str]:
    md = row.get("metadata") or {}
    routing = md.get("guest_routing")
    if not isinstance(routing, dict):
        return None
    p = str(max(1, min(65535, int(guest_port))))
    target = routing.get(p)
    if not isinstance(target, dict):
        return None
    upstream = str(target.get("upstream_http") or "").strip().rstrip("/")
    if upstream.startswith(("http://", "https://")):
        return upstream
    host = str(target.get("host") or "").strip()
    try:
        port = max(1, min(65535, int(target.get("port") or p)))
    except (TypeError, ValueError):
        port = int(p)
    if host:
        scheme = str(target.get("scheme") or "http").strip() or "http"
        return f"{scheme}://{host}:{port}"
    return None


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

    execution_for_row = getattr(manager, "_execution_for_row", None)
    execution = execution_for_row(row) if callable(execution_for_row) else manager.execution
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

    execution_for_row = getattr(manager, "_execution_for_row", None)
    execution = execution_for_row(row) if callable(execution_for_row) else manager.execution
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
    return _metadata_guest_upstream_http(row, p)
