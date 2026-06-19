"""Resolve guest TCP ports for sandbox Pods/Services (no fixed agent port)."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


def _valid_port(value: Any) -> Optional[int]:
    try:
        p = int(value)
    except (TypeError, ValueError):
        return None
    if 1 <= p <= 65535:
        return p
    return None


def ports_from_metadata(metadata: Optional[Dict[str, Any]]) -> List[int]:
    """``metadata.guest_ports`` or ``metadata.port`` / ``metadata.guest_port``."""
    md = metadata or {}
    out: List[int] = []
    raw_list = md.get("guest_ports")
    if isinstance(raw_list, (list, tuple)):
        for item in raw_list:
            p = _valid_port(item)
            if p is not None:
                out.append(p)
    for key in ("port", "guest_port"):
        p = _valid_port(md.get(key))
        if p is not None:
            out.append(p)
    return out


def ports_from_template_env(template_row: Optional[Dict[str, Any]]) -> List[int]:
    """Template ``env`` keys ``PORT``, ``GUEST_PORT``, ``LISTEN_PORT`` (user-defined only)."""
    if not template_row:
        return []
    env = dict(template_row.get("env") or {})
    out: List[int] = []
    for key in ("PORT", "GUEST_PORT", "LISTEN_PORT"):
        p = _valid_port(env.get(key))
        if p is not None:
            out.append(p)
    return out


def merge_guest_ports(*groups: Iterable[int]) -> List[int]:
    seen: set[int] = set()
    out: List[int] = []
    for group in groups:
        for p in group:
            pi = _valid_port(p)
            if pi is None or pi in seen:
                continue
            seen.add(pi)
            out.append(pi)
    return sorted(out)


def resolve_guest_ports(
    *,
    metadata: Optional[Dict[str, Any]],
    template_row: Optional[Dict[str, Any]],
    config: Any,
    include_envd: bool = True,
) -> List[int]:
    """Ports exposed on the sandbox Pod/Service and registered for data-plane routing."""
    ports = merge_guest_ports(
        ports_from_metadata(metadata),
        ports_from_template_env(template_row),
    )
    if include_envd and bool(getattr(config, "ENVD_ALWAYS_ON", True)):
        envd_p = _valid_port(getattr(config, "ENVD_PORT", 49983))
        if envd_p is not None:
            ports = merge_guest_ports(ports, [envd_p])
    return ports
