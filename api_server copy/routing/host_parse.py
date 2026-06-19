"""Format and parse E2B-style sandbox host authorities for data-plane URLs."""

from __future__ import annotations

from typing import Optional, Tuple


def format_sandbox_host(
    *,
    port: int,
    sandbox_id: str,
    sandbox_domain: str,
    debug: bool,
) -> str:
    p = int(port)
    sid = (sandbox_id or "").strip()
    if debug:
        return f"localhost:{p}"
    domain = (sandbox_domain or "").strip().lstrip(".")
    return f"{p}-{sid}.{domain}"


def format_sandbox_base_url(
    *,
    port: int,
    sandbox_id: str,
    sandbox_domain: str,
    debug: bool,
    scheme: str = "http",
    listen_port: Optional[int] = None,
) -> str:
    host = format_sandbox_host(
        port=port,
        sandbox_id=sandbox_id,
        sandbox_domain=sandbox_domain,
        debug=debug,
    )
    sch = scheme.rstrip(":/")
    default_port = 443 if sch in ("https", "wss") else 80
    lp = int(listen_port) if listen_port is not None else default_port
    if debug:
        if lp != default_port:
            return f"{sch}://127.0.0.1:{lp}"
        return f"{sch}://127.0.0.1"
    if lp != default_port:
        return f"{sch}://{host}:{lp}"
    return f"{sch}://{host}"


def parse_sandbox_host(
    host_header: str,
    *,
    sandbox_domain: str,
    debug: bool,
    sandbox_id_header: Optional[str] = None,
) -> Optional[Tuple[int, str]]:
    """Return ``(guest_port, sandbox_id)`` from a data-plane Host header."""
    raw = (host_header or "").strip()
    if not raw:
        return None

    first = raw.split(",")[0].strip()

    if debug:
        if ":" in first:
            h, _, p_s = first.rpartition(":")
            if p_s.isdigit() and h.lower() in ("localhost", "127.0.0.1"):
                sid = (sandbox_id_header or "").strip()
                if sid:
                    guest_port = int(p_s)
                    if 1 <= guest_port <= 65535:
                        return guest_port, sid

    authority = first
    if ":" in authority and not authority.startswith("["):
        host_part, _, port_str = authority.rpartition(":")
        if port_str.isdigit():
            authority = host_part

    domain = (sandbox_domain or "").strip().lstrip(".")
    suffix = f".{domain}" if domain else ""

    if suffix and authority.endswith(suffix):
        label = authority[: -len(suffix)]
        if "-" not in label:
            return None
        port_s, sid = label.split("-", 1)
        if not port_s.isdigit() or not sid:
            return None
        guest_port = int(port_s)
        if not (1 <= guest_port <= 65535):
            return None
        return guest_port, sid

    return None
