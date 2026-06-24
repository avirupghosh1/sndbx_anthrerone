from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from host_parse import format_sandbox_host, parse_ingress_host


def test_parse_ingress_host_extracts_port_and_sandbox_id() -> None:
    parsed = parse_ingress_host(
        "49983-sb-abc123.sndbx.com",
        sandbox_domain="sndbx.com",
        debug=False,
    )

    assert parsed == (49983, "sb-abc123")


def test_parse_ingress_host_ignores_authority_port() -> None:
    parsed = parse_ingress_host(
        "8765-sb-abc123.sndbx.com:443",
        sandbox_domain="sndbx.com",
        debug=False,
    )

    assert parsed == (8765, "sb-abc123")


def test_parse_ingress_host_supports_local_debug_headers() -> None:
    parsed = parse_ingress_host(
        "localhost:18080",
        sandbox_domain="sndbx.com",
        debug=True,
        sandbox_id_header="sb-local",
        guest_port_header="49983",
    )

    assert parsed == (49983, "sb-local")


def test_format_sandbox_host_uses_wildcard_domain_in_production_mode() -> None:
    host = format_sandbox_host(
        port=8765,
        sandbox_id="sb-prod",
        sandbox_domain="sndbx.com",
        debug=False,
    )

    assert host == "8765-sb-prod.sndbx.com"
