"""Parse E2B-style sandbox host authorities (``{port}-{sandbox_id}.{domain}``)."""

from __future__ import annotations

from shared.host_parse import format_sandbox_host, parse_sandbox_host

# Re-export under the proxy_service's original name for backward compat.
parse_ingress_host = parse_sandbox_host

__all__ = ["format_sandbox_host", "parse_ingress_host"]
