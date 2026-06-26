from __future__ import annotations

import hmac

from starlette.requests import Request
from starlette.responses import Response

from config import get_config


def internal_api_key_valid(request: Request) -> bool:
    cfg = get_config()
    expected = (
        getattr(cfg, "INTERNAL_API_KEY", None)
        or getattr(cfg, "CONTROL_PLANE_API_KEY", None)
        or ""
    ).strip()
    if not expected:
        return False
    provided = (request.headers.get("X-API-Key") or "").strip()
    if not provided:
        return False
    return hmac.compare_digest(provided, expected)


def unauthorized_response(detail: str = "Invalid API key") -> Response:
    return Response(detail, status_code=401, media_type="text/plain")
