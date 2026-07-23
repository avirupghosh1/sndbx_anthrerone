"""Tenant portal UI embedded inside api-service."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import get_config
from database import Database
from middleware import admin_api_key_is_valid
from template_build_progress import derive_template_build_progress, parse_build_log_lines

from . import admin_observability

router = APIRouter(prefix="/portal", tags=["portal"])

_BASE_DIR = Path(__file__).resolve().parent.parent
_TEMPLATES = Jinja2Templates(directory=str(_BASE_DIR / "portal_templates"))
_SESSION_COOKIE = "portal_session"
_ADMIN_SESSION_COOKIE = "portal_admin_session"
_CSRF_COOKIE = "portal_csrf"
_CSRF_TTL_SECONDS = 4 * 3600
_PBKDF2_ITERS = 240_000
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMITS: dict[str, list[float]] = {}


def _clip_label(value: object, head: int = 14, tail: int = 6) -> str:
    raw = str(value if value not in (None, "") else "-")
    if len(raw) <= head + tail + 3:
        return raw
    visible_head = max(6, int(head))
    return f"{raw[:visible_head]}...{raw[-max(0, int(tail)):]}" if tail else f"{raw[:visible_head]}..."


_TEMPLATES.env.filters["portal_clip"] = _clip_label


def _db() -> Database:
    cfg = get_config()
    return Database(
        cfg.DATABASE_URL,
        database_type=getattr(cfg, "DATABASE_TYPE", ""),
        database_username=getattr(cfg, "DATABASE_USERNAME", ""),
        database_password=getattr(cfg, "DATABASE_PASSWORD", ""),
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _session_secret() -> str:
    cfg = get_config()
    return (getattr(cfg, "PORTAL_SESSION_SECRET", "") or os.getenv("PORTAL_SESSION_SECRET") or "").strip()


def _session_ttl_seconds() -> int:
    return int(getattr(get_config(), "PORTAL_SESSION_TTL_HOURS", 12) or 12) * 3600


def _request_scheme(request: Optional[Request]) -> str:
    if not request:
        return ""
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
    if forwarded_proto:
        return forwarded_proto
    forwarded = request.headers.get("forwarded") or ""
    for part in forwarded.split(";"):
        key, sep, value = part.strip().partition("=")
        if sep and key.lower() == "proto":
            return value.strip('"').lower()
    return (request.url.scheme or "").strip().lower()


def _cookie_secure(request: Optional[Request] = None) -> bool:
    explicit = os.getenv("PORTAL_SESSION_COOKIE_SECURE")
    if explicit is not None and explicit.strip() != "":
        return bool(getattr(get_config(), "PORTAL_SESSION_COOKIE_SECURE", False))
    scheme = _request_scheme(request)
    if scheme:
        return scheme in {"https", "wss"}
    return bool(getattr(get_config(), "PORTAL_SESSION_COOKIE_SECURE", False))


def _client_ip(request: Request) -> str:
    forwarded = (request.headers.get("x-forwarded-for") or "").split(",", 1)[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else "unknown"


def _rate_key(request: Request, action: str, subject: str = "") -> str:
    return f"{action}:{_client_ip(request)}:{subject.strip().lower()}"


def _rate_limit_hit(key: str, *, limit: int, window_seconds: int) -> bool:
    now = time.time()
    cutoff = now - window_seconds
    with _RATE_LIMIT_LOCK:
        events = [stamp for stamp in _RATE_LIMITS.get(key, []) if stamp >= cutoff]
        events.append(now)
        _RATE_LIMITS[key] = events
        return len(events) > limit


def _rate_limit_clear(key: str) -> None:
    with _RATE_LIMIT_LOCK:
        _RATE_LIMITS.pop(key, None)


def _sign_value(payload: str) -> str:
    return hmac.new(_session_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _new_csrf_token() -> str:
    expires_at = int(time.time()) + _CSRF_TTL_SECONDS
    payload = f"{secrets.token_urlsafe(32)}:{expires_at}"
    return base64.urlsafe_b64encode(f"{payload}:{_sign_value(payload)}".encode("utf-8")).decode("ascii")


def _verify_csrf_token(token: str) -> bool:
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        nonce, expires_at_text, sig = decoded.split(":", 2)
    except Exception:
        return False
    payload = f"{nonce}:{expires_at_text}"
    if not hmac.compare_digest(sig, _sign_value(payload)):
        return False
    try:
        return int(expires_at_text) >= int(time.time())
    except ValueError:
        return False


def _csrf_token_for_request(request: Request) -> str:
    token = request.cookies.get(_CSRF_COOKIE, "")
    if token and _verify_csrf_token(token):
        return token
    return _new_csrf_token()


def _require_csrf(request: Request, submitted_token: str) -> None:
    cookie_token = request.cookies.get(_CSRF_COOKIE, "")
    if (
        not submitted_token
        or not cookie_token
        or not hmac.compare_digest(submitted_token, cookie_token)
        or not _verify_csrf_token(cookie_token)
    ):
        raise HTTPException(status_code=403, detail="Invalid CSRF token")


def _harden_response(response, *, request: Optional[Request] = None, csrf_token: Optional[str] = None):
    headers = {
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "same-origin",
        "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
        "Content-Security-Policy": (
            "default-src 'self'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'; "
            "object-src 'none'; img-src 'self' data:; style-src 'self'; script-src 'self'"
        ),
    }
    for name, value in headers.items():
        if name not in response.headers:
            response.headers[name] = value
    if csrf_token:
        response.set_cookie(
            _CSRF_COOKIE,
            csrf_token,
            httponly=True,
            samesite="strict",
            secure=_cookie_secure(request),
            max_age=_CSRF_TTL_SECONDS,
            path="/",
        )
    return response


def _json_response(payload: dict, *, status_code: int = 200):
    return _harden_response(JSONResponse(payload, status_code=status_code))


def _template_response(request: Request, name: str, context: dict, *, status_code: int = 200):
    csrf_token = _csrf_token_for_request(request)
    render_context = {
        **context,
        "csrf_token": csrf_token,
        "registration_enabled": bool(getattr(get_config(), "PORTAL_REGISTRATION_ENABLED", True)),
        "admin_login_enabled": bool(getattr(get_config(), "ADMIN_API_KEY", "")),
        "password_min_length": 12 if bool(getattr(get_config(), "IS_PRODUCTION", False)) else 8,
    }
    return _harden_response(
        _TEMPLATES.TemplateResponse(
            request,
            name,
            render_context,
            status_code=status_code,
        ),
        request=request,
        csrf_token=csrf_token,
    )


def _redirect(request: Request, url: str, *, status_code: int = 303):
    return _harden_response(
        RedirectResponse(url, status_code=status_code),
        request=request,
        csrf_token=_csrf_token_for_request(request),
    )


def _hash_password(password: str, *, salt: Optional[str] = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        _PBKDF2_ITERS,
    ).hex()
    return f"pbkdf2_sha256${_PBKDF2_ITERS}${salt}${digest}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        algo, iter_text, salt, digest = encoded.split("$", 3)
    except ValueError:
        return False
    if algo != "pbkdf2_sha256":
        return False
    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        int(iter_text),
    ).hex()
    return hmac.compare_digest(candidate, digest)


def _password_error(password: str) -> Optional[str]:
    minimum = 12 if bool(getattr(get_config(), "IS_PRODUCTION", False)) else 8
    if len(password) < minimum:
        return f"Password must be at least {minimum} characters."
    if bool(getattr(get_config(), "IS_PRODUCTION", False)):
        classes = sum(
            bool(check)
            for check in (
                any(ch.islower() for ch in password),
                any(ch.isupper() for ch in password),
                any(ch.isdigit() for ch in password),
                any(not ch.isalnum() for ch in password),
            )
        )
        if classes < 3:
            return "Password must include at least three of: lowercase, uppercase, number, symbol."
    return None


def _sign_session(client_id: str) -> str:
    expires_at = int((_utc_now() + timedelta(seconds=_session_ttl_seconds())).timestamp())
    payload = f"{client_id}:{expires_at}"
    sig = _sign_value(payload)
    return base64.urlsafe_b64encode(f"{payload}:{sig}".encode("utf-8")).decode("ascii")


def _admin_key_fingerprint() -> str:
    raw = (getattr(get_config(), "ADMIN_API_KEY", "") or "").strip()
    if not raw:
        return ""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _sign_admin_session(client_id: str) -> str:
    expires_at = int((_utc_now() + timedelta(seconds=_session_ttl_seconds())).timestamp())
    fingerprint = _admin_key_fingerprint()
    payload = f"{client_id}:{fingerprint}:{expires_at}"
    sig = _sign_value(payload)
    return base64.urlsafe_b64encode(f"{payload}:{sig}".encode("utf-8")).decode("ascii")


def _read_session(token: str) -> Optional[str]:
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        client_id, expires_at_text, sig = decoded.split(":", 2)
    except Exception:
        return None
    payload = f"{client_id}:{expires_at_text}"
    expected = hmac.new(_session_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    if int(expires_at_text) < int(_utc_now().timestamp()):
        return None
    return client_id


def _read_admin_session(token: str, expected_client_id: str) -> bool:
    try:
        decoded = base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        client_id, fingerprint, expires_at_text, sig = decoded.split(":", 3)
    except Exception:
        return False
    if client_id != expected_client_id:
        return False
    if fingerprint != _admin_key_fingerprint() or not fingerprint:
        return False
    payload = f"{client_id}:{fingerprint}:{expires_at_text}"
    if not hmac.compare_digest(sig, _sign_value(payload)):
        return False
    try:
        return int(expires_at_text) >= int(_utc_now().timestamp())
    except ValueError:
        return False


def _current_client(request: Request) -> Optional[dict]:
    token = request.cookies.get(_SESSION_COOKIE)
    if not token:
        return None
    client_id = _read_session(token)
    if not client_id:
        return None
    return _db().get_client(client_id)


def _request_is_admin(request: Request, client: dict) -> bool:
    token = request.cookies.get(_ADMIN_SESSION_COOKIE, "")
    client_id = str((client or {}).get("client_id") or "")
    return bool(client_id and token and _read_admin_session(token, client_id))


def _issue_session(response: RedirectResponse, request: Request, client_id: str) -> None:
    response.set_cookie(
        _SESSION_COOKIE,
        _sign_session(client_id),
        httponly=True,
        samesite="strict",
        secure=_cookie_secure(request),
        max_age=_session_ttl_seconds(),
        path="/",
    )


def _issue_admin_session(response: RedirectResponse, request: Request, client_id: str) -> None:
    response.set_cookie(
        _ADMIN_SESSION_COOKIE,
        _sign_admin_session(client_id),
        httponly=True,
        samesite="strict",
        secure=_cookie_secure(request),
        max_age=_session_ttl_seconds(),
        path="/",
    )


def _clear_session(response: RedirectResponse) -> None:
    response.delete_cookie(_SESSION_COOKIE, path="/")
    response.delete_cookie(_ADMIN_SESSION_COOKIE, path="/")
    response.delete_cookie(_CSRF_COOKIE, path="/")


def _hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _new_api_key_value() -> str:
    return f"sbx_{secrets.token_urlsafe(24)}"


def _normalize_api_key_name(name: str) -> str:
    return (name.strip() or "default")


def _api_key_name_exists(keys: list[dict], name: str) -> bool:
    normalized = name.casefold()
    return any(str(row.get("name") or "").strip().casefold() == normalized for row in keys)


def _ttl_remaining(lease_expires_at: Optional[str]) -> str:
    if not lease_expires_at:
        return "-"
    try:
        target = datetime.fromisoformat(lease_expires_at.replace("Z", "+00:00"))
    except ValueError:
        return "-"
    delta = target - _utc_now()
    if delta.total_seconds() <= 0:
        return "expired"
    mins, secs = divmod(int(delta.total_seconds()), 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours}h {mins}m"
    if mins:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def _format_timestamp(value: Optional[str]) -> str:
    if not value:
        return "-"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return dt.astimezone(timezone.utc).strftime("%d %b %Y, %H:%M:%S UTC")


def _short_id(value: Optional[str], *, head: int = 12, tail: int = 6) -> str:
    raw = (value or "").strip()
    if len(raw) <= head + tail + 1:
        return raw or "-"
    return f"{raw[:head]}...{raw[-tail:]}"


def _debug_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True)


def _require_client(request: Request) -> Optional[dict]:
    client = _current_client(request)
    if not client or not client.get("is_active"):
        return None
    return client


def _nav(active: str, *, admin_enabled: bool = False) -> list[dict]:
    items = [
        {"label": "Sandboxes", "href": "/portal/sandboxes", "active": active == "sandboxes"},
        {"label": "Templates", "href": "/portal/templates", "active": active == "templates"},
        {"label": "API Keys", "href": "/portal/api-keys", "active": active == "api_keys"},
    ]
    if admin_enabled:
        items.append(
            {
                "label": "Observability",
                "href": "/portal/admin/observability",
                "active": active == "observability",
            }
        )
    return items


def _format_bytes(value: object) -> str:
    try:
        raw = float(value or 0)
    except Exception:
        raw = 0.0
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    while raw >= 1024 and idx < len(units) - 1:
        raw /= 1024
        idx += 1
    if idx == 0:
        return f"{int(raw)} {units[idx]}"
    return f"{raw:.1f} {units[idx]}"


def _sparkline_points(samples: list[dict], metric_name: str) -> str:
    values: list[float] = []
    for sample in samples:
        metrics = sample.get("metrics") if isinstance(sample, dict) else {}
        metrics = metrics if isinstance(metrics, dict) else {}
        try:
            values.append(float(metrics.get(metric_name) or 0))
        except Exception:
            values.append(0.0)
    if not values:
        values = [0.0, 0.0]
    if len(values) == 1:
        values = [values[0], values[0]]
    maximum = max(values) or 1.0
    points: list[str] = []
    last = len(values) - 1
    for idx, value in enumerate(values):
        x = (idx / last) * 100.0
        y = 26.0 - ((value / maximum) * 22.0)
        points.append(f"{x:.1f},{max(2.0, min(26.0, y)):.1f}")
    return " ".join(points)


def _status_class(status: object) -> str:
    raw = str(status or "").strip().lower()
    if raw == "healthy":
        return "success"
    if raw in {"missing", "lost", "error", "failed", "rebuilding"}:
        return "danger"
    if raw in {"degraded", "warning"}:
        return "neutral"
    return ""


def _event_matches_search(event: dict, query: str) -> bool:
    needle = query.strip().lower()
    if not needle:
        return True
    haystack = json.dumps(event, sort_keys=True, default=str).lower()
    return needle in haystack


def _observability_context(
    *,
    tab: str = "gateways",
    event_search: str = "",
    event_severity: str = "",
    event_category: str = "",
    event_error_only: bool = False,
    event_page: int = 1,
) -> dict:
    summary = admin_observability.get_summary_payload()
    gateways = admin_observability.get_gateways_payload()
    warm_pools = admin_observability.get_warm_pools_payload()
    templates = admin_observability.get_templates_images_payload()
    event_limit = 40
    event_offset = max(0, int(event_page or 1) - 1) * event_limit
    severity = "error" if event_error_only else (event_severity.strip() or None)
    events_payload = admin_observability.get_events_payload(
        limit=event_limit,
        offset=event_offset,
        severity=severity,
        category=event_category.strip() or None,
    )
    events = [event for event in events_payload["events"] if _event_matches_search(event, event_search)]

    gateway_rows = []
    for row in gateways.get("gateways", []):
        history = row.get("history") if isinstance(row.get("history"), list) else []
        sandbox_rows = []
        raw_sandboxes = row.get("sandboxes")
        for sandbox in raw_sandboxes if isinstance(raw_sandboxes, list) else []:
            is_warm = bool(sandbox.get("is_warm_pool"))
            meta = dict(sandbox.get("metadata") or {})
            sandbox_rows.append(
                {
                    "sandbox_id": sandbox.get("sandbox_id") or "",
                    "sandbox_id_short": _short_id(sandbox.get("sandbox_id"), head=12, tail=6),
                    "template_id": sandbox.get("template_id") or "",
                    "state": sandbox.get("state") or "",
                    "runtime": sandbox.get("runtime") or "",
                    "cpu_limit": sandbox.get("cpu_limit") or "-",
                    "memory_limit": sandbox.get("memory_limit") or "-",
                    "created_at": sandbox.get("created_at") or "",
                    "created_at_display": _format_timestamp(sandbox.get("created_at")),
                    "lease_expires_at": sandbox.get("lease_expires_at") or "",
                    "ttl_remaining": _ttl_remaining(sandbox.get("lease_expires_at")),
                    "warm_pool_key": sandbox.get("warm_pool_key") or "",
                    "owner_client_id": sandbox.get("owner_client_id") or "",
                    "guest_ports": meta.get("guest_ports") or [],
                    "flag": "WARM" if is_warm else "USING",
                    "flag_class": "neutral" if is_warm else "success",
                    "placement": sandbox.get("warm_pool_key") if is_warm else sandbox.get("owner_client_id"),
                }
            )
        gateway_rows.append(
            {
                **row,
                "status_class": _status_class(row.get("status")),
                "cpu_sparkline": _sparkline_points(history, "cpu_millicores"),
                "memory_sparkline": _sparkline_points(history, "memory_bytes"),
                "warm_sparkline": _sparkline_points(history, "warm_sandbox_count"),
                "deletion_sparkline": _sparkline_points(history, "deletion_cost"),
                "memory_display": _format_bytes(row.get("memory_bytes")),
                "disk_display": f"{_format_bytes(row.get('disk_used_bytes'))} / {_format_bytes(row.get('disk_total_bytes'))}",
                "disk_percent": int(max(0.0, min(1.0, float(row.get("disk_used_ratio") or 0.0))) * 100),
                "sandboxes": sandbox_rows,
            }
        )

    warm_rows = []
    for row in warm_pools.get("warm_pools", []):
        history = row.get("history") if isinstance(row.get("history"), list) else []
        warm_rows.append(
            {
                **row,
                "status_class": _status_class(row.get("status")),
                "ready_sparkline": _sparkline_points(history, "ready_count"),
                "deficit_sparkline": _sparkline_points(history, "deficit"),
            }
        )

    template_rows = [
        {**row, "status_class": _status_class(row.get("status"))}
        for row in templates.get("templates", [])
    ]
    event_rows = [
        {
            **event,
            "status_class": _status_class("error" if event.get("severity") == "error" else event.get("severity")),
            "timestamp_display": _format_timestamp(event.get("timestamp")),
            "metadata_pretty": _debug_json(event.get("metadata") or {}),
        }
        for event in events
    ]
    return {
        "active_tab": tab,
        "summary": summary,
        "gateways": gateway_rows,
        "warm_pools": warm_rows,
        "templates": template_rows,
        "events": event_rows,
        "events_filters": {
            "search": event_search,
            "severity": event_severity,
            "category": event_category,
            "error_only": event_error_only,
            "page": max(1, int(event_page or 1)),
            "limit": event_limit,
            "offset": event_offset,
            "has_next": len(events_payload["events"]) >= event_limit,
        },
    }


def _portal_context(
    request: Request,
    client: dict,
    *,
    active: str,
    template_tab: str = "list",
    new_api_key: Optional[str] = None,
    api_key_error: Optional[str] = None,
) -> dict:
    db = _db()
    sandboxes = db.list_sandboxes(limit=500, offset=0, owner_client_id=client["client_id"])
    builds = db.list_template_builds_for_client(client["client_id"], limit=100)
    templates = db.list_sandbox_templates(client["client_id"])
    keys = db.list_api_keys_for_client(client["client_id"], include_revoked=True)
    key_rows = [
        {
            **row,
            "created_at_display": _format_timestamp(row.get("created_at")),
            "last_used_at_display": _format_timestamp(row.get("last_used_at")),
        }
        for row in keys
    ]
    sandbox_rows = []
    for row in sandboxes:
        meta = dict(row.get("metadata") or {})
        sandbox_rows.append(
            {
                "sandbox_id": row["sandbox_id"],
                "template_id": row.get("template_id") or "",
                "state": row.get("state") or "",
                "runtime": row.get("runtime") or "",
                "cpu_limit": row.get("cpu_limit") or "-",
                "memory_limit": row.get("memory_limit") or "-",
                "disk_limit": row.get("disk_limit") or meta.get("disk_limit") or "-",
                "created_at": row.get("created_at") or "",
                "created_at_display": _format_timestamp(row.get("created_at")),
                "updated_at": row.get("updated_at") or "",
                "updated_at_display": _format_timestamp(row.get("updated_at")),
                "lease_expires_at": row.get("lease_expires_at") or "",
                "lease_expires_at_display": _format_timestamp(row.get("lease_expires_at")),
                "ttl_remaining": _ttl_remaining(row.get("lease_expires_at")),
                "container_id": row.get("container_id") or "",
                "container_id_short": _short_id(row.get("container_id"), head=16, tail=8),
                "guest_ports": meta.get("guest_ports") or [],
                "allow_public_traffic": bool(meta.get("allow_public_traffic")),
                "metadata_pretty": _debug_json(meta),
            }
        )
    template_rows = [
        {
            "template_id": row.get("template_alias") or row.get("template_id") or "",
            "internal_id": row.get("template_id") or "",
            "base_image": row.get("base_image") or "",
            "start_cmd": row.get("start_cmd") or "",
            "ready_cmd": row.get("ready_cmd") or "",
            "warm_snapshot_image": row.get("warm_snapshot_image") or "",
            "warm_snapshot_image_short": _short_id(row.get("warm_snapshot_image"), head=18, tail=10),
            "registry_image_ref": row.get("registry_image_ref") or "",
            "build_error": row.get("build_error") or "",
            "created_at": row.get("created_at") or "",
            "created_at_display": _format_timestamp(row.get("created_at")),
            "updated_at": row.get("updated_at") or "",
            "updated_at_display": _format_timestamp(row.get("updated_at")),
        }
        for row in templates
    ]
    build_rows = [
        {
            "build_id": row["build_id"],
            "template_id": row.get("template_alias") or row.get("template_id") or "",
            "requested_mode": row.get("requested_mode") or "",
            "effective_mode": row.get("effective_mode") or "",
            "status": row.get("status") or "",
            "image_tag": row.get("image_tag") or "",
            "image_tag_short": _short_id(row.get("image_tag"), head=18, tail=10),
            "registry_image_ref": row.get("registry_image_ref") or "",
            "created_at": row.get("created_at") or "",
            "created_at_display": _format_timestamp(row.get("created_at")),
            "completed_at": row.get("completed_at") or "",
            "completed_at_display": _format_timestamp(row.get("completed_at")),
            "build_log": row.get("build_log") or "",
            "error_text": row.get("error_text") or "",
            "progress": derive_template_build_progress(row),
        }
        for row in builds
    ]
    return {
        "request": request,
        "client": client,
        "nav_items": _nav(active, admin_enabled=_request_is_admin(request, client)),
        "active_section": active,
        "template_tab": template_tab,
        "sandboxes": sandbox_rows,
        "templates_rows": template_rows,
        "build_rows": build_rows,
        "api_keys": key_rows,
        "new_api_key": new_api_key,
        "api_key_error": api_key_error,
        "running_count": sum(1 for row in sandbox_rows if row["state"] == "running"),
        "template_count": len(template_rows),
        "build_count": len(build_rows),
    }


def _serialize_build_for_portal(row: dict) -> dict:
    progress = derive_template_build_progress(row)
    return {
        "build_id": row.get("build_id") or "",
        "template_id": row.get("template_alias") or row.get("template_id") or "",
        "internal_template_id": row.get("template_id") or "",
        "requested_mode": row.get("requested_mode") or "",
        "effective_mode": row.get("effective_mode") or "",
        "status": row.get("status") or "",
        "image_tag": row.get("image_tag") or "",
        "registry_image_ref": row.get("registry_image_ref") or "",
        "gateway_instance_id": row.get("gateway_instance_id") or "",
        "created_at": row.get("created_at") or "",
        "created_at_display": _format_timestamp(row.get("created_at")),
        "updated_at": row.get("updated_at") or "",
        "updated_at_display": _format_timestamp(row.get("updated_at")),
        "completed_at": row.get("completed_at") or "",
        "completed_at_display": _format_timestamp(row.get("completed_at")),
        "error_text": row.get("error_text") or "",
        "progress": {
            "percent": progress["percent"],
            "phase": progress["phase"],
            "latest_comment": progress["latest_comment"],
        },
        "log_lines": parse_build_log_lines(row.get("build_log") or ""),
    }


@router.get("", response_class=HTMLResponse)
async def portal_root(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    return _redirect(request, "/portal/templates")


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    if _require_client(request):
        return _redirect(request, "/portal/templates")
    return _template_response(request, "portal_auth.html", {"request": request, "mode": "login", "error": error})


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, error: str = ""):
    if not bool(getattr(get_config(), "PORTAL_REGISTRATION_ENABLED", True)):
        return _redirect(request, "/portal/login?error=Registration%20is%20disabled.")
    if _require_client(request):
        return _redirect(request, "/portal/templates")
    return _template_response(request, "portal_auth.html", {"request": request, "mode": "register", "error": error})


@router.post("/register")
async def register(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    display_name: str = Form(""),
    csrf_token: str = Form(""),
):
    _require_csrf(request, csrf_token)
    rate_key = _rate_key(request, "register", email)
    if _rate_limit_hit(rate_key, limit=5, window_seconds=30 * 60):
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "register", "error": "Too many registration attempts. Try again later."},
            status_code=429,
        )
    if not bool(getattr(get_config(), "PORTAL_REGISTRATION_ENABLED", True)):
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "login", "error": "Registration is disabled."},
            status_code=403,
        )
    normalized = email.strip().lower()
    db = _db()
    if not normalized or "@" not in normalized:
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "register", "error": "Enter a valid email address."},
            status_code=400,
        )
    password_error = _password_error(password)
    if password_error:
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "register", "error": password_error},
            status_code=400,
        )
    if db.get_client_by_email(normalized):
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "register", "error": "That email is already registered."},
            status_code=409,
        )
    client = db.create_client(
        client_id=f"cl-{secrets.token_hex(8)}",
        email=normalized,
        password_hash=_hash_password(password),
        display_name=display_name.strip(),
    )
    _rate_limit_clear(rate_key)
    response = _redirect(request, "/portal/templates")
    _issue_session(response, request, client["client_id"])
    return response


@router.post("/login")
async def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    csrf_token: str = Form(""),
    admin_api_key: str = Form(""),
):
    _require_csrf(request, csrf_token)
    normalized = email.strip().lower()
    rate_key = _rate_key(request, "login", normalized)
    if _rate_limit_hit(rate_key, limit=10, window_seconds=15 * 60):
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "login", "error": "Too many login attempts. Try again later."},
            status_code=429,
        )
    client = _db().get_client_by_email(normalized)
    if not client or not client.get("password_hash") or not _verify_password(password, client["password_hash"]):
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "login", "error": "Invalid email or password."},
            status_code=401,
        )
    admin_requested = bool((admin_api_key or "").strip())
    if admin_requested and not admin_api_key_is_valid(admin_api_key):
        return _template_response(
            request,
            "portal_auth.html",
            {"request": request, "mode": "login", "error": "Invalid admin API key."},
            status_code=403,
        )
    _rate_limit_clear(rate_key)
    response = _redirect(request, "/portal/admin/observability" if admin_requested else "/portal/templates")
    _issue_session(response, request, client["client_id"])
    if admin_requested:
        _issue_admin_session(response, request, client["client_id"])
    return response


@router.post("/admin/session")
async def admin_session(request: Request, admin_api_key: str = Form(...), csrf_token: str = Form("")):
    _require_csrf(request, csrf_token)
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    if not admin_api_key_is_valid(admin_api_key):
        raise HTTPException(status_code=403, detail="Invalid admin API key")
    response = _redirect(request, "/portal/admin/observability")
    _issue_admin_session(response, request, client["client_id"])
    return response


@router.post("/logout")
async def logout(request: Request, csrf_token: str = Form("")):
    _require_csrf(request, csrf_token)
    response = _redirect(request, "/portal/login")
    _clear_session(response)
    return response


@router.get("/sandboxes", response_class=HTMLResponse)
async def sandboxes_page(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    return _template_response(
        request,
        "portal_shell.html",
        _portal_context(request, client, active="sandboxes"),
    )


@router.get("/templates", response_class=HTMLResponse)
async def templates_page(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    return _template_response(
        request,
        "portal_shell.html",
        _portal_context(request, client, active="templates", template_tab="list"),
    )


@router.get("/templates/builds", response_class=HTMLResponse)
async def template_builds_page(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    return _template_response(
        request,
        "portal_shell.html",
        _portal_context(request, client, active="templates", template_tab="builds"),
    )


@router.get("/templates/builds/{build_id}.json")
async def template_build_json(request: Request, build_id: str):
    client = _require_client(request)
    if not client:
        return _json_response({"detail": "Authentication required"}, status_code=401)
    build = _db().get_template_build(build_id)
    if not build:
        return _json_response({"detail": "Build not found"}, status_code=404)
    if str(build.get("owner_client_id") or "") != str(client["client_id"]):
        return _json_response({"detail": "Build not found"}, status_code=404)
    return _json_response(_serialize_build_for_portal(build))


@router.get("/api-keys", response_class=HTMLResponse)
async def api_keys_page(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    return _template_response(
        request,
        "portal_shell.html",
        _portal_context(request, client, active="api_keys"),
    )


@router.get("/admin/observability", response_class=HTMLResponse)
async def admin_observability_page(request: Request):
    return _redirect(request, "/portal/admin/observability/gateways")


@router.get("/admin/observability/gateways", response_class=HTMLResponse)
async def admin_observability_gateways_page(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    if not _request_is_admin(request, client):
        raise HTTPException(status_code=403, detail="Admin access required")
    context = _portal_context(request, client, active="observability")
    context["observability"] = _observability_context(tab="gateways")
    return _template_response(request, "portal_shell.html", context)


@router.get("/admin/observability/health", response_class=HTMLResponse)
async def admin_observability_health_page(request: Request):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    if not _request_is_admin(request, client):
        raise HTTPException(status_code=403, detail="Admin access required")
    context = _portal_context(request, client, active="observability")
    context["observability"] = _observability_context(tab="health")
    return _template_response(request, "portal_shell.html", context)


@router.get("/admin/observability/events", response_class=HTMLResponse)
async def admin_observability_events_page(
    request: Request,
    q: str = "",
    severity: str = "",
    category: str = "",
    error_only: bool = False,
    page: int = Query(1, ge=1),
):
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    if not _request_is_admin(request, client):
        raise HTTPException(status_code=403, detail="Admin access required")
    context = _portal_context(request, client, active="observability")
    context["observability"] = _observability_context(
        tab="events",
        event_search=q,
        event_severity=severity,
        event_category=category,
        event_error_only=error_only,
        event_page=page,
    )
    return _template_response(request, "portal_shell.html", context)


@router.post("/api-keys", response_class=HTMLResponse)
async def create_api_key(request: Request, name: str = Form(...), csrf_token: str = Form("")):
    _require_csrf(request, csrf_token)
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    db = _db()
    key_name = _normalize_api_key_name(name)
    if _api_key_name_exists(db.list_api_keys_for_client(client["client_id"], include_revoked=True), key_name):
        return _template_response(
            request,
            "portal_shell.html",
            _portal_context(
                request,
                client,
                active="api_keys",
                api_key_error=f'An API key named "{key_name}" already exists.',
            ),
            status_code=400,
        )
    api_key_value = _new_api_key_value()
    db.create_api_key(
        key_id=f"key-{secrets.token_hex(8)}",
        client_id=client["client_id"],
        name=key_name,
        key_prefix=api_key_value[:8],
        key_hash=_hash_api_key(api_key_value),
    )
    return _template_response(
        request,
        "portal_shell.html",
        _portal_context(request, client, active="api_keys", new_api_key=api_key_value),
    )


@router.post("/api-keys/{key_id}/revoke")
async def revoke_api_key(request: Request, key_id: str, csrf_token: str = Form("")):
    _require_csrf(request, csrf_token)
    client = _require_client(request)
    if not client:
        return _redirect(request, "/portal/login")
    _db().revoke_api_key(key_id, client["client_id"])
    return _redirect(request, "/portal/api-keys")
