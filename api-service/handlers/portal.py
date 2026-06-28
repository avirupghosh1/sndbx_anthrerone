"""Tenant portal UI embedded inside api-service."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import get_config
from database import Database

router = APIRouter(prefix="/portal", tags=["portal"])

_BASE_DIR = Path(__file__).resolve().parent.parent
_TEMPLATES = Jinja2Templates(directory=str(_BASE_DIR / "portal_templates"))
_SESSION_COOKIE = "portal_session"
_SESSION_TTL_DAYS = 30
_PBKDF2_ITERS = 240_000


def _db() -> Database:
    return Database(get_config().DATABASE_PATH)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _session_secret() -> str:
    cfg = get_config()
    return (os.getenv("PORTAL_SESSION_SECRET") or getattr(cfg, "API_KEY", "") or "dev-secret").strip()


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


def _sign_session(client_id: str) -> str:
    expires_at = int((_utc_now() + timedelta(days=_SESSION_TTL_DAYS)).timestamp())
    payload = f"{client_id}:{expires_at}"
    sig = hmac.new(_session_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
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


def _current_client(request: Request) -> Optional[dict]:
    token = request.cookies.get(_SESSION_COOKIE)
    if not token:
        return None
    client_id = _read_session(token)
    if not client_id:
        return None
    return _db().get_client(client_id)


def _issue_session(response: RedirectResponse, client_id: str) -> None:
    response.set_cookie(
        _SESSION_COOKIE,
        _sign_session(client_id),
        httponly=True,
        samesite="lax",
        secure=False,
        max_age=_SESSION_TTL_DAYS * 24 * 3600,
        path="/",
    )


def _clear_session(response: RedirectResponse) -> None:
    response.delete_cookie(_SESSION_COOKIE, path="/")


def _hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _new_api_key_value() -> str:
    return f"sbx_{secrets.token_urlsafe(24)}"


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


def _nav(active: str) -> list[dict]:
    return [
        {"label": "Sandboxes", "href": "/portal/sandboxes", "active": active == "sandboxes"},
        {"label": "Templates", "href": "/portal/templates", "active": active == "templates"},
        {"label": "API Keys", "href": "/portal/api-keys", "active": active == "api_keys"},
    ]


def _portal_context(
    request: Request,
    client: dict,
    *,
    active: str,
    template_tab: str = "list",
    new_api_key: Optional[str] = None,
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
            "created_at": row.get("created_at") or "",
            "created_at_display": _format_timestamp(row.get("created_at")),
            "completed_at": row.get("completed_at") or "",
            "completed_at_display": _format_timestamp(row.get("completed_at")),
            "build_log": row.get("build_log") or "",
            "error_text": row.get("error_text") or "",
        }
        for row in builds
    ]
    return {
        "request": request,
        "client": client,
        "nav_items": _nav(active),
        "active_section": active,
        "template_tab": template_tab,
        "sandboxes": sandbox_rows,
        "templates_rows": template_rows,
        "build_rows": build_rows,
        "api_keys": key_rows,
        "new_api_key": new_api_key,
        "running_count": sum(1 for row in sandbox_rows if row["state"] == "running"),
        "template_count": len(template_rows),
        "build_count": len(build_rows),
    }


@router.get("", response_class=HTMLResponse)
async def portal_root(request: Request):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    return RedirectResponse("/portal/templates", status_code=303)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    if _require_client(request):
        return RedirectResponse("/portal/templates", status_code=303)
    return _TEMPLATES.TemplateResponse("portal_auth.html", {"request": request, "mode": "login", "error": error})


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, error: str = ""):
    if _require_client(request):
        return RedirectResponse("/portal/templates", status_code=303)
    return _TEMPLATES.TemplateResponse("portal_auth.html", {"request": request, "mode": "register", "error": error})


@router.post("/register")
async def register(request: Request, email: str = Form(...), password: str = Form(...), display_name: str = Form("")):
    normalized = email.strip().lower()
    db = _db()
    if not normalized or "@" not in normalized:
        return _TEMPLATES.TemplateResponse(
            "portal_auth.html",
            {"request": request, "mode": "register", "error": "Enter a valid email address."},
            status_code=400,
        )
    if len(password) < 8:
        return _TEMPLATES.TemplateResponse(
            "portal_auth.html",
            {"request": request, "mode": "register", "error": "Password must be at least 8 characters."},
            status_code=400,
        )
    if db.get_client_by_email(normalized):
        return _TEMPLATES.TemplateResponse(
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
    response = RedirectResponse("/portal/templates", status_code=303)
    _issue_session(response, client["client_id"])
    return response


@router.post("/login")
async def login(request: Request, email: str = Form(...), password: str = Form(...)):
    client = _db().get_client_by_email(email.strip().lower())
    if not client or not client.get("password_hash") or not _verify_password(password, client["password_hash"]):
        return _TEMPLATES.TemplateResponse(
            "portal_auth.html",
            {"request": request, "mode": "login", "error": "Invalid email or password."},
            status_code=401,
        )
    response = RedirectResponse("/portal/templates", status_code=303)
    _issue_session(response, client["client_id"])
    return response


@router.post("/logout")
async def logout():
    response = RedirectResponse("/portal/login", status_code=303)
    _clear_session(response)
    return response


@router.get("/sandboxes", response_class=HTMLResponse)
async def sandboxes_page(request: Request):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    return _TEMPLATES.TemplateResponse(
        "portal_shell.html",
        _portal_context(request, client, active="sandboxes"),
    )


@router.get("/templates", response_class=HTMLResponse)
async def templates_page(request: Request):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    return _TEMPLATES.TemplateResponse(
        "portal_shell.html",
        _portal_context(request, client, active="templates", template_tab="list"),
    )


@router.get("/templates/builds", response_class=HTMLResponse)
async def template_builds_page(request: Request):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    return _TEMPLATES.TemplateResponse(
        "portal_shell.html",
        _portal_context(request, client, active="templates", template_tab="builds"),
    )


@router.get("/api-keys", response_class=HTMLResponse)
async def api_keys_page(request: Request):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    return _TEMPLATES.TemplateResponse(
        "portal_shell.html",
        _portal_context(request, client, active="api_keys"),
    )


@router.post("/api-keys", response_class=HTMLResponse)
async def create_api_key(request: Request, name: str = Form(...)):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    api_key_value = _new_api_key_value()
    _db().create_api_key(
        key_id=f"key-{secrets.token_hex(8)}",
        client_id=client["client_id"],
        name=(name.strip() or "default"),
        key_prefix=api_key_value[:8],
        key_hash=_hash_api_key(api_key_value),
    )
    return _TEMPLATES.TemplateResponse(
        "portal_shell.html",
        _portal_context(request, client, active="api_keys", new_api_key=api_key_value),
    )


@router.post("/api-keys/{key_id}/revoke")
async def revoke_api_key(request: Request, key_id: str):
    client = _require_client(request)
    if not client:
        return RedirectResponse("/portal/login", status_code=303)
    _db().revoke_api_key(key_id, client["client_id"])
    return RedirectResponse("/portal/api-keys", status_code=303)
