"""Authentication and tenant authorization helpers."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from functools import lru_cache
from threading import Lock
from typing import Optional

from fastapi import HTTPException, Request, status

from config import get_config
from database import Database

_BOOTSTRAP_LOCK = Lock()
_BOOTSTRAP_CLIENT_ID = "bootstrap-local-client"
_BOOTSTRAP_EMAIL = "bootstrap@local.invalid"
_PASSWORD_DISABLED_HASH = "!"


@dataclass(frozen=True)
class ApiKeyPrincipal:
    client_id: str
    key_id: str
    key_name: str
    key_prefix: str
    email: str
    display_name: str
    is_active: bool


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def api_key_prefix(api_key: str) -> str:
    raw = (api_key or "").strip()
    if len(raw) <= 8:
        return raw
    return raw[:8]


@lru_cache(maxsize=4)
def _db_for_url(database_url: str) -> Database:
    return Database(database_url)


def _db() -> Database:
    return _db_for_url(get_config().DATABASE_URL)


def _internal_expected_key() -> str:
    cfg = get_config()
    return (
        os.getenv("INTERNAL_API_KEY")
        or os.getenv("CONTROL_PLANE_API_KEY")
        or getattr(cfg, "RUNTIME_GATEWAY_API_KEY", "")
        or ""
    ).strip()


def ensure_bootstrap_client_and_key() -> Optional[dict]:
    cfg = get_config()
    bootstrap_key = (getattr(cfg, "API_KEY", "") or "").strip()
    if not bootstrap_key:
        return None
    db = _db()
    hashed = hash_api_key(bootstrap_key)
    existing = db.get_api_key_principal(hashed)
    if existing:
        return existing
    with _BOOTSTRAP_LOCK:
        existing = db.get_api_key_principal(hashed)
        if existing:
            return existing
        client = db.get_client(_BOOTSTRAP_CLIENT_ID)
        if not client:
            db.create_client(
                client_id=_BOOTSTRAP_CLIENT_ID,
                email=_BOOTSTRAP_EMAIL,
                password_hash=_PASSWORD_DISABLED_HASH,
                display_name="Local Bootstrap",
            )
        return db.create_api_key(
            key_id=f"key-{secrets.token_hex(8)}",
            client_id=_BOOTSTRAP_CLIENT_ID,
            name="bootstrap",
            key_prefix=api_key_prefix(bootstrap_key),
            key_hash=hashed,
        )


def validate_internal_api_key(request: Request) -> str:
    api_key = (request.headers.get("X-API-Key") or "").strip()
    expected = _internal_expected_key()
    if not expected or not api_key or not hmac.compare_digest(api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal API key",
        )
    return api_key


async def validate_api_key(request: Request) -> ApiKeyPrincipal:
    """Extract and validate a client API key from request headers."""
    api_key = (request.headers.get("X-API-Key") or "").strip()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-API-Key header required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    ensure_bootstrap_client_and_key()
    db = _db()
    row = db.get_api_key_principal(hash_api_key(api_key))
    if not row or row.get("revoked_at"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not row.get("is_active", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Client is disabled",
        )
    db.touch_api_key_used(str(row["key_id"]))
    return ApiKeyPrincipal(
        client_id=str(row["client_id"]),
        key_id=str(row["key_id"]),
        key_name=str(row.get("name") or ""),
        key_prefix=str(row.get("key_prefix") or ""),
        email=str(row.get("email") or ""),
        display_name=str(row.get("display_name") or ""),
        is_active=bool(row.get("is_active")),
    )


def add_api_key(key: str) -> None:
    """Backwards-compatible helper used by local tests / scripts."""
    raw = (key or "").strip()
    if not raw:
        return
    ensure_bootstrap_client_and_key()
    db = _db()
    hashed = hash_api_key(raw)
    if db.get_api_key_principal(hashed):
        return
    db.create_api_key(
        key_id=f"key-{secrets.token_hex(8)}",
        client_id=_BOOTSTRAP_CLIENT_ID,
        name="legacy-added",
        key_prefix=api_key_prefix(raw),
        key_hash=hashed,
    )


def remove_api_key(key: str) -> None:
    """Backwards-compatible helper used by local tests / scripts."""
    raw = (key or "").strip()
    if not raw:
        return
    db = _db()
    row = db.get_api_key_principal(hash_api_key(raw))
    if not row:
        return
    db.revoke_api_key(str(row["key_id"]), str(row["client_id"]))
