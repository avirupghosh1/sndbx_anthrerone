"""Authentication and tenant authorization helpers."""

from __future__ import annotations

import hashlib
import hmac
import base64
import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache
from threading import Lock
from typing import Any, Optional

from fastapi import HTTPException, Request, status

from async_runner import run_io
from config import get_config
from database import Database

_BOOTSTRAP_LOCK = Lock()
_API_KEY_CACHE_LOCK = Lock()
_API_KEY_AUTH_CACHE: dict[str, tuple[float, ApiKeyPrincipal]] = {}
_BOOTSTRAP_CLIENT_ID = "bootstrap-local-client"
_BOOTSTRAP_EMAIL = "bootstrap@local.invalid"
_PASSWORD_DISABLED_HASH = "!"
_API_KEY_CACHE_MAX_ENTRIES = 4096


@dataclass(frozen=True)
class ApiKeyPrincipal:
    client_id: str
    key_id: str
    key_name: str
    key_prefix: str
    email: str
    display_name: str
    is_active: bool
    auth_type: str = "api_key"
    token_id: str = ""
    expires_at: Optional[int] = None


class ClientAuthError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def api_key_prefix(api_key: str) -> str:
    raw = (api_key or "").strip()
    if len(raw) <= 8:
        return raw
    return raw[:8]


def _principal_from_row(
    row: dict,
    *,
    auth_type: str,
    token_id: str = "",
    expires_at: Optional[int] = None,
) -> ApiKeyPrincipal:
    return ApiKeyPrincipal(
        client_id=str(row["client_id"]),
        key_id=str(row.get("key_id") or ""),
        key_name=str(row.get("name") or ""),
        key_prefix=str(row.get("key_prefix") or ""),
        email=str(row.get("email") or ""),
        display_name=str(row.get("display_name") or ""),
        is_active=bool(row.get("is_active")),
        auth_type=auth_type,
        token_id=token_id,
        expires_at=expires_at,
    )


def _api_key_cache_ttl_seconds() -> float:
    cfg = get_config()
    try:
        return max(0.0, float(getattr(cfg, "AUTH_API_KEY_CACHE_TTL_SEC", 30.0) or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _get_cached_api_key_principal(key_hash: str) -> Optional[ApiKeyPrincipal]:
    ttl = _api_key_cache_ttl_seconds()
    if ttl <= 0:
        return None
    now = time.monotonic()
    with _API_KEY_CACHE_LOCK:
        cached = _API_KEY_AUTH_CACHE.get(key_hash)
        if not cached:
            return None
        expires_at, principal = cached
        if expires_at > now:
            return principal
        _API_KEY_AUTH_CACHE.pop(key_hash, None)
    return None


def _set_cached_api_key_principal(key_hash: str, principal: ApiKeyPrincipal) -> None:
    ttl = _api_key_cache_ttl_seconds()
    if ttl <= 0:
        return
    now = time.monotonic()
    with _API_KEY_CACHE_LOCK:
        if len(_API_KEY_AUTH_CACHE) >= _API_KEY_CACHE_MAX_ENTRIES:
            expired = [key for key, (expires_at, _) in _API_KEY_AUTH_CACHE.items() if expires_at <= now]
            for key in expired:
                _API_KEY_AUTH_CACHE.pop(key, None)
            if len(_API_KEY_AUTH_CACHE) >= _API_KEY_CACHE_MAX_ENTRIES:
                _API_KEY_AUTH_CACHE.clear()
        _API_KEY_AUTH_CACHE[key_hash] = (now + ttl, principal)


def clear_api_key_auth_cache() -> None:
    with _API_KEY_CACHE_LOCK:
        _API_KEY_AUTH_CACHE.clear()


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    raw = (data or "").encode("ascii")
    raw += b"=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode(raw)


def _json_for_jwt(data: dict[str, Any]) -> bytes:
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _looks_like_jwt(token: str) -> bool:
    return token.count(".") == 2


def _jwt_signature(signing_input: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), signing_input.encode("ascii"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def _jwt_secrets() -> list[str]:
    cfg = get_config()
    current = (getattr(cfg, "AUTH_JWT_SECRET", "") or "").strip()
    previous = [
        str(item).strip()
        for item in getattr(cfg, "AUTH_JWT_PREVIOUS_SECRETS", [])
        if str(item).strip()
    ]
    return [item for item in [current, *previous] if item]


def issue_access_token(principal: ApiKeyPrincipal, *, ttl_seconds: Optional[int] = None) -> dict[str, Any]:
    """Issue a short-lived HS256 JWT access token for an authenticated client."""
    cfg = get_config()
    secret = (getattr(cfg, "AUTH_JWT_SECRET", "") or "").strip()
    if not secret:
        raise RuntimeError("AUTH_JWT_SECRET is not configured")
    now = int(time.time())
    ttl = int(ttl_seconds or getattr(cfg, "AUTH_JWT_ACCESS_TTL_SEC", 3600) or 3600)
    ttl = max(60, min(86400, ttl))
    exp = now + ttl
    jti = f"jwt-{uuid.uuid4().hex}"
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": getattr(cfg, "AUTH_JWT_ISSUER", "agent-sandbox"),
        "aud": getattr(cfg, "AUTH_JWT_AUDIENCE", "agent-sandbox-api"),
        "typ": "access",
        "sub": principal.client_id,
        "client_id": principal.client_id,
        "key_id": principal.key_id,
        "key_prefix": principal.key_prefix,
        "email": principal.email,
        "name": principal.key_name,
        "iat": now,
        "nbf": now,
        "exp": exp,
        "jti": jti,
    }
    signing_input = ".".join((_b64url_encode(_json_for_jwt(header)), _b64url_encode(_json_for_jwt(payload))))
    token = f"{signing_input}.{_jwt_signature(signing_input, secret)}"
    return {
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": ttl,
        "expires_at": exp,
        "issued_at": now,
        "jti": jti,
    }


def _decode_verified_jwt(token: str) -> dict[str, Any]:
    try:
        header_raw, payload_raw, signature = token.split(".", 2)
        header = json.loads(_b64url_decode(header_raw).decode("utf-8"))
        payload = json.loads(_b64url_decode(payload_raw).decode("utf-8"))
    except Exception as ex:  # noqa: BLE001
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Invalid access token") from ex
    if header.get("alg") != "HS256":
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Unsupported access token algorithm")
    signing_input = f"{header_raw}.{payload_raw}"
    if not any(
        hmac.compare_digest(_jwt_signature(signing_input, secret), signature)
        for secret in _jwt_secrets()
    ):
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Invalid access token signature")

    cfg = get_config()
    now = int(time.time())
    leeway = int(getattr(cfg, "AUTH_JWT_LEEWAY_SEC", 30) or 30)
    exp = payload.get("exp")
    if not isinstance(exp, int) or now > exp + leeway:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Access token expired")
    nbf = payload.get("nbf")
    if isinstance(nbf, int) and now + leeway < nbf:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Access token is not valid yet")
    iat = payload.get("iat")
    if isinstance(iat, int) and now + leeway < iat:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Access token issued in the future")
    if payload.get("iss") != getattr(cfg, "AUTH_JWT_ISSUER", "agent-sandbox"):
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Invalid access token issuer")
    expected_aud = getattr(cfg, "AUTH_JWT_AUDIENCE", "agent-sandbox-api")
    aud = payload.get("aud")
    if isinstance(aud, list):
        audience_ok = expected_aud in aud
    else:
        audience_ok = aud == expected_aud
    if not audience_ok:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Invalid access token audience")
    if payload.get("typ") != "access":
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Invalid access token type")
    return payload


def authenticate_api_key_value(api_key: str, *, touch: bool = True) -> ApiKeyPrincipal:
    cfg = get_config()
    if not bool(getattr(cfg, "AUTH_API_KEYS_ENABLED", True)):
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "API key authentication is disabled")
    raw = (api_key or "").strip()
    if not raw:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "API key required")
    hashed = hash_api_key(raw)
    cached = _get_cached_api_key_principal(hashed)
    if cached is not None:
        return cached
    ensure_bootstrap_client_and_key()
    db = _db()
    row = db.get_api_key_principal(hashed)
    if not row or row.get("revoked_at"):
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
    if not row.get("is_active", False):
        raise ClientAuthError(status.HTTP_403_FORBIDDEN, "Client is disabled")
    if touch:
        db.touch_api_key_used(str(row["key_id"]))
    principal = _principal_from_row(row, auth_type="api_key")
    _set_cached_api_key_principal(hashed, principal)
    return principal


def authenticate_jwt_value(token: str, *, touch: bool = True) -> ApiKeyPrincipal:
    payload = _decode_verified_jwt((token or "").strip())
    client_id = str(payload.get("client_id") or payload.get("sub") or "").strip()
    key_id = str(payload.get("key_id") or "").strip()
    if not client_id:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Access token missing subject")
    db = _db()
    if key_id:
        row = db.get_api_key_principal_by_id(key_id)
        if not row or row.get("client_id") != client_id or row.get("revoked_at"):
            raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Access token credential has been revoked")
        if not row.get("is_active", False):
            raise ClientAuthError(status.HTTP_403_FORBIDDEN, "Client is disabled")
        if touch:
            db.touch_api_key_used(key_id)
        return _principal_from_row(
            row,
            auth_type="jwt",
            token_id=str(payload.get("jti") or ""),
            expires_at=int(payload["exp"]),
        )

    client = db.get_client(client_id)
    if not client or not client.get("is_active", False):
        raise ClientAuthError(status.HTTP_403_FORBIDDEN, "Client is disabled")
    return ApiKeyPrincipal(
        client_id=client_id,
        key_id="",
        key_name="jwt",
        key_prefix="",
        email=str(client.get("email") or ""),
        display_name=str(client.get("display_name") or ""),
        is_active=True,
        auth_type="jwt",
        token_id=str(payload.get("jti") or ""),
        expires_at=int(payload["exp"]),
    )


def authenticate_client_credential(
    credential: str,
    *,
    allow_jwt: bool = True,
    allow_api_key: bool = True,
    touch: bool = True,
) -> ApiKeyPrincipal:
    raw = (credential or "").strip()
    if not raw:
        raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Authentication credential required")
    if allow_jwt and _looks_like_jwt(raw):
        return authenticate_jwt_value(raw, touch=touch)
    if allow_api_key:
        return authenticate_api_key_value(raw, touch=touch)
    raise ClientAuthError(status.HTTP_401_UNAUTHORIZED, "Unsupported authentication credential")


@lru_cache(maxsize=4)
def _db_for_url(
    database_url: str,
    database_type: str = "",
    database_username: str = "",
    database_password: str = "",
) -> Database:
    return Database(
        database_url,
        database_type=database_type,
        database_username=database_username,
        database_password=database_password,
    )


def _db() -> Database:
    cfg = get_config()
    return _db_for_url(
        cfg.DATABASE_URL,
        getattr(cfg, "DATABASE_TYPE", ""),
        getattr(cfg, "DATABASE_USERNAME", ""),
        getattr(cfg, "DATABASE_PASSWORD", ""),
    )


def _internal_expected_key() -> str:
    cfg = get_config()
    return (
        os.getenv("INTERNAL_API_KEY")
        or os.getenv("CONTROL_PLANE_API_KEY")
        or getattr(cfg, "INTERNAL_API_KEY", "")
        or getattr(cfg, "RUNTIME_GATEWAY_API_KEY", "")
        or ""
    ).strip()


def _admin_expected_key() -> str:
    cfg = get_config()
    return (os.getenv("ADMIN_API_KEY") or getattr(cfg, "ADMIN_API_KEY", "") or "").strip()


def admin_api_key_is_valid(api_key: str) -> bool:
    expected = _admin_expected_key()
    raw = (api_key or "").strip()
    return bool(expected and raw and hmac.compare_digest(raw, expected))


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


def validate_admin_api_key(request: Request) -> str:
    api_key = (request.headers.get("X-Admin-API-Key") or "").strip()
    if not api_key:
        auth = (request.headers.get("Authorization") or "").strip()
        scheme, _, token = auth.partition(" ")
        if scheme.lower() == "bearer":
            api_key = token.strip()
    if not admin_api_key_is_valid(api_key):
        detail = "Admin API key is not configured" if not _admin_expected_key() else "Invalid admin API key"
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
        )
    return api_key


async def validate_api_key(request: Request) -> ApiKeyPrincipal:
    """Extract and validate a client credential from request headers.

    ``X-API-Key`` is always treated as an API key. ``Authorization: Bearer`` accepts
    signed JWT access tokens first; non-JWT bearer values remain supported for SDKs
    when ``AUTH_BEARER_API_KEYS_ENABLED=true``.
    """
    api_key = (request.headers.get("X-API-Key") or "").strip()
    bearer = ""
    auth = (request.headers.get("Authorization") or "").strip()
    scheme, _, token = auth.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        bearer = token.strip()
    if not api_key and not bearer:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-API-Key or Authorization Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        if api_key:
            return await run_io(authenticate_api_key_value, api_key)
        cfg = get_config()
        return await run_io(
            authenticate_client_credential,
            bearer,
            allow_jwt=True,
            allow_api_key=bool(getattr(cfg, "AUTH_BEARER_API_KEYS_ENABLED", True)),
        )
    except ClientAuthError as ex:
        headers = {"WWW-Authenticate": "Bearer"} if ex.status_code == status.HTTP_401_UNAUTHORIZED else None
        raise HTTPException(
            status_code=ex.status_code,
            detail=ex.detail,
            headers=headers,
        ) from ex


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
    clear_api_key_auth_cache()
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
    clear_api_key_auth_cache()
