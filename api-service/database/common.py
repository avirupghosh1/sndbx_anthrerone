"""Shared database helpers and URL normalization."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

POSTGRES_SCHEMES = ("postgres://", "postgresql://")
MONGODB_SCHEMES = ("mongodb://", "mongodb+srv://")
DATABASE_TYPES = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "pg": "postgres",
    "mongo": "mongo",
    "mongodb": "mongo",
}

_UPLOAD_CHUNK_BYTES = 1024 * 1024


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_postgres_url(database_url: str) -> bool:
    return (database_url or "").strip().startswith(POSTGRES_SCHEMES)


def _is_mongodb_url(database_url: str) -> bool:
    return (database_url or "").strip().startswith(MONGODB_SCHEMES)


def _normalize_database_type(database_type: Optional[str], database_url: str = "") -> str:
    raw = (database_type or "").strip().lower()
    if raw:
        if raw not in DATABASE_TYPES:
            raise ValueError("DATABASE_TYPE must be postgres or mongo")
        return DATABASE_TYPES[raw]
    url = (database_url or "").strip()
    if _is_mongodb_url(url):
        return "mongo"
    if _is_postgres_url(url):
        return "postgres"
    raise ValueError("DATABASE_TYPE is required when DATABASE_URL does not include a database scheme")


def _with_scheme(database_url: str, database_type: str) -> str:
    url = (database_url or "").strip()
    if _is_postgres_url(url) or _is_mongodb_url(url):
        return url
    if not url:
        return url
    if database_type == "postgres":
        return f"postgresql://{url}"
    return f"mongodb://{url}"


def _replace_credential_placeholders(database_url: str, username: str, password: str) -> str:
    url = database_url
    for placeholder in ("${DATABASE_USERNAME}", "{DATABASE_USERNAME}", "<username>"):
        if placeholder in url:
            url = url.replace(placeholder, quote(username, safe=""))
    for placeholder in (
        "${DATABASE_PASSWORD}",
        "{DATABASE_PASSWORD}",
        "<password>",
        "${MONGODB_PASSWORD}",
        "{MONGODB_PASSWORD}",
    ):
        if placeholder in url:
            url = url.replace(placeholder, quote(password, safe=""))
    return url


def _inject_url_credentials(database_url: str, username: str, password: str) -> str:
    url = _replace_credential_placeholders(database_url, username, password)
    parts = urlsplit(url)
    if not parts.netloc:
        return url
    if "@" not in parts.netloc:
        if not username:
            return url
        userinfo = quote(username, safe="")
        if password:
            userinfo = f"{userinfo}:{quote(password, safe='')}"
        return urlunsplit((parts.scheme, f"{userinfo}@{parts.netloc}", parts.path, parts.query, parts.fragment))

    userinfo, hosts = parts.netloc.rsplit("@", 1)
    if ":" in userinfo:
        existing_username, existing_password = userinfo.split(":", 1)
        if not existing_password and password:
            userinfo = f"{existing_username}:{quote(password, safe='')}"
    elif password:
        userinfo = f"{userinfo}:{quote(password, safe='')}"
    return urlunsplit((parts.scheme, f"{userinfo}@{hosts}", parts.path, parts.query, parts.fragment))


def _resolve_postgres_url(
    database_url: str,
    database_username: Optional[str] = None,
    database_password: Optional[str] = None,
) -> str:
    url = _with_scheme(database_url, "postgres")
    username = (database_username if database_username is not None else os.getenv("DATABASE_USERNAME") or "").strip()
    password = (database_password if database_password is not None else os.getenv("DATABASE_PASSWORD") or "").strip()
    return _inject_url_credentials(url, username, password)


def _resolve_mongodb_url(
    database_url: str,
    mongodb_password: Optional[str] = None,
    database_username: Optional[str] = None,
    database_password: Optional[str] = None,
) -> str:
    url = _with_scheme((database_url or "").strip(), "mongo")
    username = (database_username if database_username is not None else os.getenv("DATABASE_USERNAME") or "").strip()
    password = (
        database_password
        if database_password is not None
        else mongodb_password
        if mongodb_password is not None
        else os.getenv("DATABASE_PASSWORD")
        or os.getenv("MONGODB_PASSWORD")
        or ""
    ).strip()
    return _inject_url_credentials(url, username, password)


def _mongodb_database_name(database_url: str) -> str:
    configured = (os.getenv("MONGODB_DATABASE") or "").strip()
    if configured:
        return configured
    path = urlsplit(database_url).path.lstrip("/")
    if path:
        return unquote(path.split("/", 1)[0])
    raise ValueError("MongoDB DATABASE_URL must include a database name or set MONGODB_DATABASE")


def _template_build_upload_id(owner_client_id: str, namespace: str, object_key: str) -> str:
    raw = "\0".join((owner_client_id or "", namespace or "", object_key or ""))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = [
    "Any",
    "Dict",
    "List",
    "Optional",
    "datetime",
    "timedelta",
    "timezone",
    "Lock",
    "json",
    "os",
    "socket",
    "time",
    "uuid",
    "_UPLOAD_CHUNK_BYTES",
    "_utc_now_iso",
    "_normalize_database_type",
    "_resolve_postgres_url",
    "_resolve_mongodb_url",
    "_mongodb_database_name",
    "_template_build_upload_id",
]
