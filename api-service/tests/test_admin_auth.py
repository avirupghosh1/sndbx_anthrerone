import os

import pytest
from fastapi import HTTPException
from starlette.requests import Request

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ["API_KEY"] = "client-key"
os.environ["INTERNAL_API_KEY"] = "internal-key"
os.environ["ADMIN_API_KEY"] = "admin-key"

from middleware.auth import admin_api_key_is_valid, validate_admin_api_key  # noqa: E402


def _request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/admin/observability/summary",
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()],
        }
    )


def test_admin_api_key_accepts_only_admin_key():
    assert admin_api_key_is_valid("admin-key")
    assert not admin_api_key_is_valid("client-key")
    assert not admin_api_key_is_valid("internal-key")
    assert not admin_api_key_is_valid("")


def test_validate_admin_api_key_rejects_normal_client_keys():
    with pytest.raises(HTTPException) as exc:
        validate_admin_api_key(_request({"X-Admin-API-Key": "client-key"}))
    assert exc.value.status_code == 401

    with pytest.raises(HTTPException) as exc:
        validate_admin_api_key(_request({"X-Admin-API-Key": "internal-key"}))
    assert exc.value.status_code == 401

    assert validate_admin_api_key(_request({"X-Admin-API-Key": "admin-key"})) == "admin-key"
    assert validate_admin_api_key(_request({"Authorization": "Bearer admin-key"})) == "admin-key"
