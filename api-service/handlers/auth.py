"""Client authentication endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from middleware import ApiKeyPrincipal, issue_access_token, validate_api_key

router = APIRouter(prefix="/auth", tags=["auth"])


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


@router.post("/token")
async def create_access_token(
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
) -> dict[str, Any]:
    """Exchange a valid API key or unexpired JWT for a short-lived JWT access token."""
    body = await _json_body(request)
    ttl = body.get("ttl_seconds") or body.get("expires_in")
    try:
        ttl_seconds = int(ttl) if ttl not in (None, "") else None
    except (TypeError, ValueError) as ex:
        raise HTTPException(status_code=400, detail="ttl_seconds must be an integer") from ex
    token = issue_access_token(
        principal,
        ttl_seconds=ttl_seconds,
    )
    return {
        **token,
        "client_id": principal.client_id,
        "key_id": principal.key_id,
        "auth_type": principal.auth_type,
    }


@router.get("/me")
async def auth_me(principal: ApiKeyPrincipal = Depends(validate_api_key)) -> dict[str, Any]:
    return {
        "client_id": principal.client_id,
        "key_id": principal.key_id,
        "key_name": principal.key_name,
        "key_prefix": principal.key_prefix,
        "email": principal.email,
        "display_name": principal.display_name,
        "auth_type": principal.auth_type,
        "token_id": principal.token_id,
        "expires_at": principal.expires_at,
    }
