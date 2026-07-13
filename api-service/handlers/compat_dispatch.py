"""Dispatch shared SDK compatibility routes to the correct provider adapter.

Some SDKs use the same HTTP path with different response contracts. FastAPI
chooses the first matching route, so shared paths must be owned here instead of
accidentally depending on router include order.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request

from middleware import ApiKeyPrincipal, validate_api_key
from orchestrator import SandboxManager

from . import daytona_compat, e2b_compat


router = APIRouter(tags=["compat-dispatch"])


def _header(request: Request, name: str) -> str:
    return (request.headers.get(name) or "").strip()


def _provider_for_snapshots(request: Request) -> str:
    explicit = (
        _header(request, "x-sndbx-compat-provider")
        or _header(request, "x-compat-provider")
        or request.query_params.get("provider")
        or ""
    ).strip().lower()
    if explicit in {"daytona", "e2b"}:
        return explicit

    daytona_source = _header(request, "x-daytona-source").lower()
    daytona_version = _header(request, "x-daytona-sdk-version")
    user_agent = _header(request, "user-agent").lower()
    query = request.query_params

    if daytona_source.startswith("sdk-python") or daytona_version:
        return "daytona"
    if "e2b-python-sdk/" in user_agent or _header(request, "publisher").lower() == "e2b":
        return "e2b"
    if "sandboxID" in query or "nextToken" in query:
        return "e2b"
    if any(key in query for key in ("page", "name", "sort", "order")):
        return "daytona"
    return "e2b"


@router.get("/snapshots")
async def list_snapshots(
    request: Request,
    sandbox_id: Optional[str] = Query(default=None, alias="sandboxID"),
    next_token: Optional[str] = Query(default=None, alias="nextToken"),
    page: Optional[int] = 1,
    limit: Optional[int] = 100,
    name: Optional[str] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    provider = _provider_for_snapshots(request)
    if provider == "daytona":
        return await daytona_compat.list_snapshots(
            page=page,
            limit=limit,
            name=name,
            principal=principal,
            sandbox_manager=sandbox_manager,
        )
    return await e2b_compat.list_snapshots(
        sandbox_id=sandbox_id,
        next_token=next_token,
        limit=limit,
        principal=principal,
        sandbox_manager=sandbox_manager,
    )
