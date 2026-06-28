"""Tenant-scoped authorization helpers."""

from __future__ import annotations

from fastapi import HTTPException, status

from .auth import ApiKeyPrincipal


def ensure_sandbox_access(principal: ApiKeyPrincipal, sandbox: dict, sandbox_id: str) -> dict:
    owner_client_id = (sandbox.get("owner_client_id") or "").strip()
    if owner_client_id and owner_client_id != principal.client_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Sandbox not found: {sandbox_id}",
        )
    return sandbox


def ensure_template_access(principal: ApiKeyPrincipal, template: dict, template_id: str) -> dict:
    owner_client_id = (template.get("owner_client_id") or "").strip()
    if owner_client_id and owner_client_id != principal.client_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown template_id: {template_id}",
        )
    return template


def public_template_id_for_row(row: dict) -> str:
    alias = (row.get("template_alias") or "").strip()
    return alias or str(row.get("template_id") or "")
