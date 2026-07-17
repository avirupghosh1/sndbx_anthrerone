"""Public documentation portal embedded inside api-service."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape

from docs_content import (
    SECTIONS,
    get_default_slug,
    get_page,
    get_section,
    not_found_page,
    page_toc,
    sidebar_groups,
    top_nav,
)

router = APIRouter(prefix="/docs", tags=["docs"])

_BASE_DIR = Path(__file__).resolve().parent.parent
_TEMPLATES = Jinja2Templates(directory=str(_BASE_DIR / "portal_templates"))
_INLINE_CODE_RE = re.compile(r"`([^`]+)`")


def _inline_code(value: object) -> Markup:
    text = "" if value is None else str(value)
    chunks: list[str] = []
    cursor = 0
    for match in _INLINE_CODE_RE.finditer(text):
        chunks.append(str(escape(text[cursor : match.start()])))
        chunks.append(f"<code>{escape(match.group(1))}</code>")
        cursor = match.end()
    chunks.append(str(escape(text[cursor:])))
    return Markup("".join(chunks))


_TEMPLATES.env.filters["inline_code"] = _inline_code


def _context(request: Request, section_id: str, slug: str, page: dict) -> dict:
    active_section = get_section(section_id) or SECTIONS[0]
    return {
        "request": request,
        "page": page,
        "toc": page_toc(page),
        "top_nav": top_nav(active_section["id"]),
        "active_section": active_section,
        "sidebar_groups": sidebar_groups(active_section["id"], slug),
    }


def _render_docs(
    request: Request,
    *,
    section_id: str,
    slug: str,
    page: dict,
    status_code: int = 200,
) -> HTMLResponse:
    return _TEMPLATES.TemplateResponse(
        request,
        "docs_shell.html",
        _context(request, section_id, slug, page),
        status_code=status_code,
    )


@router.get("", response_class=HTMLResponse)
async def docs_root() -> RedirectResponse:
    return RedirectResponse("/docs/documentation/overview", status_code=307)


@router.get("/", response_class=HTMLResponse)
async def docs_root_slash() -> RedirectResponse:
    return RedirectResponse("/docs/documentation/overview", status_code=307)


@router.get("/{section_id}", response_class=HTMLResponse)
async def docs_section(request: Request, section_id: str):
    default_slug = get_default_slug(section_id)
    if not default_slug:
        page = not_found_page("documentation")
        return _render_docs(
            request,
            section_id="documentation",
            slug="not-found",
            page=page,
            status_code=404,
        )
    return RedirectResponse(f"/docs/{section_id}/{default_slug}", status_code=307)


@router.get("/{section_id}/{slug:path}", response_class=HTMLResponse)
async def docs_page(request: Request, section_id: str, slug: str) -> HTMLResponse:
    default_slug = get_default_slug(section_id)
    if not default_slug:
        page = not_found_page("documentation")
        return _render_docs(
            request,
            section_id="documentation",
            slug="not-found",
            page=page,
            status_code=404,
        )

    normalized_slug = (slug or default_slug).strip("/")
    page = get_page(section_id, normalized_slug)
    if page is None:
        page = not_found_page(section_id)
        return _render_docs(
            request,
            section_id=section_id,
            slug="not-found",
            page=page,
            status_code=404,
        )
    return _render_docs(request, section_id=section_id, slug=normalized_slug, page=page)
