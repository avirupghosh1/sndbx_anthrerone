"""Map sandbox ``template_id`` (alias or image ref) to a concrete Docker image / FC template id."""

from __future__ import annotations

from typing import Optional

_DEFAULT_TEMPLATE_IMAGE = "python:3.11"
_TEMPLATE_IMAGE_ALIASES = {
    "base": _DEFAULT_TEMPLATE_IMAGE,
    "default": _DEFAULT_TEMPLATE_IMAGE,
}


def resolve_sandbox_image(template_id: Optional[str]) -> str:
    if template_id is None or not str(template_id).strip():
        return _DEFAULT_TEMPLATE_IMAGE
    tid = str(template_id).strip()
    return _TEMPLATE_IMAGE_ALIASES.get(tid.lower(), tid)
