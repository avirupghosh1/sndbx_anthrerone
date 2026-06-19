"""Environment variable parsing helpers."""

from __future__ import annotations

import os


def env_bool(name: str, default: bool) -> bool:
    """Parse a boolean from an environment variable with common truthy/falsy values."""
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default
