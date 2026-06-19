"""Run blocking I/O (Docker, SQLite) off the main asyncio loop so Uvicorn can still serve /health."""

from __future__ import annotations

import asyncio
from typing import Any, Callable, TypeVar

T = TypeVar("T")


async def run_io(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Execute ``fn(*args, **kwargs)`` in a worker thread (Python 3.9+)."""
    return await asyncio.to_thread(fn, *args, **kwargs)
