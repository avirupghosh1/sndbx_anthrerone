"""Run blocking I/O off the main asyncio loop so Uvicorn can still serve /health."""

from __future__ import annotations

import asyncio
import functools
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

T = TypeVar("T")

_MAX_WORKERS = max(4, int(os.getenv("API_IO_WORKERS", "64") or "64"))
_EXECUTOR = ThreadPoolExecutor(max_workers=_MAX_WORKERS, thread_name_prefix="api-io")


async def run_io(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Execute ``fn(*args, **kwargs)`` in a worker thread (Python 3.9+)."""
    loop = asyncio.get_running_loop()
    call = functools.partial(fn, *args, **kwargs)
    return await loop.run_in_executor(_EXECUTOR, call)
