"""Authentication middleware."""

import os
from fastapi import Request, HTTPException, status
from typing import Optional

# Valid API keys (in production, use proper auth/database)
VALID_API_KEYS = {
    os.getenv("API_KEY", "test-key-12345"),
}


async def validate_api_key(request: Request) -> str:
    """Extract and validate API key from request."""
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="X-API-Key header required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if api_key not in VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return api_key


def add_api_key(key: str) -> None:
    """Add API key (for testing)."""
    VALID_API_KEYS.add(key)


def remove_api_key(key: str) -> None:
    """Remove API key (for testing)."""
    VALID_API_KEYS.discard(key)
