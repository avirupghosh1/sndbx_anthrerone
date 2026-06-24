"""Sandbox data-plane gateway (standalone proxy-service or runtime-gateway sidecar)."""

from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from config import get_config
from middleware import SandboxDataPlaneMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def health(_request: Request) -> JSONResponse:
    cfg = get_config()
    return JSONResponse(
        {
            "status": "ok",
            "role": "runtime-gateway",
            "sandbox_domain": cfg.SANDBOX_DOMAIN,
            "upstream_resolve_mode": cfg.UPSTREAM_RESOLVE_MODE,
            "control_plane_url": cfg.CONTROL_PLANE_URL,
        }
    )


async def root(_request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "message": "Sandbox data-plane gateway",
            "health": "/health",
            "routing": "{port}-{sandbox_id}." + get_config().SANDBOX_DOMAIN,
        }
    )


routes = [
    Route("/health", health),
    Route("/", root),
]

app = Starlette(routes=routes)
app = SandboxDataPlaneMiddleware(app)


if __name__ == "__main__":
    import uvicorn

    cfg = get_config()
    uvicorn.run(
        "main:app",
        host=cfg.HOST,
        port=cfg.PORT,
        log_level=cfg.LOG_LEVEL.lower(),
    )
