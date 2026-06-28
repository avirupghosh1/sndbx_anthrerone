"""FastAPI application."""

import logging
from pathlib import Path

from dotenv import load_dotenv

# Load ``api_server/.env`` before ``Config`` so ``DOCKER_HOST``, ``SANDBOX_ISOLATION``, etc. apply.
load_dotenv(Path(__file__).resolve().parent / ".env", override=False)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles

from config import get_config
from database import Database
from orchestrator import SandboxManager
from agents import AgentRuntime
from middleware import (
    api_exception_handler,
    validation_exception_handler,
    general_exception_handler,
    APIException,
    ensure_bootstrap_client_and_key,
)
from handlers import sandboxes, commands, files, agents, templates, guest_connection, sandbox_envd, internal_routing, internal, portal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Get configuration
config = get_config()

# Create FastAPI app
app = FastAPI(
    title=config.API_TITLE,
    version=config.API_VERSION,
    description=config.API_DESCRIPTION,
    debug=config.DEBUG,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

portal_static_dir = Path(__file__).resolve().parent / "portal_static"
if portal_static_dir.exists():
    app.mount("/portal/static", StaticFiles(directory=str(portal_static_dir)), name="portal-static")

# Initialize database
db = Database(config.DATABASE_PATH)

from orchestrator.execution_backend import build_execution_backend

_execution_backend = build_execution_backend(config)
sandbox_manager = SandboxManager(db, execution=_execution_backend)
agent_runtime = AgentRuntime(sandbox_manager)

# Set manager instances for dependency injection
SandboxManager.instance = sandbox_manager
agents.set_agent_runtime(agent_runtime)

# Add exception handlers
app.add_exception_handler(APIException, api_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, general_exception_handler)

# Include routers
app.include_router(sandboxes.router)
app.include_router(commands.router)
app.include_router(files.router)
app.include_router(agents.router)
app.include_router(templates.router)
app.include_router(guest_connection.router)
app.include_router(sandbox_envd.router)
app.include_router(internal_routing.router)
app.include_router(internal.router)
app.include_router(portal.router)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    sm = SandboxManager.__dict__.get("instance")
    warm = getattr(sm, "warm_pool", None) if sm else None
    out: dict = {
        "status": "ok",
        "version": config.API_VERSION,
        "api_service_role": getattr(config, "API_SERVICE_ROLE", "control"),
        "sandbox_runtime": sm.get_execution_kind() if sm else None,
        "docker_host": getattr(config, "DOCKER_HOST", None),
    }
    if sm is not None:
        blocker = sm.describe_docker_workload_blocker()
        if blocker is not None:
            out["execution_plane_ok"] = False
            out["execution_plane_detail"] = blocker
        else:
            out["execution_plane_ok"] = True
        out["sandbox_runtime"] = sm.get_execution_kind()
    if warm is not None:
        try:
            out["warm_pool"] = warm.stats()
        except Exception:
            out["warm_pool"] = {"enabled": True, "error": "stats_unavailable"}
    return out


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Sandbox API Server",
        "version": config.API_VERSION,
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


@app.on_event("startup")
async def startup_event():
    """Startup event."""
    ensure_bootstrap_client_and_key()
    logger.info("Starting Sandbox API Server (role=%s)", getattr(config, "API_SERVICE_ROLE", "control"))
    logger.info(f"API Key: {config.API_KEY}")
    logger.info(f"Database: {config.DATABASE_PATH}")
    logger.info(f"Execution plane: {sandbox_manager.get_execution_kind()}")
    logger.info("Docker host: %s", getattr(config, "DOCKER_HOST", "") or "<default local engine>")
    wp = getattr(sandbox_manager, "warm_pool", None)
    if wp is not None:
        logger.info("Warm sandbox pool: %s", wp.stats())
    logger.info(f"Data plane domain: {getattr(config, 'SANDBOX_DATA_PLANE_DOMAIN', 'sndbx.com')}")
    hint = sandbox_manager.describe_docker_workload_blocker()
    if hint:
        logger.warning("Execution plane not ready — sandbox creates will return 503 until fixed: %s", hint)
    try:
        reconcile = sandbox_manager.reconcile_persisted_state()
        logger.info("Sandbox state reconcile: %s", reconcile)
    except Exception as ex:  # noqa: BLE001
        logger.warning("Sandbox state reconcile failed during startup: %s", ex)


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown event."""
    logger.info("Shutting down Sandbox API Server")

    wp = getattr(sandbox_manager, "warm_pool", None)
    if wp is not None:
        try:
            wp.stop()
        except Exception as ex:  # noqa: BLE001
            logger.warning("Warm pool stop: %s", ex)
    try:
        sandbox_manager.stop_background_work()
    except Exception as ex:  # noqa: BLE001
        logger.warning("Background worker stop: %s", ex)

    # Cleanup agents
    all_agents = agent_runtime.list_agents()
    for agent_info in all_agents:
        agent_runtime.kill_agent(agent_info["agent_id"])

    ex = sandbox_manager.execution
    if hasattr(ex, "close"):
        try:
            ex.close()
        except Exception:
            pass

    logger.info("Cleanup complete")


def create_app():
    """Create and configure FastAPI control-plane app (no ingress middleware)."""
    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.DEBUG,
        log_level=config.LOG_LEVEL.lower(),
    )
