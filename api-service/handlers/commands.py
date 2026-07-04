"""Command execution endpoints."""

import time

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse

from async_runner import run_io
from models import RunCommandRequest, CommandResponse
from middleware import (
    ApiKeyPrincipal,
    SandboxNotFoundException,
    SandboxRuntimeLostException,
    ensure_sandbox_access,
    validate_api_key,
)
from orchestrator import SandboxManager
from config import get_config
from database import Database

router = APIRouter(prefix="/sandboxes", tags=["commands"])


def _owned_sandbox_or_404(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, sandbox_id: str) -> dict:
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)
    return ensure_sandbox_access(principal, sandbox, sandbox_id)


def _ensure_live_sandbox(sandbox_manager: SandboxManager, sandbox_id: str) -> None:
    reason = sandbox_manager.get_sandbox_runtime_failure(sandbox_id)
    if reason:
        raise SandboxRuntimeLostException(sandbox_id, reason)


@router.post("/{sandbox_id}/commands/run", response_model=CommandResponse)
async def run_command(
    sandbox_id: str,
    request: RunCommandRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Run command in sandbox."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Run command
    start_time = time.time()
    result = await run_io(
        sandbox_manager.run_command,
        sandbox_id,
        request.command,
        request.cwd,
        request.env,
        request.timeout,
        request.user,
    )

    if not result:
        raise HTTPException(status_code=500, detail="Failed to run command")

    execution_time = time.time() - start_time

    return CommandResponse(
        exit_code=result["exit_code"],
        stdout=result["stdout"],
        stderr=result["stderr"],
        pid=result["pid"],
        execution_time=execution_time,
    )


@router.post("/{sandbox_id}/commands/run/stream")
async def run_command_stream(
    sandbox_id: str,
    request: RunCommandRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Run a command and stream stdout/stderr as **Server-Sent Events** (E2B-style).

    Each event is one line: ``data: {"type":"stdout"|"stderr","chunk":"..."}\\n\\n``,
    then a final ``data: {"type":"exit","exit_code":0}\\n\\n``. On failure you may see
    ``{"type":"error","message":"..."}`` before ``exit``.

    Docker uses Engine ``exec_start`` with ``stream=True`` for incremental chunks, then a final ``exit`` event.
    """
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    return StreamingResponse(
        sandbox_manager.iter_run_command_sse(
            sandbox_id,
            request.command,
            request.cwd,
            request.env,
            request.timeout,
            request.user,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/{sandbox_id}/commands")
async def list_commands(
    sandbox_id: str,
    limit: int = 100,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Get command history."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get command history
    db = Database(get_config().DATABASE_URL)
    history = db.get_command_history(sandbox_id, limit=limit)

    return {"commands": history}
