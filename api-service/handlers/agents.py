"""Agent endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from typing import Optional

from async_runner import run_io
from models import (
    SpawnAgentRequest,
    KillAgentRequest,
    AgentMessage,
    AgentResponse,
    AgentMessageResponse,
)
from middleware import (
    ApiKeyPrincipal,
    ensure_sandbox_access,
    validate_api_key,
    SandboxNotFoundException,
    SandboxRuntimeLostException,
    AgentNotFoundException,
)
from orchestrator import SandboxManager
from agents import AgentRuntime

router = APIRouter(prefix="/sandboxes", tags=["agents"])

# Global agent runtime (in production, make this singleton)
agent_runtime = None


def _owned_sandbox_or_404(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, sandbox_id: str) -> dict:
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)
    return ensure_sandbox_access(principal, sandbox, sandbox_id)


def _ensure_live_sandbox(sandbox_manager: SandboxManager, sandbox_id: str) -> None:
    reason = sandbox_manager.get_sandbox_runtime_failure(sandbox_id)
    if reason:
        raise SandboxRuntimeLostException(sandbox_id, reason)


def get_agent_runtime():
    """Get agent runtime instance."""
    global agent_runtime
    return agent_runtime


def set_agent_runtime(runtime):
    """Set agent runtime instance."""
    global agent_runtime
    agent_runtime = runtime


@router.post("/{sandbox_id}/agents/spawn", response_model=AgentResponse)
async def spawn_agent(
    sandbox_id: str,
    request: SpawnAgentRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Spawn agent in sandbox."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get agent runtime
    runtime = get_agent_runtime()
    if not runtime:
        raise HTTPException(status_code=500, detail="Agent runtime not available")

    cfg = dict(request.config or {})
    if request.auto_start is not None:
        cfg["auto_start"] = request.auto_start

    # Spawn agent (writes agent file via Docker; keep off the event loop)
    agent_id = await run_io(
        runtime.spawn_agent,
        sandbox_id,
        request.agent_name,
        request.agent_code,
        cfg,
    )

    if not agent_id:
        raise HTTPException(status_code=500, detail="Failed to spawn agent")

    # Get agent status
    status = runtime.get_agent_status(agent_id)

    return AgentResponse(
        agent_id=agent_id,
        agent_name=request.agent_name,
        state=status["state"],
        created_at=status.get("created_at", ""),
        config=cfg,
    )


@router.get("/{sandbox_id}/agents")
async def list_agents(
    sandbox_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List agents in sandbox."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get agent runtime
    runtime = get_agent_runtime()
    if not runtime:
        raise HTTPException(status_code=500, detail="Agent runtime not available")

    # List agents
    agents = runtime.list_agents(sandbox_id=sandbox_id)

    return {"agents": agents}


@router.get("/{sandbox_id}/agents/{agent_id}", response_model=AgentResponse)
async def get_agent(
    sandbox_id: str,
    agent_id: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Get agent info."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get agent runtime
    runtime = get_agent_runtime()
    if not runtime:
        raise HTTPException(status_code=500, detail="Agent runtime not available")

    # Get agent status
    status = runtime.get_agent_status(agent_id)
    if not status:
        raise AgentNotFoundException(agent_id)

    return AgentResponse(
        agent_id=agent_id,
        agent_name=status["agent_name"],
        state=status["state"],
        created_at="",  # TODO: add to status
        config=status.get("config", {}),
    )


@router.post("/{sandbox_id}/agents/{agent_id}/kill")
async def kill_agent(
    sandbox_id: str,
    agent_id: str,
    request: Optional[KillAgentRequest] = None,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Kill agent."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get agent runtime
    runtime = get_agent_runtime()
    if not runtime:
        raise HTTPException(status_code=500, detail="Agent runtime not available")

    # Kill agent
    force = request.force if request else False
    success = runtime.kill_agent(agent_id, force=force)

    if not success:
        raise AgentNotFoundException(agent_id)

    return {"success": True, "agent_id": agent_id}


@router.post("/{sandbox_id}/agents/{agent_id}/messages")
async def send_agent_message(
    sandbox_id: str,
    agent_id: str,
    request: AgentMessage,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Send message to agent."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get agent runtime
    runtime = get_agent_runtime()
    if not runtime:
        raise HTTPException(status_code=500, detail="Agent runtime not available")

    # Send message
    success = runtime.send_agent_message(
        agent_id=agent_id,
        message_type=request.message_type,
        content=request.content,
    )

    if not success:
        raise AgentNotFoundException(agent_id)

    return {"success": True, "agent_id": agent_id}


@router.get("/{sandbox_id}/agents/{agent_id}/messages")
async def get_agent_messages(
    sandbox_id: str,
    agent_id: str,
    limit: int = 100,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Get agent messages."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Get agent runtime
    runtime = get_agent_runtime()
    if not runtime:
        raise HTTPException(status_code=500, detail="Agent runtime not available")

    # Get messages
    messages = runtime.get_agent_messages(agent_id, limit=limit)

    return {"agent_id": agent_id, "messages": messages}
