"""Agent runtime system."""

import uuid
import subprocess
import json
import logging
import threading
import time
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class AgentState(str, Enum):
    """Agent state."""
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    FAILED = "failed"
    IDLE = "idle"


@dataclass
class AgentConfig:
    """Agent configuration."""
    agent_name: str
    agent_code: Optional[str] = None
    auto_start: bool = True
    max_memory: int = 512  # MB
    timeout: int = 3600  # seconds
    # If True: API thread runs python3 once in the sandbox then exits (good for one-shot "build" scripts).
    # If False: re-run the script every exec_interval_sec until kill (legacy behaviour).
    single_run: bool = False
    exec_interval_sec: float = 1.0


class Agent:
    """Pseudo-agent that runs in sandbox."""

    def __init__(
        self,
        sandbox_manager,
        sandbox_id: str,
        agent_id: str,
        config: AgentConfig,
    ):
        self.sandbox_manager = sandbox_manager
        self.sandbox_id = sandbox_id
        self.agent_id = agent_id
        self.config = config
        self.state = AgentState.IDLE

        self.process = None
        self.messages = []
        self.message_handlers: Dict[str, Callable] = {}
        
        self._stop_event = threading.Event()
        self._thread = None

    def start(self) -> bool:
        """Start agent."""
        logger.info(f"Starting agent {self.agent_id}")

        if self.config.auto_start and self.config.agent_code:
            # Write agent code to sandbox
            agent_file = f"/tmp/agent_{self.agent_id}.py"
            success = self.sandbox_manager.write_file(
                self.sandbox_id,
                agent_file,
                self.config.agent_code
            )

            if not success:
                logger.error(f"Failed to write agent code for {self.agent_id}")
                self.state = AgentState.FAILED
                return False

            # Run agent in background
            self._thread = threading.Thread(target=self._run_agent_loop)
            self._thread.daemon = True
            self._thread.start()

            self.state = AgentState.RUNNING
            return True

        return False

    def _run_agent_loop(self):
        """Run agent in continuous loop."""
        agent_file = f"/tmp/agent_{self.agent_id}.py"
        cmd_timeout = min(float(self.config.timeout), 600.0)

        try:
            while not self._stop_event.is_set():
                try:
                    result = self.sandbox_manager.run_command(
                        self.sandbox_id,
                        f"python3 {agent_file}",
                        timeout=cmd_timeout,
                    )

                    if result["exit_code"] == 0:
                        logger.info(f"Agent {self.agent_id} executed successfully")
                    else:
                        logger.warning(
                            f"Agent {self.agent_id} failed: {result['stderr']}"
                        )

                    if self.config.single_run:
                        break
                    time.sleep(max(0.1, float(self.config.exec_interval_sec)))

                except Exception as e:
                    logger.error(f"Error running agent {self.agent_id}: {e}")
                    time.sleep(max(0.1, float(self.config.exec_interval_sec)))

            # single_run finished without kill: mark idle/stopped so API reflects completion
            if self.config.single_run and self.state == AgentState.RUNNING:
                self.state = AgentState.STOPPED

        except Exception as e:
            logger.error(f"Agent loop failed for {self.agent_id}: {e}")
            self.state = AgentState.FAILED
        finally:
            # Drop bootstrap script uploaded for this agent (avoids clutter in /tmp).
            if self.config.agent_code:
                try:
                    self.sandbox_manager.delete_file(
                        self.sandbox_id, agent_file, recursive=False
                    )
                except Exception as exc:
                    logger.warning(
                        "Could not remove agent bootstrap %s: %s", agent_file, exc
                    )

    def send_message(self, message_type: str, content: Dict[str, Any]) -> bool:
        """Send message to agent."""
        message_id = f"msg-{uuid.uuid4().hex[:12]}"

        message = {
            "message_id": message_id,
            "message_type": message_type,
            "content": content,
            "timestamp": time.time(),
        }

        self.messages.append(message)

        # Call message handler if registered
        if message_type in self.message_handlers:
            try:
                self.message_handlers[message_type](message)
            except Exception as e:
                logger.error(f"Error in message handler: {e}")

        return True

    def register_message_handler(self, message_type: str, handler: Callable) -> None:
        """Register handler for message type."""
        self.message_handlers[message_type] = handler

    def stop(self) -> bool:
        """Stop agent."""
        logger.info(f"Stopping agent {self.agent_id}")
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=5)

        self.state = AgentState.STOPPED
        return True

    def pause(self) -> bool:
        """Pause agent."""
        self.state = AgentState.PAUSED
        return True

    def resume(self) -> bool:
        """Resume agent."""
        self.state = AgentState.RUNNING
        return True

    def get_status(self) -> Dict[str, Any]:
        """Get agent status."""
        return {
            "agent_id": self.agent_id,
            "agent_name": self.config.agent_name,
            "state": self.state.value,
            "message_count": len(self.messages),
            "config": {
                "auto_start": self.config.auto_start,
                "max_memory": self.config.max_memory,
                "timeout": self.config.timeout,
                "single_run": self.config.single_run,
                "exec_interval_sec": self.config.exec_interval_sec,
            }
        }


class AgentRuntime:
    """Runtime for managing agents in sandbox."""

    def __init__(self, sandbox_manager):
        self.sandbox_manager = sandbox_manager
        self.agents: Dict[str, Agent] = {}

    def spawn_agent(
        self,
        sandbox_id: str,
        agent_name: str,
        agent_code: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Spawn new agent in sandbox.
        
        Returns agent_id on success, None on failure.
        """
        agent_id = f"agent-{uuid.uuid4().hex[:12]}"
        config = config or {}

        agent_config = AgentConfig(
            agent_name=agent_name,
            agent_code=agent_code,
            auto_start=config.get("auto_start", True),
            max_memory=config.get("max_memory", 512),
            timeout=int(config.get("timeout", 3600)),
            single_run=bool(config.get("single_run", False)),
            exec_interval_sec=float(
                config.get("exec_interval_sec", config.get("exec_interval_seconds", 1.0))
            ),
        )

        agent = Agent(
            self.sandbox_manager,
            sandbox_id,
            agent_id,
            agent_config,
        )

        if not agent.start():
            logger.error(f"Failed to start agent {agent_id}")
            return None

        self.agents[agent_id] = agent

        logger.info(f"Agent spawned: {agent_id}")
        return agent_id

    def get_agent(self, agent_id: str) -> Optional[Agent]:
        """Get agent by ID."""
        return self.agents.get(agent_id)

    def send_agent_message(
        self,
        agent_id: str,
        message_type: str,
        content: Dict[str, Any],
    ) -> bool:
        """Send message to agent."""
        agent = self.get_agent(agent_id)
        if not agent:
            logger.error(f"Agent not found: {agent_id}")
            return False

        return agent.send_message(message_type, content)

    def list_agents(self, sandbox_id: Optional[str] = None) -> list:
        """List all agents (optionally filtered by sandbox)."""
        agents = []
        for agent_id, agent in self.agents.items():
            if sandbox_id is None or agent.sandbox_id == sandbox_id:
                agents.append({
                    "agent_id": agent_id,
                    "sandbox_id": agent.sandbox_id,
                    "agent_name": agent.config.agent_name,
                    "state": agent.state.value,
                })
        return agents

    def kill_agent(self, agent_id: str, force: bool = False) -> bool:
        """Kill agent."""
        agent = self.get_agent(agent_id)
        if not agent:
            logger.error(f"Agent not found: {agent_id}")
            return False

        success = agent.stop()

        if success:
            del self.agents[agent_id]
            logger.info(f"Agent killed: {agent_id}")

        return success

    def pause_agent(self, agent_id: str) -> bool:
        """Pause agent."""
        agent = self.get_agent(agent_id)
        if not agent:
            return False

        return agent.pause()

    def resume_agent(self, agent_id: str) -> bool:
        """Resume agent."""
        agent = self.get_agent(agent_id)
        if not agent:
            return False

        return agent.resume()

    def get_agent_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get agent status."""
        agent = self.get_agent(agent_id)
        if not agent:
            return None

        return agent.get_status()

    def get_agent_messages(self, agent_id: str, limit: int = 100) -> list:
        """Get agent messages."""
        agent = self.get_agent(agent_id)
        if not agent:
            return []

        return agent.messages[-limit:]

    def cleanup_sandbox_agents(self, sandbox_id: str) -> int:
        """Clean up all agents in sandbox."""
        agent_ids = [
            agent_id for agent_id, agent in self.agents.items()
            if agent.sandbox_id == sandbox_id
        ]

        for agent_id in agent_ids:
            self.kill_agent(agent_id)

        return len(agent_ids)
