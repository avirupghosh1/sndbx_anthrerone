"""Shared helpers for Docker-backed sandbox execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orchestrator.protocols import SandboxExecutionPlane


def is_container_like_execution(execution: Any) -> bool:
    """True when bootstrap/exec/file paths match Docker-style sandboxes."""
    if bool(getattr(execution, "is_container_like", False)):
        return True
    from orchestrator.container_manager import ContainerManager

    return isinstance(execution, ContainerManager)


def supports_data_plane_routing(kind: str) -> bool:
    return (kind or "").strip().lower() in ("docker", "gvisor")


def workload_blocker_message(execution: "SandboxExecutionPlane") -> str | None:
    fn = getattr(execution, "describe_docker_unavailable", None)
    if callable(fn):
        return fn()
    return None
