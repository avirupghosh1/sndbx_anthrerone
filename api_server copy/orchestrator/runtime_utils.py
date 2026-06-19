"""Shared helpers for Docker and Kubernetes sandbox execution backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orchestrator.protocols import SandboxExecutionPlane


def is_container_like_execution(execution: Any) -> bool:
    """True when bootstrap/exec/file paths match Docker-style sandboxes (incl. K8s pods)."""
    from orchestrator.container_manager import ContainerManager
    from orchestrator.k8s_pod_manager import K8sPodManager

    return isinstance(execution, (ContainerManager, K8sPodManager))


def is_k8s_execution(execution: Any) -> bool:
    from orchestrator.k8s_pod_manager import K8sPodManager

    return isinstance(execution, K8sPodManager)


def supports_data_plane_routing(kind: str) -> bool:
    return (kind or "").strip().lower() in ("docker", "gvisor", "k8s", "kubernetes")


def workload_blocker_message(execution: "SandboxExecutionPlane") -> str | None:
    fn = getattr(execution, "describe_docker_unavailable", None)
    if callable(fn):
        return fn()
    return None
