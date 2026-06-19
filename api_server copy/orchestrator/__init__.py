"""Orchestrator module exports."""

from .container_manager import ContainerManager, ContainerConfig
from .firecracker_plane import FirecrackerVmmPlane, FC_BUNDLE_SCHEME, FC_WARM_DOCKERLESS_MARKER
from .lima_plane import LimaVmPlane, LIMA_WARM_DOCKERLESS_MARKER
from .sandbox_manager import SandboxManager
from .execution_backend import build_execution_backend
from .protocols import SandboxExecutionPlane
from .template_image import resolve_sandbox_image

__all__ = [
    "ContainerManager",
    "ContainerConfig",
    "FirecrackerVmmPlane",
    "FC_BUNDLE_SCHEME",
    "FC_WARM_DOCKERLESS_MARKER",
    "LimaVmPlane",
    "LIMA_WARM_DOCKERLESS_MARKER",
    "SandboxManager",
    "SandboxExecutionPlane",
    "build_execution_backend",
    "resolve_sandbox_image",
]
