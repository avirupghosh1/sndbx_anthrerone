"""Construct the sandbox execution plane: Docker Engine with optional gVisor."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from .container_manager import ContainerManager
from .protocols import SandboxExecutionPlane

if TYPE_CHECKING:
    from config import Config

logger = logging.getLogger(__name__)


def build_execution_backend(config: "Config | None" = None) -> SandboxExecutionPlane:
    """Return runtime-gateway control plane or direct Docker ``ContainerManager``."""
    if config is None:
        from config import get_config

        config = get_config()

    engine = (getattr(config, "SANDBOX_ENGINE", None) or "docker").strip().lower()
    if engine not in ("docker", "container", "containers"):
        raise RuntimeError(
            f"Unsupported SANDBOX_ENGINE={engine!r}. This service supports Docker Engine only."
        )
    runtime = (getattr(config, "SANDBOX_RUNTIME", None) or "docker").strip().lower()
    if runtime not in ("docker", "container", "containers", "runc", "runsc", "gvisor"):
        raise RuntimeError(
            f"Unsupported SANDBOX_RUNTIME={runtime!r}. Use docker/runc or gvisor/runsc."
        )

    oci = config.docker_oci_runtime()
    if bool(getattr(config, "is_control_plane", lambda: False)()) and (
        (getattr(config, "RUNTIME_GATEWAY_URL", "") or "").strip()
        or (getattr(config, "RUNTIME_GATEWAY_TARGETS_JSON", "") or "").strip()
    ):
        from .runtime_gateway_execution import RuntimeGatewayControlPlane

        kind = "gvisor" if oci else "docker"
        logger.info(
            "Sandbox execution: runtime-gateway control plane (kind=%s); API will not connect to dockerd",
            kind,
        )
        return RuntimeGatewayControlPlane(backend_kind=kind)

    # Direct Docker is a local/combined fallback only. docker-py ``from_env()`` reads
    # ``DOCKER_HOST`` / TLS env from the process environment.
    dh = (getattr(config, "DOCKER_HOST", None) or "").strip()
    if dh:
        os.environ["DOCKER_HOST"] = dh
        logger.info("Docker client will use DOCKER_HOST from configuration")
    logger.info(
        "Docker plane: SANDBOX_ISOLATION=%r SANDBOX_DOCKER_OCI_RUNTIME=%r -> oci_runtime=%r",
        getattr(config, "SANDBOX_ISOLATION", ""),
        getattr(config, "SANDBOX_DOCKER_OCI_RUNTIME", ""),
        oci,
    )
    if oci:
        logger.info("Sandbox execution: Docker Engine + gVisor (oci_runtime=%s)", oci)
    else:
        logger.info("Sandbox execution: Docker Engine (default OCI runtime, typically runc)")
    return ContainerManager(oci_runtime=oci)
