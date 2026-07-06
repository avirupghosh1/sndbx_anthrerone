"""Construct the sandbox execution plane: Docker, Firecracker, or Lima VMs."""

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
    """Return ``LimaVmPlane``, ``FirecrackerVmmPlane``, or Docker ``ContainerManager``."""
    if config is None:
        from config import get_config

        config = get_config()

    if config.use_lima_vm_sandboxes():
        from .lima_plane import LimaVmPlane

        iso = (getattr(config, "SANDBOX_ISOLATION", "") or "").strip().lower()
        eng = (getattr(config, "SANDBOX_ENGINE", None) or "docker").strip().lower()
        if eng in ("firecracker", "fc", "microvm"):
            logger.warning(
                "SANDBOX_ISOLATION=%s selects Lima VMs; ignoring SANDBOX_ENGINE=%s",
                iso,
                eng,
            )
        logger.info("Sandbox execution: Lima VMs (SANDBOX_ISOLATION=%s)", iso)
        return LimaVmPlane(config)

    if config.is_k8s_runtime():
        from .k8s_pod_manager import K8sPodManager

        oci = config.docker_oci_runtime()
        logger.info(
            "Sandbox execution: Kubernetes Pods (namespace=%s, oci_runtime=%s)",
            getattr(config, "K8S_NAMESPACE", "sandboxes"),
            oci,
        )
        return K8sPodManager(oci_runtime=oci)

    engine = (getattr(config, "SANDBOX_ENGINE", None) or "docker").strip().lower()
    if engine in ("firecracker", "fc", "microvm"):
        from .firecracker_plane import FirecrackerVmmPlane

        logger.info("Sandbox execution: Firecracker microVMs (SANDBOX_ENGINE=%s)", engine)
        return FirecrackerVmmPlane(config)

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
