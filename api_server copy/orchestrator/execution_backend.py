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

        logger.info(
            "Sandbox execution: Kubernetes Pods (namespace=%s)",
            getattr(config, "K8S_NAMESPACE", "sandboxes"),
        )
        return K8sPodManager()

    engine = (getattr(config, "SANDBOX_ENGINE", None) or "docker").strip().lower()
    if engine in ("firecracker", "fc", "microvm"):
        from .firecracker_plane import FirecrackerVmmPlane

        logger.info("Sandbox execution: Firecracker microVMs (SANDBOX_ENGINE=%s)", engine)
        return FirecrackerVmmPlane(config)

    oci = config.docker_oci_runtime()
    # docker-py ``from_env()`` reads ``DOCKER_HOST`` / TLS env from the process environment.
    # Sync from Config so a single source (env or ``.env`` via ``main``) reliably targets a remote VM daemon.
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
