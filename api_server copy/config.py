"""Configuration."""

import os
import sys
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _default_ingress_auto_publish_upstream() -> bool:
    """Docker Desktop (macOS/Windows) cannot reach container bridge IPs from the API host."""
    return sys.platform in ("darwin", "win32")


def _resolve_ingress_auto_publish_upstream() -> bool:
    """Prefer ``SANDBOX_INGRESS_AUTO_PUBLISH_UPSTREAM``; fall back to legacy envd-only flag."""
    if os.getenv("SANDBOX_INGRESS_AUTO_PUBLISH_UPSTREAM") is not None:
        return _env_bool("SANDBOX_INGRESS_AUTO_PUBLISH_UPSTREAM", True)
    if os.getenv("SANDBOX_INGRESS_PUBLISH_ENVD_UPSTREAM") is not None:
        return _env_bool("SANDBOX_INGRESS_PUBLISH_ENVD_UPSTREAM", True)
    return _default_ingress_auto_publish_upstream()


class Config:
    """Application configuration."""

    # Server
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", 8000))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # API
    API_KEY: str = os.getenv("API_KEY", "test-key-12345")
    API_TITLE: str = "Sandbox API Server"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "REST API server for managing sandboxes and agents"
    
    # Database
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "sandboxes.db")

    # Sandbox VM engine: ``docker`` (local dev), ``k8s`` (cluster pods), or ``firecracker``.
    SANDBOX_ENGINE: str = os.getenv("SANDBOX_ENGINE", "k8s").strip().lower()

    # Docker
    DOCKER_HOST: Optional[str] = os.getenv("DOCKER_HOST", None)  # e.g. ssh://user@linux-vm — see docs/REMOTE_SANDBOX_VM.md
    # macOS + Colima (no /var/run/docker.sock): unix://${HOME}/.colima/default/docker.sock — see docs/E2B_DROPIN_TESTING.md
    
    # Sandbox defaults
    DEFAULT_TEMPLATE: str = os.getenv("DEFAULT_TEMPLATE", "python:3.11")
    DEFAULT_CPU_LIMIT: str = os.getenv("DEFAULT_CPU_LIMIT", "1")
    DEFAULT_MEMORY_LIMIT: str = os.getenv("DEFAULT_MEMORY_LIMIT", "512m")
    DEFAULT_TIMEOUT: int = int(os.getenv("DEFAULT_TIMEOUT", 3600))

    # Warm pool: pre-create sandboxes matching this profile for faster POST /sandboxes (Docker or Firecracker engine).
    SANDBOX_WARM_POOL_SIZE: int = int(os.getenv("SANDBOX_WARM_POOL_SIZE", "0"))
    SANDBOX_WARM_POOL_TEMPLATE_ID: str = os.getenv(
        "SANDBOX_WARM_POOL_TEMPLATE_ID",
        os.getenv("DEFAULT_TEMPLATE", "python:3.11"),
    )
    SANDBOX_WARM_POOL_CPU: str = os.getenv("SANDBOX_WARM_POOL_CPU", os.getenv("DEFAULT_CPU_LIMIT", "1"))
    SANDBOX_WARM_POOL_MEMORY: str = os.getenv(
        "SANDBOX_WARM_POOL_MEMORY", os.getenv("DEFAULT_MEMORY_LIMIT", "512m")
    )
    SANDBOX_WARM_POOL_TIMEOUT: int = int(
        os.getenv("SANDBOX_WARM_POOL_TIMEOUT", os.getenv("DEFAULT_TIMEOUT", "3600"))
    )
    # How many sandboxes this process may provision in parallel per warm-pool segment (1 = sequential).
    # Firecracker: keep ≤ ``FIRECRACKER_TAP_SLOTS`` total concurrent boots across segments and cold creates.
    SANDBOX_WARM_POOL_PROVISION_CONCURRENCY: int = max(
        1,
        int(os.getenv("SANDBOX_WARM_POOL_PROVISION_CONCURRENCY", "1")),
    )

    # Docker ``docker commit`` repository prefix for POST /sandboxes/{id}/snapshot (local image names)
    SANDBOX_SNAPSHOT_REPO: str = os.getenv("SANDBOX_SNAPSHOT_REPO", "mysandbox-snap")

    # One-time custom template build (base image + start_cmd + settle) before ``docker commit``
    TEMPLATE_BUILD_CPU: str = os.getenv("TEMPLATE_BUILD_CPU", "2")
    TEMPLATE_BUILD_MEMORY: str = os.getenv("TEMPLATE_BUILD_MEMORY", "2g")
    # After settle, repeatedly run ``ready_cmd`` (shell) until exit 0 or this timeout (0 = skip).
    TEMPLATE_READY_TIMEOUT_SEC: int = int(os.getenv("TEMPLATE_READY_TIMEOUT_SEC", "600"))
    # ``POST /templates/from-dockerfile`` — ``docker build`` wall-clock cap (``docker_cli`` mode only).
    TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC: int = int(os.getenv("TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC", "3600"))
    # ``parsed`` (default): Dockerfile parsed → exec in build container → ``docker commit`` + warm snapshot.
    # ``docker_cli``: legacy ``docker build`` on the API host.
    TEMPLATE_DOCKERFILE_BUILD_MODE: str = os.getenv("TEMPLATE_DOCKERFILE_BUILD_MODE", "parsed").strip().lower()
    # Per-``RUN`` exec timeout during parsed Dockerfile builds.
    TEMPLATE_DOCKERFILE_RUN_TIMEOUT_SEC: float = float(os.getenv("TEMPLATE_DOCKERFILE_RUN_TIMEOUT_SEC", "7200"))
    # Docker SDK HTTP timeout (``docker commit`` on large images can exceed 60s default).
    TEMPLATE_DOCKER_CLIENT_TIMEOUT_SEC: int = max(
        60, int(os.getenv("TEMPLATE_DOCKER_CLIENT_TIMEOUT_SEC", "600"))
    )

    # Workload isolation / execution:
    # - ``docker`` (default): Linux containers on Docker Engine (``SANDBOX_ENGINE=docker``).
    # - ``gvisor`` / ``runsc`` / ``gv``: Docker Engine with ``runsc`` OCI runtime.
    # - ``lima`` / ``colima`` / ``lima-vm``: one **Lima/QEMU VM per sandbox** via ``limactl`` (not Docker
    #   containers). ``SANDBOX_ENGINE`` is ignored when Lima isolation is active (see ``execution_backend``).
    # Non-empty ``SANDBOX_DOCKER_OCI_RUNTIME`` overrides ``SANDBOX_ISOLATION`` for the OCI name (Docker only).
    SANDBOX_ISOLATION: str = os.getenv("SANDBOX_ISOLATION", "docker").strip().lower()
    SANDBOX_DOCKER_OCI_RUNTIME: str = os.getenv("SANDBOX_DOCKER_OCI_RUNTIME", "").strip()

    # --- Lima VM sandboxes (``SANDBOX_ISOLATION=lima|colima``; ``limactl`` on the API host) ---
    LIMACTL_PATH: str = os.getenv("LIMACTL_PATH", "limactl").strip() or "limactl"
    LIMA_SANDBOX_TEMPLATE: str = os.getenv(
        "LIMA_SANDBOX_TEMPLATE",
        "template://ubuntu-24.04",
    ).strip()
    LIMA_CREATE_EXTRA_ARGS: str = os.getenv("LIMA_CREATE_EXTRA_ARGS", "").strip()
    LIMA_START_TIMEOUT_SEC: int = int(os.getenv("LIMA_START_TIMEOUT_SEC", "600"))
    LIMA_SHELL_USE_SUDO: str = os.getenv("LIMA_SHELL_USE_SUDO", "true").strip()

    # When the API runs in Docker, ``limactl`` is usually absent and Lima must not run nested
    # inside the container. Point these at a host (or VM) that has Lima + QEMU installed; the API
    # will run ``ssh user@host limactl …`` instead of calling ``limactl`` locally.
    LIMA_REMOTE_HOST: str = os.getenv("LIMA_REMOTE_HOST", "").strip()
    LIMA_REMOTE_LIMACTL_PATH: str = os.getenv("LIMA_REMOTE_LIMACTL_PATH", "limactl").strip() or "limactl"
    LIMA_REMOTE_SSH_EXTRA_ARGS: str = os.getenv("LIMA_REMOTE_SSH_EXTRA_ARGS", "").strip()

    def use_lima_vm_sandboxes(self) -> bool:
        """True when each sandbox is a Lima/Colima-style VM (``limactl``), not a Docker container."""
        return (self.SANDBOX_ISOLATION or "").strip().lower() in ("lima", "colima", "lima-vm")

    def docker_oci_runtime(self) -> Optional[str]:
        """Return ``runsc`` for gVisor-backed sandboxes, or ``None`` for default ``runc``."""
        import logging

        log = logging.getLogger(__name__)
        if self.use_lima_vm_sandboxes():
            return None
        raw = (self.SANDBOX_DOCKER_OCI_RUNTIME or "").strip().lower()
        if raw:
            if raw == "runsc":
                return "runsc"
            if raw in ("runc", "default", "docker"):
                return None
            log.warning(
                "SANDBOX_DOCKER_OCI_RUNTIME=%r unknown; use runsc, runc, or leave empty. Using default.",
                self.SANDBOX_DOCKER_OCI_RUNTIME,
            )
            return None
        iso = (self.SANDBOX_ISOLATION or "docker").strip().lower()
        if iso in ("gvisor", "runsc", "gv"):
            return "runsc"
        return None

    # --- Firecracker (only when ``SANDBOX_ENGINE=firecracker``; Linux + KVM + tap + SSH rootfs) ---
    FIRECRACKER_BINARY: str = os.getenv("FIRECRACKER_BINARY", "/usr/local/bin/firecracker").strip()
    FIRECRACKER_KERNEL: str = os.getenv("FIRECRACKER_KERNEL", "").strip()
    FIRECRACKER_ROOTFS: str = os.getenv("FIRECRACKER_ROOTFS", "").strip()
    FIRECRACKER_GATEWAY: str = os.getenv("FIRECRACKER_GATEWAY", "172.16.0.1").strip()
    FIRECRACKER_SUBNET_PREFIX: str = os.getenv("FIRECRACKER_SUBNET_PREFIX", "172.16.0").strip()
    FIRECRACKER_GUEST_OCTET_BASE: int = int(os.getenv("FIRECRACKER_GUEST_OCTET_BASE", "10"))
    FIRECRACKER_TAP_PATTERN: str = os.getenv("FIRECRACKER_TAP_PATTERN", "tapfc{slot}").strip()
    FIRECRACKER_TAP_SLOTS: int = int(os.getenv("FIRECRACKER_TAP_SLOTS", "8"))
    FIRECRACKER_SSH_USER: str = os.getenv("FIRECRACKER_SSH_USER", "root").strip()
    FIRECRACKER_SSH_KEY: str = os.getenv("FIRECRACKER_SSH_KEY", "").strip()
    FIRECRACKER_SSH_KNOWN_HOSTS: str = os.getenv("FIRECRACKER_SSH_KNOWN_HOSTS", "/dev/null").strip()
    FIRECRACKER_ENABLE_PCI: str = os.getenv("FIRECRACKER_ENABLE_PCI", "false").strip()
    # Try ``cp --reflink=auto`` on Linux (CoW on btrfs/xfs) before ``shutil.copy2``; big win for large ext4.
    FIRECRACKER_ROOTFS_FAST_COPY: bool = os.getenv("FIRECRACKER_ROOTFS_FAST_COPY", "true").lower() in (
        "1",
        "true",
        "yes",
    )
    # Seconds between SSH readiness probes after InstanceStart (clamped 0.05…2).
    FIRECRACKER_SSH_POLL_SEC: float = max(
        0.05,
        min(2.0, float(os.getenv("FIRECRACKER_SSH_POLL_SEC", "0.25"))),
    )
    # Directory for Firecracker full VM snapshots (``fc-bundle:``); each snapshot is a subfolder.
    FIRECRACKER_SNAPSHOT_DIR: str = os.getenv(
        "FIRECRACKER_SNAPSHOT_DIR",
        os.path.join(os.getcwd(), "fc-snapshots"),
    ).strip()
    # ``POST /templates/from-dockerfile`` under ``SANDBOX_ENGINE=firecracker``: OCI image → host ext4
    # (Docker Engine still required on the API host for build + export).
    FIRECRACKER_DOCKERFILE_ROOTFS_DIR: str = os.getenv(
        "FIRECRACKER_DOCKERFILE_ROOTFS_DIR",
        os.path.join(os.getcwd(), "fc-dockerfile-rootfs"),
    ).strip()
    FIRECRACKER_DOCKERFILE_EXT4_MIN_MB: int = max(256, int(os.getenv("FIRECRACKER_DOCKERFILE_EXT4_MIN_MB", "4096")))
    FIRECRACKER_DOCKERFILE_EXT4_MAX_MB: int = max(
        FIRECRACKER_DOCKERFILE_EXT4_MIN_MB,
        int(os.getenv("FIRECRACKER_DOCKERFILE_EXT4_MAX_MB", "65536")),
    )
    FIRECRACKER_EXT4_BUILDER_IMAGE: str = (os.getenv("FIRECRACKER_EXT4_BUILDER_IMAGE") or "alpine:3.19").strip()
    FIRECRACKER_DOCKERFILE_INJECT_SSH_PUBKEY: bool = _env_bool("FIRECRACKER_DOCKERFILE_INJECT_SSH_PUBKEY", True)

    # --- E2B drop-in (WebSocket agent proxy + traffic token; Docker engine only for WS upstream) ---
    # Shared secret for ``e2b-traffic-access-token`` (set in production; min 16 random bytes recommended).
    E2B_DROPIN_WS_SECRET: str = os.getenv("E2B_DROPIN_WS_SECRET", "").strip()
    # Legacy Docker local-dev flags (not used by control-plane K8s create/bootstrap).
    E2B_DROPIN_AGENT_PORT: int = int(os.getenv("E2B_DROPIN_AGENT_PORT", "8765"))
    E2B_DROPIN_PUBLISH_AGENT_PORT: bool = _env_bool("E2B_DROPIN_PUBLISH_AGENT_PORT", False)
    E2B_DROPIN_AUTO_START_AGENT: bool = _env_bool("E2B_DROPIN_AUTO_START_AGENT", False)
    # Hostname for upstream WS when using a published port (from another container try ``host.docker.internal``).
    E2B_DROPIN_UPSTREAM_WS_HOST: str = (os.getenv("E2B_DROPIN_UPSTREAM_WS_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    # ``websockets.connect`` opening handshake timeout (TCP + HTTP Upgrade); raise if slow network / guest still booting.
    E2B_DROPIN_UPSTREAM_OPEN_TIMEOUT_SEC: float = max(
        5.0,
        float(os.getenv("E2B_DROPIN_UPSTREAM_OPEN_TIMEOUT_SEC", "60")),
    )
    E2B_DROPIN_UPSTREAM_CONNECT_RETRIES: int = max(
        1,
        int(os.getenv("E2B_DROPIN_UPSTREAM_CONNECT_RETRIES", "3")),
    )
    E2B_DROPIN_UPSTREAM_RETRY_DELAY_SEC: float = max(
        0.0,
        float(os.getenv("E2B_DROPIN_UPSTREAM_RETRY_DELAY_SEC", "1.0")),
    )
    E2B_DROPIN_TOKEN_TTL_SEC: int = int(os.getenv("E2B_DROPIN_TOKEN_TTL_SEC", "7200"))
    # Optional override when the API is behind TLS / a gateway (must be ``wss://host`` or ``ws://host`` with no path).
    E2B_DROPIN_PUBLIC_WS_BASE: str = os.getenv("E2B_DROPIN_PUBLIC_WS_BASE", "").strip()

    # --- Envd-style guest daemon (HTTP Phase 1; Docker publish :49983 → host) ---
    ENVD_PUBLISH_PORT: bool = _env_bool("ENVD_PUBLISH_PORT", False)
    ENVD_PORT: int = max(1, min(65535, int(os.getenv("ENVD_PORT", "49983"))))
    ENVD_UPSTREAM_HTTP_HOST: str = (os.getenv("ENVD_UPSTREAM_HTTP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    # When ``ENVD_PUBLISH_PORT`` is on: copy ``envd_guest`` into the new container, pip install, start uvicorn.
    ENVD_AUTO_START: bool = _env_bool("ENVD_AUTO_START", True)
    ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC: float = max(30.0, float(os.getenv("ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC", "300")))
    # Bake ``envd_guest`` into template images at ``POST /templates`` / ``from-dockerfile`` so runtime
    # only starts uvicorn (no per-sandbox pip/tarball). Injects apt/apk install of python3 when missing
    # (e.g. bare ``ubuntu:*``). Set false to skip all embed logic.
    ENVD_EMBED_AT_TEMPLATE_BUILD: bool = _env_bool("ENVD_EMBED_AT_TEMPLATE_BUILD", True)
    # After envd ``COPY``/``RUN`` (as ``USER root``), optionally append ``USER <name>``.
    # ``auto`` (default): infer from the Dockerfile (``USER ubuntu``, ``useradd``/``adduser`` … ``ubuntu``)
    # so Custodian-style templates switch back without per-template env. Use ``none`` to force root,
    # or set a concrete login name to override inference.
    ENVD_DOCKERFILE_RESTORE_USER: str = (os.getenv("ENVD_DOCKERFILE_RESTORE_USER") or "auto").strip()

    # --- Service role (this tree is the control-plane ``api-service`` pod) ---
    # ``control``: lifecycle + metadata only; data-plane traffic goes through ``proxy-service``.
    # ``combined``: legacy single-process API + ingress middleware (not used in this deployment).
    API_SERVICE_ROLE: str = (os.getenv("API_SERVICE_ROLE") or "control").strip().lower()

    # --- Data plane (client-facing URLs; resolved by proxy-service, not this pod) ---
    SANDBOX_DATA_PLANE_ENABLED: bool = _env_bool("SANDBOX_DATA_PLANE_ENABLED", True)
    SANDBOX_DATA_PLANE_DOMAIN: str = (
        os.getenv("SANDBOX_DATA_PLANE_DOMAIN") or "sndbx.com"
    ).strip().lstrip(".")
    SANDBOX_DATA_PLANE_DEBUG: bool = _env_bool("SANDBOX_DATA_PLANE_DEBUG", False)
    SANDBOX_DEFAULT_ALLOW_PUBLIC_TRAFFIC: bool = _env_bool("SANDBOX_DEFAULT_ALLOW_PUBLIC_TRAFFIC", False)
    ENVD_ALWAYS_ON: bool = _env_bool("ENVD_ALWAYS_ON", True)
    # TCP port clients connect to on the proxy (debug: e.g. 8080; prod: 443 or omit).
    SANDBOX_DATA_PLANE_HTTP_PORT: int = int(os.getenv("SANDBOX_DATA_PLANE_HTTP_PORT", "8080"))
    SANDBOX_DATA_PLANE_LISTEN_PORT: Optional[int] = (
        int(os.getenv("SANDBOX_DATA_PLANE_LISTEN_PORT"))
        if os.getenv("SANDBOX_DATA_PLANE_LISTEN_PORT", "").strip()
        else None
    )
    SANDBOX_DATA_PLANE_SCHEME: str = (os.getenv("SANDBOX_DATA_PLANE_SCHEME") or "http").strip().rstrip(":/")

    # --- Kubernetes runtime (Linux cluster; guest port == pod containerPort) ---
    SANDBOX_RUNTIME: str = (os.getenv("SANDBOX_RUNTIME") or "k8s").strip().lower()
    K8S_NAMESPACE: str = (os.getenv("K8S_NAMESPACE") or "sandboxes").strip()
    K8S_POD_SERVICE_TEMPLATE: str = (
        os.getenv("K8S_POD_SERVICE_TEMPLATE") or "sandbox-{sandbox_id}.{namespace}.svc.cluster.local"
    ).strip()
    K8S_POD_READY_TIMEOUT_SEC: float = max(30.0, float(os.getenv("K8S_POD_READY_TIMEOUT_SEC", "180")))
    K8S_IMAGE_PULL_SECRET: str = (os.getenv("K8S_IMAGE_PULL_SECRET") or "").strip()

    def is_k8s_runtime(self) -> bool:
        rt = (self.SANDBOX_RUNTIME or "").strip().lower()
        if rt in ("docker", "runc", "container"):
            return False
        if rt in ("k8s", "kubernetes"):
            return True
        eng = (self.SANDBOX_ENGINE or "").strip().lower()
        return eng in ("k8s", "kubernetes")

    def is_control_plane(self) -> bool:
        return (self.API_SERVICE_ROLE or "control").strip().lower() != "combined"

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def get_config() -> Config:
    """Get configuration."""
    return Config()
