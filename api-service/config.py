"""Configuration."""

import json
import os
import sys
from typing import Any, Optional


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


def _normalize_database_type(raw: str, database_url: str) -> str:
    value = (raw or "").strip().lower()
    aliases = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "pg": "postgres",
        "mongo": "mongo",
        "mongodb": "mongo",
    }
    if value:
        if value not in aliases:
            raise RuntimeError("DATABASE_TYPE must be postgres or mongo")
        return aliases[value]
    if database_url.startswith(("mongodb://", "mongodb+srv://")):
        return "mongo"
    if database_url.startswith(("postgres://", "postgresql://")):
        return "postgres"
    raise RuntimeError("DATABASE_TYPE is required when DATABASE_URL does not include a database scheme")


def _require_database_url() -> str:
    value = (os.getenv("DATABASE_URL") or "").strip()
    if not value:
        raise RuntimeError("DATABASE_URL is required and must point to PostgreSQL or MongoDB")
    return value


def _database_backend(database_type: str) -> str:
    return "mongodb" if database_type == "mongo" else "postgresql"


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
    DATABASE_URL: str = _require_database_url()
    DATABASE_TYPE: str = _normalize_database_type(os.getenv("DATABASE_TYPE", ""), DATABASE_URL)
    DATABASE_BACKEND: str = _database_backend(DATABASE_TYPE)
    DATABASE_USERNAME: str = os.getenv("DATABASE_USERNAME", "").strip()
    DATABASE_PASSWORD: str = (
        os.getenv("DATABASE_PASSWORD") or os.getenv("MONGODB_PASSWORD") or ""
    ).strip()
    # Backward-compatible alias used by older callers/secrets.
    MONGODB_PASSWORD: str = DATABASE_PASSWORD

    # Sandbox engine is Docker only. Isolation can be default runc or Docker's runsc/gVisor runtime.
    SANDBOX_ENGINE: str = os.getenv("SANDBOX_ENGINE", "docker").strip().lower()

    # Docker
    DOCKER_HOST: Optional[str] = os.getenv("DOCKER_HOST", None)  # e.g. ssh://user@linux-vm; see docs/REMOTE_SANDBOX_VM.md
    # macOS + Colima (no /var/run/docker.sock): unix://${HOME}/.colima/default/docker.sock; see docs/E2B_DROPIN_TESTING.md
    
    # Sandbox defaults
    DEFAULT_TEMPLATE: str = os.getenv("DEFAULT_TEMPLATE", "python:3.11")
    DEFAULT_CPU_LIMIT: str = os.getenv("DEFAULT_CPU_LIMIT", "1")
    DEFAULT_MEMORY_LIMIT: str = os.getenv("DEFAULT_MEMORY_LIMIT", "512m")
    DEFAULT_TIMEOUT: int = int(os.getenv("DEFAULT_TIMEOUT", 3600))
    SANDBOX_LEASE_REAPER_INTERVAL_SEC: float = max(
        1.0,
        float(os.getenv("SANDBOX_LEASE_REAPER_INTERVAL_SEC", "5")),
    )
    SANDBOX_LOST_RETENTION_SEC: int = max(
        60,
        int(os.getenv("SANDBOX_LOST_RETENTION_SEC", "86400")),
    )
    RUNTIME_EXITED_CONTAINER_RETENTION_SEC: int = max(
        60,
        int(os.getenv("RUNTIME_EXITED_CONTAINER_RETENTION_SEC", "1800")),
    )
    TEMPLATE_IMAGE_RETENTION_SEC: int = max(
        300,
        int(os.getenv("TEMPLATE_IMAGE_RETENTION_SEC", "172800")),
    )
    # Optional S3-backed object storage for SDK template build context archives.
    # When disabled, uploads stay in the configured application database.
    IMAGE_BUILDING_AUTH_REQUIRED: bool = _env_bool("IMAGE_BUILDING_AUTH_REQUIRED", False)
    IMAGE_BUILDING_S3_BUCKET: str = (os.getenv("IMAGE_BUILDING_S3_BUCKET") or "").strip()
    IMAGE_BUILDING_S3_PREFIX: str = (
        os.getenv("IMAGE_BUILDING_S3_PREFIX") or "template-build-contexts"
    ).strip().strip("/")
    IMAGE_BUILDING_S3_REGION: str = (
        os.getenv("IMAGE_BUILDING_S3_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or ""
    ).strip()
    IMAGE_BUILDING_S3_ENDPOINT_URL: str = (
        os.getenv("IMAGE_BUILDING_S3_ENDPOINT_URL") or os.getenv("AWS_S3_ENDPOINT_URL") or ""
    ).strip().rstrip("/")
    IMAGE_BUILDING_S3_ACCESS_KEY_ID: str = (
        os.getenv("IMAGE_BUILDING_S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID") or ""
    ).strip()
    IMAGE_BUILDING_S3_SECRET_ACCESS_KEY: str = (
        os.getenv("IMAGE_BUILDING_S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or ""
    ).strip()
    IMAGE_BUILDING_S3_SESSION_TOKEN: str = (
        os.getenv("IMAGE_BUILDING_S3_SESSION_TOKEN") or os.getenv("AWS_SESSION_TOKEN") or ""
    ).strip()
    if IMAGE_BUILDING_AUTH_REQUIRED:
        _missing_image_building_s3 = [
            name
            for name, value in (
                ("IMAGE_BUILDING_S3_BUCKET", IMAGE_BUILDING_S3_BUCKET),
                ("IMAGE_BUILDING_S3_REGION/AWS_REGION", IMAGE_BUILDING_S3_REGION),
                ("IMAGE_BUILDING_S3_ACCESS_KEY_ID/AWS_ACCESS_KEY_ID", IMAGE_BUILDING_S3_ACCESS_KEY_ID),
                ("IMAGE_BUILDING_S3_SECRET_ACCESS_KEY/AWS_SECRET_ACCESS_KEY", IMAGE_BUILDING_S3_SECRET_ACCESS_KEY),
            )
            if not value
        ]
        if _missing_image_building_s3:
            raise RuntimeError(
                "IMAGE_BUILDING_AUTH_REQUIRED=true requires S3 build context settings: "
                + ", ".join(_missing_image_building_s3)
            )

    # Warm pool: pre-create sandboxes matching this profile for faster POST /sandboxes.
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
    SANDBOX_WARM_POOL_PROVISION_CONCURRENCY: int = max(
        1,
        int(os.getenv("SANDBOX_WARM_POOL_PROVISION_CONCURRENCY", "1")),
    )
    SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC: float = max(
        0.0,
        float(os.getenv("SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC", "12.0")),
    )
    SANDBOX_WARM_POOL_INFLIGHT_STALE_SEC: float = max(
        30.0,
        float(os.getenv("SANDBOX_WARM_POOL_INFLIGHT_STALE_SEC", "300.0")),
    )
    SANDBOX_WARM_POOL_IDLE_POLL_SEC: float = max(
        0.1,
        min(5.0, float(os.getenv("SANDBOX_WARM_POOL_IDLE_POLL_SEC", "0.25"))),
    )

    # Docker ``docker commit`` repository prefix for POST /sandboxes/{id}/snapshot (local image names)
    SANDBOX_SNAPSHOT_REPO: str = os.getenv("SANDBOX_SNAPSHOT_REPO", "mysandbox-snap")

    # One-time custom template build (base image + start_cmd + settle) before ``docker commit``
    TEMPLATE_BUILD_CPU: str = os.getenv("TEMPLATE_BUILD_CPU", "2")
    TEMPLATE_BUILD_MEMORY: str = os.getenv("TEMPLATE_BUILD_MEMORY", "2g")
    # After settle, repeatedly run ``ready_cmd`` (shell) until exit 0 or this timeout (0 = skip).
    TEMPLATE_READY_TIMEOUT_SEC: int = int(os.getenv("TEMPLATE_READY_TIMEOUT_SEC", "600"))
    # ``POST /templates/from-dockerfile``: real Docker Engine build wall-clock cap (``docker_cli`` mode).
    TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC: int = int(os.getenv("TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC", "3600"))
    # ``parsed`` (default): Dockerfile parsed -> exec in build container -> ``docker commit`` + warm snapshot.
    # ``docker_cli``: Docker SDK build against the configured Docker Engine (local or remote ``DOCKER_HOST``).
    TEMPLATE_DOCKERFILE_BUILD_MODE: str = os.getenv("TEMPLATE_DOCKERFILE_BUILD_MODE", "parsed").strip().lower()
    # Runtime-gateway architecture: forward template build execution to the gateway pod, where the
    # resulting images persist in the same Docker graph used for sandbox creation.
    TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY: bool = _env_bool("TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY", True)
    RUNTIME_GATEWAY_URL: str = (
        os.getenv("RUNTIME_GATEWAY_URL") or "http://runtime-gateway.sandboxes.svc.cluster.local:8080"
    ).strip().rstrip("/")
    RUNTIME_GATEWAY_API_KEY: str = (
        os.getenv("RUNTIME_GATEWAY_API_KEY")
        or os.getenv("CONTROL_PLANE_API_KEY")
        or os.getenv("API_KEY")
        or ""
    ).strip()
    RUNTIME_GATEWAY_SHARD_COUNT: int = max(1, int(os.getenv("RUNTIME_GATEWAY_SHARD_COUNT", "1")))
    RUNTIME_GATEWAY_SCHEDULER: str = (os.getenv("RUNTIME_GATEWAY_SCHEDULER") or "round_robin").strip().lower()
    RUNTIME_GATEWAY_HEADLESS_SERVICE: str = (
        os.getenv("RUNTIME_GATEWAY_HEADLESS_SERVICE") or "runtime-gateway-headless"
    ).strip()
    RUNTIME_GATEWAY_NAMESPACE: str = (
        os.getenv("RUNTIME_GATEWAY_NAMESPACE") or "sandboxes"
    ).strip()
    RUNTIME_GATEWAY_STATEFULSET_NAME: str = (
        os.getenv("RUNTIME_GATEWAY_STATEFULSET_NAME") or "runtime-gateway"
    ).strip()
    RUNTIME_GATEWAY_SERVICE_PORT: int = int(os.getenv("RUNTIME_GATEWAY_SERVICE_PORT", "8080"))
    RUNTIME_GATEWAY_TARGETS_JSON: str = (os.getenv("RUNTIME_GATEWAY_TARGETS_JSON") or "").strip()
    API_SERVICE_INSTANCE_ID: str = (os.getenv("API_SERVICE_INSTANCE_ID") or os.getenv("HOSTNAME") or "api-service").strip()
    WARM_POOL_COORDINATOR_LEASE_NAME: str = (
        os.getenv("WARM_POOL_COORDINATOR_LEASE_NAME") or "warm-pool-coordinator"
    ).strip()
    WARM_POOL_COORDINATOR_LEASE_TTL_SEC: int = max(
        5, int(os.getenv("WARM_POOL_COORDINATOR_LEASE_TTL_SEC", "15"))
    )
    RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO: float = min(
        0.99,
        max(0.10, float(os.getenv("RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO", "0.80"))),
    )
    RUNTIME_GATEWAY_STATUS_CACHE_TTL_SEC: float = max(
        0.2,
        float(os.getenv("RUNTIME_GATEWAY_STATUS_CACHE_TTL_SEC", "2.0")),
    )
    WARM_POOL_IMAGE_PREFETCH_ENABLED: bool = _env_bool("WARM_POOL_IMAGE_PREFETCH_ENABLED", True)
    # Per-``RUN`` exec timeout during parsed Dockerfile builds.
    TEMPLATE_DOCKERFILE_RUN_TIMEOUT_SEC: float = float(os.getenv("TEMPLATE_DOCKERFILE_RUN_TIMEOUT_SEC", "7200"))
    # Docker SDK HTTP timeout (``docker commit`` on large images can exceed 60s default).
    TEMPLATE_DOCKER_CLIENT_TIMEOUT_SEC: int = max(
        60, int(os.getenv("TEMPLATE_DOCKER_CLIENT_TIMEOUT_SEC", "600"))
    )

    # Workload isolation / execution:
    # - ``docker`` (default): Linux containers on Docker Engine, default OCI runtime (runc).
    # - ``gvisor`` / ``runsc`` / ``gv``: Docker Engine with ``runsc`` OCI runtime.
    # Non-empty ``SANDBOX_DOCKER_OCI_RUNTIME`` overrides ``SANDBOX_ISOLATION`` for the OCI runtime name.
    SANDBOX_ISOLATION: str = os.getenv("SANDBOX_ISOLATION", "docker").strip().lower()
    SANDBOX_DOCKER_OCI_RUNTIME: str = os.getenv("SANDBOX_DOCKER_OCI_RUNTIME", "").strip()

    def docker_oci_runtime(self) -> Optional[str]:
        """Return ``runsc`` for gVisor-backed sandboxes, or ``None`` for default ``runc``."""
        import logging

        log = logging.getLogger(__name__)
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
        if iso not in ("docker", "runc", "default", "container", "containers"):
            log.warning(
                "SANDBOX_ISOLATION=%r unsupported; use docker/runc or gvisor/runsc. Using default runc.",
                self.SANDBOX_ISOLATION,
            )
        return None

    # --- E2B drop-in (WebSocket agent proxy + traffic token; Docker engine only for WS upstream) ---
    # Shared secret for ``e2b-traffic-access-token`` (set in production; min 16 random bytes recommended).
    E2B_DROPIN_WS_SECRET: str = os.getenv("E2B_DROPIN_WS_SECRET", "").strip()
    # Legacy Docker local-dev flags (not used by runtime-gateway control-plane create/bootstrap).
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

    # --- Envd-style guest daemon (HTTP Phase 1; Docker publish :49983 -> host) ---
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
    # ``control``: lifecycle + metadata only; data-plane traffic goes through ``runtime-gateway``.
    # ``combined``: legacy single-process API + ingress middleware (not used in this deployment).
    API_SERVICE_ROLE: str = (os.getenv("API_SERVICE_ROLE") or "control").strip().lower()

    # --- Data plane (client-facing URLs; resolved by runtime-gateway, not this pod) ---
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
    DAYTONA_OBJECT_STORAGE_URL: str = (os.getenv("DAYTONA_OBJECT_STORAGE_URL") or "").strip().rstrip("/")

    # --- Daytona SSH compatibility gateway ---
    # TCP SSH server in api-service. It authenticates Daytona SSH access tokens
    # and bridges SSH channels to envd PTY sessions; no sshd is required inside
    # the sandbox container.
    DAYTONA_SSH_GATEWAY_ENABLED: bool = _env_bool("DAYTONA_SSH_GATEWAY_ENABLED", True)
    DAYTONA_SSH_GATEWAY_HOST: str = (os.getenv("DAYTONA_SSH_GATEWAY_HOST") or "0.0.0.0").strip() or "0.0.0.0"
    DAYTONA_SSH_GATEWAY_PORT: int = max(
        1,
        min(65535, int(os.getenv("DAYTONA_SSH_GATEWAY_PORT", "2222") or "2222")),
    )
    DAYTONA_SSH_GATEWAY_PUBLIC_HOST: str = (os.getenv("DAYTONA_SSH_GATEWAY_PUBLIC_HOST") or "").strip()
    DAYTONA_SSH_GATEWAY_PUBLIC_PORT: int = max(
        1,
        min(
            65535,
            int(
                os.getenv(
                    "DAYTONA_SSH_GATEWAY_PUBLIC_PORT",
                    os.getenv("DAYTONA_SSH_GATEWAY_PORT", "2222") or "2222",
                )
                or "2222"
            ),
        ),
    )
    # Optional PEM private host key. If unset, api-service generates an ephemeral
    # host key on each boot and the returned SSH command disables known-host writes.
    DAYTONA_SSH_GATEWAY_HOST_KEY: str = (os.getenv("DAYTONA_SSH_GATEWAY_HOST_KEY") or "").strip()
    DAYTONA_SSH_ACCESS_DEFAULT_TTL_MIN: int = max(
        1,
        int(os.getenv("DAYTONA_SSH_ACCESS_DEFAULT_TTL_MIN", "60") or "60"),
    )
    DAYTONA_SSH_ACCESS_MAX_TTL_MIN: int = max(
        1,
        int(os.getenv("DAYTONA_SSH_ACCESS_MAX_TTL_MIN", "1440") or "1440"),
    )

    # Runtime is Docker Engine only. ``runsc``/gVisor is selected via SANDBOX_ISOLATION or
    # SANDBOX_DOCKER_OCI_RUNTIME and executed by runtime-gateway's dockerd sidecar in prod.
    SANDBOX_RUNTIME: str = (os.getenv("SANDBOX_RUNTIME") or "docker").strip().lower()
    # Guest agent listen wait during POST /sandboxes (envd starts in background by default).
    GUEST_BOOTSTRAP_AGENT_WAIT_SEC: float = max(
        1.0, float(os.getenv("GUEST_BOOTSTRAP_AGENT_WAIT_SEC", "8"))
    )
    GUEST_BOOTSTRAP_POLL_SEC: float = max(
        0.05, min(1.0, float(os.getenv("GUEST_BOOTSTRAP_POLL_SEC", "0.1")))
    )
    ENVD_BOOTSTRAP_WAIT_ON_CREATE: bool = _env_bool("ENVD_BOOTSTRAP_WAIT_ON_CREATE", False)
    # Docker/gVisor cold creates get a short create-time readiness budget; if the guest is still
    # booting, data-plane route lookup continues waiting so the first client request does not fail.
    SANDBOX_COLD_CREATE_READY_WAIT_SEC: float = max(
        0.0, float(os.getenv("SANDBOX_COLD_CREATE_READY_WAIT_SEC", "0.5") or "0.5")
    )
    SANDBOX_ROUTE_READY_WAIT_SEC: float = max(
        0.0, float(os.getenv("SANDBOX_ROUTE_READY_WAIT_SEC", "12.0") or "12.0")
    )

    def is_control_plane(self) -> bool:
        return (self.API_SERVICE_ROLE or "control").strip().lower() != "combined"

    def runtime_gateway_targets_json(self) -> list[dict[str, Any]]:
        raw = (self.RUNTIME_GATEWAY_TARGETS_JSON or "").strip()
        if not raw:
            return []
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []

    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def get_config() -> Config:
    """Get configuration."""
    return Config()
