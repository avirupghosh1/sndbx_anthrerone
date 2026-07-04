"""Configuration for the sandbox data-plane gateway."""

from __future__ import annotations

import os
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


class Config:
    """Runtime gateway / proxy-service settings."""

    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8080"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # E2B-style hostname: ``{port}-{sandbox_id}.{SANDBOX_DOMAIN}``
    SANDBOX_DOMAIN: str = (os.getenv("SANDBOX_DOMAIN") or "sndbx.com").strip().lstrip(".")
    # Debug: ``Host: localhost:8765`` + ``X-Sandbox-Id: sb-…`` (local dev only).
    SANDBOX_INGRESS_DEBUG: bool = _env_bool("SANDBOX_INGRESS_DEBUG", False)

    # Control plane (api-service) for auth + lifecycle checks.
    CONTROL_PLANE_URL: str = (os.getenv("CONTROL_PLANE_URL") or "http://api-service:8000").strip().rstrip("/")
    CONTROL_PLANE_API_KEY: str = (os.getenv("CONTROL_PLANE_API_KEY") or os.getenv("API_KEY") or "").strip()
    INTERNAL_API_KEY: str = (
        os.getenv("INTERNAL_API_KEY")
        or os.getenv("CONTROL_PLANE_API_KEY")
        or os.getenv("API_KEY")
        or ""
    ).strip()
    CONTROL_PLANE_TIMEOUT_SEC: float = max(1.0, float(os.getenv("CONTROL_PLANE_TIMEOUT_SEC", "10")))
    DOCKER_HOST: str = (os.getenv("DOCKER_HOST") or "tcp://127.0.0.1:2375").strip()
    TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC: int = int(os.getenv("TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC", "3600"))
    ENVD_EMBED_AT_TEMPLATE_BUILD: bool = _env_bool("ENVD_EMBED_AT_TEMPLATE_BUILD", True)
    ENVD_DOCKERFILE_RESTORE_USER: str = (os.getenv("ENVD_DOCKERFILE_RESTORE_USER") or "auto").strip()

    # Legacy K8s direct-resolution settings. Runtime-gateway deployments should prefer
    # ``UPSTREAM_RESOLVE_MODE=control_plane`` and trust the route returned by api-service.
    K8S_NAMESPACE: str = (os.getenv("K8S_NAMESPACE") or "sandboxes").strip()
    K8S_POD_SERVICE_TEMPLATE: str = (
        os.getenv("K8S_POD_SERVICE_TEMPLATE") or "sandbox-{sandbox_id}.{namespace}.svc.cluster.local"
    ).strip()

    # ``k8s_dns``: reconstruct K8s pod/service upstreams locally.
    # ``control_plane``: use ``upstream_http`` from api-service ``/internal/.../route``.
    UPSTREAM_RESOLVE_MODE: str = (os.getenv("UPSTREAM_RESOLVE_MODE") or "k8s_dns").strip().lower()

    UPSTREAM_CONNECT_TIMEOUT_SEC: float = max(1.0, float(os.getenv("UPSTREAM_CONNECT_TIMEOUT_SEC", "30")))
    UPSTREAM_WS_OPEN_TIMEOUT_SEC: float = max(5.0, float(os.getenv("UPSTREAM_WS_OPEN_TIMEOUT_SEC", "60")))
    UPSTREAM_WS_CONNECT_RETRIES: int = max(1, int(os.getenv("UPSTREAM_WS_CONNECT_RETRIES", "3")))
    UPSTREAM_WS_RETRY_DELAY_SEC: float = max(0.0, float(os.getenv("UPSTREAM_WS_RETRY_DELAY_SEC", "1.0")))

    # Optional: nginx ingress may inject a separate shared secret header before traffic reaches
    # runtime-gateway. This must never reuse ``X-Access-Token``, which belongs to the guest envd.
    INGRESS_SHARED_TOKEN: str = (os.getenv("INGRESS_SHARED_TOKEN") or "").strip()
    INGRESS_SHARED_TOKEN_HEADER: str = (
        os.getenv("INGRESS_SHARED_TOKEN_HEADER") or "X-Ingress-Access-Token"
    ).strip()
    GATEWAY_INSTANCE_ID: str = (
        os.getenv("GATEWAY_INSTANCE_ID") or os.getenv("HOSTNAME") or "runtime-gateway"
    ).strip()
    DOCKER_GRAPH_PATH: str = (os.getenv("DOCKER_GRAPH_PATH") or "/var/lib/docker").strip()
    DOCKER_GRAPH_CAPACITY_BYTES: int = max(0, int(os.getenv("DOCKER_GRAPH_CAPACITY_BYTES", "0") or "0"))
    TEMPLATE_REGISTRY_PUSH_ENABLED: bool = _env_bool("TEMPLATE_REGISTRY_PUSH_ENABLED", False)
    TEMPLATE_REGISTRY_REPO_PREFIX: str = (
        os.getenv("TEMPLATE_REGISTRY_REPO_PREFIX") or ""
    ).strip().rstrip("/")
    TEMPLATE_REGISTRY_LAYOUT: str = (
        os.getenv("TEMPLATE_REGISTRY_LAYOUT") or "auto"
    ).strip().lower().replace("-", "_")
    TEMPLATE_REGISTRY_SERVER: str = (
        os.getenv("TEMPLATE_REGISTRY_SERVER") or ""
    ).strip().rstrip("/")
    TEMPLATE_REGISTRY_AUTH_REQUIRED: bool = _env_bool("TEMPLATE_REGISTRY_AUTH_REQUIRED", False)
    TEMPLATE_REGISTRY_USERNAME: str = (os.getenv("TEMPLATE_REGISTRY_USERNAME") or "").strip()
    TEMPLATE_REGISTRY_PASSWORD: str = (os.getenv("TEMPLATE_REGISTRY_PASSWORD") or "").strip()


_config: Optional[Config] = None


def get_config() -> Config:
    global _config
    if _config is None:
        _config = Config()
    return _config
