from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


@dataclass(frozen=True)
class RuntimeGatewayPod:
    name: str
    namespace: str
    ordinal: int
    ready: bool
    phase: str
    deletion_timestamp: str
    pod_ip: str
    cpu_millicores: int = 0
    memory_bytes: int = 0


class KubernetesRuntimeGatewayClient:
    def __init__(self, config: Any) -> None:
        self.config = config
        host = (os.getenv("KUBERNETES_SERVICE_HOST") or "").strip()
        port = (os.getenv("KUBERNETES_SERVICE_PORT") or "443").strip()
        self.base_url = (os.getenv("KUBERNETES_SERVICE_URL") or "").strip().rstrip("/")
        if not self.base_url and host:
            self.base_url = f"https://{host}:{port}"
        self.token_path = os.getenv("KUBERNETES_SERVICE_TOKEN_PATH") or _TOKEN_PATH
        self.ca_path = os.getenv("KUBERNETES_SERVICE_CA_PATH") or _CA_PATH

    def available(self) -> bool:
        return bool(self.base_url and os.path.exists(self.token_path))

    def list_runtime_gateway_pods(self) -> List[RuntimeGatewayPod]:
        if not self.available():
            return []
        namespace = _namespace(self.config)
        selector = _label_selector(self.config)
        path = f"/api/v1/namespaces/{quote(namespace)}/pods?labelSelector={quote(selector)}"
        payload = self._request_json("GET", path)
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return []

        metrics = self._pod_metrics_by_name(namespace)
        pods: List[RuntimeGatewayPod] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            meta = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            status = item.get("status") if isinstance(item.get("status"), dict) else {}
            name = str(meta.get("name") or "").strip()
            if not name:
                continue
            metric = metrics.get(name, {})
            pods.append(
                RuntimeGatewayPod(
                    name=name,
                    namespace=namespace,
                    ordinal=_pod_ordinal(name),
                    ready=_pod_ready(status),
                    phase=str(status.get("phase") or ""),
                    deletion_timestamp=str(meta.get("deletionTimestamp") or ""),
                    pod_ip=str(status.get("podIP") or ""),
                    cpu_millicores=int(metric.get("cpu_millicores") or 0),
                    memory_bytes=int(metric.get("memory_bytes") or 0),
                )
            )
        return pods

    def patch_pod_deletion_cost(self, pod_name: str, cost: int) -> bool:
        if not self.available() or not pod_name:
            return False
        namespace = _namespace(self.config)
        body = {
            "metadata": {
                "annotations": {
                    "controller.kubernetes.io/pod-deletion-cost": str(int(cost)),
                    "sndbx.io/runtime-load-cost": str(int(cost)),
                }
            }
        }
        path = f"/api/v1/namespaces/{quote(namespace)}/pods/{quote(pod_name)}"
        try:
            self._request_json("PATCH", path, body=body, content_type="application/merge-patch+json")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("runtime-gateway deletion-cost patch failed pod=%s: %s", pod_name, exc)
            return False

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: Optional[dict[str, Any]] = None,
        content_type: str = "application/json",
    ) -> dict[str, Any]:
        import httpx

        token = _read_text(self.token_path).strip()
        headers = {"Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = content_type
        verify: bool | str = self.ca_path if os.path.exists(self.ca_path) else True
        with httpx.Client(timeout=httpx.Timeout(5.0), verify=verify) as client:
            resp = client.request(
                method.upper(),
                f"{self.base_url}{path}",
                headers=headers,
                content=json.dumps(body).encode("utf-8") if body is not None else None,
            )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def _pod_metrics_by_name(self, namespace: str) -> Dict[str, Dict[str, int]]:
        if not _metrics_enabled(self.config):
            return {}
        try:
            payload = self._request_json(
                "GET",
                f"/apis/metrics.k8s.io/v1beta1/namespaces/{quote(namespace)}/pods?"
                f"labelSelector={quote(_label_selector(self.config))}",
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("runtime-gateway metrics.k8s.io unavailable: %s", exc)
            return {}
        items = payload.get("items") if isinstance(payload, dict) else []
        out: Dict[str, Dict[str, int]] = {}
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, dict):
                continue
            meta = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            name = str(meta.get("name") or "").strip()
            containers = item.get("containers") if isinstance(item.get("containers"), list) else []
            cpu = 0
            mem = 0
            for container in containers:
                usage = container.get("usage") if isinstance(container, dict) else {}
                cpu += _parse_cpu_millicores(str(usage.get("cpu") or "0"))
                mem += _parse_memory_bytes(str(usage.get("memory") or "0"))
            if name:
                out[name] = {"cpu_millicores": cpu, "memory_bytes": mem}
        return out


_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, List[RuntimeGatewayPod]]] = {}


def list_runtime_gateway_pods(config: Any, *, force_refresh: bool = False) -> List[RuntimeGatewayPod]:
    client = KubernetesRuntimeGatewayClient(config)
    if not client.available():
        return []
    ttl = max(0.2, float(getattr(config, "RUNTIME_GATEWAY_POD_DISCOVERY_TTL_SEC", 5.0) or 5.0))
    key = f"{_namespace(config)}:{_label_selector(config)}"
    now = time.time()
    if not force_refresh:
        with _cache_lock:
            cached = _cache.get(key)
            if cached and now - cached[0] <= ttl:
                return list(cached[1])
    pods = client.list_runtime_gateway_pods()
    with _cache_lock:
        _cache[key] = (now, list(pods))
    return pods


def patch_runtime_gateway_deletion_cost(config: Any, pod_name: str, cost: int) -> bool:
    return KubernetesRuntimeGatewayClient(config).patch_pod_deletion_cost(pod_name, cost)


def _namespace(config: Any) -> str:
    return (
        getattr(config, "RUNTIME_GATEWAY_NAMESPACE", None)
        or os.getenv("POD_NAMESPACE")
        or "sandboxes"
    ).strip()


def _label_selector(config: Any) -> str:
    explicit = str(getattr(config, "RUNTIME_GATEWAY_POD_LABEL_SELECTOR", "") or "").strip()
    if explicit:
        return explicit
    app = str(getattr(config, "RUNTIME_GATEWAY_POD_LABEL_APP", "") or "agent-sandbox").strip()
    component = str(getattr(config, "RUNTIME_GATEWAY_POD_LABEL_COMPONENT", "") or "runtime-gateway").strip()
    tier = str(getattr(config, "RUNTIME_GATEWAY_POD_LABEL_TIER", "") or "").strip()
    parts = [f"app={app}", f"component={component}"]
    if tier:
        parts.append(f"tier={tier}")
    return ",".join(parts)


def _metrics_enabled(config: Any) -> bool:
    return bool(getattr(config, "RUNTIME_GATEWAY_POD_METRICS_ENABLED", True))


def _pod_ready(status: dict[str, Any]) -> bool:
    if str(status.get("phase") or "") != "Running":
        return False
    conditions = status.get("conditions") if isinstance(status.get("conditions"), list) else []
    for cond in conditions:
        if not isinstance(cond, dict):
            continue
        if cond.get("type") == "Ready":
            return str(cond.get("status") or "").lower() == "true"
    return False


def _pod_ordinal(name: str) -> int:
    match = re.search(r"-(\d+)$", name or "")
    return int(match.group(1)) if match else 0


def _parse_cpu_millicores(value: str) -> int:
    raw = (value or "0").strip()
    try:
        if raw.endswith("n"):
            return int(float(raw[:-1]) / 1_000_000)
        if raw.endswith("u"):
            return int(float(raw[:-1]) / 1_000)
        if raw.endswith("m"):
            return int(float(raw[:-1]))
        return int(float(raw) * 1000)
    except ValueError:
        return 0


def _parse_memory_bytes(value: str) -> int:
    raw = (value or "0").strip()
    units = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, factor in units.items():
        if raw.endswith(suffix):
            try:
                return int(float(raw[: -len(suffix)]) * factor)
            except ValueError:
                return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()
