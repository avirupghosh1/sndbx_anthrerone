from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


class KubernetesLeaseClient:
    """Small in-cluster Kubernetes Lease client for warm-pool leader election."""

    def __init__(self, config: Any) -> None:
        self.config = config
        host = (os.getenv("KUBERNETES_SERVICE_HOST") or "").strip()
        port = (os.getenv("KUBERNETES_SERVICE_PORT") or "443").strip()
        self.base_url = (os.getenv("KUBERNETES_SERVICE_URL") or "").strip().rstrip("/")
        if not self.base_url and host:
            self.base_url = f"https://{host}:{port}"
        self.token_path = os.getenv("KUBERNETES_SERVICE_TOKEN_PATH") or _TOKEN_PATH
        self.ca_path = os.getenv("KUBERNETES_SERVICE_CA_PATH") or _CA_PATH
        self.namespace = (
            getattr(config, "WARM_POOL_COORDINATOR_LEASE_NAMESPACE", None)
            or os.getenv("POD_NAMESPACE")
            or getattr(config, "RUNTIME_GATEWAY_NAMESPACE", None)
            or "sandboxes"
        ).strip()
        self.identity = (
            getattr(config, "API_SERVICE_INSTANCE_ID", None)
            or os.getenv("HOSTNAME")
            or "api-service"
        ).strip()
        self.duration_seconds = max(
            5,
            int(getattr(config, "WARM_POOL_COORDINATOR_LEASE_TTL_SEC", 15) or 15),
        )

    def available(self) -> bool:
        return bool(self.base_url and os.path.exists(self.token_path))

    def try_acquire_or_renew(self, name: str) -> bool:
        lease_name = (name or "").strip()
        if not lease_name or not self.available():
            return False

        now_dt = datetime.now(timezone.utc)
        now = _k8s_timestamp(now_dt)
        status, lease = self._request("GET", self._lease_path(lease_name))
        if status == 404:
            return self._create_lease(lease_name, now)
        if status >= 400:
            raise RuntimeError(f"Kubernetes Lease get failed HTTP {status}: {lease}")

        spec = lease.get("spec") if isinstance(lease.get("spec"), dict) else {}
        holder = str(spec.get("holderIdentity") or "").strip()
        renew_time = str(spec.get("renewTime") or "").strip()
        duration = max(1, int(spec.get("leaseDurationSeconds") or self.duration_seconds))
        expired = _lease_expired(renew_time, duration, now_dt)
        if holder and holder != self.identity and not expired:
            return False

        acquire_time = str(spec.get("acquireTime") or now).strip() if holder == self.identity else now
        transitions = int(spec.get("leaseTransitions") or 0)
        if holder and holder != self.identity:
            transitions += 1
        return self._replace_lease(
            lease_name,
            lease,
            acquire_time=acquire_time,
            renew_time=now,
            lease_transitions=transitions,
        )

    def _create_lease(self, name: str, now: str) -> bool:
        body = self._lease_body(
            name,
            resource_version=None,
            acquire_time=now,
            renew_time=now,
            lease_transitions=0,
        )
        status, payload = self._request("POST", self._leases_path(), body=body)
        if status in (200, 201):
            return True
        if status == 409:
            return False
        if status >= 400:
            raise RuntimeError(f"Kubernetes Lease create failed HTTP {status}: {payload}")
        return False

    def _replace_lease(
        self,
        name: str,
        lease: dict[str, Any],
        *,
        acquire_time: str,
        renew_time: str,
        lease_transitions: int,
    ) -> bool:
        metadata = lease.get("metadata") if isinstance(lease.get("metadata"), dict) else {}
        resource_version = str(metadata.get("resourceVersion") or "").strip()
        if not resource_version:
            return False
        body = self._lease_body(
            name,
            resource_version=resource_version,
            acquire_time=acquire_time,
            renew_time=renew_time,
            lease_transitions=lease_transitions,
        )
        status, payload = self._request("PUT", self._lease_path(name), body=body)
        if 200 <= status < 300:
            return True
        if status == 409:
            return False
        if status >= 400:
            raise RuntimeError(f"Kubernetes Lease update failed HTTP {status}: {payload}")
        return False

    def _lease_body(
        self,
        name: str,
        *,
        resource_version: Optional[str],
        acquire_time: str,
        renew_time: str,
        lease_transitions: int,
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {"name": name, "namespace": self.namespace}
        if resource_version:
            metadata["resourceVersion"] = resource_version
        return {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": metadata,
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": self.duration_seconds,
                "acquireTime": acquire_time,
                "renewTime": renew_time,
                "leaseTransitions": max(0, int(lease_transitions)),
            },
        }

    def _leases_path(self) -> str:
        return f"/apis/coordination.k8s.io/v1/namespaces/{quote(self.namespace)}/leases"

    def _lease_path(self, name: str) -> str:
        return f"{self._leases_path()}/{quote(name)}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[dict[str, Any]] = None,
    ) -> tuple[int, dict[str, Any]]:
        import httpx

        token = _read_text(self.token_path).strip()
        headers = {"Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        verify: bool | str = self.ca_path if os.path.exists(self.ca_path) else True
        with httpx.Client(timeout=httpx.Timeout(5.0), verify=verify) as client:
            resp = client.request(
                method.upper(),
                f"{self.base_url}{path}",
                headers=headers,
                content=json.dumps(body).encode("utf-8") if body is not None else None,
            )
        try:
            payload = resp.json() if resp.content else {}
        except Exception:
            payload = {"body": resp.text}
        return resp.status_code, payload if isinstance(payload, dict) else {}


def _lease_expired(renew_time: str, duration_seconds: int, now: datetime) -> bool:
    renewed_at = _parse_k8s_timestamp(renew_time)
    if renewed_at is None:
        return True
    return now >= renewed_at + timedelta(seconds=max(1, int(duration_seconds)))


def _parse_k8s_timestamp(value: str) -> Optional[datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if "." in raw:
        head, tail = raw.split(".", 1)
        zone = ""
        for marker in ("+", "-"):
            idx = tail.find(marker)
            if idx > 0:
                zone = tail[idx:]
                tail = tail[:idx]
                break
        raw = f"{head}.{tail[:6]}{zone}"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _k8s_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()
