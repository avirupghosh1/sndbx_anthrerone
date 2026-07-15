from __future__ import annotations

import json
import logging
import time
from urllib.parse import quote
from dataclasses import asdict
from typing import Any, Dict, Iterator, Optional

import httpx

from .container_manager import ContainerConfig

logger = logging.getLogger(__name__)


class RuntimeGatewayControlPlane:
    """Control-plane marker for docker/gVisor sandboxes executed by runtime-gateway.

    This object intentionally does not expose a Docker client. It lets API code keep
    backend-kind decisions while all container/image operations go through a selected
    runtime-gateway shard.
    """

    is_container_like = True

    def __init__(self, *, backend_kind: str = "docker") -> None:
        self._backend_kind = "gvisor" if (backend_kind or "").strip().lower() == "gvisor" else "docker"

    def get_backend_kind(self) -> str:
        return self._backend_kind

    def check_docker(self) -> bool:
        return True

    def describe_docker_unavailable(self) -> Optional[str]:
        return None

    def _unsupported(self) -> None:
        raise RuntimeError("runtime-gateway target must be selected before executing container operations")

    def create_container(self, name: str, config: ContainerConfig) -> Optional[str]:
        self._unsupported()

    def run_command(self, *args, **kwargs) -> Dict[str, Any]:
        self._unsupported()

    def read_file(self, *args, **kwargs) -> Optional[str]:
        self._unsupported()

    def write_file(self, *args, **kwargs) -> bool:
        self._unsupported()

    def list_files(self, *args, **kwargs) -> Optional[list]:
        self._unsupported()

    def delete_file(self, *args, **kwargs) -> bool:
        self._unsupported()

    def create_directory(self, *args, **kwargs) -> bool:
        self._unsupported()

    def get_container_stats(self, *args, **kwargs) -> Optional[Dict[str, Any]]:
        self._unsupported()

    def kill_container(self, *args, **kwargs) -> bool:
        self._unsupported()

    def is_container_running(self, *args, **kwargs) -> bool:
        self._unsupported()

    def pause_instance(self, *args, **kwargs) -> bool:
        self._unsupported()

    def resume_instance(self, *args, **kwargs) -> bool:
        self._unsupported()


class RuntimeGatewayExecution:
    """Execution-plane client backed by runtime-gateway internal HTTP APIs.

    The control plane keeps scheduling/state decisions, while runtime-gateway owns
    Docker credentials, image pulls, and container operations.
    """

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        backend_kind: str = "docker",
        timeout: float = 600.0,
    ) -> None:
        self._api_base = (api_base or "").strip().rstrip("/")
        self._api_key = (api_key or "").strip()
        self._backend_kind = "gvisor" if (backend_kind or "").strip().lower() == "gvisor" else "docker"
        self._timeout = max(1.0, float(timeout))
        self.is_container_like = True

    def get_backend_kind(self) -> str:
        return self._backend_kind

    def _headers(self) -> Dict[str, str]:
        if not self._api_key:
            raise RuntimeError("RUNTIME_GATEWAY_API_KEY is not configured")
        return {"X-API-Key": self._api_key}

    def _request(self, method: str, path: str, *, json_body: Optional[dict] = None) -> dict:
        if not self._api_base:
            raise RuntimeError("runtime-gateway api_base is not configured")
        url = f"{self._api_base}{path}"
        try:
            with httpx.Client(timeout=httpx.Timeout(self._timeout)) as client:
                resp = client.request(method, url, json=json_body, headers=self._headers())
        except httpx.RequestError as exc:
            raise RuntimeError(f"runtime-gateway request failed {method} {path}: {exc}") from exc
        if resp.status_code >= 400:
            detail = resp.text[:1000]
            try:
                data = resp.json()
                detail = str(data.get("detail") or data.get("error") or data)
            except Exception:
                pass
            raise RuntimeError(f"runtime-gateway {method} {path} HTTP {resp.status_code}: {detail}")
        try:
            data = resp.json()
        except Exception as exc:
            raise RuntimeError(f"runtime-gateway {method} {path} returned non-JSON response") from exc
        return data if isinstance(data, dict) else {"data": data}

    def _request_bytes(self, method: str, path: str, *, content: bytes) -> dict:
        if not self._api_base:
            raise RuntimeError("runtime-gateway api_base is not configured")
        url = f"{self._api_base}{path}"
        try:
            with httpx.Client(timeout=httpx.Timeout(self._timeout)) as client:
                resp = client.request(
                    method,
                    url,
                    content=content,
                    headers={**self._headers(), "Content-Type": "application/x-tar"},
                )
        except httpx.RequestError as exc:
            raise RuntimeError(f"runtime-gateway request failed {method} {path}: {exc}") from exc
        if resp.status_code >= 400:
            detail = resp.text[:1000]
            try:
                data = resp.json()
                detail = str(data.get("detail") or data.get("error") or data)
            except Exception:
                pass
            raise RuntimeError(f"runtime-gateway {method} {path} HTTP {resp.status_code}: {detail}")
        try:
            data = resp.json()
        except Exception as exc:
            raise RuntimeError(f"runtime-gateway {method} {path} returned non-JSON response") from exc
        return data if isinstance(data, dict) else {"data": data}

    def _safe(self, default: Any, fn, *args, **kwargs) -> Any:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning("runtime-gateway execution call failed: %s", exc)
            return default

    def check_docker(self) -> bool:
        return bool(self._safe(False, lambda: self._request("GET", "/internal/runtime/docker/check").get("ok")))

    def pull_image(self, image: str) -> bool:
        body = {"image": (image or "").strip()}
        return bool(self._safe(False, lambda: self._request("POST", "/internal/runtime/images/pull", json_body=body).get("ok")))

    def image_exists(self, image_ref: str) -> bool:
        body = {"image": (image_ref or "").strip()}
        return bool(self._safe(False, lambda: self._request("POST", "/internal/runtime/images/exists", json_body=body).get("exists")))

    def registry_image_exists(self, image_ref: str) -> bool:
        body = {"image": (image_ref or "").strip(), "timeout": 60}
        return bool(
            self._safe(
                False,
                lambda: self._request("POST", "/internal/runtime/images/registry-exists", json_body=body).get("exists"),
            )
        )

    def push_image_to_registry(self, image_ref: str, template_id: str, timeout: int = 600) -> Optional[str]:
        body = {
            "image": (image_ref or "").strip(),
            "template_id": (template_id or "").strip(),
            "timeout": int(timeout),
        }
        data = self._safe({}, self._request, "POST", "/internal/runtime/images/push", json_body=body)
        ref = str(data.get("registry_image_ref") or "").strip() if isinstance(data, dict) and data.get("ok") else ""
        return ref or None

    def create_container(self, name: str, config: ContainerConfig) -> Optional[str]:
        payload = {"name": name, "config": asdict(config)}
        data = self._safe({}, self._request, "POST", "/internal/runtime/containers/create", json_body=payload)
        cid = str(data.get("container_id") or "").strip() if isinstance(data, dict) else ""
        return cid or None

    def run_command(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "command": command,
            "cwd": cwd,
            "env": env,
            "timeout": timeout,
            "user": user,
        }
        data = self._safe(
            {"exit_code": -1, "stdout": "", "stderr": "runtime-gateway exec failed", "pid": -1},
            self._request,
            "POST",
            f"/internal/runtime/containers/{container_id}/exec",
            json_body=payload,
        )
        return data if isinstance(data, dict) else {"exit_code": -1, "stdout": "", "stderr": "", "pid": -1}

    def run_command_stream(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        payload = {
            "command": command,
            "cwd": cwd,
            "env": env,
            "timeout": timeout,
            "user": user,
        }
        url = f"{self._api_base}/internal/runtime/containers/{container_id}/exec/stream"
        try:
            with httpx.Client(timeout=httpx.Timeout(self._timeout)) as client:
                with client.stream("POST", url, json=payload, headers=self._headers()) as resp:
                    if resp.status_code >= 400:
                        yield {"type": "error", "message": resp.text[:1000]}
                        yield {"type": "exit", "exit_code": -1}
                        return
                    for line in resp.iter_lines():
                        if not line.startswith("data: "):
                            continue
                        raw = line[6:].strip()
                        if not raw:
                            continue
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(data, dict):
                            yield data
        except Exception as exc:  # noqa: BLE001
            yield {"type": "error", "message": str(exc)}
            yield {"type": "exit", "exit_code": -1}

    def read_file(self, container_id: str, path: str) -> Optional[str]:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/files/read", json_body={"path": path})
        return data.get("content") if isinstance(data, dict) and data.get("ok") else None

    def write_file(self, container_id: str, path: str, content: str) -> bool:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/files/write", json_body={"path": path, "content": content})
        return bool(isinstance(data, dict) and data.get("ok"))

    def list_files(self, container_id: str, path: str = "/") -> Optional[list]:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/files/list", json_body={"path": path})
        return data.get("entries") if isinstance(data, dict) and data.get("ok") else None

    def delete_file(self, container_id: str, path: str, recursive: bool = False) -> bool:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/files/delete", json_body={"path": path, "recursive": recursive})
        return bool(isinstance(data, dict) and data.get("ok"))

    def create_directory(self, container_id: str, path: str, mode: int = 0o755) -> bool:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/files/mkdir", json_body={"path": path, "mode": mode})
        return bool(isinstance(data, dict) and data.get("ok"))

    def put_archive_to_container(self, container_id: str, path: str, data: bytes) -> bool:
        dest = quote((path or "/").strip() or "/", safe="")
        result = self._safe(
            {},
            self._request_bytes,
            "POST",
            f"/internal/runtime/containers/{container_id}/files/archive?path={dest}",
            content=data or b"",
        )
        return bool(isinstance(result, dict) and result.get("ok"))

    def get_container_stats(self, container_id: str) -> Optional[Dict[str, Any]]:
        data = self._safe({}, self._request, "GET", f"/internal/runtime/containers/{container_id}/stats")
        return data.get("stats") if isinstance(data, dict) and data.get("ok") else None

    def commit_filesystem_snapshot(self, container_id: str, repository: str, tag: str, *, pause_during_commit: bool = True) -> Optional[str]:
        data = self._safe(
            {},
            self._request,
            "POST",
            f"/internal/runtime/containers/{container_id}/commit",
            json_body={"repository": repository, "tag": tag, "pause_during_commit": pause_during_commit},
        )
        ref = str(data.get("image_ref") or "").strip() if isinstance(data, dict) else ""
        return ref or None

    def kill_container(self, container_id: str, force: bool = True) -> bool:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/kill", json_body={"force": force})
        return bool(isinstance(data, dict) and data.get("ok"))

    def get_container_internal_ipv4(self, container_id: str) -> Optional[str]:
        data = self._safe({}, self._request, "GET", f"/internal/runtime/containers/{container_id}/network")
        ip = str(data.get("internal_ipv4") or "").strip() if isinstance(data, dict) else ""
        return ip or None

    def get_container_tcp_host_port(self, container_id: str, container_port: int) -> Optional[int]:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/ports", json_body={"container_port": int(container_port)})
        try:
            return int(data.get("host_port")) if isinstance(data, dict) and data.get("host_port") else None
        except (TypeError, ValueError):
            return None

    def image_start_cmd_shell(self, image_ref: str) -> str:
        data = self._safe({}, self._request, "POST", "/internal/runtime/images/metadata", json_body={"image": image_ref})
        return str(data.get("start_cmd_shell") or "") if isinstance(data, dict) else ""

    def image_env_dict(self, image_ref: str) -> Dict[str, str]:
        data = self._safe({}, self._request, "POST", "/internal/runtime/images/metadata", json_body={"image": image_ref})
        env = data.get("env") if isinstance(data, dict) else {}
        return env if isinstance(env, dict) else {}

    def image_default_user(self, image_ref: str) -> str:
        data = self._safe({}, self._request, "POST", "/internal/runtime/images/metadata", json_body={"image": image_ref})
        return str(data.get("default_user") or "root") if isinstance(data, dict) else "root"

    def pause_instance(self, container_id: str) -> bool:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/pause")
        return bool(isinstance(data, dict) and data.get("ok"))

    def resume_instance(self, container_id: str) -> bool:
        data = self._safe({}, self._request, "POST", f"/internal/runtime/containers/{container_id}/resume")
        return bool(isinstance(data, dict) and data.get("ok"))

    def is_container_running(self, container_id: str) -> bool:
        return self.get_container_state(container_id) == "running"

    def get_container_state(self, container_id: str) -> str:
        data = self._safe({}, self._request, "GET", f"/internal/runtime/containers/{container_id}/state")
        return str(data.get("state") or "unknown") if isinstance(data, dict) else "unknown"

    def prune_exited_containers(self, older_than_seconds: int) -> int:
        data = self._safe({}, self._request, "POST", "/internal/runtime/prune/containers", json_body={"older_than_seconds": int(older_than_seconds)})
        return int(data.get("removed") or 0) if isinstance(data, dict) else 0

    def prune_generated_images(self, *, keep_refs: set[str], older_than_seconds: int, repo_prefixes: list[str]) -> int:
        data = self._safe(
            {},
            self._request,
            "POST",
            "/internal/runtime/prune/images",
            json_body={
                "keep_refs": sorted(str(x) for x in keep_refs),
                "older_than_seconds": int(older_than_seconds),
                "repo_prefixes": list(repo_prefixes),
            },
        )
        return int(data.get("removed") or 0) if isinstance(data, dict) else 0
