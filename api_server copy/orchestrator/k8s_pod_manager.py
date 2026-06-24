"""Kubernetes Pod + headless Service sandbox execution (same API surface as ContainerManager)."""

from __future__ import annotations

import base64
import logging
import re
import shlex
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Dict, List, Optional

from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from .container_manager import ContainerConfig

logger = logging.getLogger(__name__)

SANDBOX_CONTAINER_NAME = "sandbox"


def _valid_port_int(value: Any) -> bool:
    try:
        p = int(value)
    except (TypeError, ValueError):
        return False
    return 1 <= p <= 65535


def _dns_safe_name(raw: str, *, max_len: int = 63) -> str:
    s = re.sub(r"[^a-z0-9-]", "-", (raw or "").strip().lower())
    s = re.sub(r"-+", "-", s).strip("-")
    if not s:
        s = "sandbox"
    return s[:max_len].rstrip("-")


def _memory_to_k8s(limit: str) -> str:
    raw = (limit or "512m").strip()
    if raw.lower().endswith("mi") or raw.lower().endswith("gi"):
        return raw
    m = re.match(r"^(\d+(?:\.\d+)?)([mMgG])?$", raw)
    if not m:
        return "512Mi"
    val, unit = m.group(1), (m.group(2) or "m").lower()
    if unit == "g":
        return f"{val}Gi"
    return f"{val}Mi"


def _cpu_to_k8s(limit: str) -> str:
    raw = (limit or "1").strip()
    if raw.endswith("m"):
        return raw
    try:
        f = float(raw)
        if f < 1:
            return f"{int(f * 1000)}m"
        return str(f) if "." in raw else raw
    except ValueError:
        return "1"


def _sandbox_id_from_pod_name(pod_name: str) -> str:
    name = (pod_name or "").strip()
    if name.startswith("sandbox-"):
        return name[len("sandbox-") :]
    return name


class K8sPodManager:
    """Creates one Pod + headless Service per sandbox; guest ports == containerPort (no host publish)."""

    def __init__(self, oci_runtime: Optional[str] = None) -> None:
        from config import get_config

        self._cfg = get_config()
        self._oci_runtime = oci_runtime
        self._namespace = (getattr(self._cfg, "K8S_NAMESPACE", None) or "sandboxes").strip()
        self._core: Optional[client.CoreV1Api] = None
        self._k8s_error: Optional[str] = None
        self._docker_inspect = None
        self._ensure_k8s_client()

    def _ensure_k8s_client(self) -> bool:
        if self._core is not None:
            return True
        try:
            try:
                k8s_config.load_incluster_config()
                logger.info("Kubernetes client: in-cluster config")
            except k8s_config.ConfigException:
                k8s_config.load_kube_config()
                logger.info("Kubernetes client: kubeconfig")
            self._core = client.CoreV1Api()
            self._k8s_error = None
            return True
        except Exception as exc:  # noqa: BLE001
            self._k8s_error = f"{type(exc).__name__}: {exc}"
            logger.warning("Kubernetes client init failed: %s", self._k8s_error)
            self._core = None
            return False

    def _optional_docker(self):
        if self._docker_inspect is not None:
            return self._docker_inspect
        try:
            import docker

            dh = (getattr(self._cfg, "DOCKER_HOST", None) or "").strip()
            if dh:
                import os

                os.environ["DOCKER_HOST"] = dh
            self._docker_inspect = docker.from_env(timeout=30)
        except Exception:  # noqa: BLE001
            self._docker_inspect = False
        return self._docker_inspect if self._docker_inspect is not False else None

    def get_backend_kind(self) -> str:
        return "k8s"

    def check_docker(self) -> bool:
        return self._ensure_k8s_client()

    def describe_docker_unavailable(self) -> Optional[str]:
        if not self._ensure_k8s_client():
            return f"Kubernetes API unavailable: {self._k8s_error or 'unknown'}"
        try:
            self._core.list_namespaced_pod(self._namespace, limit=1)
        except ApiException as exc:
            return f"Kubernetes API error: HTTP {exc.status} {exc.reason}"
        except Exception as exc:  # noqa: BLE001
            return f"Kubernetes API error: {type(exc).__name__}: {exc}"
        return None

    def close(self) -> None:
        self._core = None

    def _service_name(self, pod_name: str) -> str:
        return _dns_safe_name(pod_name)

    def _wait_pod_running(self, pod_name: str, *, timeout_sec: float = 180.0) -> bool:
        assert self._core is not None
        poll = float(getattr(self._cfg, "K8S_POD_READY_POLL_SEC", 0.2) or 0.2)
        poll = max(0.1, min(2.0, poll))
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                pod = self._core.read_namespaced_pod(pod_name, self._namespace)
            except ApiException:
                time.sleep(poll)
                continue
            phase = (pod.status.phase or "").strip()
            if phase == "Running":
                statuses = pod.status.container_statuses or []
                if statuses and all(s.ready for s in statuses):
                    return True
            if phase in ("Failed", "Succeeded"):
                logger.error("Pod %s entered terminal phase %s", pod_name, phase)
                return False
            time.sleep(poll)
        logger.error("Pod %s not ready within %.0fs", pod_name, timeout_sec)
        return False

    def _wait_pod_phase_running(self, pod_name: str, *, timeout_sec: float = 180.0) -> bool:
        assert self._core is not None
        poll = float(getattr(self._cfg, "K8S_POD_READY_POLL_SEC", 0.2) or 0.2)
        poll = max(0.1, min(2.0, poll))
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                pod = self._core.read_namespaced_pod(pod_name, self._namespace)
            except ApiException:
                time.sleep(poll)
                continue
            phase = (pod.status.phase or "").strip()
            if phase == "Running":
                return True
            if phase in ("Failed", "Succeeded"):
                logger.error("Pod %s entered terminal phase %s", pod_name, phase)
                return False
            time.sleep(poll)
        logger.error("Pod %s not running within %.0fs", pod_name, timeout_sec)
        return False

    def _ensure_headless_service(self, pod_name: str, sandbox_id: str, ports: List[int]) -> bool:
        assert self._core is not None
        svc_name = self._service_name(pod_name)
        port_specs = [
            client.V1ServicePort(name=f"p{p}", port=int(p), target_port=int(p), protocol="TCP")
            for p in ports
        ]
        svc = client.V1Service(
            metadata=client.V1ObjectMeta(
                name=svc_name,
                labels={"app": "sandbox", "sandbox-id": sandbox_id},
            ),
            spec=client.V1ServiceSpec(
                cluster_ip="None",
                selector={"sandbox-id": sandbox_id},
                ports=port_specs,
            ),
        )
        try:
            self._core.create_namespaced_service(self._namespace, svc)
            logger.info("Created headless Service %s for sandbox %s", svc_name, sandbox_id)
            return True
        except ApiException as exc:
            if exc.status == 409:
                return True
            logger.error("Service create failed %s: %s", svc_name, exc)
            return False

    def create_container(self, name: str, config: ContainerConfig) -> Optional[str]:
        if not self._ensure_k8s_client():
            return None

        pod_name = _dns_safe_name(name)
        sandbox_id = _sandbox_id_from_pod_name(pod_name)
        envd_port = max(1, min(65535, int(getattr(config, "envd_port", 49983))))
        guest_ports = sorted(
            {
                max(1, min(65535, int(p)))
                for p in (config.guest_ports or [])
                if _valid_port_int(p)
            }
        )

        env_list = []
        for k, v in (config.environment or {}).items():
            env_list.append(client.V1EnvVar(name=str(k), value=str(v)))

        pull_secret = (getattr(self._cfg, "K8S_IMAGE_PULL_SECRET", None) or "").strip()
        image_pull_secrets = (
            [client.V1LocalObjectReference(name=pull_secret)] if pull_secret else None
        )
        pull_policy = (getattr(self._cfg, "K8S_SANDBOX_IMAGE_PULL_POLICY", None) or "Never").strip()
        startup_cmd = list(config.startup_command or []) or None
        readiness_port = int(config.readiness_tcp_port or 0) if config.readiness_tcp_port else 0
        readiness_probe = None
        if 1 <= readiness_port <= 65535:
            readiness_probe = client.V1Probe(
                tcp_socket=client.V1TCPSocketAction(port=readiness_port),
                initial_delay_seconds=0,
                period_seconds=1,
                timeout_seconds=1,
                failure_threshold=120,
            )

        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=pod_name,
                labels={
                    "app": "sandbox",
                    "sandbox-id": sandbox_id,
                },
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                image_pull_secrets=image_pull_secrets,
                runtime_class_name="gvisor" if self._oci_runtime == "runsc" else None,
                containers=[
                    client.V1Container(
                        name=SANDBOX_CONTAINER_NAME,
                        image=config.image,
                        image_pull_policy=pull_policy or None,
                        command=startup_cmd or ["/bin/bash"],
                        stdin=True,
                        tty=True,
                        env=env_list or None,
                        readiness_probe=readiness_probe,
                        ports=[
                            client.V1ContainerPort(container_port=int(p), name=f"p{int(p)}")
                            for p in guest_ports
                        ]
                        or None,
                        resources=client.V1ResourceRequirements(
                            limits={
                                "cpu": _cpu_to_k8s(config.cpu_limit),
                                "memory": _memory_to_k8s(config.memory_limit),
                            },
                            requests={
                                "cpu": _cpu_to_k8s(config.cpu_limit),
                                "memory": _memory_to_k8s(config.memory_limit),
                            },
                        ),
                    )
                ],
            ),
        )

        assert self._core is not None
        try:
            self._core.create_namespaced_pod(self._namespace, pod)
            logger.info("Created Pod %s image=%s sandbox_id=%s", pod_name, config.image, sandbox_id)
        except ApiException as exc:
            logger.error("Pod create failed %s: %s", pod_name, exc)
            return None

        timeout_sec = float(getattr(self._cfg, "K8S_POD_READY_TIMEOUT_SEC", 180.0) or 180.0)
        if startup_cmd:
            ready_ok = self._wait_pod_phase_running(pod_name, timeout_sec=timeout_sec)
        else:
            ready_ok = self._wait_pod_running(pod_name, timeout_sec=timeout_sec)
        if not ready_ok:
            self.kill_container(pod_name, force=True)
            return None

        if not self._ensure_headless_service(pod_name, sandbox_id, guest_ports):
            if guest_ports:
                self.kill_container(pod_name, force=True)
                return None

        return pod_name

    def run_command(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self._ensure_k8s_client():
            return {"exit_code": -1, "stdout": "", "stderr": "Kubernetes client unavailable", "pid": -1}

        pod_name = (container_id or "").strip()
        exec_cmd = ["/bin/sh", "-c", command]
        if cwd and cwd != "/":
            exec_cmd = ["/bin/sh", "-c", f"cd {shlex.quote(cwd)} && {command}"]
        if user == "root":
            inner = exec_cmd[-1]
            inner_q = shlex.quote(inner)
            exec_cmd = [
                "/bin/sh",
                "-c",
                (
                    'if [ "$(id -u)" = "0" ]; then '
                    f"exec /bin/sh -c {inner_q}; "
                    "elif command -v sudo >/dev/null 2>&1; then "
                    f"exec sudo -E -n sh -c {inner_q}; "
                    "else "
                    "echo 'root exec requested but sudo is unavailable and current user is not root' >&2; "
                    "exit 1; "
                    "fi"
                ),
            ]
        elif user:
            inner = exec_cmd[-1]
            target = shlex.quote(user)
            inner_q = shlex.quote(inner)
            exec_cmd = [
                "/bin/sh",
                "-c",
                (
                    f'if [ "$(id -un)" = {target} ]; then '
                    f"exec /bin/sh -c {inner_q}; "
                    "elif command -v sudo >/dev/null 2>&1; then "
                    f"exec sudo -E -n -u {target} /bin/sh -c {inner_q}; "
                    "else "
                    f"exec su -m -s /bin/sh {target} -c {inner_q}; "
                    "fi"
                ),
            ]

        def _exec() -> tuple[int, str, str]:
            # Use a dedicated ApiClient to avoid polluting the shared connection pool with WebSocket upgrades
            from kubernetes import client
            core_isolated = client.CoreV1Api(api_client=client.ApiClient())
            try:
                resp = stream(
                    core_isolated.connect_get_namespaced_pod_exec,
                    pod_name,
                    self._namespace,
                    command=exec_cmd,
                    container=SANDBOX_CONTAINER_NAME,
                    stderr=True,
                    stdin=False,
                    stdout=True,
                    tty=False,
                    _preload_content=False,
                )
                stdout_parts: list[str] = []
                stderr_parts: list[str] = []
                while resp.is_open():
                    resp.update(timeout=1)
                    if resp.peek_stdout():
                        stdout_parts.append(resp.read_stdout())
                    if resp.peek_stderr():
                        stderr_parts.append(resp.read_stderr())
                code = resp.returncode if resp.returncode is not None else 0
                return int(code), "".join(stdout_parts), "".join(stderr_parts)
            finally:
                core_isolated.api_client.close()

        exec_timeout = float(timeout) if timeout is not None else 30.0
        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(_exec)
            try:
                code, stdout, stderr = fut.result(timeout=exec_timeout)
            except FuturesTimeout:
                return {
                    "exit_code": 124,
                    "stdout": "",
                    "stderr": f"Command timed out after {exec_timeout} seconds",
                    "pid": -1,
                }
            except ApiException as exc:
                return {"exit_code": -1, "stdout": "", "stderr": str(exc), "pid": -1}

        return {"exit_code": code, "stdout": stdout, "stderr": stderr, "pid": code}

    def put_archive_to_container(self, container_id: str, path: str, data: bytes) -> bool:
        if not data:
            return bool(self.run_command(container_id, f"mkdir -p {shlex.quote(path)}").get("exit_code") == 0)
        dest = (path or "/").rstrip("/") or "/"
        b64 = base64.b64encode(data).decode("ascii")
        chunk = 48000
        tmp = f"/tmp/sandbox-upload-{int(time.time())}.tar"
        script = (
            f"set -eu\nmkdir -p {shlex.quote(dest)}\n"
            f"rm -f {shlex.quote(tmp)}\n"
            f"touch {shlex.quote(tmp)}\n"
        )
        for i in range(0, len(b64), chunk):
            part = b64[i : i + chunk]
            script += f"printf '%s' {shlex.quote(part)} >> {shlex.quote(tmp)}\n"
        script += (
            f"base64 -d {shlex.quote(tmp)} | tar xf - -C {shlex.quote(dest)}\n"
            f"rm -f {shlex.quote(tmp)}\n"
        )
        r = self.run_command(container_id, script, timeout=300.0, user="root")
        return int(r.get("exit_code") or 0) == 0

    def read_file(self, container_id: str, path: str) -> Optional[str]:
        r = self.run_command(container_id, f"cat {shlex.quote(path)}", timeout=60.0)
        if int(r.get("exit_code") or 0) != 0:
            return None
        return r.get("stdout") or ""

    def write_file(self, container_id: str, path: str, content: str) -> bool:
        parent = str(path).rsplit("/", 1)[0] or "/"
        b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
        script = (
            f"set -eu\nmkdir -p {shlex.quote(parent)}\n"
            f"printf '%s' {shlex.quote(b64)} | base64 -d > {shlex.quote(path)}\n"
        )
        return int(self.run_command(container_id, script, timeout=120.0).get("exit_code") or 0) == 0

    def list_files(self, container_id: str, path: str = "/") -> Optional[list]:
        from pathlib import PurePosixPath
        r = self.run_command(container_id, f"ls -la {shlex.quote(path)}", timeout=30.0)
        if int(r.get("exit_code") or 0) != 0:
            return None
        
        output = r.get("stdout") or ""
        base = path.rstrip("/") or "/"
        entries = []
        for line in output.splitlines():
            line = line.strip()
            if not line or line.startswith("total "):
                continue
            parts = line.split()
            if len(parts) < 9:
                continue
            perms = parts[0]
            name = " ".join(parts[8:])
            if name in (".", ".."):
                continue
            try:
                size = int(parts[4])
            except ValueError:
                size = 0
            modified_at = " ".join(parts[5:8])
            full_path = str(PurePosixPath(base) / name)
            if perms.startswith("d"):
                typ = "directory"
            elif perms.startswith("l"):
                typ = "symlink"
            else:
                typ = "file"
            entries.append({
                "path": full_path,
                "name": name,
                "type": typ,
                "size": size,
                "permissions": perms,
                "modified_at": modified_at,
            })
        return entries

    def delete_file(self, container_id: str, path: str, recursive: bool = False) -> bool:
        flag = "-rf" if recursive else "-f"
        r = self.run_command(container_id, f"rm {flag} {shlex.quote(path)}", timeout=30.0)
        return int(r.get("exit_code") or 0) == 0

    def create_directory(self, container_id: str, path: str, mode: int = 0o755) -> bool:
        r = self.run_command(container_id, f"mkdir -p {shlex.quote(path)}", timeout=30.0)
        return int(r.get("exit_code") or 0) == 0

    def get_container_stats(self, container_id: str) -> Optional[Dict[str, Any]]:
        return {"status": "running" if self.is_container_running(container_id) else "unknown"}

    def kill_container(self, container_id: str, force: bool = True) -> bool:
        if not self._ensure_k8s_client():
            return False
        pod_name = (container_id or "").strip()
        svc_name = self._service_name(pod_name)
        assert self._core is not None
        ok = True
        for fn, name in (
            (self._core.delete_namespaced_service, svc_name),
            (self._core.delete_namespaced_pod, pod_name),
        ):
            try:
                fn(name, self._namespace, grace_period_seconds=0 if force else 30)
            except ApiException as exc:
                if exc.status != 404:
                    logger.warning("Delete %s failed: %s", name, exc)
                    ok = False
        return ok

    def is_container_running(self, container_id: str) -> bool:
        if not self._ensure_k8s_client():
            return False
        assert self._core is not None
        try:
            pod = self._core.read_namespaced_pod((container_id or "").strip(), self._namespace)
        except ApiException:
            return False
        return (pod.status.phase or "").strip() == "Running"

    def pause_instance(self, container_id: str) -> bool:
        r = self.run_command(container_id, "kill -STOP 1", timeout=10.0, user="root")
        return int(r.get("exit_code") or 0) == 0

    def resume_instance(self, container_id: str) -> bool:
        r = self.run_command(container_id, "kill -CONT 1", timeout=10.0, user="root")
        return int(r.get("exit_code") or 0) == 0

    def get_container_internal_ipv4(self, container_id: str) -> Optional[str]:
        if not self._ensure_k8s_client():
            return None
        assert self._core is not None
        try:
            pod = self._core.read_namespaced_pod((container_id or "").strip(), self._namespace)
        except ApiException:
            return None
        return (pod.status.pod_ip or "").strip() or None

    def get_container_tcp_host_port(self, container_id: str, container_port: int) -> Optional[int]:
        """K8s: guest port is exposed 1:1 on the pod Service (no host remapping)."""
        if not self.is_container_running(container_id):
            return None
        p = max(1, min(65535, int(container_port)))
        return p

    def image_start_cmd_shell(self, image_ref: str) -> str:
        dc = self._optional_docker()
        if not dc:
            return ""
        try:
            img = dc.images.get(image_ref)
            cfg = img.attrs.get("Config") or {}
            cmd = cfg.get("Cmd") or []
            entry = cfg.get("Entrypoint") or []
            parts = list(entry) + list(cmd)
            return " ".join(shlex.quote(str(x)) for x in parts if x)
        except Exception:  # noqa: BLE001
            return ""

    def image_env_dict(self, image_ref: str) -> Dict[str, str]:
        dc = self._optional_docker()
        if not dc:
            return {}
        try:
            img = dc.images.get(image_ref)
            cfg = img.attrs.get("Config") or {}
            env = cfg.get("Env") or []
            out: Dict[str, str] = {}
            for item in env:
                if "=" in str(item):
                    k, v = str(item).split("=", 1)
                    out[k] = v
            return out
        except Exception:  # noqa: BLE001
            return {}

    def image_default_user(self, image_ref: str) -> str:
        dc = self._optional_docker()
        if not dc:
            return "root"
        try:
            img = dc.images.get(image_ref)
            user = (img.attrs.get("Config") or {}).get("User") or ""
            return str(user).strip() or "root"
        except Exception:  # noqa: BLE001
            return "root"

    def commit_filesystem_snapshot(self, container_id: str, repo: str, tag: str) -> Optional[str]:
        if not self._ensure_k8s_client():
            return None
        pod_name = (container_id or "").strip()
        assert self._core is not None
        try:
            pod = self._core.read_namespaced_pod(pod_name, self._namespace)
        except Exception as e:
            logger.error("Failed to read pod %s for commit: %s", pod_name, e)
            return None
        
        statuses = pod.status.container_statuses or []
        if not statuses:
            logger.error("Pod %s has no container statuses", pod_name)
            return None
            
        cid_str = statuses[0].container_id
        if not cid_str or not cid_str.startswith("docker://"):
            logger.error("Pod %s container_id %s is not a docker:// runtime", pod_name, cid_str)
            return None
            
        raw_cid = cid_str[len("docker://"):]
        dc = self._optional_docker()
        if not dc:
            logger.error("Docker client not available to commit %s", raw_cid)
            return None
            
        try:
            container = dc.containers.get(raw_cid)
            container.commit(repository=repo, tag=tag)
            return f"{repo}:{tag}"
        except Exception as e:
            logger.error("Docker commit failed for %s: %s", raw_cid, e)
            return None

    def pull_image(self, image: str) -> bool:
        """K8s pulls on schedule; optional pre-pull via docker when DOCKER_HOST is set."""
        dc = self._optional_docker()
        if not dc:
            return True
        try:
            dc.images.pull(image)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("Optional docker pull %s failed: %s", image, exc)
            return True
