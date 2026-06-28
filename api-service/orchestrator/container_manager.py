"""Container management using Docker."""

import base64
import io
import os
import re
import uuid
import shlex
import socket as std_socket
import struct
import tarfile
import threading
import time
from datetime import datetime, timezone
import docker
import subprocess
import logging
from pathlib import PurePosixPath
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Dict, Iterator, List, Optional, TYPE_CHECKING
from dataclasses import dataclass

from docker.utils.socket import SocketError, frames_iter_no_tty

if TYPE_CHECKING:
    from docker.models.containers import Container

logger = logging.getLogger(__name__)


def _sanitize_read_text(raw: bytes) -> str:
    """Decode file/exec bytes for the text read API (NULs break ``compile()``)."""
    text = raw.decode("utf-8", errors="replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    return text.replace("\x00", "")


@dataclass
class ContainerConfig:
    """Container configuration."""

    image: str
    cpu_limit: str = "1"
    memory_limit: str = "512m"
    timeout: int = 3600
    environment: Optional[Dict[str, str]] = None
    volumes: Optional[Dict[str, Dict[str, str]]] = None
    # Firecracker: optional host path to ext4 rootfs (overrides default from env).
    rootfs_path: Optional[str] = None
    # Firecracker: boot from a snapshot bundle ref ``fc-bundle:…`` (see ``firecracker_plane``).
    fc_bundle_ref: Optional[str] = None
    # Guest TCP ports exposed on the workload (K8s containerPort / optional Docker publish).
    guest_ports: Optional[List[int]] = None
    # Legacy Docker-only host publish (local dev); ignored on K8s runtime.
    publish_e2b_agent_port: bool = False
    e2b_agent_port: int = 8765
    publish_envd_port: bool = False
    envd_port: int = 49983
    # K8s-only: optional PID 1 command override used to launch guest daemons before pod readiness.
    startup_command: Optional[List[str]] = None
    # K8s-only: optional readiness TCP socket port. When set, pod Ready waits for the guest listener.
    readiness_tcp_port: Optional[int] = None


class ContainerManager:
    """Manages container lifecycle (Docker Engine; optional ``runsc`` / gVisor OCI runtime)."""

    def __init__(self, oci_runtime: Optional[str] = None):
        self._oci_runtime: Optional[str] = "runsc" if (oci_runtime or "").strip().lower() == "runsc" else None
        self._docker_connect_error: Optional[str] = None
        self.client = None
        # Lazy connect: daemon may start after the API (Colima/Docker); retry on each operation if needed.
        self._ensure_docker_client()

    def _ensure_docker_client(self) -> bool:
        """Return True if ``self.client`` is usable; retry ``docker.from_env()`` when unset."""
        if self.client is not None:
            return True
        try:
            from config import get_config

            cfg = get_config()
            api_timeout = max(60, int(getattr(cfg, "TEMPLATE_DOCKER_CLIENT_TIMEOUT_SEC", 600) or 600))
            self.client = docker.from_env(timeout=api_timeout)
            self._docker_connect_error = None
            logger.info("Docker client connected (docker.from_env)")
            return True
        except Exception as e:
            self._docker_connect_error = f"{type(e).__name__}: {e}"
            logger.warning("docker.from_env() failed: %s", self._docker_connect_error)
            self.client = None
            return False

    def describe_docker_unavailable(self) -> Optional[str]:
        """Human-readable reason when ``self.client`` is missing or daemon is down."""
        if not self._ensure_docker_client():
            if self._docker_connect_error:
                extra = ""
                if "No such file" in self._docker_connect_error or "FileNotFoundError" in self._docker_connect_error:
                    extra = (
                        " This usually means the default Docker socket is missing (e.g. no /var/run/docker.sock). "
                        "On macOS with Colima: `colima start` then "
                        '`export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"` '
                        "(or `docker context use colima`) and restart the API."
                    )
                return (
                    "Docker SDK failed to create a client (docker.from_env). "
                    f"{self._docker_connect_error}. "
                    "Typical fixes: start a Docker engine (Docker Desktop, Colima, OrbStack, or Linux dockerd), fix DOCKER_HOST, or on Linux add your user to the docker group."
                    + extra
                )
            return "Docker client is not initialized."
        try:
            self.client.ping()
        except Exception as e:
            return (
                f"Docker daemon did not respond to ping: {type(e).__name__}: {e}. "
                "Is the daemon running?"
            )
        return None

    def check_docker(self) -> bool:
        """Check if Docker is available."""
        if not self._ensure_docker_client():
            return False
        try:
            self.client.ping()
            return True
        except Exception:
            return False

    def pull_image(self, image: str) -> bool:
        """Pull Docker image."""
        if not self._ensure_docker_client():
            return False

        try:
            logger.info(f"Pulling image: {image}")
            self.client.images.pull(image)
            return True
        except Exception as e:
            logger.error(f"Failed to pull image {image}: {e}")
            return False

    def create_container(
        self,
        name: str,
        config: ContainerConfig,
    ) -> Optional[str]:
        """Create container.
        
        Returns container ID on success, None on failure.
        """
        if not self._ensure_docker_client():
            logger.error(
                "Docker client not available%s",
                f": {self._docker_connect_error}" if self._docker_connect_error else "",
            )
            return None

        try:
            # Ensure image exists
            try:
                self.client.images.get(config.image)
            except docker.errors.ImageNotFound:
                logger.info(f"Image not found locally, pulling {config.image}")
                self.pull_image(config.image)

            # Parse resource limits
            mem_limit = config.memory_limit  # e.g., "512m"
            cpu_quota = self._parse_cpu_limit(config.cpu_limit)

            logger.info(f"Creating container: {name} (oci_runtime={self._oci_runtime or 'default'})")
            run_kwargs: Dict[str, Any] = dict(
                image=config.image,
                command="/bin/bash",
                detach=True,
                stdin_open=True,
                tty=True,
                name=name,
                mem_limit=mem_limit,
                cpu_quota=cpu_quota,
                environment=config.environment or {},
                volumes=config.volumes or {},
                network_disabled=False,
                cap_drop=["NET_RAW"],
                read_only=False,
                restart_policy={"Name": "no"},
            )
            ports_map: Dict[str, Any] = dict(run_kwargs.get("ports") or {})
            for gp in config.guest_ports or []:
                p = max(1, min(65535, int(gp)))
                ports_map[f"{p}/tcp"] = None
            if getattr(config, "publish_e2b_agent_port", False):
                ap = max(1, min(65535, int(getattr(config, "e2b_agent_port", 8765))))
                ports_map[f"{ap}/tcp"] = None
            if getattr(config, "publish_envd_port", False):
                ep = max(1, min(65535, int(getattr(config, "envd_port", 49983))))
                ports_map[f"{ep}/tcp"] = None
            if ports_map:
                run_kwargs["ports"] = ports_map
            if self._oci_runtime:
                run_kwargs["runtime"] = self._oci_runtime
            container = self.client.containers.run(**run_kwargs)

            logger.info(f"Container created: {container.id[:12]}")
            return container.id

        except Exception as e:
            logger.error(f"Failed to create container {name}: {e}")
            return None

    def image_exists(self, image_ref: str) -> bool:
        if not self._ensure_docker_client():
            return False
        try:
            self.client.images.get((image_ref or "").strip())
            return True
        except Exception:  # noqa: BLE001
            return False

    def prune_exited_containers(self, older_than_seconds: int) -> int:
        if not self._ensure_docker_client():
            return 0
        cutoff = time.time() - max(0, int(older_than_seconds))
        removed = 0
        try:
            for container in self.client.containers.list(all=True, filters={"status": "exited"}):
                finished = (container.attrs.get("State") or {}).get("FinishedAt")
                if finished:
                    try:
                        ts = datetime.fromisoformat(str(finished).replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        ts = 0.0
                    if ts and ts > cutoff:
                        continue
                try:
                    container.remove(force=True)
                    removed += 1
                except Exception:
                    continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("Exited container prune failed: %s", exc)
        return removed

    def prune_generated_images(
        self,
        *,
        keep_refs: set[str],
        older_than_seconds: int,
        repo_prefixes: list[str],
    ) -> int:
        if not self._ensure_docker_client():
            return 0
        cutoff = time.time() - max(0, int(older_than_seconds))
        removed = 0
        prefixes = [p for p in repo_prefixes if p]
        try:
            for image in self.client.images.list():
                tags = [str(t).strip() for t in (image.tags or []) if str(t).strip()]
                if not tags:
                    continue
                if any(tag in keep_refs for tag in tags):
                    continue
                repo_matches = False
                for tag in tags:
                    repo = tag.split(":", 1)[0]
                    if any(repo.startswith(prefix) for prefix in prefixes):
                        repo_matches = True
                        break
                if not repo_matches:
                    continue
                created = image.attrs.get("Created")
                if created:
                    try:
                        ts = datetime.fromisoformat(str(created).replace("Z", "+00:00")).timestamp()
                    except ValueError:
                        ts = 0.0
                    if ts and ts > cutoff:
                        continue
                try:
                    self.client.images.remove(image.id, force=False, noprune=False)
                    removed += 1
                except Exception:
                    continue
        except Exception as exc:  # noqa: BLE001
            logger.warning("Generated image prune failed: %s", exc)
        return removed

    def run_command(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute command in container.
        
        Returns dict with exit_code, stdout, stderr, pid.
        """
        if not self._ensure_docker_client():
            return {
                "exit_code": -1,
                "stdout": "",
                "stderr": "Docker client not available",
                "pid": -1,
            }

        try:
            container = self.client.containers.get(container_id)

            def _exec():
                # Run through a shell so pipelines, redirects, &&, and globs behave like a terminal.
                exec_cmd = ["/bin/sh", "-c", command]
                kw: Dict[str, Any] = dict(
                    cmd=exec_cmd,
                    workdir=cwd or "/",
                    user=user or "root",
                )
                # Only pass ``environment`` when the caller supplies vars; otherwise inherit the
                # container's env (e.g. Dockerfile / template ``ENV`` baked into ``containers.run``).
                if env:
                    kw["environment"] = env
                return container.exec_run(**kw)

            # docker-py 7+ removed exec_run(timeout=...); enforce API deadline in the caller thread.
            exec_timeout = float(timeout) if timeout is not None else 30.0
            with ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(_exec)
                try:
                    result = fut.result(timeout=exec_timeout)
                except FuturesTimeout:
                    logger.warning(
                        "Command timed out after %.1fs (exec may still run in container): %s",
                        exec_timeout,
                        container_id[:12],
                    )
                    return {
                        "exit_code": 124,
                        "stdout": "",
                        "stderr": f"Command timed out after {exec_timeout} seconds",
                        "pid": -1,
                    }

            stdout = result.output.decode("utf-8", errors="replace") if result.output else ""

            logger.info(f"Command executed in {container_id[:12]}: exit_code={result.exit_code}")

            return {
                "exit_code": result.exit_code,
                "stdout": stdout,
                "stderr": "",  # Docker doesn't separate stderr
                "pid": result.exit_code,  # Not real PID from Docker
            }

        except Exception as e:
            logger.error(f"Failed to execute command in {container_id}: {e}")
            return {
                "exit_code": -1,
                "stdout": "",
                "stderr": str(e),
                "pid": -1,
            }

    def put_archive_to_container(self, container_id: str, path: str, data: bytes) -> bool:
        """Upload a tarball to ``path`` in the container (same mechanism as ``COPY`` / ``put_archive``).

        Under **gVisor** (``runsc``), stages the tarball in ``/tmp`` via shell base64 chunks
        (same as :meth:`write_file`), then runs ``tar xf <file> -C <dest>`` — Engine
        ``put_archive`` often 404s, and ``tar xf -`` over a TTY exec is rejected by GNU tar.
        """
        if not self._ensure_docker_client():
            return False
        try:
            container = self.client.containers.get(container_id)
            dest = (path or "/").rstrip("/") or "/"
            mkdir = container.exec_run(
                ["/bin/sh", "-c", f"mkdir -p {shlex.quote(dest)}"],
                user="root",
            )
            if mkdir.exit_code != 0:
                out = mkdir.output
                err = out.decode("utf-8", errors="replace") if isinstance(out, (bytes, bytearray)) else str(out)
                logger.error(
                    "put_archive_to_container mkdir failed %s path=%r exit=%s err=%r",
                    container_id[:12],
                    dest,
                    mkdir.exit_code,
                    err[:2000],
                )
                return False
            if not data:
                return True
            if self._oci_runtime == "runsc":
                return self._put_archive_via_staged_tarfile(container, dest, data)
            return bool(container.put_archive(dest, data))
        except Exception as e:
            logger.error("put_archive_to_container %s path=%r: %s", container_id[:12], path, e)
            return False

    def _exec_env_list(self, env: Optional[Dict[str, str]]) -> Optional[List[str]]:
        if not env:
            return None
        return [f"{k}={v}" for k, v in env.items()]

    def run_command_stream(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Stream exec stdout/stderr as small dict events, then one final ``exit`` event.

        Yields ``{"type":"stdout"|"stderr","chunk":str}``, optional ``{"type":"error"}``,
        then ``{"type":"exit","exit_code":int}``. Uses ``exec_start(..., stream=True, demux=True)``.

        If ``timeout`` is set (>0), a soft wall clock stops reading chunks; the exec may
        continue running in the container.
        """
        exec_id: Optional[str] = None
        exit_code = -1

        if not self._ensure_docker_client():
            yield {"type": "error", "message": "Docker client not available"}
            yield {"type": "exit", "exit_code": -1}
            return

        try:
            container = self.client.containers.get(container_id)
            api = self.client.api
            exec_cmd = ["/bin/sh", "-c", command]
            exec_payload = api.exec_create(
                container.id,
                exec_cmd,
                stdout=True,
                stderr=True,
                stdin=False,
                tty=False,
                privileged=False,
                user=user or "root",
                environment=self._exec_env_list(env or {}),
                workdir=cwd or "/",
            )
            exec_id = exec_payload.get("Id")
            if not exec_id:
                yield {"type": "error", "message": "exec_create returned no Id"}
            else:
                stream = api.exec_start(exec_id, stream=True, demux=True)
                deadline: Optional[float] = None
                if timeout is not None and float(timeout) > 0:
                    deadline = time.monotonic() + float(timeout)

                cancel = threading.Event()

                def _watchdog() -> None:
                    if deadline is None:
                        return
                    delay = max(0.0, deadline - time.monotonic())
                    time.sleep(delay)
                    cancel.set()

                if deadline is not None:
                    threading.Thread(target=_watchdog, daemon=True).start()

                try:
                    for stdout_b, stderr_b in stream:
                        if cancel.is_set():
                            yield {
                                "type": "error",
                                "message": (
                                    f"Command exceeded soft timeout ({timeout}s); "
                                    "exec may still run in the container"
                                ),
                            }
                            break
                        if not stdout_b and not stderr_b:
                            continue
                        if stdout_b:
                            yield {
                                "type": "stdout",
                                "chunk": stdout_b.decode("utf-8", errors="replace"),
                            }
                        if stderr_b:
                            yield {
                                "type": "stderr",
                                "chunk": stderr_b.decode("utf-8", errors="replace"),
                            }
                except Exception as loop_ex:  # noqa: BLE001
                    logger.error("run_command_stream read error %s: %s", container_id[:12], loop_ex)
                    yield {"type": "error", "message": str(loop_ex)}
        except Exception as e:
            logger.error("run_command_stream failed in %s: %s", container_id[:12], e)
            yield {"type": "error", "message": str(e)}
        finally:
            if self.client and exec_id:
                try:
                    raw = self.client.api.exec_inspect(exec_id).get("ExitCode")
                    if raw is not None:
                        exit_code = int(raw)
                except Exception as ex:  # noqa: BLE001
                    logger.debug("exec_inspect after stream: %s", ex)
            yield {"type": "exit", "exit_code": exit_code}

    def _read_file_via_base64(self, container: "Container", path: str) -> Optional[str]:
        """Read file bytes via ``base64`` / ``openssl`` inside the container (TTY exec, no tar).

        Avoids ``get_archive`` member selection and ``cat`` quirks under ``runsc``.
        """
        q = shlex.quote(path)
        cmd = (
            "if command -v base64 >/dev/null 2>&1; then "
            f"base64 -w0 {q} 2>/dev/null || base64 {q}; "
            f"elif command -v openssl >/dev/null 2>&1; then openssl base64 -A -in {q}; "
            "else exit 127; fi"
        )
        try:
            result = container.exec_run(
                ["/bin/sh", "-c", cmd],
                stdout=True,
                stderr=True,
                tty=True,
                demux=False,
                user="root",
            )
            if result.exit_code != 0:
                return None
            out_b = result.output
            if not isinstance(out_b, (bytes, bytearray)):
                out_b = bytes(out_b) if out_b else b""
            out_b = re.sub(rb"\s+", b"", out_b)
            if not out_b:
                return _sanitize_read_text(b"")
            raw = base64.b64decode(out_b)
            return _sanitize_read_text(raw)
        except Exception as ex:
            logger.debug("base64 read failed path=%r: %s", path, ex)
            return None

    def _read_file_via_get_archive(self, container: "Container", path: str) -> Optional[str]:
        """Read a single file via Engine ``get_archive`` (no exec attach / multiplex).

        Useful when ``runsc`` mishandles raw-stream framing on ``exec`` attach; tar copy
        from container uses a different path.
        """
        try:
            stream, stat = container.get_archive(path)
            expected_size: Optional[int] = None
            expected_name = ""
            if isinstance(stat, dict):
                sz = stat.get("size")
                if sz is not None:
                    try:
                        expected_size = int(sz)
                    except (TypeError, ValueError):
                        expected_size = None
                expected_name = (stat.get("name") or "").strip()

            buf = io.BytesIO()
            for chunk in stream:
                buf.write(chunk)
            buf.seek(0)
            with tarfile.open(fileobj=buf, mode="r") as tf:
                want_norm = path.lstrip("/")
                base = os.path.basename(path.rstrip("/")) or want_norm

                def _is_pax_or_meta(m: tarfile.TarInfo) -> bool:
                    if not m.isfile():
                        return True
                    n = m.name.rstrip("\x00")
                    bn = os.path.basename(n)
                    if "PaxHeader" in n or bn.startswith("PaxHeader") or bn.startswith(".PaxHeader"):
                        return True
                    if n in (".", "./.", "@PaxHeader"):
                        return True
                    return False

                members = [m for m in tf.getmembers() if m.isfile() and not _is_pax_or_meta(m)]

                if expected_size is not None:
                    sized = [m for m in members if m.size == expected_size]
                    if len(sized) == 1:
                        f = tf.extractfile(sized[0])
                        if f is not None:
                            return _sanitize_read_text(f.read())
                    if len(sized) > 1:
                        for m in sized:
                            bn = os.path.basename(m.name.rstrip("\x00"))
                            if expected_name and bn == expected_name:
                                f = tf.extractfile(m)
                                if f is not None:
                                    return _sanitize_read_text(f.read())
                        f = tf.extractfile(sized[0])
                        if f is not None:
                            return _sanitize_read_text(f.read())

                scored: List[tuple[int, tarfile.TarInfo]] = []
                for m in members:
                    n = m.name.rstrip("\x00").lstrip("./")
                    if n == want_norm:
                        scored.append((300, m))
                    elif n.endswith("/" + base) or n == base:
                        scored.append((200, m))
                    else:
                        scored.append((100, m))
                scored.sort(key=lambda x: -x[0])
                for _score, member in scored:
                    f = tf.extractfile(member)
                    if f is not None:
                        return _sanitize_read_text(f.read())
            return None
        except Exception as ex:
            logger.debug("get_archive read failed path=%r: %s", path, ex)
            return None

    def read_file(
        self,
        container_id: str,
        path: str,
    ) -> Optional[str]:
        """Read file from container."""
        if not self._ensure_docker_client():
            return None

        try:
            container = self.client.containers.get(container_id)

            # runsc: base64 inside the container (TTY exec, no tar) then get_archive, then cat.
            if self._oci_runtime == "runsc":
                via_b64 = self._read_file_via_base64(container, path)
                if via_b64 is not None:
                    return via_b64
                via_tar = self._read_file_via_get_archive(container, path)
                if via_tar is not None:
                    return via_tar

            # TTY exec disables multiplex framing (docker-py ``frames_iter_tty``); stderr
            # merges into stdout — fine for ``cat`` on an existing file.
            result = container.exec_run(
                ["/bin/cat", path],
                stdout=True,
                stderr=True,
                tty=True,
                demux=False,
                user="root",
            )

            if result.exit_code == 0:
                out_b = result.output
                if not isinstance(out_b, (bytes, bytearray)):
                    out_b = bytes(out_b) if out_b else b""
                return _sanitize_read_text(out_b)
            else:
                out_b = result.output
                if not isinstance(out_b, (bytes, bytearray)):
                    out_b = bytes(out_b) if out_b else b""
                logger.error(
                    "Failed to read file %s: exit=%s tail=%r",
                    path,
                    result.exit_code,
                    out_b[-500:],
                )
                return None

        except Exception as e:
            logger.error(f"Failed to read file {path} from {container_id}: {e}")
            return None

    def _write_file_via_shell_base64(self, container: "Container", path: str, data: bytes) -> bool:
        """Write ``data`` using repeated ``exec_run`` shell snippets (no hijacked attach).

        ``runsc`` + hijacked stdin (multiplex or TTY) is fragile and can fail closed or
        corrupt the file. This path only uses short ``/bin/sh -c`` commands: ``printf`` a
        standalone base64 chunk, ``base64 -d``, redirect to the file (``>`` then ``>>``).
        """
        quoted_path = shlex.quote(path)
        if not data:
            r = container.exec_run(["/bin/sh", "-c", f": >{quoted_path}"], user="root")
            if r.exit_code != 0:
                logger.error("write_file empty truncate failed path=%r exit=%s", path, r.exit_code)
                return False
            return True

        # Keep each argv under typical ARG_MAX; encoded length is ~4/3 of binary.
        chunk_sz = 2400
        offset = 0
        while offset < len(data):
            piece = data[offset : offset + chunk_sz]
            enc = base64.b64encode(piece).decode("ascii")
            redir = ">" if offset == 0 else ">>"
            inner = f"printf '%s' {shlex.quote(enc)} | base64 -d {redir} {quoted_path}"
            r = container.exec_run(["/bin/sh", "-c", inner], user="root")
            if r.exit_code != 0:
                out = r.output
                if isinstance(out, (bytes, bytearray)):
                    err = out.decode("utf-8", errors="replace")[:2000]
                else:
                    err = str(out)[:2000]
                logger.error(
                    "write_file shell base64 chunk failed path=%r offset=%s exit=%s err=%r",
                    path,
                    offset,
                    r.exit_code,
                    err,
                )
                return False
            offset += len(piece)
        return True

    def _write_file_via_exec_stdin(self, container: "Container", path: str, data: bytes) -> bool:
        """Write ``data`` to ``path`` using **multiplexed** exec attach stdin (``tty=False``).

        Used only as a **fallback** under ``runsc`` when :meth:`_write_file_via_shell_base64`
        fails (e.g. no ``base64`` in the image). Prefer shell base64: ``runsc`` can leak
        multiplex framing into ``cat`` output when stdin framing is mishandled.
        """
        api = container.client.api
        quoted = shlex.quote(path)
        cmd = ["/bin/sh", "-c", f"cat >{quoted}"]
        try:
            ex = api.exec_create(
                container.id,
                cmd,
                stdin=True,
                stdout=True,
                stderr=True,
                tty=False,
                user="root",
            )
            exec_id = ex["Id"]
            sock = api.exec_start(exec_id, socket=True, tty=False)
        except Exception as ex:
            logger.error("write_file exec_create/start failed: %s", ex)
            return False

        try:
            raw = getattr(sock, "_sock", sock)
            for s in (sock, raw):
                if hasattr(s, "settimeout"):
                    try:
                        s.settimeout(None)
                    except OSError:
                        pass

            def _send_all(payload: bytes) -> None:
                """Hijacked exec may return ``SocketIO`` (``write``) or a real socket (``sendall``)."""
                for candidate in (sock, raw):
                    if candidate is not None and hasattr(candidate, "sendall"):
                        candidate.sendall(payload)  # type: ignore[union-attr]
                        return
                mv = memoryview(payload)
                while len(mv):
                    n = sock.write(mv)  # type: ignore[union-attr]
                    if n is None or n == 0:
                        raise OSError("socket write stalled")
                    mv = mv[n:]
                flush = getattr(sock, "flush", None)
                if flush:
                    flush()

            def _shutdown_wr() -> None:
                for candidate in (sock, raw, getattr(sock, "raw", None)):
                    if candidate is None:
                        continue
                    sh = getattr(candidate, "shutdown", None)
                    if sh is None:
                        continue
                    try:
                        sh(std_socket.SHUT_WR)
                        return
                    except (OSError, TypeError, ValueError, AttributeError):
                        continue

            chunk_sz = 1024 * 1024
            for i in range(0, len(data), chunk_sz):
                piece = data[i : i + chunk_sz]
                _send_all(struct.pack(">BxxxL", 0, len(piece)) + piece)

            _shutdown_wr()

            stderr_acc: List[bytes] = []
            try:
                for stream, payload in frames_iter_no_tty(sock):
                    if stream == 2 and payload:
                        stderr_acc.append(payload)
            except SocketError:
                pass

            info = api.exec_inspect(exec_id)
            exit_code = int(info.get("ExitCode", -1))
            if exit_code != 0:
                err = b"".join(stderr_acc).decode("utf-8", errors="replace")[:2000]
                logger.error(
                    "write_file exec stdin (multiplex) failed path=%r exit=%s stderr=%r",
                    path,
                    exit_code,
                    err,
                )
                return False
            return True
        except Exception as ex:
            logger.error("write_file exec stdin transport failed: %s", ex)
            return False
        finally:
            try:
                sock.close()
            except Exception:
                pass

    def _put_archive_via_staged_tarfile(self, container: "Container", dest_dir: str, data: bytes) -> bool:
        """Extract ``data`` (POSIX tar bytes) under ``dest_dir`` without Engine ``put_archive``.

        **gVisor:** ``put_archive`` often 404s. Streaming into ``tar xf -`` over an exec with
        ``tty=True`` makes **stdin a TTY**; GNU tar then refuses to read an archive from fd 0
        (*"Refusing to read archive contents from terminal (missing -f option?)"*).

        **Approach:** stage the tarball with :meth:`_write_file_via_shell_base64` (same runsc-safe
        path as :meth:`write_file`), then ``tar xf <file> -C <dest>`` via a normal non-attach exec.
        """
        if not data:
            return True
        dest = (dest_dir or "/").rstrip("/") or "/"
        blob = f"/tmp/.api_put_archive_{uuid.uuid4().hex}.tar"
        if not self._write_file_via_shell_base64(container, blob, data):
            logger.error("put_archive runsc: staging tarball failed path=%r", blob)
            return False
        try:
            er = container.exec_run(["tar", "xf", blob, "-C", dest], user="root")
            if er.exit_code != 0:
                out = er.output
                if isinstance(out, (bytes, bytearray)):
                    tail = out.decode("utf-8", errors="replace")[-4000:]
                else:
                    tail = str(out)[:4000]
                logger.error(
                    "put_archive runsc: tar xf failed dest=%r exit=%s tail=%r",
                    dest,
                    er.exit_code,
                    tail,
                )
                return False
            return True
        finally:
            try:
                container.exec_run(["rm", "-f", blob], user="root")
            except Exception:
                pass

    def write_file(
        self,
        container_id: str,
        path: str,
        content: str,
    ) -> bool:
        """Write file to container.

        **Default (``runc``):** Docker Engine **put_archive** (same mechanism as
        ``docker cp``): a small tar stream is extracted under the file's parent
        directory. Avoids **exec argv / ARG_MAX** limits.

        **gVisor (``runsc``):** ``put_archive`` / ``docker cp`` is unreliable; we
        stream bytes to ``/bin/sh -c 'cat >path'`` over an **exec attach** socket
        instead (see https://gvisor.dev/docs/user_guide/faq/ — in-sandbox copy).
        """
        if not self._ensure_docker_client():
            return False

        try:
            container = self.client.containers.get(container_id)
            posix = PurePosixPath(path)
            filename = posix.name
            if not filename:
                logger.error("write_file: invalid path (no basename): %r", path)
                return False
            dest_dir = (
                str(posix.parent)
                if posix.parent != PurePosixPath(".")
                else "."
            )

            mkdir = container.exec_run(
                ["mkdir", "-p", dest_dir],
                user="root",
            )
            if mkdir.exit_code != 0:
                err = mkdir.output.decode("utf-8", errors="replace") if mkdir.output else ""
                logger.error(
                    "write_file mkdir failed path=%r dest_dir=%r exit=%s err=%r",
                    path,
                    dest_dir,
                    mkdir.exit_code,
                    err[:2000],
                )
                return False

            data_bytes = content.encode("utf-8").replace(b"\x00", b"")

            if self._oci_runtime == "runsc":
                # Prefer shell + base64 chunks: hijacked stdin (TTY or multiplex) often breaks on runsc.
                if not self._write_file_via_shell_base64(container, path, data_bytes):
                    if not self._write_file_via_exec_stdin(container, path, data_bytes):
                        return False
            else:
                tarbuf = io.BytesIO()
                with tarfile.open(fileobj=tarbuf, mode="w") as tf:
                    info = tarfile.TarInfo(name=filename)
                    info.size = len(data_bytes)
                    tf.addfile(info, io.BytesIO(data_bytes))
                tar_payload = tarbuf.getvalue()

                if not container.put_archive(dest_dir, tar_payload):
                    logger.error(
                        "write_file put_archive failed container_id=%s path=%r",
                        container_id[:12],
                        path,
                    )
                    return False

            verify = container.exec_run(["test", "-f", path], user="root")
            if verify.exit_code != 0:
                err = (
                    verify.output.decode("utf-8", errors="replace")
                    if verify.output
                    else ""
                )
                logger.error(
                    "write_file verify failed path=%r stderr=%r oci=%r",
                    path,
                    err[:500],
                    self._oci_runtime or "default",
                )
                return False
            logger.info(
                "write_file verified file present container_id=%s path=%r bytes=%s",
                container_id[:12],
                path,
                len(data_bytes),
            )
            return True

        except Exception as e:
            logger.error(f"Failed to write file {path} to {container_id}: {e}")
            return False

    def list_files(
        self,
        container_id: str,
        path: str = "/",
    ) -> Optional[list]:
        """List files in container directory."""
        if not self._ensure_docker_client():
            return None

        try:
            container = self.client.containers.get(container_id)

            result = container.exec_run(
                ["/bin/sh", "-c", f"ls -la {shlex.quote(path)}"],
                stdout=True,
                stderr=True,
            )

            if result.exit_code == 0:
                output = result.output.decode("utf-8", errors="replace")
                base = path.rstrip("/") or "/"
                entries = []
                for line in output.split("\n")[1:]:  # skip "total N"
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split()
                    # Typical: perms links user group size mon day time name...
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
                    entries.append(
                        {
                            "path": full_path,
                            "name": name,
                            "type": typ,
                            "size": size,
                            "permissions": perms,
                            "modified_at": modified_at,
                        }
                    )
                return entries
            else:
                logger.error(f"Failed to list {path}: {result.output}")
                return None

        except Exception as e:
            logger.error(f"Failed to list files {path} in {container_id}: {e}")
            return None

    def delete_file(
        self,
        container_id: str,
        path: str,
        recursive: bool = False,
    ) -> bool:
        """Delete file from container."""
        if not self._ensure_docker_client():
            return False

        try:
            container = self.client.containers.get(container_id)

            cmd = f"rm {'-r' if recursive else ''} {path}".strip()
            result = container.exec_run(cmd=cmd)

            if result.exit_code == 0:
                logger.info(f"File deleted: {path}")
                return True
            else:
                logger.error(f"Failed to delete {path}: {result.output}")
                return False

        except Exception as e:
            logger.error(f"Failed to delete {path} from {container_id}: {e}")
            return False

    def create_directory(
        self,
        container_id: str,
        path: str,
        mode: int = 0o755,
    ) -> bool:
        """Create directory in container."""
        if not self._ensure_docker_client():
            return False

        try:
            container = self.client.containers.get(container_id)

            result = container.exec_run(
                cmd=f"mkdir -p {path}"
            )

            if result.exit_code == 0:
                logger.info(f"Directory created: {path}")
                return True
            else:
                logger.error(f"Failed to create directory {path}: {result.output}")
                return False

        except Exception as e:
            logger.error(f"Failed to create directory {path} in {container_id}: {e}")
            return False

    def get_container_stats(self, container_id: str) -> Optional[Dict[str, Any]]:
        """Get container resource usage."""
        if not self._ensure_docker_client():
            return None

        try:
            container = self.client.containers.get(container_id)
            stats = container.stats(stream=False)

            memory_usage = stats["memory_stats"]["usage"]
            memory_limit = stats["memory_stats"]["limit"]
            cpu_delta = (
                stats["cpu_stats"]["cpu_usage"]["total_usage"] -
                stats["precpu_stats"]["cpu_usage"]["total_usage"]
            )
            system_cpu_delta = (
                stats["cpu_stats"]["system_cpu_usage"] -
                stats["precpu_stats"]["system_cpu_usage"]
            )
            cpu_percent = (cpu_delta / system_cpu_delta) * 100.0

            return {
                "memory_usage": memory_usage,
                "memory_limit": memory_limit,
                "cpu_percent": cpu_percent,
                "uptime": stats.get("pids_stats", {}).get("pids_current", 0),
            }

        except Exception as e:
            logger.error(f"Failed to get stats for {container_id}: {e}")
            return None

    def commit_filesystem_snapshot(
        self,
        container_id: str,
        repository: str,
        tag: str,
        *,
        pause_during_commit: bool = True,
    ) -> Optional[str]:
        """Persist the container writable layer as a new local image (``docker commit``).

        This captures **filesystem state** (plus image layers below), not RAM/process
        registers. For in-place freeze/resume of the same container, use pause/unpause.
        """
        if not self._ensure_docker_client():
            return None
        try:
            container = self.client.containers.get(container_id)
            img = container.commit(
                repository=repository,
                tag=tag,
                pause=pause_during_commit,
            )
            tags = getattr(img, "tags", None) or []
            if tags:
                return str(tags[0])
            return f"{repository}:{tag}"
        except Exception as e:
            logger.error("commit_filesystem_snapshot %s: %s", container_id[:12], e)
            return None

    def kill_container(self, container_id: str, force: bool = True) -> bool:
        """Kill container."""
        if not self._ensure_docker_client():
            return False

        try:
            container = self.client.containers.get(container_id)

            if force:
                container.kill()
            else:
                container.stop(timeout=10)

            container.remove()
            logger.info(f"Container killed: {container_id[:12]}")
            return True
        except docker.errors.NotFound:
            logger.info("Container already absent: %s", container_id[:12])
            return True

        except Exception as e:
            logger.error(f"Failed to kill container {container_id}: {e}")
            return False

    def get_container_internal_ipv4(self, container_id: str) -> Optional[str]:
        """First non-empty IPv4 from Docker ``NetworkSettings`` (prefers ``bridge``)."""
        if not self._ensure_docker_client():
            return None
        try:
            container = self.client.containers.get(container_id)
            nets = (container.attrs or {}).get("NetworkSettings", {}).get("Networks") or {}
            order = []
            if "bridge" in nets:
                order.append("bridge")
            order.extend(k for k in nets if k not in order)
            for name in order:
                cfg = nets.get(name) or {}
                ip = (cfg.get("IPAddress") or "").strip()
                if ip:
                    return ip
        except Exception as e:
            logger.debug("get_container_internal_ipv4 %s: %s", container_id[:12], e)
        return None

    def get_container_tcp_host_port(self, container_id: str, container_port: int) -> Optional[int]:
        """Host TCP port mapped to ``container_port`` (requires ``ports={f'{port}/tcp': None}`` at create)."""
        if not self._ensure_docker_client():
            return None
        p = max(1, min(65535, int(container_port)))
        key = f"{p}/tcp"
        try:
            container = self.client.containers.get(container_id)
            container.reload()
            bindings = (container.attrs or {}).get("NetworkSettings", {}).get("Ports") or {}
            lst = bindings.get(key) or []
            for b in lst:
                hp = (b or {}).get("HostPort")
                if hp:
                    return int(hp)
        except Exception as e:
            logger.debug("get_container_tcp_host_port %s: %s", container_id[:12], e)
        return None

    def image_start_cmd_shell(self, image_ref: str) -> str:
        """Shell command from image ``Entrypoint`` + ``Cmd`` (Docker ``CMD``/``ENTRYPOINT``)."""
        if not self._ensure_docker_client():
            return ""
        ref = (image_ref or "").strip()
        if not ref:
            return ""
        try:
            img = self.client.images.get(ref)
            cfg = (img.attrs or {}).get("Config") or {}
            ep = cfg.get("Entrypoint") or []
            cmd = cfg.get("Cmd") or []
            if isinstance(ep, str):
                ep = [ep]
            if isinstance(cmd, str):
                cmd = [cmd]
            parts = [str(x) for x in list(ep or []) + list(cmd or []) if str(x).strip()]
            if not parts:
                return ""
            wd = (cfg.get("WorkingDir") or "/").strip() or "/"
            full = " ".join(shlex.quote(p) for p in parts)
            if wd not in ("/", "."):
                return f"cd {shlex.quote(wd)} && {full}"
            return full
        except Exception as e:
            logger.debug("image_start_cmd_shell %r: %s", ref, e)
            return ""

    def image_env_dict(self, image_ref: str) -> Dict[str, str]:
        """``ENV`` from image config as ``{key: value}``."""
        if not self._ensure_docker_client():
            return {}
        ref = (image_ref or "").strip()
        if not ref:
            return {}
        try:
            img = self.client.images.get(ref)
            raw = ((img.attrs or {}).get("Config") or {}).get("Env") or []
            out: Dict[str, str] = {}
            for item in raw:
                s = str(item)
                if "=" in s:
                    k, v = s.split("=", 1)
                    out[k] = v
            return out
        except Exception as e:
            logger.debug("image_env_dict %r: %s", ref, e)
            return {}

    def image_default_user(self, image_ref: str) -> str:
        """``Config.User`` from image metadata (empty → ``root``)."""
        if not self._ensure_docker_client():
            return "root"
        ref = (image_ref or "").strip()
        if not ref:
            return "root"
        try:
            img = self.client.images.get(ref)
            user = ((img.attrs or {}).get("Config") or {}).get("User") or ""
            u = str(user).strip() or "root"
            return u
        except Exception as e:
            logger.debug("image_default_user %r: %s", ref, e)
            return "root"

    def get_backend_kind(self) -> str:
        return "gvisor" if self._oci_runtime == "runsc" else "docker"

    def pause_instance(self, container_id: str) -> bool:
        """Pause container (Docker-only)."""
        if not self._ensure_docker_client():
            return False
        try:
            container = self.client.containers.get(container_id)
            container.pause()
            return True
        except Exception as e:
            logger.error("pause_instance %s: %s", container_id[:12], e)
            return False

    def resume_instance(self, container_id: str) -> bool:
        """Resume paused container (Docker-only)."""
        if not self._ensure_docker_client():
            return False
        try:
            container = self.client.containers.get(container_id)
            container.unpause()
            return True
        except Exception as e:
            logger.error("resume_instance %s: %s", container_id[:12], e)
            return False

    def is_container_running(self, container_id: str) -> bool:
        """Check if container is running."""
        if not self._ensure_docker_client():
            return False

        try:
            container = self.client.containers.get(container_id)
            return container.status == "running"

        except Exception as e:
            logger.error(f"Failed to check container status {container_id}: {e}")
            return False

    @staticmethod
    def _parse_cpu_limit(cpu_limit: str) -> int:
        """Parse CPU limit string to quota.
        
        Examples:
        - "1" -> 100000 (1 CPU = 100000 microseconds per 100ms)
        - "0.5" -> 50000 (0.5 CPU)
        - "2" -> 200000 (2 CPUs)
        """
        try:
            cpu_float = float(cpu_limit)
            return int(cpu_float * 100000)
        except ValueError:
            return 100000  # Default to 1 CPU
