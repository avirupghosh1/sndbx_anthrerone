"""Lima VM execution plane: one Lima instance per sandbox (``limactl``).

Enable with ``SANDBOX_ISOLATION=lima`` or ``SANDBOX_ISOLATION=colima`` (alias).

**Where ``limactl`` runs:** normally on the **same machine as the API process** (macOS/Linux with
QEMU). If the API runs **inside Docker**, install Lima in the container (usually a bad idea) **or**
set **`LIMA_REMOTE_HOST=user@lima-host`** so every call becomes ``ssh user@lima-host limactl …``
against a host that has Lima (recommended for containerized APIs).

**SANDBOX_ENGINE** stays ``docker`` by default; Lima is selected only via **``SANDBOX_ISOLATION``**,
so ``SANDBOX_ENGINE=firecracker`` + ``SANDBOX_ISOLATION=lima`` would still use Firecracker — avoid
that combination. Prefer ``SANDBOX_ENGINE=docker`` (default) when using Lima isolation.

Warm ``docker commit`` template snapshots are not used; see ``LIMA_WARM_DOCKERLESS_MARKER`` in
``sandbox_manager``.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional

from .container_manager import ContainerConfig, _sanitize_read_text

logger = logging.getLogger(__name__)

LIMA_WARM_DOCKERLESS_MARKER = "__lima_vm__"


def _memory_gib_flag(mem_limit: str) -> str:
    """Value for ``limactl create --memory`` — plain float in **GiB** (no ``GiB`` suffix; Lima parses ``ParseFloat``)."""
    s = (mem_limit or "512m").strip().lower()
    try:
        if s.endswith("g"):
            gib = max(0.5, float(s[:-1]))
        elif s.endswith("m"):
            mib = int(s[:-1])
            gib = max(0.5, mib / 1024.0)
        else:
            mib = int(s) // (1024 * 1024)
            gib = max(0.5, mib / 1024.0)
    except ValueError:
        gib = 1.0
    # Compact decimal so argv is valid for Go float32 (e.g. 0.5 not "0.5GiB").
    text = f"{gib:.10g}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "1"


def _cpus(cpu_limit: str) -> int:
    try:
        return max(1, min(32, int(float((cpu_limit or "1").strip()))))
    except ValueError:
        return 1


class LimaVmPlane:
    """One Lima/QEMU VM per ``container_id`` (instance name ``msbx-…``)."""

    def __init__(self, config: Any):
        self._cfg = config

    def _limactl(self) -> str:
        return (getattr(self._cfg, "LIMACTL_PATH", None) or "limactl").strip() or "limactl"

    def _lima_remote_host(self) -> str:
        return (getattr(self._cfg, "LIMA_REMOTE_HOST", None) or "").strip()

    def _lima_cmd(self, limactl_argv: List[str]) -> List[str]:
        """Build argv: either ``[limactl, …]`` or ``[ssh, …, host, limactl, …]``."""
        remote = self._lima_remote_host()
        if remote:
            extras = shlex.split(getattr(self._cfg, "LIMA_REMOTE_SSH_EXTRA_ARGS", "") or "")
            rlc = (getattr(self._cfg, "LIMA_REMOTE_LIMACTL_PATH", None) or "limactl").strip() or "limactl"
            return (
                ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no"]
                + extras
                + [remote, rlc]
                + limactl_argv
            )
        lc = self._limactl()
        exe = lc if os.path.isabs(lc) else (shutil.which(lc.split("/")[-1]) or lc)
        return [exe] + limactl_argv

    def _dec_err(self, r: subprocess.CompletedProcess) -> str:
        e = r.stderr or b""
        return e.decode("utf-8", errors="replace") if isinstance(e, (bytes, bytearray)) else str(e or "")

    def get_backend_kind(self) -> str:
        return "lima"

    def check_docker(self) -> bool:
        """Protocol hook: ensure ``limactl`` is usable (locally or via ``LIMA_REMOTE_HOST``)."""
        remote = self._lima_remote_host()
        if remote:
            if not shutil.which("ssh"):
                logger.warning("Lima: LIMA_REMOTE_HOST is set but ``ssh`` is not on PATH")
                return False
            r = subprocess.run(self._lima_cmd(["list"]), capture_output=True, timeout=20.0, check=False)
            if r.returncode != 0:
                logger.warning(
                    "Lima: remote ``limactl`` via SSH failed for %r: %s",
                    remote,
                    self._dec_err(r)[:1500],
                )
            return r.returncode == 0
        lc = self._limactl()
        ok = bool(shutil.which(lc.split("/")[-1]) or shutil.which(lc))
        if not ok:
            logger.warning(
                "Lima: %r not found on PATH. If the API runs in Docker, set LIMA_REMOTE_HOST=user@host "
                "where that host has Lima/QEMU (see docs/LIMA_SANDBOX.md), or run the API on the host.",
                lc,
            )
        return ok

    def _run(
        self,
        args: List[str],
        *,
        timeout: Optional[float] = None,
    ) -> subprocess.CompletedProcess:
        cmd = self._lima_cmd(args)
        return subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    def _instance_exists(self, name: str) -> bool:
        """True if Lima knows this instance name (running or stopped)."""
        r = self._run(["show-ssh", name], timeout=15.0)
        return r.returncode == 0

    def _wait_shell(self, inst: str, deadline_s: float) -> bool:
        t0 = time.monotonic()
        while time.monotonic() - t0 < deadline_s:
            r = self._run(["shell", inst, "--", "true"], timeout=15.0)
            if r.returncode == 0:
                return True
            time.sleep(1.5)
        return False

    def _resolve_template(self, config: ContainerConfig) -> str:
        img = (config.image or "").strip()
        if img.startswith("template://") or img.endswith((".yaml", ".yml")) or os.path.isfile(img):
            return img
        return (getattr(self._cfg, "LIMA_SANDBOX_TEMPLATE", None) or "template://ubuntu-24.04").strip()

    def _create_extra_args(self) -> List[str]:
        raw = (getattr(self._cfg, "LIMA_CREATE_EXTRA_ARGS", None) or "").strip()
        if not raw:
            return []
        return shlex.split(raw)

    def create_container(self, name: str, config: ContainerConfig) -> Optional[str]:
        if not self.check_docker():
            return None
        inst = f"msbx-{uuid.uuid4().hex[:12]}"
        safe = re.sub(r"[^a-z0-9-]", "-", inst.lower())
        tpl = self._resolve_template(config)
        cpus = _cpus(config.cpu_limit)
        mem_gib = _memory_gib_flag(config.memory_limit)
        extra = self._create_extra_args()

        create_args: List[str] = [
            "create",
            "--tty=false",
            "--name",
            safe,
            "--cpus",
            str(cpus),
            "--memory",
            mem_gib,
        ] + extra
        create_args.append(tpl)

        logger.info("Lima: creating instance %r template=%r cpus=%s mem=%s GiB", safe, tpl, cpus, mem_gib)
        r = self._run(create_args, timeout=900.0)
        if r.returncode != 0:
            logger.error("limactl create failed: %s", self._dec_err(r)[:4000])
            return None

        st = self._run(["start", safe], timeout=float(getattr(self._cfg, "LIMA_START_TIMEOUT_SEC", 600) or 600))
        if st.returncode != 0:
            logger.error("limactl start failed for %r: %s", safe, self._dec_err(st)[:4000])
            self._run(["delete", "-f", safe], timeout=120.0)
            return None

        wait_s = float(getattr(self._cfg, "LIMA_START_TIMEOUT_SEC", 600) or 600)
        if not self._wait_shell(safe, min(wait_s, 600.0)):
            logger.error("Lima instance %r did not become shell-ready in time", safe)
            self._run(["delete", "-f", safe], timeout=120.0)
            return None

        logger.info("Lima VM ready: %s", safe)
        return safe

    def _get(self, container_id: str) -> bool:
        """Accept only instances created by this plane (``msbx-`` prefix)."""
        if not container_id.startswith("msbx-"):
            return False
        return self._instance_exists(container_id)

    def _shell_argv(
        self,
        inst: str,
        inner: str,
        *,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        user: Optional[str] = None,
    ) -> List[str]:
        wd = cwd or "/"
        parts: List[str] = [f"cd {shlex.quote(wd)}"]
        if env:
            for k, v in env.items():
                parts.append(f"export {shlex.quote(k)}={shlex.quote(str(v))}")
        parts.append(inner)
        script = " && ".join(parts)
        su = (user or "root").strip() or "root"
        use_sudo = str(getattr(self._cfg, "LIMA_SHELL_USE_SUDO", "true")).lower() in ("1", "true", "yes")
        if su != "root":
            full = f"sudo -n -u {shlex.quote(su)} bash -lc {shlex.quote(script)}"
        elif use_sudo:
            full = f"sudo -n bash -lc {shlex.quote(script)}"
        else:
            full = f"bash -lc {shlex.quote(script)}"
        return self._lima_cmd(["shell", inst, "--", "bash", "-lc", full])

    def run_command(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self._get(container_id):
            return {"exit_code": -1, "stdout": "", "stderr": "unknown lima instance", "pid": -1}
        inner = command
        cmd = self._shell_argv(container_id, inner, cwd=cwd, env=env, user=user)
        try:
            r = subprocess.run(
                cmd,
                capture_output=True,
                timeout=float(timeout) if timeout is not None else 3600.0,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {"exit_code": -1, "stdout": "", "stderr": f"timeout after {timeout}s", "pid": -1}
        return {
            "exit_code": int(r.returncode),
            "stdout": (r.stdout or b"").decode("utf-8", errors="replace"),
            "stderr": (r.stderr or b"").decode("utf-8", errors="replace"),
            "pid": -1,
        }

    def run_command_stream(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Iterator[Dict[str, Any]]:
        if not self._get(container_id):
            yield {"type": "error", "message": "unknown lima instance"}
            yield {"type": "exit", "exit_code": -1}
            return
        inner = command
        cmd = self._shell_argv(container_id, inner, cwd=cwd, env=env, user=user)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
            )
        except OSError as ex:
            yield {"type": "error", "message": str(ex)}
            yield {"type": "exit", "exit_code": -1}
            return
        assert proc.stdout is not None
        deadline = time.monotonic() + float(timeout) if timeout and float(timeout) > 0 else None
        try:
            while True:
                if deadline is not None and time.monotonic() > deadline:
                    proc.kill()
                    yield {"type": "error", "message": f"Command exceeded timeout ({timeout}s)"}
                    break
                line = proc.stdout.readline()
                if not line:
                    break
                yield {"type": "stdout", "chunk": line.decode("utf-8", errors="replace")}
            err = proc.stderr.read() if proc.stderr else b""
            if err:
                yield {"type": "stderr", "chunk": err.decode("utf-8", errors="replace")}
        finally:
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        yield {"type": "exit", "exit_code": int(proc.returncode or 0)}

    def _write_via_shell_base64(self, inst: str, path: str, data: bytes) -> bool:
        quoted_path = shlex.quote(path)
        use_sudo = str(getattr(self._cfg, "LIMA_SHELL_USE_SUDO", "true")).lower() in ("1", "true", "yes")
        if not data:
            inner = f": > {quoted_path}"
            full = f"sudo -n /bin/sh -c {shlex.quote(inner)}" if use_sudo else inner
            r = self._run(["shell", inst, "--", "bash", "-lc", full], timeout=60.0)
            return r.returncode == 0
        chunk_sz = 2400
        offset = 0
        while offset < len(data):
            piece = data[offset : offset + chunk_sz]
            enc = base64.b64encode(piece).decode("ascii")
            redir = ">" if offset == 0 else ">>"
            inner = f"printf '%s' {shlex.quote(enc)} | base64 -d {redir} {quoted_path}"
            if use_sudo:
                wrapped = f"sudo -n /bin/sh -c {shlex.quote(inner)}"
            else:
                wrapped = f"/bin/sh -c {shlex.quote(inner)}"
            r = self._run(["shell", inst, "--", "bash", "-lc", wrapped], timeout=120.0)
            if r.returncode != 0:
                logger.error(
                    "lima write_file chunk failed inst=%r offset=%s: %s",
                    inst,
                    offset,
                    self._dec_err(r)[:1500],
                )
                return False
            offset += len(piece)
        return True

    def read_file(self, container_id: str, path: str) -> Optional[str]:
        if not self._get(container_id):
            return None
        r = self._run(
            ["shell", container_id, "--", "bash", "-lc", f"sudo -n base64 -w0 {shlex.quote(path)}"],
            timeout=120.0,
        )
        if r.returncode != 0:
            r2 = self._run(
                ["shell", container_id, "--", "bash", "-lc", f"sudo -n /bin/cat {shlex.quote(path)}"],
                timeout=120.0,
            )
            if r2.returncode != 0:
                return None
            raw = r2.stdout or b""
            return _sanitize_read_text(raw)
        out_b = re.sub(rb"\s+", b"", r.stdout or b"")
        try:
            raw = base64.b64decode(out_b)
        except Exception:
            return None
        return _sanitize_read_text(raw)

    def write_file(self, container_id: str, path: str, content: str) -> bool:
        if not self._get(container_id):
            return False
        parent = os.path.dirname(path) or "/"
        mk = self._run(
            ["shell", container_id, "--", "bash", "-lc", f"sudo -n mkdir -p {shlex.quote(parent)}"],
            timeout=60.0,
        )
        if mk.returncode != 0:
            return False
        data = content.encode("utf-8").replace(b"\x00", b"")
        return self._write_via_shell_base64(container_id, path, data)

    def list_files(self, container_id: str, path: str = "/") -> Optional[list]:
        if not self._get(container_id):
            return None
        r = self._run(
            ["shell", container_id, "--", "/bin/sh", "-c", f"sudo -n ls -la {shlex.quote(path)}"],
            timeout=60.0,
        )
        if r.returncode != 0:
            return None
        out = (r.stdout or b"").decode("utf-8", errors="replace")
        base = path.rstrip("/") or "/"
        entries = []
        for line in out.split("\n")[1:]:
            line = line.strip()
            if not line:
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
            full_path = f"{base.rstrip('/')}/{name}" if base != "/" else f"/{name}"
            entries.append(
                {
                    "name": name,
                    "path": full_path,
                    "is_directory": perms.startswith("d"),
                    "size": size,
                    "permissions": perms,
                    "modified_at": modified_at,
                }
            )
        return entries

    def delete_file(self, container_id: str, path: str, recursive: bool = False) -> bool:
        if not self._get(container_id):
            return False
        cmd = f"sudo -n rm -rf {shlex.quote(path)}" if recursive else f"sudo -n rm -f {shlex.quote(path)}"
        r = self._run(["shell", container_id, "--", "bash", "-lc", cmd], timeout=120.0)
        return r.returncode == 0

    def create_directory(self, container_id: str, path: str, mode: int = 0o755) -> bool:
        if not self._get(container_id):
            return False
        modestr = format(mode & 0o777, "o")
        r = self._run(
            ["shell", container_id, "--", "bash", "-lc", f"sudo -n mkdir -p -m {modestr} {shlex.quote(path)}"],
            timeout=60.0,
        )
        return r.returncode == 0

    def get_container_stats(self, container_id: str) -> Optional[Dict[str, Any]]:
        if not self._get(container_id):
            return None
        r = self._run(
            ["shell", container_id, "--", "bash", "-lc", "grep -E '^MemTotal:|^MemAvailable:' /proc/meminfo 2>/dev/null || true"],
            timeout=15.0,
        )
        txt = (r.stdout or b"").decode("utf-8", errors="replace")
        return {"raw_meminfo": txt.strip(), "backend": "lima"}

    def kill_container(self, container_id: str, force: bool = True) -> bool:
        r = self._run(["delete", "-f", container_id], timeout=180.0)
        if r.returncode != 0:
            logger.warning("limactl delete %r: %s", container_id, self._dec_err(r)[:2000])
        return True

    def is_container_running(self, container_id: str) -> bool:
        if not self._get(container_id):
            return False
        r = self._run(["shell", container_id, "--", "true"], timeout=8.0)
        return r.returncode == 0

    def pause_instance(self, instance_id: str) -> bool:
        r = self._run(["stop", instance_id], timeout=120.0)
        return r.returncode == 0

    def resume_instance(self, instance_id: str) -> bool:
        r = self._run(["start", instance_id], timeout=float(getattr(self._cfg, "LIMA_START_TIMEOUT_SEC", 600) or 600))
        return r.returncode == 0
