"""Firecracker microVM execution plane (Linux + KVM + tap + SSH guest).

Requires a **Linux** host with ``/dev/kvm``, the ``firecracker`` binary, an uncompressed
**vmlinux**, and an **ext4 rootfs** with **sshd** and your SSH public key baked in.
Typical deployment: run the API **inside Colima** (``colima ssh``) or on a Linux VM
where ``DOCKER_HOST`` already points — same machine then runs Firecracker sandboxes.

Warm pool: ``MultiWarmSandboxPool`` is unchanged; it still calls ``SandboxManager``,
which uses this plane when ``SANDBOX_ENGINE=firecracker``. Full VM snapshots use
Firecracker ``/snapshot/create`` and ``/snapshot/load`` (see ``docs/FIRECRACKER.md``).
"""

from __future__ import annotations

import http.client
import itertools
import json
import logging
import os
import random
import re
import sys
import shlex
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import uuid
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from .container_manager import ContainerConfig, _sanitize_read_text

logger = logging.getLogger(__name__)

# Marker stored in ``sandbox_templates.warm_snapshot_image`` so Docker-based warm
# snapshot build is skipped while template + warm pool logic still runs.
FC_WARM_DOCKERLESS_MARKER = "__fc_rootfs__"


def _mem_mib(mem_limit: str) -> int:
    s = (mem_limit or "512m").strip().lower()
    if s.endswith("g"):
        return max(128, int(float(s[:-1]) * 1024))
    if s.endswith("m"):
        return max(128, int(s[:-1]))
    try:
        return max(128, int(s) // (1024 * 1024))
    except ValueError:
        return 512


def _vcpu(cpu_limit: str) -> int:
    try:
        v = int(float((cpu_limit or "1").strip()))
        return max(1, min(32, v))
    except ValueError:
        return 1


class _FcUnixClient:
    """Minimal HTTP/1.1 over Firecracker's Unix API socket."""

    def __init__(self, sock_path: str):
        self.sock_path = sock_path

    def request(self, method: str, path: str, body: Optional[dict] = None) -> tuple[int, bytes]:
        uds = self.sock_path

        class Conn(http.client.HTTPConnection):
            def connect(self_inner):
                self_inner.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self_inner.sock.connect(uds)

        conn = Conn("localhost")
        try:
            payload = json.dumps(body).encode("utf-8") if body is not None else None
            headers = {"Host": "localhost", "Content-Type": "application/json", "Accept": "application/json"}
            conn.request(method, path, body=payload, headers=headers)
            resp = conn.getresponse()
            code = resp.status
            data = resp.read()
            return code, data
        finally:
            conn.close()


# Prefix for ``image_ref`` / ``from_snapshot_image`` pointing at a snapshot bundle directory.
FC_BUNDLE_SCHEME = "fc-bundle:"


@dataclass
class _VmState:
    proc: subprocess.Popen
    api_sock: str
    workdir: str
    guest_ip: str
    tap_name: str
    ssh_user: str
    ssh_key: str
    slot: int = 0


class FirecrackerVmmPlane:
    """Firecracker-backed sandboxes: one microVM per ``container_id`` (``fc-…``)."""

    def __init__(self, config: Any):
        self._cfg = config
        self._lock = threading.Lock()
        self._vms: Dict[str, _VmState] = {}
        self._slot = itertools.count(0)

    def get_backend_kind(self) -> str:
        return "firecracker"

    def check_docker(self) -> bool:
        """Misnamed protocol hook: verify Firecracker assets exist on this host."""
        fc = (getattr(self._cfg, "FIRECRACKER_BINARY", None) or "/usr/local/bin/firecracker").strip()
        ker = (getattr(self._cfg, "FIRECRACKER_KERNEL", None) or "").strip()
        root = (getattr(self._cfg, "FIRECRACKER_ROOTFS", None) or "").strip()
        key = (getattr(self._cfg, "FIRECRACKER_SSH_KEY", None) or "").strip()
        ok = bool(fc and os.path.isfile(fc) and ker and os.path.isfile(ker) and root and os.path.isfile(root) and key and os.path.isfile(key))
        if not ok:
            logger.warning(
                "Firecracker check failed: fc=%s ker=%s root=%s key=%s",
                fc,
                ker or "(unset)",
                root or "(unset)",
                key or "(unset)",
            )
        return ok

    def _next_slot(self) -> int:
        nslots = max(1, int(getattr(self._cfg, "FIRECRACKER_TAP_SLOTS", 8)))
        return next(self._slot) % nslots

    def _guest_ip(self, slot: int) -> str:
        prefix = (getattr(self._cfg, "FIRECRACKER_SUBNET_PREFIX", "172.16.0") or "172.16.0").strip()
        base = int(getattr(self._cfg, "FIRECRACKER_GUEST_OCTET_BASE", 10))
        last = base + slot
        if last > 253:
            last = 10 + (slot % 200)
        return f"{prefix}.{last}"

    def _tap_name(self, slot: int) -> str:
        pat = (getattr(self._cfg, "FIRECRACKER_TAP_PATTERN", "tapfc{slot}") or "tapfc{slot}").strip()
        return pat.format(slot=slot)

    def _ssh_base(self, st: _VmState) -> List[str]:
        return [
            "ssh",
            "-i",
            st.ssh_key,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"UserKnownHostsFile={getattr(self._cfg, 'FIRECRACKER_SSH_KNOWN_HOSTS', '/dev/null')}",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=2",
            f"{st.ssh_user}@{st.guest_ip}",
        ]

    def _ssh_run(
        self,
        st: _VmState,
        remote_argv: List[str],
        *,
        timeout: Optional[float] = None,
        stdin_data: Optional[bytes] = None,
    ) -> subprocess.CompletedProcess:
        cmd = self._ssh_base(st) + remote_argv
        return subprocess.run(
            cmd,
            input=stdin_data,
            capture_output=True,
            timeout=timeout,
            check=False,
        )

    def _wait_ssh(self, st: _VmState, deadline_s: float = 120.0) -> bool:
        poll = float(getattr(self._cfg, "FIRECRACKER_SSH_POLL_SEC", 0.25) or 0.25)
        poll = max(0.05, min(2.0, poll))
        t0 = time.monotonic()
        while time.monotonic() - t0 < deadline_s:
            remaining = deadline_s - (time.monotonic() - t0)
            if remaining <= 0:
                break
            r = self._ssh_run(st, ["true"], timeout=min(10.0, max(0.5, remaining)))
            if r.returncode == 0:
                return True
            time.sleep(poll)
        return False

    def _copy_rootfs_into_place(self, root_src: str, root_rw: str) -> bool:
        """Copy golden ext4 into per-VM path; prefer Linux CoW ``cp --reflink=auto`` when enabled."""
        use_reflink = bool(getattr(self._cfg, "FIRECRACKER_ROOTFS_FAST_COPY", True))
        if use_reflink and sys.platform.startswith("linux"):
            try:
                if os.path.lexists(root_rw):
                    os.unlink(root_rw)
            except OSError:
                pass
            try:
                r = subprocess.run(
                    ["cp", "--reflink=auto", root_src, root_rw],
                    capture_output=True,
                    timeout=7200,
                    check=False,
                )
                if r.returncode == 0 and os.path.isfile(root_rw) and os.path.getsize(root_rw) > 0:
                    return True
            except (OSError, subprocess.SubprocessError) as ex:
                logger.debug("Firecracker: reflink copy unavailable (%s); falling back to shutil.copy2", ex)
        try:
            shutil.copy2(root_src, root_rw)
        except OSError as ex:
            logger.error("Firecracker: copy rootfs failed: %s", ex)
            return False
        return bool(os.path.isfile(root_rw) and os.path.getsize(root_rw) > 0)

    @staticmethod
    def _host_fsync_file(path: str) -> None:
        """Best-effort flush of host backing file to stable storage (virtio-blk guest → host cache)."""
        try:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    def _fc_put(self, api_sock: str, path: str, body: dict) -> tuple[int, bytes]:
        return _FcUnixClient(api_sock).request("PUT", path, body)

    def _fc_patch(self, api_sock: str, path: str, body: dict) -> tuple[int, bytes]:
        return _FcUnixClient(api_sock).request("PATCH", path, body)

    def _decode_fc_bundle_dir(self, bundle_ref: str) -> Optional[str]:
        if not bundle_ref.startswith(FC_BUNDLE_SCHEME):
            return None
        raw = bundle_ref[len(FC_BUNDLE_SCHEME) :].strip()
        if not raw:
            return None
        return os.path.abspath(urllib.parse.unquote(raw))

    def commit_filesystem_snapshot(
        self,
        container_id: str,
        repository: str,
        tag: str,
        *,
        pause_during_commit: bool = True,
        **_kwargs: Any,
    ) -> Optional[str]:
        """Full Firecracker microVM snapshot (guest RAM + devices + rootfs copy at pause).

        Returns ``fc-bundle:<urlencoded-abs-path>`` for use as ``from_snapshot_image``.
        """
        st = self._get(container_id)
        if not st:
            logger.error("Firecracker snapshot: unknown instance %s", container_id)
            return None
        base = (getattr(self._cfg, "FIRECRACKER_SNAPSHOT_DIR", None) or "").strip() or os.path.join(
            os.getcwd(), "fc-snapshots"
        )
        safe_tag = re.sub(r"[^a-zA-Z0-9._-]+", "-", tag).strip("-")[:160] or "snap"
        bundle_dir = os.path.abspath(os.path.join(base, safe_tag))
        os.makedirs(bundle_dir, mode=0o755, exist_ok=True)
        state_path = os.path.join(bundle_dir, "vm.snap")
        mem_path = os.path.join(bundle_dir, "vm.mem")
        paused = False
        try:
            r = self.run_command(
                container_id,
                "/bin/sh -c 'sync; command -v blockdev >/dev/null 2>&1 && blockdev --flushbufs /dev/vda 2>/dev/null || true'",
                timeout=120.0,
            )
            if int(r.get("exit_code") or 0) != 0:
                logger.warning("Firecracker snapshot: guest flush/sync stderr: %s", (r.get("stderr") or "")[:500])
            if pause_during_commit:
                if not self.pause_instance(container_id):
                    logger.error("Firecracker snapshot: pause failed for %s", container_id)
                    return None
                paused = True
            code, data = self._fc_put(
                st.api_sock,
                "/snapshot/create",
                {
                    "snapshot_type": "Full",
                    "snapshot_path": state_path,
                    "mem_file_path": mem_path,
                },
            )
            if code not in (200, 201, 204):
                logger.error("Firecracker snapshot/create failed %s: %r", code, data[:800])
                return None
            root_live = os.path.join(st.workdir, "rootfs.ext4")
            self._host_fsync_file(root_live)
            try:
                shutil.copy2(root_live, os.path.join(bundle_dir, "rootfs.ext4"))
            except OSError as ex:
                logger.error("Firecracker snapshot: rootfs copy failed: %s", ex)
                return None
            self._host_fsync_file(os.path.join(bundle_dir, "rootfs.ext4"))
            fc_ver = ""
            try:
                c2, vbody = _FcUnixClient(st.api_sock).request("GET", "/version", None)
                if c2 == 200 and vbody:
                    fc_ver = json.loads(vbody.decode("utf-8", errors="replace")).get("firecracker_version", "")
            except (OSError, json.JSONDecodeError, ValueError):
                pass
            manifest = {
                "backend": "firecracker",
                "tap_slot": st.slot,
                "guest_ip": st.guest_ip,
                "tap_name": st.tap_name,
                "firecracker_version": fc_ver,
            }
            try:
                with open(os.path.join(bundle_dir, "manifest.json"), "w", encoding="utf-8") as mf:
                    json.dump(manifest, mf, indent=2)
            except OSError as ex:
                logger.error("Firecracker snapshot: manifest write failed: %s", ex)
                return None
            return FC_BUNDLE_SCHEME + urllib.parse.quote(bundle_dir, safe="/")
        finally:
            if paused:
                if not self.resume_instance(container_id):
                    logger.error("Firecracker snapshot: resume failed for %s — instance may stay paused", container_id)

    def _create_vm_from_fc_bundle(self, name: str, config: ContainerConfig, bundle_ref: str) -> Optional[str]:
        """Boot from a bundle produced by ``commit_filesystem_snapshot``."""
        if not self.check_docker():
            return None
        bundle_dir = self._decode_fc_bundle_dir(bundle_ref)
        if not bundle_dir or not os.path.isdir(bundle_dir):
            logger.error("Firecracker restore: invalid bundle ref %r", bundle_ref)
            return None
        snap_f = os.path.join(bundle_dir, "vm.snap")
        mem_f = os.path.join(bundle_dir, "vm.mem")
        root_golden = os.path.join(bundle_dir, "rootfs.ext4")
        man_f = os.path.join(bundle_dir, "manifest.json")
        if not all(os.path.isfile(p) for p in (snap_f, mem_f, root_golden)):
            logger.error("Firecracker restore: incomplete bundle in %s", bundle_dir)
            return None

        slot = -1
        manifest_guest_ip: Optional[str] = None
        if os.path.isfile(man_f):
            try:
                with open(man_f, "r", encoding="utf-8") as mf:
                    man = json.load(mf)
                slot = int(man.get("tap_slot", -1))
                manifest_guest_ip = (man.get("guest_ip") or "").strip() or None
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as ex:
                logger.warning("Firecracker restore: manifest read failed: %s", ex)
                slot = -1
        if slot < 0:
            slot = self._next_slot()
        tap = self._tap_name(slot)
        guest_ip = manifest_guest_ip or self._guest_ip(slot)

        ssh_key = (getattr(self._cfg, "FIRECRACKER_SSH_KEY", None) or "").strip()
        ssh_user = (getattr(self._cfg, "FIRECRACKER_SSH_USER", None) or "root").strip() or "root"
        fc_bin = (getattr(self._cfg, "FIRECRACKER_BINARY", None) or "").strip() or "/usr/local/bin/firecracker"
        if not os.path.exists(f"/sys/class/net/{tap}"):
            logger.error("Firecracker restore: tap %r missing (slot=%s)", tap, slot)
            return None

        vm_token = uuid.uuid4().hex[:12]
        cid = f"fc-{vm_token}"
        workdir = tempfile.mkdtemp(prefix=f"fc-{vm_token}-")
        api_sock = os.path.join(workdir, "api.sock")
        root_rw = os.path.join(workdir, "rootfs.ext4")
        try:
            shutil.copy2(root_golden, root_rw)
            shutil.copy2(snap_f, os.path.join(workdir, "vm.snap"))
            shutil.copy2(mem_f, os.path.join(workdir, "vm.mem"))
        except OSError as ex:
            logger.error("Firecracker restore: staging copy failed: %s", ex)
            shutil.rmtree(workdir, ignore_errors=True)
            return None
        local_snap = os.path.abspath(os.path.join(workdir, "vm.snap"))
        local_mem = os.path.abspath(os.path.join(workdir, "vm.mem"))

        fc_cmd = [fc_bin, "--api-sock", api_sock]
        if str(getattr(self._cfg, "FIRECRACKER_ENABLE_PCI", "")).lower() in ("1", "true", "yes"):
            fc_cmd.append("--enable-pci")
        try:
            proc = subprocess.Popen(
                fc_cmd,
                cwd=workdir,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as ex:
            logger.error("Firecracker restore: spawn failed: %s", ex)
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        for _wait in range(50):
            if os.path.exists(api_sock):
                break
            time.sleep(0.1)
        if proc.poll() is not None:
            logger.error("Firecracker restore: firecracker exited before API ready")
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        http = _FcUnixClient(api_sock)
        load_bodies: List[dict] = [
            {
                "snapshot_path": local_snap,
                "mem_file_path": local_mem,
                "resume_vm": True,
                "track_dirty_pages": False,
                "network_overrides": [{"iface_id": "net0", "host_dev_name": tap}],
            },
            {
                "snapshot_path": local_snap,
                "mem_file_path": local_mem,
                "resume_vm": True,
                "track_dirty_pages": False,
            },
        ]
        last_data = b""
        loaded = False
        for body in load_bodies:
            try:
                code, last_data = http.request("PUT", "/snapshot/load", body)
                if code in (200, 201, 204):
                    loaded = True
                    break
                logger.warning("Firecracker snapshot/load returned %s: %r", code, last_data[:500])
            except OSError as ex:
                logger.warning("Firecracker snapshot/load transport: %s", ex)
        if not loaded:
            logger.error("Firecracker restore: snapshot/load failed: %r", last_data[:1200])
            proc.terminate()
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        st = _VmState(
            proc=proc,
            api_sock=api_sock,
            workdir=workdir,
            guest_ip=guest_ip,
            tap_name=tap,
            ssh_user=ssh_user,
            ssh_key=ssh_key,
            slot=int(slot),
        )
        if not self._wait_ssh(st, deadline_s=90.0):
            logger.error("Firecracker restore: SSH not up for %s", guest_ip)
            proc.terminate()
            shutil.rmtree(workdir, ignore_errors=True)
            return None
        with self._lock:
            self._vms[cid] = st
        logger.info("Firecracker VM %s restored from bundle guest_ip=%s tap=%s", cid, guest_ip, tap)
        return cid

    def create_container(self, name: str, config: ContainerConfig) -> Optional[str]:
        ref = (getattr(config, "fc_bundle_ref", None) or "").strip()
        if ref.startswith(FC_BUNDLE_SCHEME):
            return self._create_vm_from_fc_bundle(name, config, ref)
        if not self.check_docker():
            return None
        fc_bin = (getattr(self._cfg, "FIRECRACKER_BINARY", None) or "").strip() or "/usr/local/bin/firecracker"
        kernel = (getattr(self._cfg, "FIRECRACKER_KERNEL", None) or "").strip()
        root_default = (getattr(self._cfg, "FIRECRACKER_ROOTFS", None) or "").strip()
        ssh_key = (getattr(self._cfg, "FIRECRACKER_SSH_KEY", None) or "").strip()
        ssh_user = (getattr(self._cfg, "FIRECRACKER_SSH_USER", None) or "root").strip() or "root"
        gw = (getattr(self._cfg, "FIRECRACKER_GATEWAY", None) or "172.16.0.1").strip()

        root_src = (config.rootfs_path or "").strip() or root_default
        if not root_src or not os.path.isfile(root_src):
            logger.error("Firecracker: missing rootfs (config.rootfs_path / FIRECRACKER_ROOTFS)")
            return None

        slot = self._next_slot()
        tap = self._tap_name(slot)
        guest_ip = self._guest_ip(slot)
        if not os.path.exists(f"/sys/class/net/{tap}"):
            logger.error(
                "Firecracker: tap %r missing (create it on the host, e.g. ``ip tuntap add dev %s mode tap`` + bridge). slot=%s",
                tap,
                tap,
                slot,
            )
            return None

        vm_token = uuid.uuid4().hex[:12]
        cid = f"fc-{vm_token}"
        workdir = tempfile.mkdtemp(prefix=f"fc-{vm_token}-")
        api_sock = os.path.join(workdir, "api.sock")
        root_rw = os.path.join(workdir, "rootfs.ext4")
        if not self._copy_rootfs_into_place(root_src, root_rw):
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        vcpu = _vcpu(config.cpu_limit)
        mem_mib = _mem_mib(config.memory_limit)
        guest_mac = f"AA:FC:{slot:02x}:{random.randint(0, 255):02x}:{random.randint(0, 255):02x}:{random.randint(0, 255):02x}"

        boot_args = (
            f"reboot=k panic=1 pci=off console=ttyS0 "
            f"root=/dev/vda rw "
            f"ip={guest_ip}::{gw}:255.255.255.0::eth0:off"
        )

        fc_cmd = [fc_bin, "--api-sock", api_sock]
        if str(getattr(self._cfg, "FIRECRACKER_ENABLE_PCI", "")).lower() in ("1", "true", "yes"):
            fc_cmd.append("--enable-pci")

        try:
            proc = subprocess.Popen(
                fc_cmd,
                cwd=workdir,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError as ex:
            logger.error("Firecracker: failed to spawn binary: %s", ex)
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        for _wait in range(50):
            if os.path.exists(api_sock):
                break
            time.sleep(0.1)
        if proc.poll() is not None:
            logger.error("Firecracker process exited early (check kernel/rootfs/tap and ``dmesg``)")
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        http = _FcUnixClient(api_sock)
        steps = [
            ("PUT", "/machine-config", {"vcpu_count": vcpu, "mem_size_mib": mem_mib}),
            (
                "PUT",
                "/boot-source",
                {"kernel_image_path": kernel, "boot_args": boot_args},
            ),
            (
                "PUT",
                "/drives/rootfs",
                {
                    "drive_id": "rootfs",
                    "path_on_host": "rootfs.ext4",
                    "is_root_device": True,
                    "is_read_only": False,
                },
            ),
            (
                "PUT",
                "/network-interfaces/net0",
                {"iface_id": "net0", "guest_mac": guest_mac, "host_dev_name": tap},
            ),
            ("PUT", "/actions", {"action_type": "InstanceStart"}),
        ]
        try:
            for _method, path, body in steps:
                code, data = http.request("PUT", path, body)
                if code not in (200, 201, 204):
                    logger.error(
                        "Firecracker API error PUT %s -> %s %r",
                        path,
                        code,
                        data[:800],
                    )
                    proc.terminate()
                    try:
                        proc.wait(timeout=3.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    shutil.rmtree(workdir, ignore_errors=True)
                    return None
        except OSError as ex:
            logger.error("Firecracker API transport failed: %s", ex)
            proc.terminate()
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        st = _VmState(
            proc=proc,
            api_sock=api_sock,
            workdir=workdir,
            guest_ip=guest_ip,
            tap_name=tap,
            ssh_user=ssh_user,
            ssh_key=ssh_key,
            slot=int(slot),
        )
        if not self._wait_ssh(st):
            logger.error("Firecracker: SSH never came up for %s (%s)", cid, guest_ip)
            proc.terminate()
            shutil.rmtree(workdir, ignore_errors=True)
            return None

        with self._lock:
            self._vms[cid] = st
        logger.info("Firecracker VM %s guest_ip=%s tap=%s", cid, guest_ip, tap)
        return cid

    def _get(self, container_id: str) -> Optional[_VmState]:
        with self._lock:
            return self._vms.get(container_id)

    def run_command(
        self,
        container_id: str,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        user: Optional[str] = None,
    ) -> Dict[str, Any]:
        st = self._get(container_id)
        if not st:
            return {"exit_code": -1, "stdout": "", "stderr": "unknown firecracker instance", "pid": -1}
        wd = cwd or "/"
        user = user or "root"
        env_prefix = ""
        if env:
            parts = []
            for k, v in env.items():
                parts.append(f"export {shlex.quote(k)}={shlex.quote(str(v))}; ")
            env_prefix = "".join(parts)
        inner = f"cd {shlex.quote(wd)} && {env_prefix}/bin/sh -c {shlex.quote(command)}"
        remote = ["sudo", "-u", user, "/bin/sh", "-c", inner] if user != "root" else ["/bin/sh", "-c", inner]
        try:
            r = self._ssh_run(st, remote, timeout=float(timeout) if timeout is not None else 3600.0)
        except subprocess.TimeoutExpired:
            return {
                "exit_code": -1,
                "stdout": "",
                "stderr": f"ssh timeout after {timeout}s",
                "pid": -1,
            }
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
        st = self._get(container_id)
        if not st:
            yield {"type": "error", "message": "unknown firecracker instance"}
            yield {"type": "exit", "exit_code": -1}
            return
        wd = cwd or "/"
        user = user or "root"
        env_prefix = ""
        if env:
            parts = []
            for k, v in env.items():
                parts.append(f"export {shlex.quote(k)}={shlex.quote(str(v))}; ")
            env_prefix = "".join(parts)
        inner = f"cd {shlex.quote(wd)} && {env_prefix}/bin/sh -c {shlex.quote(command)}"
        remote = ["sudo", "-u", user, "/bin/sh", "-c", inner] if user != "root" else ["/bin/sh", "-c", inner]
        cmd = self._ssh_base(st) + remote
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=False,
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

    def read_file(self, container_id: str, path: str) -> Optional[str]:
        st = self._get(container_id)
        if not st:
            return None
        r = self._ssh_run(st, ["/bin/cat", path], timeout=120.0)
        if r.returncode != 0:
            return None
        return _sanitize_read_text(r.stdout or b"")

    def write_file(self, container_id: str, path: str, content: str) -> bool:
        st = self._get(container_id)
        if not st:
            return False
        parent = os.path.dirname(path) or "/"
        mk = self._ssh_run(st, ["/bin/mkdir", "-p", parent], timeout=60.0)
        if mk.returncode != 0:
            return False
        fd, tmp = tempfile.mkstemp(prefix="fcw-", suffix=".bin")
        os.close(fd)
        try:
            with open(tmp, "wb") as f:
                f.write(content.encode("utf-8").replace(b"\x00", b""))
            dst = f"{st.ssh_user}@{st.guest_ip}:{path}"
            r = subprocess.run(
                [
                    "scp",
                    "-q",
                    "-i",
                    st.ssh_key,
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    f"UserKnownHostsFile={getattr(self._cfg, 'FIRECRACKER_SSH_KNOWN_HOSTS', '/dev/null')}",
                    "-o",
                    "BatchMode=yes",
                    tmp,
                    dst,
                ],
                capture_output=True,
                timeout=300,
                check=False,
            )
            return r.returncode == 0
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def list_files(self, container_id: str, path: str = "/") -> Optional[list]:
        st = self._get(container_id)
        if not st:
            return None
        r = self._ssh_run(st, ["/bin/sh", "-c", f"ls -la {shlex.quote(path)}"], timeout=60.0)
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
        st = self._get(container_id)
        if not st:
            return False
        if recursive:
            cmd = f"rm -rf {shlex.quote(path)}"
        else:
            cmd = f"rm -f {shlex.quote(path)}"
        r = self._ssh_run(st, ["/bin/sh", "-c", cmd], timeout=120.0)
        return r.returncode == 0

    def create_directory(self, container_id: str, path: str, mode: int = 0o755) -> bool:
        st = self._get(container_id)
        if not st:
            return False
        modestr = format(mode & 0o777, "o")
        r = self._ssh_run(st, ["/bin/mkdir", "-p", "-m", modestr, path], timeout=60.0)
        return r.returncode == 0

    def get_container_stats(self, container_id: str) -> Optional[Dict[str, Any]]:
        st = self._get(container_id)
        if not st:
            return None
        r = self._ssh_run(st, ["/bin/sh", "-c", "grep -E '^MemTotal:|^MemAvailable:' /proc/meminfo || true"], timeout=10.0)
        txt = (r.stdout or b"").decode("utf-8", errors="replace")
        return {"raw_meminfo": txt.strip(), "backend": "firecracker"}

    def kill_container(self, container_id: str, force: bool = True) -> bool:
        st = self._get(container_id)
        if not st:
            return False
        with self._lock:
            self._vms.pop(container_id, None)
        try:
            st.proc.terminate()
            st.proc.wait(timeout=8 if not force else 3)
        except subprocess.TimeoutExpired:
            st.proc.kill()
        try:
            shutil.rmtree(st.workdir, ignore_errors=True)
        except OSError:
            pass
        return True

    def is_container_running(self, container_id: str) -> bool:
        st = self._get(container_id)
        if not st:
            return False
        if st.proc.poll() is not None:
            return False
        r = self._ssh_run(st, ["true"], timeout=3.0)
        return r.returncode == 0

    def pause_instance(self, instance_id: str) -> bool:
        st = self._get(instance_id)
        if not st:
            return False
        code, _ = self._fc_patch(st.api_sock, "/vm", {"state": "Paused"})
        return code in (200, 201, 204)

    def resume_instance(self, instance_id: str) -> bool:
        st = self._get(instance_id)
        if not st:
            return False
        code, _ = self._fc_patch(st.api_sock, "/vm", {"state": "Resumed"})
        return code in (200, 201, 204)
