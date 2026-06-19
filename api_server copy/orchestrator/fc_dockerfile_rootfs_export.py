"""Export a local Docker OCI image to an **ext4** file for Firecracker guest root.

Used when ``SANDBOX_ENGINE=firecracker`` and templates are built from a Dockerfile: the API still
builds with **Docker Engine**, then materializes ``warm_snapshot_image`` as a host ``*.ext4`` path.

Requires ``docker`` on ``PATH``, a pullable **privileged** helper image (default ``alpine:3.19``),
and Linux-style loop mounts inside that helper (``docker run --privileged``).
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from config import Config

logger = logging.getLogger(__name__)


def _sanitize_template_id_for_path(template_id: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (template_id or "tpl").strip()) or "tpl"
    return s[:56]


def read_ssh_public_key_line(private_key_path: str) -> Optional[str]:
    """Return one ``authorized_keys`` line for ``private_key_path`` (``.pub`` or ``ssh-keygen -y``)."""
    pk = (private_key_path or "").strip()
    if not pk or not os.path.isfile(pk):
        return None
    pub = pk + ".pub"
    if os.path.isfile(pub):
        line = Path(pub).read_text(encoding="utf-8", errors="replace").strip().splitlines()
        return line[0].strip() if line else None
    try:
        r = subprocess.run(
            ["ssh-keygen", "-y", "-f", pk],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if r.returncode != 0 or not (r.stdout or "").strip():
            logger.warning("ssh-keygen -y failed for %s: %s", pk, (r.stderr or "")[:300])
            return None
        ktype_key = (r.stdout or "").strip().splitlines()[0].strip()
        return f"{ktype_key} firecracker-api-injected"
    except (OSError, subprocess.SubprocessError) as ex:
        logger.warning("Could not derive pubkey from %s: %s", pk, ex)
        return None


def export_oci_image_to_ext4_file(
    *,
    image_ref: str,
    dest_ext4: Path,
    size_mb: int,
    builder_image: str,
    ssh_pubkey_line: Optional[str],
) -> None:
    """Create ``dest_ext4`` from ``docker export`` of ``image_ref``.

    Raises ``RuntimeError`` on failure.
    """
    if not shutil.which("docker"):
        raise RuntimeError("docker CLI not found on PATH (required to export OCI image to ext4)")
    img = (image_ref or "").strip()
    if not img:
        raise RuntimeError("empty image_ref for ext4 export")

    dest_ext4 = dest_ext4.resolve()
    dest_ext4.parent.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="fc-oci2ext4-"))
    tar_path = work / "root.tar"
    cname = f"fc-export-{uuid.uuid4().hex[:12]}"
    try:
        r0 = subprocess.run(
            ["docker", "create", "--name", cname, img],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        if r0.returncode != 0:
            raise RuntimeError(f"docker create failed: {(r0.stderr or r0.stdout or '')[:4000]}")
        r1 = subprocess.run(
            ["docker", "export", cname, "-o", str(tar_path)],
            capture_output=True,
            text=True,
            timeout=7200,
            check=False,
        )
        if r1.returncode != 0:
            raise RuntimeError(f"docker export failed: {(r1.stderr or r1.stdout or '')[:4000]}")
        if not tar_path.is_file() or tar_path.stat().st_size < 64:
            raise RuntimeError("docker export produced empty or missing root.tar")

        if dest_ext4.exists():
            try:
                dest_ext4.unlink()
            except OSError as ex:
                raise RuntimeError(f"cannot replace {dest_ext4}: {ex}") from ex

        pubkey_path = work / "pubkey.txt"
        if ssh_pubkey_line and ssh_pubkey_line.strip():
            pubkey_path.write_text(ssh_pubkey_line.strip() + "\n", encoding="utf-8")
        else:
            pubkey_path.write_text("", encoding="utf-8")

        out_dir = str(dest_ext4.parent.resolve())
        out_name = dest_ext4.name
        inner = f"""set -eu
apk add --no-cache e2fsprogs tar util-linux >/dev/null
OUT="/hostout/{out_name}"
rm -f "$OUT"
dd if=/dev/zero of="$OUT" bs=1M count={int(size_mb)}
mkfs.ext4 -F "$OUT"
mkdir -p /mnt
mount -t ext4 -o loop "$OUT" /mnt
tar -xf /work/root.tar -C /mnt
if [ -s /work/pubkey.txt ]; then
  mkdir -p /mnt/root/.ssh
  chmod 700 /mnt/root/.ssh || true
  if ! grep -qxF -f /work/pubkey.txt /mnt/root/.ssh/authorized_keys 2>/dev/null; then
    cat /work/pubkey.txt >> /mnt/root/.ssh/authorized_keys
  fi
  chmod 600 /mnt/root/.ssh/authorized_keys || true
fi
umount /mnt || true
e2fsck -f -p "$OUT" >/dev/null 2>&1 || true
resize2fs "$OUT" >/dev/null 2>&1 || true
"""

        r2 = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--privileged",
                "-v",
                f"{work}:/work:ro",
                "-v",
                f"{out_dir}:/hostout",
                (builder_image or "alpine:3.19").strip(),
                "sh",
                "-ec",
                inner,
            ],
            capture_output=True,
            text=True,
            timeout=7200,
            check=False,
        )
        if r2.returncode != 0:
            log = f"{r2.stdout or ''}\n{r2.stderr or ''}"
            raise RuntimeError(f"privileged ext4 build failed (exit {r2.returncode}): {log[-12000:]}")

        if not dest_ext4.is_file() or dest_ext4.stat().st_size < 1024 * 1024:
            raise RuntimeError(f"ext4 output missing or too small: {dest_ext4}")
    finally:
        subprocess.run(
            ["docker", "rm", "-f", cname],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        shutil.rmtree(work, ignore_errors=True)


def materialize_firecracker_template_ext4(
    cfg: "Config",
    *,
    oci_image_ref: str,
    template_id: str,
) -> str:
    """Build host ``*.ext4`` under ``FIRECRACKER_DOCKERFILE_ROOTFS_DIR``; return absolute path."""
    root_dir = (getattr(cfg, "FIRECRACKER_DOCKERFILE_ROOTFS_DIR", None) or "").strip() or os.path.join(
        os.getcwd(), "fc-dockerfile-rootfs"
    )
    Path(root_dir).mkdir(parents=True, exist_ok=True)
    fn = f"{_sanitize_template_id_for_path(template_id)}-{uuid.uuid4().hex[:10]}.ext4"
    dest = Path(root_dir) / fn

    # Size from a throwaway export would double I/O; size from `docker image inspect` instead.
    size_mb = max(512, int(getattr(cfg, "FIRECRACKER_DOCKERFILE_EXT4_MIN_MB", 4096) or 4096))
    cap = max(size_mb, int(getattr(cfg, "FIRECRACKER_DOCKERFILE_EXT4_MAX_MB", 65536) or 65536))
    try:
        insp = subprocess.run(
            ["docker", "image", "inspect", "-f", "{{.Size}}", oci_image_ref],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if insp.returncode == 0 and (insp.stdout or "").strip().isdigit():
            b = int(insp.stdout.strip())
            size_mb = int(b / (1024 * 1024) * 1.35) + 1024
            size_mb = max(size_mb, int(getattr(cfg, "FIRECRACKER_DOCKERFILE_EXT4_MIN_MB", 4096) or 4096))
            size_mb = min(size_mb, cap)
    except (OSError, subprocess.SubprocessError, ValueError) as ex:
        logger.debug("image inspect for size failed, using min MB: %s", ex)

    inj = bool(getattr(cfg, "FIRECRACKER_DOCKERFILE_INJECT_SSH_PUBKEY", True))
    pub = None
    if inj:
        pub = read_ssh_public_key_line(getattr(cfg, "FIRECRACKER_SSH_KEY", "") or "")

    builder = (getattr(cfg, "FIRECRACKER_EXT4_BUILDER_IMAGE", None) or "alpine:3.19").strip()

    logger.info(
        "Firecracker Dockerfile template: exporting %r to %s (~%s MiB)",
        oci_image_ref,
        dest,
        size_mb,
    )
    try:
        export_oci_image_to_ext4_file(
            image_ref=oci_image_ref,
            dest_ext4=dest,
            size_mb=size_mb,
            builder_image=builder,
            ssh_pubkey_line=pub,
        )
    except RuntimeError:
        raise
    except Exception as ex:
        raise RuntimeError(f"ext4 export failed: {ex}") from ex
    return str(dest.resolve())
