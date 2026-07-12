"""Bake ``envd_guest`` into Docker template images at **build** time (no per-sandbox pip/tarball).

When ``ENVD_EMBED_AT_TEMPLATE_BUILD`` is enabled, the API layers ``/opt/envd_guest`` + ``pip install``
into logical templates during:

- ``POST /templates`` one-shot warm snapshot (``docker commit`` path),
- ``POST /templates/from-dockerfile`` (parsed and ``docker_cli`` modes).

Sandboxes created from those images only need a lightweight **uvicorn start** at runtime
(see ``SandboxManager._bootstrap_envd_daemon``) when ``ENVD_AUTO_START`` is on.
"""

from __future__ import annotations

import io
import logging
import re
import shutil
import tarfile
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

ENVD_BAKE_MARKER = "/opt/envd_guest/.mysandbox_envd_baked"
ENVD_BAKE_VERSION = "connect-v1"

# Ubuntu 22.04+ blocks system-wide pip (PEP 668); try --break-system-packages, then plain pip.
ENVD_PIP_INSTALL_SHELL = (
    "python3 -m pip install --no-cache-dir -q --break-system-packages "
    "-r /opt/envd_guest/requirements.txt 2>/dev/null "
    "|| python3 -m pip install --no-cache-dir -q -r /opt/envd_guest/requirements.txt"
)

# ``python3`` alone is not enough (e.g. ubuntu:22.04 after ``apt install nodejs``).
ENVD_ENSURE_PYTHON_PIP_SHELL = """set -eu
if python3 -m pip --version >/dev/null 2>&1; then exit 0; fi
if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y --no-install-recommends python3 python3-pip ca-certificates
  exit 0
fi
if command -v apk >/dev/null 2>&1; then
  apk add --no-cache python3 py3-pip
  exit 0
fi
echo 'envd template bake: install python3-pip (apt/apk) or use a Python base image' >&2
exit 1
"""

ENVD_ENSURE_PYTHON_PIP_ONELINER = (
    "if python3 -m pip --version >/dev/null 2>&1; then :; "
    "elif command -v apt-get >/dev/null 2>&1; then "
    "export DEBIAN_FRONTEND=noninteractive && apt-get update -qq && "
    "DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "
    "python3 python3-pip ca-certificates; "
    "elif command -v apk >/dev/null 2>&1; then "
    "apk add --no-cache python3 py3-pip; "
    "else echo 'mysandbox envd embed: install python3-pip (apt/apk) or use a Python base image' >&2; "
    "exit 1; "
    "fi"
)


def envd_guest_source_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "envd_guest"


def envd_guest_tarball_bytes() -> Optional[bytes]:
    """Tar ``api_server/envd_guest`` for ``put_archive`` into the container under ``/opt``."""
    root = envd_guest_source_dir()
    if not root.is_dir():
        return None
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for p in sorted(root.rglob("*")):
            if p.is_dir():
                continue
            if "__pycache__" in p.parts or p.suffix in (".pyc", ".pyo"):
                continue
            arc = "envd_guest/" + p.relative_to(root).as_posix()
            tf.add(p, arcname=arc)
    return buf.getvalue()


def write_envd_guest_build_context(dest_dir: Path) -> bool:
    """Copy tree to ``dest_dir`` (e.g. ``…/tpl-df-xxx/envd_guest``) for ``docker build`` context."""
    root = envd_guest_source_dir()
    if not root.is_dir():
        return False
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    shutil.copytree(
        root,
        dest_dir,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )
    return True


def _skip_envd_restore_user(restore_user: str) -> bool:
    u = (restore_user or "").strip().lower()
    return u in ("", "none", "skip", "-", "false", "0")


def _dockerfile_lines_without_comment_only(dockerfile_text: str) -> str:
    """Drop full-line comments so ``# useradd ubuntu`` does not false-positive."""
    kept: list[str] = []
    for line in (dockerfile_text or "").splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def infer_envd_restore_user_from_dockerfile(dockerfile_text: str) -> str:
    """Pick a trailing ``USER`` for the envd injection layer from the Dockerfile text.

    Heuristic: if the template clearly defines or switches to login ``ubuntu`` (``USER ubuntu``,
    ``useradd``/``adduser`` creating ``ubuntu``), return ``"ubuntu"`` so the injected ``USER root``
    block can switch back after ``pip install``. Otherwise ``"none"`` (safe for slim bases that
    have no ``ubuntu`` passwd entry).
    """
    body = _dockerfile_lines_without_comment_only(dockerfile_text)
    collapsed = re.sub(r"\\\s*\n\s*", " ", body)
    if re.search(r"(?im)^\s*USER\s+ubuntu\b", collapsed):
        return "ubuntu"
    if re.search(r"(?i)\buseradd\b[^;\n#]*\bubuntu\b", collapsed):
        return "ubuntu"
    if re.search(r"(?i)\badduser\b[^;\n#]*\bubuntu\b", collapsed):
        return "ubuntu"
    return "none"


def resolve_envd_restore_user_for_embed(dockerfile_text: str, config_raw: str) -> str:
    """Resolve ``restore_user`` for :func:`dockerfile_append_envd_layer`.

    * ``auto`` (default), empty → infer from ``dockerfile_text``.
    * ``none``/``false``/… → never append trailing ``USER``.
    * Any other non-empty string → use as explicit account name (override / escape hatch).
    """
    v = (config_raw or "auto").strip().lower()
    if v in ("", "auto"):
        inferred = infer_envd_restore_user_from_dockerfile(dockerfile_text)
        logger.info(
            "envd Dockerfile restore user: inferred=%r (ENVD_DOCKERFILE_RESTORE_USER=auto)",
            inferred,
        )
        return inferred
    if v in ("none", "skip", "-", "false", "0"):
        return "none"
    return (config_raw or "").strip()


def dockerfile_append_envd_layer(*, restore_user: str = "none") -> str:
    """Instructions appended to user Dockerfiles (``docker_cli`` template builds).

    Installs ``python3`` + ``pip`` via ``apt-get`` or ``apk`` when missing so bare ``ubuntu:*`` /
    ``alpine`` bases work; images that already have ``python3`` skip package installs.

    Uses ``USER root`` for the envd ``COPY``/``RUN`` block because many images end with a non-root
    ``USER``: ``COPY`` leaves ``/opt/envd_guest`` root-owned, so ``touch`` would otherwise fail.

    The **trailing** ``USER`` is controlled by ``restore_user`` (see
    :func:`resolve_envd_restore_user_for_embed`): by default it is inferred from the Dockerfile so
    generic bases stay on root while Custodian-style templates that ``useradd ubuntu`` / ``USER
    ubuntu`` switch back automatically. Set ``ENVD_DOCKERFILE_RESTORE_USER`` to a concrete account
    or ``none`` only when you need to override inference.
    """
    run_block = (
        "\n# --- mysandbox: envd guest HTTP (auto-injected) ---\n"
        "USER root\n"
        "COPY envd_guest /opt/envd_guest\n"
        "RUN set -eux; "
        f"{ENVD_ENSURE_PYTHON_PIP_ONELINER}; "
        f"{ENVD_PIP_INSTALL_SHELL} && printf '%s\\n' '{ENVD_BAKE_VERSION}' > {ENVD_BAKE_MARKER}\n"
    )
    if _skip_envd_restore_user(restore_user):
        return run_block
    ru = restore_user.strip()
    return run_block + f"USER {ru}\n"


def _ensure_python3_for_envd_bake(
    run_command: Callable[..., Any],
    container_id: str,
    *,
    install_timeout_sec: float,
) -> bool:
    """Ensure ``python3 -m pip`` works in a build container (parsed snapshot / bake path)."""
    r = run_command(
        container_id,
        ENVD_ENSURE_PYTHON_PIP_SHELL,
        timeout=float(install_timeout_sec),
        user="root",
    )
    if int(r.get("exit_code") or 0) != 0:
        detail = (r.get("stderr") or r.get("stdout") or "").strip()
        logger.warning(
            "envd template bake: could not install python3-pip in container=%s detail=%s",
            container_id[:12],
            detail[:2500],
        )
        return False
    return True


def uvicorn_envd_start_background_script(port: int) -> str:
    """Shell snippet: spawn uvicorn on ``port`` in the background (no listen wait)."""
    p = max(1, min(65535, int(port)))
    return f""": > /tmp/envd.log
for pid in /proc/[0-9]*; do
  cmd="$(tr '\\000' ' ' < "$pid/cmdline" 2>/dev/null || true)"
  case "$cmd" in
    *"envd_guest.server:app"*"--port {p}"*) kill "${{pid##*/}}" 2>/dev/null || true ;;
  esac
done
sleep 0.1
if command -v setsid >/dev/null 2>&1; then
  setsid -f env PYTHONPATH=/opt python3 -m uvicorn envd_guest.server:app --host 0.0.0.0 --port {p} >>/tmp/envd.log 2>&1 &
else
  nohup env PYTHONPATH=/opt python3 -m uvicorn envd_guest.server:app --host 0.0.0.0 --port {p} >>/tmp/envd.log 2>&1 &
fi"""


def envd_health_wait_loop_script(
    port: int,
    *,
    max_seconds: float = 15.0,
    poll_seconds: float = 0.25,
    log_path: str = "",
) -> str:
    """Poll envd ``/health`` until the running guest reports the expected Connect phase."""
    import shlex

    p = max(1, min(65535, int(port)))
    poll = max(0.05, min(1.0, float(poll_seconds)))
    max_iters = max(1, int(float(max_seconds) / poll))
    log_tail = ""
    if log_path:
        log_tail = (
            f"\necho '--- {log_path} ---'\n"
            f"cat {shlex.quote(log_path)} 2>/dev/null || true\n"
        )
    return f"""for i in $(seq 1 {max_iters}); do
  if command -v python3 >/dev/null 2>&1; then
    python3 -c "import json,sys,urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:{p}/health', timeout=1)); sys.exit(0 if data.get('phase') == '{ENVD_BAKE_VERSION}' else 1)" 2>/dev/null && exit 0
  fi
  sleep {poll}
done{log_tail}exit 1"""


def guest_tcp_wait_loop_script(
    port: int,
    *,
    max_seconds: float = 8.0,
    poll_seconds: float = 0.1,
    log_path: str = "",
) -> str:
    """Poll localhost ``port`` until open or timeout."""
    import shlex

    p = max(1, min(65535, int(port)))
    poll = max(0.05, min(1.0, float(poll_seconds)))
    max_iters = max(1, int(float(max_seconds) / poll))
    log_tail = ""
    if log_path:
        log_tail = (
            f"\necho '--- {log_path} ---'\n"
            f"cat {shlex.quote(log_path)} 2>/dev/null || true\n"
        )
    return f"""for i in $(seq 1 {max_iters}); do
  if command -v python3 >/dev/null 2>&1; then
    python3 -c "import socket,sys;s=socket.socket();r=s.connect_ex(('127.0.0.1',{p}));sys.exit(0 if r==0 else 1)" 2>/dev/null && exit 0
  fi
  if (echo >/dev/tcp/127.0.0.1/{p}) 2>/dev/null; then exit 0; fi
  sleep {poll}
done{log_tail}exit 1"""


def uvicorn_envd_start_script(port: int) -> str:
    """Shell snippet: background uvicorn on ``port`` and wait until Connect envd is healthy."""
    p = max(1, min(65535, int(port)))
    return (
        f"set -eu\n{uvicorn_envd_start_background_script(p)}\n"
        + envd_health_wait_loop_script(p, max_seconds=15.0, poll_seconds=0.25, log_path="/tmp/envd.log")
    )


def container_has_baked_envd(
    run_command: Callable[..., Any],
    container_id: str,
    *,
    timeout: float = 10.0,
) -> bool:
    # Avoid a failing ``test`` exit (would log exit_code=1 as if it were an error) when the image
    # is simply not pre-baked yet — use stdout markers with shell exit 0.
    r = run_command(
        container_id,
        (
            f"if test -f {ENVD_BAKE_MARKER} && test -f /opt/envd_guest/server.py "
            f"&& test \"$(cat {ENVD_BAKE_MARKER} 2>/dev/null || true)\" = {ENVD_BAKE_VERSION!r}; "
            f"then echo __ENVD_BAKED__; else echo __ENVD_NOT_BAKED__; fi"
        ),
        timeout=timeout,
    )
    out = (r.get("stdout") or "").strip()
    return "__ENVD_BAKED__" in out and int(r.get("exit_code") or 0) == 0


def bake_envd_guest_into_container(
    *,
    put_archive_to_container: Callable[..., Any],
    run_command: Callable[..., Any],
    container_id: str,
    pip_timeout_sec: float,
) -> bool:
    """Put tarball under ``/opt``, ``pip install`` requirements, write bake marker. Idempotent."""
    if container_has_baked_envd(run_command, container_id):
        logger.info("envd template bake: already present in %s", container_id[:12])
        return True
    tb = envd_guest_tarball_bytes()
    if not tb:
        logger.warning("envd template bake: api_server/envd_guest missing on API host")
        return False
    if not put_archive_to_container(container_id, "/opt", tb):
        logger.warning("envd template bake: put_archive failed container=%s", container_id[:12])
        return False
    inst_to = max(120.0, float(pip_timeout_sec), 600.0)
    if not _ensure_python3_for_envd_bake(run_command, container_id, install_timeout_sec=inst_to):
        return False
    pip = run_command(
        container_id,
        ENVD_PIP_INSTALL_SHELL,
        timeout=float(pip_timeout_sec),
        user="root",
    )
    if int(pip.get("exit_code") or 0) != 0:
        detail = (pip.get("stderr") or pip.get("stdout") or "").strip()
        logger.warning(
            "envd template bake: pip failed container=%s detail=%s",
            container_id[:12],
            detail[:2500],
        )
        return False
    mk = run_command(
        container_id,
        f"printf '%s\\n' '{ENVD_BAKE_VERSION}' > {ENVD_BAKE_MARKER}",
        timeout=30.0,
        user="root",
    )
    if int(mk.get("exit_code") or 0) != 0:
        logger.warning("envd template bake: could not write marker container=%s", container_id[:12])
        return False
    logger.info("envd template bake: embedded into container %s", container_id[:12])
    return True


def should_embed_envd_at_template_build(config: Any) -> bool:
    return bool(getattr(config, "ENVD_EMBED_AT_TEMPLATE_BUILD", True))
