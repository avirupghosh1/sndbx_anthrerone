from __future__ import annotations

import io
import logging
import os
import re
import threading
import shutil
import tarfile
import tempfile
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

import docker

logger = logging.getLogger(__name__)

ENVD_BAKE_MARKER = "/opt/envd_guest/.mysandbox_envd_baked"
ENVD_BAKE_VERSION = "connect-v1"
ENVD_PIP_INSTALL_SHELL = (
    "python3 -m pip install --no-cache-dir -q --break-system-packages "
    "-r /opt/envd_guest/requirements.txt 2>/dev/null "
    "|| python3 -m pip install --no-cache-dir -q -r /opt/envd_guest/requirements.txt"
)
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


def _docker_host() -> str:
    return (os.environ.get("DOCKER_HOST") or "tcp://127.0.0.1:2375").strip()


def _sanitize_for_tag(template_id: str) -> str:
    s = re.sub(r"[^a-z0-9._-]+", "-", template_id.strip().lower()).strip("-") or "tpl"
    return s[:48]


def _sanitize_registry_tag_component(value: str, *, max_len: int = 64) -> str:
    s = re.sub(r"[^A-Za-z0-9_.-]+", "-", (value or "").strip()).strip(".-")
    return (s or "latest")[:max_len]


def _entry_to_text(entry: object) -> str:
    if not isinstance(entry, dict):
        return str(entry or "")
    stream = str(entry.get("stream") or "")
    if stream:
        return stream
    error = str(entry.get("error") or "")
    if error:
        return error.rstrip("\n") + "\n"
    status = str(entry.get("status") or "")
    progress = str(entry.get("progress") or "")
    if status:
        return f"{status} {progress}".rstrip() + "\n"
    aux = entry.get("aux")
    if isinstance(aux, dict) and aux:
        return f"{aux}\n"
    return ""


def _build_context_to_tar_gz(root: Path) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tf:
        for path in sorted(root.rglob("*")):
            tf.add(path, arcname=path.relative_to(root).as_posix())
    return out.getvalue()


def _skip_envd_restore_user(restore_user: str) -> bool:
    return (restore_user or "").strip().lower() in ("", "none", "skip", "-", "false", "0")


def _dockerfile_lines_without_comment_only(dockerfile_text: str) -> str:
    kept: list[str] = []
    for line in (dockerfile_text or "").splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def infer_envd_restore_user_from_dockerfile(dockerfile_text: str) -> str:
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
    v = (config_raw or "auto").strip().lower()
    if v in ("", "auto"):
        return infer_envd_restore_user_from_dockerfile(dockerfile_text)
    if v in ("none", "skip", "-", "false", "0"):
        return "none"
    return (config_raw or "").strip()


def dockerfile_append_envd_layer(*, restore_user: str = "none") -> str:
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
    return run_block + f"USER {restore_user.strip()}\n"


def envd_guest_source_dir() -> Optional[Path]:
    candidates = [
        Path(os.environ.get("ENVD_GUEST_SOURCE_DIR", "")).expanduser() if os.environ.get("ENVD_GUEST_SOURCE_DIR") else None,
        Path(__file__).resolve().parent / "api_server_envd_guest",
        Path(__file__).resolve().parent.parent / "api-service" / "envd_guest",
    ]
    for candidate in candidates:
        if candidate and candidate.is_dir():
            return candidate
    return None


def write_envd_guest_build_context(dest_dir: Path) -> bool:
    root = envd_guest_source_dir()
    if root is None:
        return False
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    shutil.copytree(
        root,
        dest_dir,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )
    return True


def envd_guest_tarball_bytes() -> Optional[bytes]:
    root = envd_guest_source_dir()
    if root is None:
        return None
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for p in sorted(root.rglob("*")):
            if p.is_dir():
                continue
            if "__pycache__" in p.parts or p.suffix in (".pyc", ".pyo"):
                continue
            tf.add(p, arcname="envd_guest/" + p.relative_to(root).as_posix())
    return buf.getvalue()


def _prepare_build_context(
    *,
    dockerfile: str,
    context_tar_gzip: Optional[bytes],
    embed_envd: bool,
    restore_user_mode: str,
) -> bytes:
    tmp = Path(tempfile.mkdtemp(prefix="gw-tpl-df-"))
    try:
        if context_tar_gzip:
            with tarfile.open(fileobj=io.BytesIO(context_tar_gzip), mode="r:gz") as tf:
                try:
                    tf.extractall(tmp, filter="data")
                except TypeError:
                    tf.extractall(tmp)
        df_text = dockerfile
        if embed_envd and write_envd_guest_build_context(tmp / "envd_guest"):
            restore_user = resolve_envd_restore_user_for_embed(dockerfile, restore_user_mode)
            df_text = (dockerfile.rstrip() + dockerfile_append_envd_layer(restore_user=restore_user)).lstrip()
        (tmp / "Dockerfile").write_text(df_text, encoding="utf-8")
        return _build_context_to_tar_gz(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _docker_api_client(timeout: int):
    return docker.APIClient(base_url=_docker_host(), timeout=timeout)


def _docker_client(timeout: int):
    return docker.DockerClient(base_url=_docker_host(), timeout=timeout)


_registry_login_lock = threading.Lock()
_registry_login_cache: set[str] = set()


def _registry_auth_config() -> Optional[dict]:
    from config import get_config

    cfg = get_config()
    server = (getattr(cfg, "TEMPLATE_REGISTRY_SERVER", "") or "").strip().rstrip("/")
    if not server:
        server = _registry_server_from_repo_prefix(
            str(getattr(cfg, "TEMPLATE_REGISTRY_REPO_PREFIX", "") or "")
        )
    username = (getattr(cfg, "TEMPLATE_REGISTRY_USERNAME", "") or "").strip()
    password = getattr(cfg, "TEMPLATE_REGISTRY_PASSWORD", "") or ""
    if not (server and username and password):
        return None
    return {
        "username": username,
        "password": password,
        "serveraddress": server,
    }


def _registry_auth_config_for_ref(image_ref: str) -> Optional[dict]:
    auth = _registry_auth_config()
    if not auth:
        return None
    configured_server = str(auth.get("serveraddress") or "").strip().rstrip("/")
    image_server = _registry_server_from_image_ref(image_ref)
    if configured_server and image_server and configured_server == image_server:
        return auth
    return None


def _registry_server_from_image_ref(image_ref: str) -> str:
    first, sep, _rest = (image_ref or "").strip().partition("/")
    if not sep:
        return ""
    if "." in first or ":" in first or first == "localhost":
        return first.rstrip("/")
    return ""


def _registry_server_from_repo_prefix(repo_prefix: str) -> str:
    first = (repo_prefix or "").strip().split("/", 1)[0].strip()
    if "." in first or ":" in first or first == "localhost":
        return first.rstrip("/")
    return ""


def _registry_cache_ref_for_image_ref(image_ref: str) -> str:
    from config import get_config

    ref = (image_ref or "").strip()
    if not ref or "@" in ref:
        return ""
    cfg = get_config()
    if not bool(getattr(cfg, "TEMPLATE_REGISTRY_CACHE_ENABLED", False)):
        return ""
    cache_server = (getattr(cfg, "TEMPLATE_REGISTRY_CACHE_SERVER", "") or "").strip().rstrip("/")
    if not cache_server:
        return ""
    image_server = _registry_server_from_image_ref(ref)
    if not image_server:
        return ""
    upstream_server = (
        getattr(cfg, "TEMPLATE_REGISTRY_CACHE_UPSTREAM_SERVER", "")
        or getattr(cfg, "TEMPLATE_REGISTRY_SERVER", "")
        or ""
    ).strip().rstrip("/")
    if upstream_server and image_server.lower() != upstream_server.lower():
        return ""
    _server, _sep, path = ref.partition("/")
    if not path:
        return ""
    return f"{cache_server}/{path}"


def _split_repository_tag(image_ref: str) -> tuple[str, Optional[str]]:
    ref = (image_ref or "").strip()
    slash = ref.rfind("/")
    colon = ref.rfind(":")
    if colon > slash:
        return ref[:colon], ref[colon + 1 :] or None
    return ref, None


def _is_ecr_repo_prefix(repo_prefix: str) -> bool:
    server = _registry_server_from_repo_prefix(repo_prefix).lower()
    return (
        server == "public.ecr.aws"
        or ".dkr.ecr." in server
        or server.endswith(".amazonaws.com")
    )


def _resolve_registry_layout(*, repo_prefix: str, requested_layout: str) -> str:
    layout = (requested_layout or "auto").strip().lower().replace("-", "_")
    if layout in ("single", "single_repo", "single_repository"):
        return "single_repository"
    if layout in ("per_template", "repository_per_template", "subrepository"):
        return "repository_per_template"
    if layout != "auto":
        raise ValueError(
            "TEMPLATE_REGISTRY_LAYOUT must be auto, repository_per_template, or single_repository"
        )
    if _is_ecr_repo_prefix(repo_prefix):
        return "single_repository"
    return "repository_per_template"


def ensure_registry_login(*, timeout: int) -> None:
    auth_config = _registry_auth_config()
    server = str((auth_config or {}).get("serveraddress") or "")
    username = str((auth_config or {}).get("username") or "")
    password = str((auth_config or {}).get("password") or "")
    from config import get_config

    cfg = get_config()
    auth_required = bool(getattr(cfg, "TEMPLATE_REGISTRY_AUTH_REQUIRED", False))
    if auth_required and not (server and username and password):
        raise RuntimeError(
            "template registry auth is required but TEMPLATE_REGISTRY_SERVER/USERNAME/PASSWORD are incomplete"
        )
    if not (server and username and password):
        return
    cache_key = f"{server}|{username}"
    with _registry_login_lock:
        if cache_key in _registry_login_cache:
            return
        client = _docker_client(timeout)
        try:
            client.login(
                username=username,
                password=password,
                registry=server,
                reauth=True,
            )
            _registry_login_cache.add(cache_key)
        finally:
            client.close()


def _published_template_ref(
    *,
    local_ref: str,
    template_id: str,
    repo_prefix: str,
    layout: str = "auto",
) -> str:
    base = (repo_prefix or "").strip().rstrip("/")
    if not base:
        return local_ref
    tag = "latest"
    if ":" in local_ref.rsplit("/", 1)[-1]:
        _name, tag = local_ref.rsplit(":", 1)
    resolved_layout = _resolve_registry_layout(
        repo_prefix=base,
        requested_layout=layout,
    )
    if resolved_layout == "single_repository":
        template_part = _sanitize_for_tag(template_id)
        tag_part = _sanitize_registry_tag_component(tag)
        published_tag = _sanitize_registry_tag_component(
            f"{template_part}-{tag_part}",
            max_len=128,
        )
        return f"{base}:{published_tag}"
    repo = f"{base}/{_sanitize_for_tag(template_id)}"
    return f"{repo}:{tag}"


def push_image_to_registry(
    *,
    local_ref: str,
    template_id: str,
    repo_prefix: str,
    timeout: int,
) -> str:
    from config import get_config

    cfg = get_config()
    target_ref = _published_template_ref(
        local_ref=local_ref,
        template_id=template_id,
        repo_prefix=repo_prefix,
        layout=str(getattr(cfg, "TEMPLATE_REGISTRY_LAYOUT", "auto") or "auto"),
    )
    if target_ref == local_ref:
        return local_ref
    ensure_registry_login(timeout=timeout)
    client = _docker_client(timeout)
    try:
        image = client.images.get(local_ref)
        image.tag(target_ref)
        repo, tag = target_ref.rsplit(":", 1)
        auth_config = _registry_auth_config()
        for entry in client.api.push(
            repo,
            tag=tag,
            stream=True,
            decode=True,
            auth_config=auth_config,
        ):
            if isinstance(entry, dict) and entry.get("error"):
                raise RuntimeError(str(entry.get("error")))
        return target_ref
    finally:
        client.close()


def stream_build_image_from_dockerfile(
    *,
    dockerfile: str,
    image_tag: Optional[str],
    template_id: str,
    build_args: Optional[Dict[str, str]],
    context_tar_gzip: Optional[bytes],
    build_timeout_sec: int,
    embed_envd: bool,
    restore_user_mode: str,
) -> Iterator[dict]:
    tag = (
        (image_tag or "").strip()
        or f"mysandbox-df-{_sanitize_for_tag(template_id)}:{uuid.uuid4().hex[:12]}"
    )
    build_context = _prepare_build_context(
        dockerfile=dockerfile,
        context_tar_gzip=context_tar_gzip,
        embed_envd=embed_envd,
        restore_user_mode=restore_user_mode,
    )
    clean_build_args = {k.strip(): v for k, v in (build_args or {}).items() if k.strip()}
    api = _docker_api_client(max(60, int(build_timeout_sec)))
    try:
        stream = api.build(
            fileobj=io.BytesIO(build_context),
            custom_context=True,
            encoding="gzip",
            dockerfile="Dockerfile",
            tag=tag,
            rm=True,
            forcerm=True,
            pull=False,
            buildargs=clean_build_args or None,
            decode=True,
        )
        full_log: list[str] = []
        for entry in stream:
            text = _entry_to_text(entry)
            if text:
                full_log.append(text)
                yield {"type": "log", "line": text}
            if isinstance(entry, dict) and entry.get("error"):
                raise RuntimeError(f"docker build failed: {''.join(full_log)[-12000:]}")
        yield {"type": "result", "image_tag": tag, "build_log": "".join(full_log)}
    finally:
        try:
            api.close()
        except Exception:
            pass


def build_image_from_dockerfile(**kwargs) -> tuple[str, str]:
    tag = ""
    log = ""
    for event in stream_build_image_from_dockerfile(**kwargs):
        if event.get("type") == "result":
            tag = str(event.get("image_tag") or "")
            log = str(event.get("build_log") or "")
    if not tag:
        raise RuntimeError("docker build produced no image tag")
    return tag, log


class LocalDockerExecution:
    def __init__(self, *, timeout: int = 600) -> None:
        self._client = _docker_client(timeout)

    def close(self) -> None:
        self._client.close()

    def ensure_image(self, image: str) -> None:
        ref = (image or "").strip()
        if not ref:
            raise RuntimeError("base_image is required")
        try:
            self._client.images.get(ref)
        except docker.errors.ImageNotFound:
            last_error: Optional[Exception] = None
            pull_refs = []
            cache_ref = _registry_cache_ref_for_image_ref(ref)
            if cache_ref and cache_ref != ref:
                pull_refs.append(cache_ref)
            pull_refs.append(ref)
            for pull_ref in pull_refs:
                for attempt in range(4):
                    try:
                        self._client.images.pull(
                            pull_ref,
                            auth_config=_registry_auth_config_for_ref(pull_ref),
                        )
                        if pull_ref != ref:
                            pulled = self._client.images.get(pull_ref)
                            repo, tag = _split_repository_tag(ref)
                            pulled.tag(repo, tag=tag)
                            logger.info("Image pulled through registry cache: source=%s local_ref=%s", pull_ref, ref)
                        return
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                        text = f"{type(exc).__name__}: {exc}".lower()
                        transient = any(
                            needle in text
                            for needle in (
                                "toomanyrequests",
                                "rate exceeded",
                                "locked for",
                                "unavailable",
                                "connection reset",
                                "read timed out",
                            )
                        )
                        if transient and attempt < 3:
                            time.sleep(min(8.0, 1.0 * (2 ** attempt)))
                            continue
                        if pull_ref != ref:
                            logger.warning(
                                "Registry cache pull failed source=%s local_ref=%s: %s",
                                pull_ref,
                                ref,
                                exc,
                            )
                        break
            raise last_error or RuntimeError(f"failed to pull image {ref}")

    def create_container(
        self,
        *,
        image: str,
        name: str,
        environment: Optional[Dict[str, str]] = None,
    ) -> str:
        self.ensure_image(image)
        container = self._client.containers.create(
            image=image,
            name=name,
            command=["/bin/sh", "-lc", "trap : TERM INT; while true; do sleep 3600; done"],
            detach=True,
            environment=environment or None,
        )
        container.start()
        return str(container.id)

    def run_command(
        self,
        container_id: str,
        command: str,
        *,
        cwd: str = "/",
        env: Optional[Dict[str, str]] = None,
        user: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Dict[str, object]:
        container = self._client.containers.get(container_id)
        exec_result = container.exec_run(
            cmd=["/bin/sh", "-lc", command],
            workdir=cwd or "/",
            environment=env or None,
            user=user or "",
            demux=True,
            stdout=True,
            stderr=True,
        )
        stdout_b, stderr_b = exec_result.output if isinstance(exec_result.output, tuple) else (exec_result.output, b"")
        return {
            "exit_code": int(exec_result.exit_code or 0),
            "stdout": (stdout_b or b"").decode("utf-8", errors="replace"),
            "stderr": (stderr_b or b"").decode("utf-8", errors="replace"),
            "pid": -1,
            "execution_time": float(timeout),
        }

    def put_archive_to_container(self, container_id: str, parent: str, data: bytes) -> bool:
        container = self._client.containers.get(container_id)
        return bool(container.put_archive(parent, data))

    def commit_filesystem_snapshot(self, container_id: str, repo: str, tag: str) -> str:
        container = self._client.containers.get(container_id)
        container.commit(repository=repo, tag=tag)
        return f"{repo}:{tag}"

    def kill_container(self, container_id: str) -> None:
        container = self._client.containers.get(container_id)
        try:
            container.remove(force=True)
        except docker.errors.NotFound:
            return


def bake_envd_guest_into_container(
    *,
    plane: LocalDockerExecution,
    container_id: str,
    pip_timeout_sec: float,
) -> bool:
    tb = envd_guest_tarball_bytes()
    if not tb:
        logger.warning("envd template bake: envd_guest source missing in runtime-gateway image")
        return False
    if not plane.put_archive_to_container(container_id, "/opt", tb):
        logger.warning("envd template bake: put_archive failed container=%s", container_id[:12])
        return False
    install = plane.run_command(
        container_id,
        ENVD_ENSURE_PYTHON_PIP_SHELL,
        timeout=max(120.0, float(pip_timeout_sec), 600.0),
        user="root",
    )
    if int(install.get("exit_code") or 0) != 0:
        return False
    pip = plane.run_command(
        container_id,
        ENVD_PIP_INSTALL_SHELL,
        timeout=float(pip_timeout_sec),
        user="root",
    )
    if int(pip.get("exit_code") or 0) != 0:
        return False
    mk = plane.run_command(
        container_id,
        f"printf '%s\\n' '{ENVD_BAKE_VERSION}' > {ENVD_BAKE_MARKER}",
        timeout=30.0,
        user="root",
    )
    return int(mk.get("exit_code") or 0) == 0


def build_registered_template_snapshot(
    *,
    template_id: str,
    base_image: str,
    env: Optional[Dict[str, str]],
    start_cmd: str,
    settle_seconds: int,
    ready_cmd: str,
    embed_envd: bool,
    envd_pip_timeout_sec: float,
    snapshot_repo: str,
) -> dict:
    plane = LocalDockerExecution(timeout=max(int(settle_seconds or 0) + 900, 1800))
    cid: Optional[str] = None
    log_lines: list[str] = []
    try:
        cid = plane.create_container(
            image=base_image,
            name=f"tpl-build-{uuid.uuid4().hex[:10]}",
            environment=env or None,
        )
        log_lines.append(f"create_container image={base_image}\n")
        sc = (start_cmd or "").strip()
        if sc:
            r = plane.run_command(cid, sc, env=env or None, timeout=3600.0)
            log_lines.append(f"start_cmd exit={int(r.get('exit_code') or 0)}\n")
            if int(r.get("exit_code") or 0) != 0:
                raise RuntimeError(f"start_cmd failed: {(r.get('stderr') or r.get('stdout') or '')[:2000]}")
        settle = max(0, min(int(settle_seconds or 20), 600))
        if settle:
            log_lines.append(f"settle_seconds={settle}\n")
            time.sleep(settle)
        ready = (ready_cmd or "").strip()
        if ready:
            deadline = time.monotonic() + 600.0
            ok_ready = False
            while time.monotonic() < deadline:
                rr = plane.run_command(cid, ready, env=env or None, timeout=120.0)
                if int(rr.get("exit_code") or 0) == 0:
                    ok_ready = True
                    log_lines.append("ready_cmd exit=0\n")
                    break
                time.sleep(2.0)
            if not ok_ready:
                raise RuntimeError("ready_cmd did not exit 0 before timeout")
        if embed_envd:
            if not bake_envd_guest_into_container(
                plane=plane,
                container_id=cid,
                pip_timeout_sec=envd_pip_timeout_sec,
            ):
                raise RuntimeError("envd template bake failed")
            log_lines.append("envd_embedded=1\n")
        repo = (snapshot_repo or "mysandbox-snap").strip().lower().replace("/", "-") or "mysandbox-snap"
        tag = re.sub(r"[^a-z0-9._-]", "-", f"tpl-{template_id}-{uuid.uuid4().hex[:10]}".lower())[:120] or "tpl"
        image_ref = plane.commit_filesystem_snapshot(cid, repo, tag)
        log_lines.append(f"commit={image_ref}\n")
        return {"image_ref": image_ref, "build_log": "".join(log_lines)}
    finally:
        if cid:
            try:
                plane.kill_container(cid)
            except Exception:
                pass
        plane.close()
