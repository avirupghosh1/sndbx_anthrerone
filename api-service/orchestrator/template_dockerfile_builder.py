"""Parse a Dockerfile and apply instructions **inside a running container** (E2B-style).

``RUN`` → ``exec`` in the build container; ``COPY``/``ADD`` (local paths) → ``put_archive``;
``WORKDIR`` / ``ENV`` / ``USER`` / ``ARG`` tracked in order.

**Single-stage only** (exactly one ``FROM``). ``ADD`` with a URL is skipped with a warning.
**Build context** directory on disk is required for ``COPY``/local ``ADD``.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import shlex
import tarfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from dockerfile_parse import DockerfileParser
except ImportError as e:  # pragma: no cover
    DockerfileParser = None  # type: ignore[misc, assignment]
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


def _require_parser() -> Any:
    if DockerfileParser is None:
        raise RuntimeError(
            "dockerfile-parse is required for parsed Dockerfile builds. "
            "Install: pip install dockerfile-parse"
        ) from _IMPORT_ERR


def _collapse_continuations(value: str) -> str:
    return re.sub(r"\\\s*\n\s*", " ", (value or "").strip())


def _parse_env_like_line(value: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    v = (value or "").strip()
    if not v:
        return out
    if "=" in v:
        for m in re.finditer(
            r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(\"([^\"]*)\"|'([^']*)'|([^\s]+))",
            v,
        ):
            key = m.group(1)
            val = (
                m.group(3)
                if m.group(3) is not None
                else (m.group(4) if m.group(4) is not None else (m.group(5) or ""))
            )
            out[key] = val
    else:
        parts = v.split(None, 1)
        if len(parts) == 2:
            out[parts[0]] = parts[1].strip("\"'")
    return out


def _expand_vars(s: str, env: Dict[str, str]) -> str:
    def braced(m: re.Match[str]) -> str:
        return env.get(m.group(1), m.group(0))

    s = re.sub(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}", braced, s)

    def bare(m: re.Match[str]) -> str:
        k = m.group(1)
        return env.get(k, m.group(0))

    return re.sub(r"\$([a-zA-Z_][a-zA-Z0-9_]*)", bare, s)


def _split_copy_value(value: str) -> tuple[list[str], Optional[str], str]:
    parts = shlex.split(_collapse_continuations(value))
    user: Optional[str] = None
    filtered: list[str] = []
    for p in parts:
        if p.startswith("--chown="):
            user = p[8:]
        elif p.startswith("--"):
            logger.warning("COPY/ADD flag not supported: %s", p)
        else:
            filtered.append(p)
    if len(filtered) < 2:
        return [], user, ""
    dest = filtered[-1]
    return filtered[:-1], user, dest


def _docker_instruction_to_shell(value: str) -> str:
    """Turn ``CMD``/``ENTRYPOINT`` JSON or shell form into a shell command string."""
    raw = _collapse_continuations(value)
    if not raw:
        return ""
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return raw
        if isinstance(parsed, list) and parsed:
            return " ".join(shlex.quote(str(x)) for x in parsed)
        return raw
    return raw


def extract_start_cmd_from_dockerfile(dockerfile: str) -> str:
    """Last ``ENTRYPOINT`` + ``CMD`` as a shell command (honours final ``WORKDIR``)."""
    _require_parser()
    dfp = DockerfileParser()
    dfp.content = dockerfile
    workdir = "/"
    entrypoint = ""
    cmd = ""
    for st in dfp.structure:
        instr = (st.get("instruction") or "").strip().upper()
        val = _collapse_continuations(st.get("value") or "")
        if instr == "WORKDIR":
            workdir = (val or workdir).strip() or "/"
        elif instr == "ENTRYPOINT":
            entrypoint = _docker_instruction_to_shell(val)
        elif instr == "CMD":
            cmd = _docker_instruction_to_shell(val)
    if entrypoint and cmd:
        full = f"{entrypoint} {cmd}".strip()
    else:
        full = entrypoint or cmd
    full = (full or "").strip()
    if not full:
        return ""
    wd = (workdir or "/").strip() or "/"
    if wd not in ("/", "."):
        return f"cd {shlex.quote(wd)} && {full}"
    return full


def extract_env_from_dockerfile(dockerfile: str) -> Dict[str, str]:
    """Merged ``ENV`` key/value pairs from the Dockerfile (in order)."""
    _require_parser()
    dfp = DockerfileParser()
    dfp.content = dockerfile
    env_map: Dict[str, str] = {}
    for st in dfp.structure:
        if (st.get("instruction") or "").strip().upper() == "ENV":
            env_map.update(_parse_env_like_line(_collapse_continuations(st.get("value") or "")))
    return env_map


def extract_base_image_from_dockerfile(dockerfile: str) -> str:
    """Return the image ref from the first ``FROM`` (single-stage builds)."""
    _require_parser()
    dfp = DockerfileParser()
    dfp.content = dockerfile
    for st in dfp.structure:
        if st.get("instruction") == "FROM":
            val = _collapse_continuations(st.get("value") or "")
            if re.search(r"\s+as\s+", val, flags=re.I):
                val = re.split(r"\s+as\s+", val, maxsplit=1, flags=re.I)[0].strip()
            return val.split()[0].strip()
    raise RuntimeError("Dockerfile has no FROM instruction")


def apply_dockerfile_inside_container(
    *,
    run_command: Callable[..., Dict[str, Any]],
    put_archive_bytes: Callable[[str, bytes], bool],
    container_id: str,
    dockerfile: str,
    context_dir: Optional[Path],
    build_args: Optional[Dict[str, str]],
    run_timeout: float,
) -> tuple[List[str], Dict[str, str]]:
    """Walk Dockerfile instructions and mutate ``container_id``.

    Returns ``(logs, env_from_dockerfile)`` where ``env_from_dockerfile`` is the merged result of
    ``ENV`` instructions (for persisting into the template row so sandboxes inherit them).
    """
    _require_parser()
    dfp = DockerfileParser()
    dfp.content = dockerfile

    from_instr = [
        x for x in dfp.structure if (x.get("instruction") or "").strip().upper() == "FROM"
    ]
    if len(from_instr) != 1:
        raise RuntimeError("Dockerfile must contain exactly one FROM (single-stage only).")

    logs: List[str] = []
    workdir = "/"
    exec_user: Optional[str] = "root"
    # Build-time ARGs (CLI build_args win over Dockerfile defaults)
    arg_map: Dict[str, str] = dict(build_args or {})
    env_map: Dict[str, str] = {}

    def merged_exec_env() -> Dict[str, str]:
        m = dict(env_map)
        for k, v in arg_map.items():
            m.setdefault(k, v)
        return m

    for ins in dfp.structure:
        instr = (ins.get("instruction") or "").strip().upper()
        raw_val = ins.get("value") or ""
        val = _collapse_continuations(raw_val)
        exp_ctx = {**arg_map, **env_map}
        val_exp = _expand_vars(val, exp_ctx)

        if instr == "FROM":
            continue

        # ``dockerfile-parse`` surfaces ``# …`` lines as a synthetic ``COMMENT`` instruction.
        if instr == "COMMENT":
            continue

        if instr == "ARG":
            for k, v in _parse_env_like_line(val).items():
                if k not in arg_map:
                    arg_map[k] = _expand_vars(v, {**arg_map, **env_map})
            logs.append(f"ARG {val_exp[:200]}")
            continue

        if instr == "ENV":
            for k, v in _parse_env_like_line(val).items():
                env_map[k] = _expand_vars(v, {**arg_map, **env_map})
            logs.append(f"ENV {val_exp[:200]}")
            continue

        if instr == "WORKDIR":
            workdir = val_exp.strip() or "/"
            r = run_command(
                container_id,
                f"mkdir -p {shlex.quote(workdir)}",
                cwd="/",
                env=merged_exec_env(),
                user=exec_user,
                timeout=120.0,
            )
            ec = int(r.get("exit_code") or 0)
            logs.append(f"WORKDIR {workdir} mkdir_exit={ec}")
            if ec != 0:
                raise RuntimeError(
                    f"WORKDIR mkdir failed (exit {ec}) for {workdir!r} as user={exec_user!r}. "
                    "Put ``USER`` after ``WORKDIR``/``COPY``, or ``RUN useradd`` before switching user."
                )
            continue

        if instr == "USER":
            exec_user = val_exp.strip() or "root"
            logs.append(f"USER {exec_user}")
            continue

        if instr == "RUN":
            if not val_exp.strip():
                continue
            r = run_command(
                container_id,
                val_exp,
                cwd=workdir,
                env=merged_exec_env(),
                user=exec_user,
                timeout=run_timeout,
            )
            ec = int(r.get("exit_code") or 0)
            logs.append(f"RUN exit={ec} cmd={val_exp[:400]}")
            if ec != 0:
                raise RuntimeError(
                    f"RUN failed (exit {ec}): {val_exp[:800]!r} out={(r.get('stdout') or '')[:1500]!r}"
                )
            continue

        if instr in ("COPY", "ADD"):
            if context_dir is None:
                raise RuntimeError(f"{instr} requires build context (extract a context tar on the host).")
            sources, _chown, dest = _split_copy_value(val_exp)
            if not sources or not dest:
                logger.warning("Skipping %s (parse): %r", instr, val_exp)
                continue
            if instr == "ADD" and (sources[0].startswith("http://") or sources[0].startswith("https://")):
                logger.warning("ADD with URL not supported in parsed mode: %s", sources[0])
                continue

            ctx = context_dir.resolve()

            def _context_root_matches(hp_path: Path) -> bool:
                """True when ``hp_path`` is the whole build context directory (``COPY . …``)."""
                if not hp_path.is_dir():
                    return False
                try:
                    return os.path.samefile(hp_path, ctx)
                except OSError:
                    return os.path.normpath(hp_path) == os.path.normpath(ctx)

            def resolve_dest_path(dest_raw: str) -> str:
                """Resolve COPY destination (``.`` / ``./`` = current WORKDIR)."""
                dr = (dest_raw or "").strip()
                if not dr or dr in (".", "./"):
                    w = (workdir or "/").strip() or "/"
                    return w if w.startswith("/") else "/" + w
                if dr.startswith("/"):
                    return dr
                base = (workdir or "/").rstrip("/") or "/"
                return str(Path(base) / dr)

            def _dest_wants_dir_contents(dest_raw: str, src_raw: str) -> bool:
                """Docker: ``COPY dir/ .`` or ``COPY dir existing_dir/`` copies directory *contents*."""
                dr = (dest_raw or "").strip()
                sr = (src_raw or "").strip()
                return dr in (".", "./") or dr.endswith("/") or sr.endswith("/")

            def _put_tree_from_dir(src_dir: Path, dest_root: str, *, label: str) -> None:
                mk = run_command(
                    container_id,
                    f"mkdir -p {shlex.quote(dest_root)}",
                    cwd="/",
                    env=merged_exec_env(),
                    user=exec_user,
                    timeout=120.0,
                )
                if int(mk.get("exit_code") or 0) != 0:
                    raise RuntimeError(
                        f"{instr} mkdir failed for {dest_root!r} as user={exec_user!r} "
                        f"(exit {mk.get('exit_code')}). Use ``root`` for filesystem setup or reorder ``USER``."
                    )
                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w") as tf:
                    for root, _dirs, files in os.walk(src_dir):
                        for name in files:
                            abs_p = Path(root) / name
                            try:
                                rel = abs_p.relative_to(src_dir)
                            except ValueError:
                                continue
                            tf.add(str(abs_p), arcname=str(rel).replace(os.sep, "/"))
                buf.seek(0)
                data = buf.read()
                if not put_archive_bytes(dest_root, data):
                    raise RuntimeError(f"{instr} put_archive failed {label!r} -> {dest_root!r} (dir contents)")
                logs.append(f"{instr} {label!r} -> {dest_root!r} (dir contents)")

            for src in sources:
                src_stripped = (src or "").strip()
                src_path_part = src_stripped.rstrip("/") or "."
                hp = (context_dir / src_path_part).resolve()
                if not str(hp).startswith(str(ctx)):
                    raise RuntimeError(f"COPY path escapes context: {src!r}")
                if not hp.exists():
                    raise RuntimeError(f"COPY source not in context: {src!r}")

                # ``COPY . /app`` or ``COPY . .`` — copy entire build context tree into a directory.
                if _context_root_matches(hp) and src.strip().rstrip("/") in (".", "./"):
                    dest_root = resolve_dest_path(dest)
                    mk = run_command(
                        container_id,
                        f"mkdir -p {shlex.quote(dest_root)}",
                        cwd="/",
                        env=merged_exec_env(),
                        user=exec_user,
                        timeout=120.0,
                    )
                    if int(mk.get("exit_code") or 0) != 0:
                        raise RuntimeError(
                            f"{instr} mkdir failed for {dest_root!r} as user={exec_user!r} "
                            f"(exit {mk.get('exit_code')}). Use ``root`` for filesystem setup or reorder ``USER``."
                        )
                    buf = io.BytesIO()
                    with tarfile.open(fileobj=buf, mode="w") as tf:
                        for root, _dirs, files in os.walk(hp):
                            for name in files:
                                abs_p = Path(root) / name
                                try:
                                    rel = abs_p.relative_to(hp)
                                except ValueError:
                                    continue
                                tf.add(str(abs_p), arcname=str(rel).replace(os.sep, "/"))
                    buf.seek(0)
                    data = buf.read()
                    if not put_archive_bytes(dest_root, data):
                        raise RuntimeError(f"{instr} put_archive failed {src!r} -> {dest_root!r} (context tree)")
                    logs.append(f"{instr} {src!r} -> {dest_root!r} (context tree)")
                    continue

                if hp.is_dir():
                    # Docker copies directory *contents* into the destination path for
                    # ``COPY dir /dst`` and ``COPY dir/ /dst``. Only multi-source COPY
                    # targets preserve each source basename under the destination dir.
                    if len(sources) > 1:
                        dest_root = os.path.join(
                            resolve_dest_path(dest.rstrip("/")),
                            os.path.basename(str(hp)),
                        )
                    else:
                        dest_root = resolve_dest_path(
                            dest.rstrip("/") if dest.rstrip().endswith("/") else dest
                        )
                    _put_tree_from_dir(hp, dest_root, label=src_stripped)
                    continue

                if dest.endswith("/") or len(sources) > 1:
                    dest_path = os.path.join(
                        resolve_dest_path(dest.rstrip("/")),
                        os.path.basename(str(hp)),
                    )
                else:
                    dest_path = resolve_dest_path(dest)
                    if hp.is_dir() and not dest_path.endswith("/"):
                        dest_path = str(Path(dest_path) / os.path.basename(str(hp)))

                parent = str(Path(dest_path).parent)
                mkp = run_command(
                    container_id,
                    f"mkdir -p {shlex.quote(parent)}",
                    cwd="/",
                    env=merged_exec_env(),
                    user=exec_user,
                    timeout=120.0,
                )
                if int(mkp.get("exit_code") or 0) != 0:
                    raise RuntimeError(
                        f"{instr} mkdir failed for {parent!r} as user={exec_user!r} "
                        f"(exit {mkp.get('exit_code')}). Reorder ``USER`` after ``COPY``/``WORKDIR``."
                    )

                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w") as tf:
                    if hp.is_file():
                        tf.add(hp, arcname=os.path.basename(dest_path))
                    else:
                        tf.add(hp, arcname=os.path.basename(str(hp)) or "dir", recursive=True)
                buf.seek(0)
                data = buf.read()
                par = str(Path(dest_path).parent)
                if not put_archive_bytes(par, data):
                    raise RuntimeError(f"{instr} put_archive failed {src!r} -> {dest_path!r}")
                logs.append(f"{instr} {src!r} -> {dest_path!r}")
            continue

        if instr in ("EXPOSE", "VOLUME", "LABEL", "STOPSIGNAL", "ONBUILD", "SHELL", "HEALTHCHECK", "CMD", "ENTRYPOINT"):
            logs.append(f"skip {instr}")
            continue

        logger.warning("Unsupported Dockerfile instruction ignored: %s %s", instr, val_exp[:120])

    return logs, dict(env_map)
