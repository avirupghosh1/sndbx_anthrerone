"""Derived progress and log presentation for template builds."""

from __future__ import annotations

import re
from typing import Any


_CLASSIC_STEP_RE = re.compile(r"\bStep\s+(\d+)\s*/\s*(\d+)\b", re.IGNORECASE)
_BUILDKIT_STEP_RE = re.compile(r"#\d+\s+(?:\[[^\]]+\]\s+)?\[[^\]]*?(\d+)\s*/\s*(\d+)[^\]]*?\]")
_ERROR_RE = re.compile(r"\b(error|failed|fatal|exception|traceback)\b", re.IGNORECASE)
_WARNING_RE = re.compile(r"\b(warn|warning|deprecated)\b", re.IGNORECASE)
_PUSH_RE = re.compile(r"\b(push|pushing|pushed|registry|publish|publishing)\b", re.IGNORECASE)


def classify_log_line(line: str) -> str:
    text = str(line or "")
    if _ERROR_RE.search(text):
        return "error"
    if _WARNING_RE.search(text):
        return "warning"
    return "info"


def parse_build_log_lines(build_log: object) -> list[dict[str, Any]]:
    lines = str(build_log or "").splitlines()
    return [
        {"number": idx + 1, "text": line, "severity": classify_log_line(line)}
        for idx, line in enumerate(lines)
    ]


def _docker_step_progress(build_log: str) -> tuple[int, str]:
    latest_step: tuple[int, int] | None = None
    for match in _CLASSIC_STEP_RE.finditer(build_log):
        current = int(match.group(1))
        total = max(1, int(match.group(2)))
        latest_step = (current, total)
    for match in _BUILDKIT_STEP_RE.finditer(build_log):
        current = int(match.group(1))
        total = max(1, int(match.group(2)))
        if latest_step is None or (current / total) >= (latest_step[0] / latest_step[1]):
            latest_step = (current, total)
    if latest_step is None:
        return 0, ""
    current, total = latest_step
    percent = 5 + round(min(1.0, current / total) * 75)
    return min(80, max(5, percent)), f"Docker build step {current}/{total}"


def derive_template_build_progress(row: dict[str, Any] | None) -> dict[str, Any]:
    build = row or {}
    status = str(build.get("status") or "unknown").strip().lower() or "unknown"
    build_log = str(build.get("build_log") or "")
    lines = parse_build_log_lines(build_log)
    latest_comment = ""
    for entry in reversed(lines):
        if str(entry.get("text") or "").strip():
            latest_comment = str(entry["text"]).strip()
            break

    percent, phase = _docker_step_progress(build_log)
    lower_log = build_log.lower()
    if _PUSH_RE.search(lower_log):
        percent = max(percent, 88 if ("pushed" in lower_log or "published" in lower_log) else 82)
        phase = "Publishing image"

    effective_mode = str(build.get("effective_mode") or build.get("requested_mode") or "").strip()
    if not phase:
        phase = "Queued" if status in {"queued", "pending"} else "Building template"
    if status in {"success", "completed", "complete"}:
        percent = 100
        phase = "Template ready"
    elif status in {"failed", "error"}:
        percent = max(5 if build_log else 0, percent)
        phase = "Build failed"
        latest_comment = str(build.get("error_text") or latest_comment or "Build failed")
    elif status == "running":
        percent = max(5, percent)
        if effective_mode == "parsed" and percent < 20:
            percent = 20
    elif status in {"queued", "pending"}:
        percent = max(0, percent)

    return {
        "status": status,
        "percent": int(max(0, min(100, percent))),
        "phase": phase,
        "latest_comment": latest_comment,
        "log_lines": lines,
    }
