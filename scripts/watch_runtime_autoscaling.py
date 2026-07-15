#!/usr/bin/env python3
"""Live runtime-gateway StatefulSet autoscaling view.

Defaults match the local no-cloud manifests. Override for Helm releases:

  NAMESPACE=spr-apps \
  HPA=agent-sandbox-qa6-tier1-runtime-gateway \
  SELECTOR='app=agent-sandbox,tier=qa6-tier1,component=runtime-gateway' \
  scripts/watch_runtime_autoscaling.py
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Any
from urllib.parse import quote


NAMESPACE = os.getenv("NAMESPACE", "sandboxes")
HPA_NAME = os.getenv("HPA", os.getenv("RUNTIME_GATEWAY_HPA", "runtime-gateway"))
SELECTOR = os.getenv("SELECTOR", os.getenv("RUNTIME_GATEWAY_SELECTOR", "app=runtime-gateway"))
REFRESH_SEC = max(1.0, float(os.getenv("REFRESH_SEC", "2")))
HPA_TOLERANCE = max(0.0, float(os.getenv("HPA_TOLERANCE", "0.10")))
KUBECTL = os.getenv("KUBECTL", "kubectl")


def kubectl_json(*args: str) -> dict[str, Any]:
    cmd = [KUBECTL, "-n", NAMESPACE, *args, "-o", "json"]
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    return json.loads(out)


def kubectl_raw(path: str) -> dict[str, Any]:
    cmd = [KUBECTL, "get", "--raw", path]
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    return json.loads(out)


def cpu_millicores(value: str) -> int:
    raw = str(value or "0").strip()
    try:
        if raw.endswith("n"):
            return int(float(raw[:-1]) / 1_000_000)
        if raw.endswith("u"):
            return int(float(raw[:-1]) / 1_000)
        if raw.endswith("m"):
            return int(float(raw[:-1]))
        return int(float(raw) * 1000)
    except ValueError:
        return 0


def memory_bytes(value: str) -> int:
    raw = str(value or "0").strip()
    units = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    for suffix, factor in units.items():
        if raw.endswith(suffix):
            try:
                return int(float(raw[: -len(suffix)]) * factor)
            except ValueError:
                return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def pod_ordinal(name: str) -> int:
    match = re.search(r"-(\d+)$", name or "")
    return int(match.group(1)) if match else 0


def terminal_clear() -> None:
    sys.stdout.write("\033[2J\033[H")


def percent(used: int, requested: int) -> float | None:
    if requested <= 0:
        return None
    return (float(used) / float(requested)) * 100.0


def metric_targets(hpa: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for metric in hpa.get("spec", {}).get("metrics", []) or []:
        if metric.get("type") != "Resource":
            continue
        resource = metric.get("resource") or {}
        name = str(resource.get("name") or "")
        target = resource.get("target") or {}
        if target.get("type") == "Utilization" and target.get("averageUtilization") is not None:
            out[name] = int(target["averageUtilization"])
    return out


def pod_requests_by_name(pods: dict[str, Any]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for item in pods.get("items", []) or []:
        name = item.get("metadata", {}).get("name") or ""
        cpu_req = 0
        mem_req = 0
        for container in item.get("spec", {}).get("containers", []) or []:
            requests = container.get("resources", {}).get("requests", {}) or {}
            cpu_req += cpu_millicores(requests.get("cpu", "0"))
            mem_req += memory_bytes(requests.get("memory", "0"))
        if name:
            out[name] = {"cpu": cpu_req, "memory": mem_req}
    return out


def pod_metrics_by_name() -> dict[str, dict[str, int]]:
    path = f"/apis/metrics.k8s.io/v1beta1/namespaces/{NAMESPACE}/pods?labelSelector={quote(SELECTOR)}"
    payload = kubectl_raw(path)
    out: dict[str, dict[str, int]] = {}
    for item in payload.get("items", []) or []:
        name = item.get("metadata", {}).get("name") or ""
        cpu = 0
        mem = 0
        for container in item.get("containers", []) or []:
            usage = container.get("usage", {}) or {}
            cpu += cpu_millicores(usage.get("cpu", "0"))
            mem += memory_bytes(usage.get("memory", "0"))
        if name:
            out[name] = {"cpu": cpu, "memory": mem}
    return out


def deletion_cost(pod: dict[str, Any]) -> str:
    annotations = pod.get("metadata", {}).get("annotations", {}) or {}
    return str(
        annotations.get("controller.kubernetes.io/pod-deletion-cost")
        or annotations.get("sndbx.io/runtime-load-cost")
        or "-"
    )


def desired_replicas(current_replicas: int, current_pct: float | None, target_pct: int | None) -> str:
    if current_pct is None or not target_pct:
        return "?"
    return str(max(1, math.ceil(float(current_replicas) * current_pct / float(target_pct))))


def metric_summary(name: str, current_pct: float | None, target_pct: int | None, current_replicas: int) -> str:
    if current_pct is None or not target_pct:
        return f"{name}=unavailable"
    down_below = target_pct * (1.0 - HPA_TOLERANCE)
    up_above = target_pct * (1.0 + HPA_TOLERANCE)
    desired = desired_replicas(current_replicas, current_pct, target_pct)
    return (
        f"{name} current={current_pct:.1f}% target={target_pct}% "
        f"scale_down_below={down_below:.1f}% scale_up_above={up_above:.1f}% desired={desired}"
    )


def format_pod_load(name: str, usage: dict[str, int], requests: dict[str, int], cost: str) -> str:
    cpu_pct = percent(usage.get("cpu", 0), requests.get("cpu", 0))
    mem_pct = percent(usage.get("memory", 0), requests.get("memory", 0))
    cpu_text = f"{usage.get('cpu', 0)}m/{requests.get('cpu', 0)}m"
    mem_text = f"{usage.get('memory', 0) // (1024 * 1024)}Mi/{requests.get('memory', 0) // (1024 * 1024)}Mi"
    cpu_suffix = f" {cpu_pct:.1f}%" if cpu_pct is not None else " n/a"
    mem_suffix = f" {mem_pct:.1f}%" if mem_pct is not None else " n/a"
    return f"{name}:cpu={cpu_text}{cpu_suffix},mem={mem_text}{mem_suffix},cost={cost}"


def render_once() -> str:
    hpa = kubectl_json("get", "hpa", HPA_NAME)
    pods = kubectl_json("get", "pods", "-l", SELECTOR)
    metrics = pod_metrics_by_name()
    requests = pod_requests_by_name(pods)
    targets = metric_targets(hpa)

    pod_items = sorted(
        pods.get("items", []) or [],
        key=lambda item: (pod_ordinal(item.get("metadata", {}).get("name") or ""), item.get("metadata", {}).get("name") or ""),
    )
    names = [item.get("metadata", {}).get("name") or "" for item in pod_items]

    total_cpu_used = sum(metrics.get(name, {}).get("cpu", 0) for name in names)
    total_cpu_req = sum(requests.get(name, {}).get("cpu", 0) for name in names)
    total_mem_used = sum(metrics.get(name, {}).get("memory", 0) for name in names)
    total_mem_req = sum(requests.get(name, {}).get("memory", 0) for name in names)
    cpu_avg = percent(total_cpu_used, total_cpu_req)
    mem_avg = percent(total_mem_used, total_mem_req)

    spec = hpa.get("spec", {}) or {}
    status = hpa.get("status", {}) or {}
    current_replicas = int(status.get("currentReplicas") or len(names) or 0)
    min_replicas = int(spec.get("minReplicas") or 1)
    max_replicas = int(spec.get("maxReplicas") or 0)
    hpa_desired = status.get("desiredReplicas", "?")

    total = (
        f"TOTAL cpu={total_cpu_used}m/{total_cpu_req}m"
        f"{f' {cpu_avg:.1f}%' if cpu_avg is not None else ' n/a'} "
        f"mem={total_mem_used // (1024 * 1024)}Mi/{total_mem_req // (1024 * 1024)}Mi"
        f"{f' {mem_avg:.1f}%' if mem_avg is not None else ' n/a'}"
    )
    row1 = (
        f"HPA {NAMESPACE}/{HPA_NAME} replicas current={current_replicas} desired={hpa_desired} "
        f"min={min_replicas} max={max_replicas} | "
        f"{metric_summary('cpu', cpu_avg, targets.get('cpu'), current_replicas)} | "
        f"{metric_summary('memory', mem_avg, targets.get('memory'), current_replicas)} | {total}"
    )

    pod_parts = []
    for item in pod_items:
        name = item.get("metadata", {}).get("name") or ""
        pod_parts.append(format_pod_load(name, metrics.get(name, {}), requests.get(name, {}), deletion_cost(item)))
    row2 = "PODS ordinal_order | " + (" | ".join(pod_parts) if pod_parts else "none")
    return f"{time.strftime('%Y-%m-%d %H:%M:%S')}\n{row1}\n{row2}\n"


def main() -> int:
    if not shutil.which(KUBECTL):
        print(f"{KUBECTL!r} not found in PATH", file=sys.stderr)
        return 127
    while True:
        try:
            output = render_once()
        except subprocess.CalledProcessError as exc:
            output = f"kubectl failed:\n{exc.output or exc}\n"
        except Exception as exc:  # noqa: BLE001
            output = f"watch failed: {type(exc).__name__}: {exc}\n"
        terminal_clear()
        sys.stdout.write(output)
        sys.stdout.flush()
        time.sleep(REFRESH_SEC)


if __name__ == "__main__":
    raise SystemExit(main())
