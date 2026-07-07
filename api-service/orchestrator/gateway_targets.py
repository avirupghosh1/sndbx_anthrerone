from __future__ import annotations

import itertools
from dataclasses import dataclass
from threading import Lock
from typing import Any, Iterable, List, Optional


@dataclass(frozen=True)
class GatewayTarget:
    instance_id: str
    api_base: str
    route_base: str


class GatewayTargetSelector:
    def __init__(self) -> None:
        self._rr = itertools.count()
        self._lock = Lock()

    def choose(self, targets: List[GatewayTarget], scheduler: str = "round_robin") -> GatewayTarget:
        if not targets:
            raise RuntimeError("no runtime-gateway targets configured")
        mode = (scheduler or "round_robin").strip().lower()
        if mode not in ("round_robin", "rr"):
            return targets[0]
        with self._lock:
            idx = next(self._rr) % len(targets)
        return targets[idx]


def _trim_url(value: str) -> str:
    return (value or "").strip().rstrip("/")


def build_gateway_targets(config: Any) -> List[GatewayTarget]:
    explicit = list(getattr(config, "runtime_gateway_targets_json", lambda: [])() or [])
    out: List[GatewayTarget] = []
    for idx, item in enumerate(explicit):
        if not isinstance(item, dict):
            continue
        api_base = _trim_url(str(item.get("api_base") or item.get("url") or ""))
        route_base = _trim_url(str(item.get("route_base") or api_base))
        instance_id = str(item.get("instance_id") or item.get("id") or f"runtime-gateway-{idx}").strip()
        if api_base and route_base:
            out.append(
                GatewayTarget(
                    instance_id=instance_id,
                    api_base=api_base,
                    route_base=route_base,
                )
            )
    if out:
        return out

    shard_count = max(1, int(getattr(config, "RUNTIME_GATEWAY_SHARD_COUNT", 1) or 1))
    if shard_count == 1:
        api_base = _trim_url(getattr(config, "RUNTIME_GATEWAY_URL", "") or "")
        if api_base:
            return [
                GatewayTarget(
                    instance_id="runtime-gateway-0",
                    api_base=api_base,
                    route_base=api_base,
                )
            ]
        return []

    namespace = (
        getattr(config, "RUNTIME_GATEWAY_NAMESPACE", None)
        or "sandboxes"
    ).strip()
    sts = (getattr(config, "RUNTIME_GATEWAY_STATEFULSET_NAME", None) or "runtime-gateway").strip()
    headless = (getattr(config, "RUNTIME_GATEWAY_HEADLESS_SERVICE", None) or f"{sts}-headless").strip()
    http_port = int(getattr(config, "RUNTIME_GATEWAY_SERVICE_PORT", 8080) or 8080)
    for idx in range(shard_count):
        host = f"{sts}-{idx}.{headless}.{namespace}.svc.cluster.local"
        api_base = f"http://{host}:{http_port}"
        out.append(
            GatewayTarget(
                instance_id=f"{sts}-{idx}",
                api_base=api_base,
                route_base=api_base,
            )
        )
    return out


def target_for_instance(
    targets: Iterable[GatewayTarget],
    instance_id: str,
) -> Optional[GatewayTarget]:
    want = (instance_id or "").strip()
    for target in targets:
        if target.instance_id == want:
            return target
    return None
