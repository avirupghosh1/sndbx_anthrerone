#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import getpass
import os
import re
import sys
import time
from pathlib import Path

try:
    from e2b import AsyncSandbox as E2BAsyncSandbox
except Exception:  # noqa: BLE001
    E2BAsyncSandbox = None

ROOT = Path(__file__).resolve().parent
MY_SDK_ROOT = ROOT / "intern_1strepo" / "my_sandbox_sdk"
if (MY_SDK_ROOT / "my_sdk").is_dir():
    sys.path.insert(0, str(MY_SDK_ROOT))

from my_sdk.api import APIEndpoints
from my_sdk.api.async_client import AsyncAPIClient

TEMPLATE_ALIAS = "python:3.11"


def resolve_template_id(env_name: str) -> str:
    env_name = env_name.strip().lower()
    template_id = f"{TEMPLATE_ALIAS}-{env_name}"
    if env_name == "dev":
        username = re.sub(r"[^a-z0-9-]", "-", getpass.getuser().lower()).strip("-")
        if username:
            template_id = f"{template_id}-{username}"
    return template_id


async def measure_local(
    template_id: str,
    api_url: str,
    api_key: str,
    timeout_seconds: int,
    request_timeout: float,
) -> tuple[float, str, str, str, str]:
    api = AsyncAPIClient(api_url.rstrip("/"), api_key, request_timeout=request_timeout)
    body = {
        "template_id": template_id,
        "metadata": {"guest_ports": [8765, 49983]},
        "timeout": int(timeout_seconds),
    }
    started = time.perf_counter()
    response = await api.post(APIEndpoints.SANDBOX_CREATE, json=body)
    elapsed = time.perf_counter() - started
    metadata = response.get("metadata") if isinstance(response.get("metadata"), dict) else {}
    source = str(metadata.get("sandbox_allocation_source") or "unknown")
    alloc_wait = metadata.get("sandbox_allocation_acquire_wait_seconds")
    alloc_wait_s = "" if alloc_wait is None else str(alloc_wait)
    gateway = str(response.get("gateway_instance_id") or "")
    sandbox_id = str(response.get("sandbox_id") or "")
    return elapsed, sandbox_id, source, gateway, alloc_wait_s


async def kill_local(
    sandbox_id: str,
    api_url: str,
    api_key: str,
    request_timeout: float,
) -> bool:
    sandbox_id = sandbox_id.strip()
    if not sandbox_id:
        return False
    api = AsyncAPIClient(api_url.rstrip("/"), api_key, request_timeout=request_timeout)
    endpoint = APIEndpoints.format(APIEndpoints.SANDBOX_KILL, sandbox_id=sandbox_id)
    await api.post(endpoint, json={})
    return True


async def probe_local_ready(
    sandbox_id: str,
    api_url: str,
    api_key: str,
    internal_api_key: str,
    request_timeout: float,
) -> None:
    sid = sandbox_id.strip()
    if not sid:
        return
    api = AsyncAPIClient(api_url.rstrip("/"), api_key, request_timeout=request_timeout)
    internal = AsyncAPIClient(api_url.rstrip("/"), internal_api_key, request_timeout=request_timeout)
    started = time.perf_counter()
    try:
        envd = await api.get(f"/sandboxes/{sid}/envd-connection")
        print(
            f"local_probe sandbox_id={sid} probe=envd_connection seconds={time.perf_counter() - started:.3f} "
            f"ok=true envd_port={envd.get('envd_port', '-')}",
            flush=True,
        )
    except Exception as ex:  # noqa: BLE001
        print(
            f"local_probe sandbox_id={sid} probe=envd_connection seconds={time.perf_counter() - started:.3f} "
            f"ok=false error={type(ex).__name__}:{ex}",
            flush=True,
        )
    for port in (49983, 8765):
        started = time.perf_counter()
        try:
            route = await internal.get(f"/internal/sandboxes/{sid}/route", params={"port": port})
            print(
                f"local_probe sandbox_id={sid} probe=route port={port} "
                f"seconds={time.perf_counter() - started:.3f} ok=true upstream={route.get('upstream_http', '-')}",
                flush=True,
            )
        except Exception as ex:  # noqa: BLE001
            print(
                f"local_probe sandbox_id={sid} probe=route port={port} "
                f"seconds={time.perf_counter() - started:.3f} ok=false error={type(ex).__name__}:{ex}",
                flush=True,
            )


async def measure_e2b(template_id: str, api_key: str, timeout_seconds: int) -> tuple[float, str]:
    if E2BAsyncSandbox is None:
        raise RuntimeError("e2b is not installed in this interpreter")
    started = time.perf_counter()
    create = getattr(E2BAsyncSandbox, "beta_create", None)
    if create is not None:
        sandbox = await create(
            template=template_id,
            timeout=timeout_seconds,
            auto_pause=True,
            envs={},
            network={"allow_public_traffic": False},
            api_key=api_key,
        )
    else:
        sandbox = await E2BAsyncSandbox.create(
            template=template_id,
            timeout=timeout_seconds,
            envs={},
            network={"allow_public_traffic": False},
            api_key=api_key,
        )
    elapsed = time.perf_counter() - started
    try:
        return elapsed, sandbox.sandbox_id
    finally:
        await sandbox.kill()


async def main() -> int:
    parser = argparse.ArgumentParser(description="Compare E2B vs local sandbox create time.")
    parser.add_argument("--env", default="dev", choices=["dev", "qa", "prod"])
    parser.add_argument("--template-id", default="", help="Override the common template alias.")
    parser.add_argument("--timeout", type=int, default=600, help="Sandbox timeout seconds.")
    parser.add_argument("--count", type=int, default=15, help="Number of local creates to run.")
    parser.add_argument("--sleep-between", type=float, default=0.0, help="Seconds to wait between local creates.")
    parser.add_argument(
        "--kill-after-create",
        action="store_true",
        help="Kill each local sandbox after measuring create latency.",
    )
    parser.add_argument(
        "--kill-at-end",
        action="store_true",
        help="Kill all local sandboxes after the timing loop completes.",
    )
    parser.add_argument("--request-timeout", type=float, default=900.0, help="HTTP request timeout seconds.")
    parser.add_argument(
        "--local-api-url",
        default=os.environ.get("SANDBOX_API_URL", "http://127.0.0.1:8001"),
        help="Local sandbox API base URL.",
    )
    parser.add_argument(
        "--local-api-key",
        default="sbx_Ns2qdbDAOYtN8ckESsTmCQ0Y285DnWzO",
        help="Local sandbox API key.",
    )
    parser.add_argument(
        "--internal-api-key",
        default="test-key-12345",
        help="Internal API key for immediate route readiness probes.",
    )
    parser.add_argument(
        "--probe-after-create",

        action="store_true",
        help="Immediately probe envd connection and guest routes after each create.",
    )
    parser.add_argument(
        "--skip-e2b",
        action="store_true",
        help="Only measure the local sandbox.",
    )
    args = parser.parse_args()

    template_id = args.template_id.strip() or resolve_template_id(args.env)

    print(f"template_id={template_id}", flush=True)
    print(f"local_api_url={args.local_api_url}", flush=True)
    sources: Counter[str] = Counter()
    gateways: Counter[str] = Counter()
    created_ids: list[str] = []
    for i in range(1, max(1, int(args.count)) + 1):
        if i > 1 and float(args.sleep_between) > 0:
            await asyncio.sleep(float(args.sleep_between))
        local_elapsed, local_id, source, gateway, alloc_wait = await measure_local(
            template_id=template_id,
            api_url=args.local_api_url,
            api_key=args.local_api_key,
            timeout_seconds=args.timeout,
            request_timeout=args.request_timeout,
        )
        sources[source] += 1
        gateways[gateway or "-"] += 1
        if local_id:
            created_ids.append(local_id)
        print(
            f"local_create_index={i} local_create_seconds={local_elapsed:.3f} "
            f"source={source} alloc_wait_seconds={alloc_wait or '-'} "
            f"gateway={gateway or '-'} sandbox_id={local_id}",
            flush=True,
        )
        if args.probe_after_create and local_id:
            await probe_local_ready(
                sandbox_id=local_id,
                api_url=args.local_api_url,
                api_key=args.local_api_key,
                internal_api_key=args.internal_api_key,
                request_timeout=args.request_timeout,
            )
        if args.kill_after_create and local_id:
            try:
                await kill_local(
                    sandbox_id=local_id,
                    api_url=args.local_api_url,
                    api_key=args.local_api_key,
                    request_timeout=args.request_timeout,
                )
                print(f"local_kill_index={i} sandbox_id={local_id} killed=true", flush=True)
            except Exception as ex:  # noqa: BLE001
                print(
                    f"local_kill_index={i} sandbox_id={local_id} killed=false error={type(ex).__name__}:{ex}",
                    flush=True,
                )

    if args.kill_at_end and not args.kill_after_create:
        for i, local_id in enumerate(created_ids, start=1):
            try:
                await kill_local(
                    sandbox_id=local_id,
                    api_url=args.local_api_url,
                    api_key=args.local_api_key,
                    request_timeout=args.request_timeout,
                )
                print(f"local_kill_index={i} sandbox_id={local_id} killed=true", flush=True)
            except Exception as ex:  # noqa: BLE001
                print(
                    f"local_kill_index={i} sandbox_id={local_id} killed=false error={type(ex).__name__}:{ex}",
                    flush=True,
                )

    print(f"source_counts={dict(sources)}", flush=True)
    print(f"gateway_counts={dict(gateways)}", flush=True)
    return 0
    e2b_api_key = "e2b_6b8a925bd66634d41f2f02e1bb7434e19212dbe3"
    if not e2b_api_key:
        print("skip_e2b_reason=E2B_API_KEY not set", flush=True)
        return 0
    if E2BAsyncSandbox is None:
        print("skip_e2b_reason=e2b package not installed", flush=True)
        return 0

    e2b_elapsed, e2b_id = await measure_e2b(
        template_id=template_id,
        api_key=e2b_api_key,
        timeout_seconds=args.timeout,
    )
    print(f"e2b_create_seconds={e2b_elapsed:.3f} sandbox_id={e2b_id}", flush=True)
    print(f"delta_local_minus_e2b_seconds={local_elapsed - e2b_elapsed:.3f}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
