import importlib.util
import time
from pathlib import Path
from types import SimpleNamespace

_MODULE_PATH = Path(__file__).resolve().parents[1] / "orchestrator" / "warm_sandbox_pool.py"
_SPEC = importlib.util.spec_from_file_location("warm_sandbox_pool", _MODULE_PATH)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
WarmSandboxPool = _MODULE.WarmSandboxPool
MultiWarmSandboxPool = _MODULE.MultiWarmSandboxPool


class FakeManager:
    def __init__(self):
        self._config = SimpleNamespace(SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC=30.0)
        self.acquire_calls = 0
        self.claimed = None

    def acquire_warm_pool_sandbox(self, **_kwargs):
        self.acquire_calls += 1
        return self.claimed

    def warm_pool_ready_count(self, _pool_key):
        return 0

    def warm_pool_key(self, template_id, cpu_limit, memory_limit, timeout):
        return f"{template_id}|{cpu_limit}|{memory_limit}"


def test_try_acquire_can_skip_waiting_for_ready_sandbox():
    manager = FakeManager()
    pool = WarmSandboxPool(
        manager,
        logical_template_id="custom-template",
        cpu_limit="1",
        memory_limit="512m",
        timeout=600,
        pool_size=1,
    )

    started = time.monotonic()
    sandbox_id = pool.try_acquire(
        "custom-template",
        {},
        "1",
        "512m",
        600,
        wait_for_ready=False,
    )

    assert sandbox_id is None
    assert manager.acquire_calls == 1
    assert time.monotonic() - started < 0.2


def test_multi_pool_falls_back_to_db_claim_when_local_pool_does_not_handoff():
    manager = FakeManager()
    manager.claimed = {"sandbox_id": "sb-direct"}
    cfg = SimpleNamespace(
        SANDBOX_WARM_POOL_SIZE=1,
        SANDBOX_WARM_POOL_TEMPLATE_ID="custom-template",
        DEFAULT_TEMPLATE="python:3.11",
        SANDBOX_WARM_POOL_CPU="1",
        DEFAULT_CPU_LIMIT="1",
        SANDBOX_WARM_POOL_MEMORY="512m",
        DEFAULT_MEMORY_LIMIT="512m",
        SANDBOX_WARM_POOL_TIMEOUT=600,
        DEFAULT_TIMEOUT=600,
    )
    multi = MultiWarmSandboxPool(manager, cfg)
    local_pool = WarmSandboxPool(
        manager,
        logical_template_id="custom-template",
        cpu_limit="1",
        memory_limit="512m",
        timeout=600,
        pool_size=1,
    )
    local_pool._stop.set()
    multi._pools[local_pool.pool_key] = local_pool

    sandbox_id = multi.try_acquire(
        "custom-template",
        {},
        "1",
        "512m",
        600,
        wait_for_ready=False,
    )

    assert sandbox_id == "sb-direct"
    assert manager.acquire_calls == 1
