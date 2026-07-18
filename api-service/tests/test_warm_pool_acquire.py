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


class FakeManager:
    def __init__(self):
        self._config = SimpleNamespace(SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC=30.0)
        self.acquire_calls = 0

    def acquire_warm_pool_sandbox(self, **_kwargs):
        self.acquire_calls += 1
        return None

    def warm_pool_ready_count(self, _pool_key):
        return 0


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
