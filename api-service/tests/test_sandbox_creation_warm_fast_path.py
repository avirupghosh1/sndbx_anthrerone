import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_KEY", "client-key")
os.environ.setdefault("INTERNAL_API_KEY", "internal-key")
os.environ.setdefault("ADMIN_API_KEY", "admin-key")

from orchestrator.sandbox_creation_ops import SandboxCreationOpsMixin  # noqa: E402


class FakeDB:
    def __init__(self):
        self.template = {
            "template_id": "materialized-template",
            "base_image": "python:3.11",
            "warm_snapshot_image": "registry.local/templates/materialized-template:abc123",
            "registry_image_ref": "registry.local/templates/materialized-template:abc123",
        }

    def get_sandbox_template(self, template_id):
        return self.template if template_id == "materialized-template" else None


class FakePool:
    def __init__(self):
        self.calls = []

    def try_acquire(
        self,
        template_id,
        metadata,
        cpu_limit,
        memory_limit,
        timeout,
        *,
        owner_client_id=None,
        owner_api_key_id=None,
        wait_for_ready=True,
    ):
        self.calls.append(
            {
                "template_id": template_id,
                "metadata": metadata,
                "cpu_limit": cpu_limit,
                "memory_limit": memory_limit,
                "timeout": timeout,
                "owner_client_id": owner_client_id,
                "owner_api_key_id": owner_api_key_id,
                "wait_for_ready": wait_for_ready,
            }
        )
        return "sb-warm"


class FakeManager(SandboxCreationOpsMixin):
    def __init__(self):
        self._config = SimpleNamespace(
            SANDBOX_WARM_POOL_SIZE=4,
            SANDBOX_WARM_POOL_TEMPLATE_ID="python:3.11",
            DEFAULT_TEMPLATE="python:3.11",
        )
        self.db = FakeDB()
        self.warm_pool = FakePool()
        self.ready_count_keys = []
        self.ensure_calls = 0

    def _resolve_template_alias_for_create(self, requested_template_id, row, owner_client_id):
        return requested_template_id, row

    def warm_pool_key(self, template_id, cpu_limit, memory_limit, timeout):
        return f"{template_id}|{cpu_limit}|{memory_limit}"

    def warm_pool_ready_count(self, warm_pool_key):
        self.ready_count_keys.append(warm_pool_key)
        return 4

    def _ensure_template_runtime_image(self, template_id, row):
        self.ensure_calls += 1
        raise AssertionError("warm-pool handoff should happen before template image verification")

    def _create_sandbox_fresh(self, **_kwargs):
        raise AssertionError("ready warm-pool rows should avoid cold create")


def test_ready_warm_pool_handoff_skips_request_time_image_verify():
    manager = FakeManager()

    sandbox_id = manager.create_sandbox(
        "materialized-template",
        metadata={"guest_ports": [8765]},
        cpu_limit="1",
        memory_limit="512m",
        timeout=600,
        owner_client_id="client-a",
        owner_api_key_id="key-a",
    )

    assert sandbox_id == "sb-warm"
    assert manager.ensure_calls == 0
    assert manager.ready_count_keys == ["materialized-template|1|512m"]
    assert manager.warm_pool.calls[0]["wait_for_ready"] is False
    assert manager.warm_pool.calls[0]["owner_client_id"] == "client-a"
