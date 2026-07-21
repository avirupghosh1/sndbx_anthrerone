from types import SimpleNamespace

from orchestrator.gateway_targets import GatewayTarget
from orchestrator.sandbox_gateway_ops import SandboxGatewayOpsMixin


class FakeExecution:
    def get_backend_kind(self):
        return "docker"


class FakeDB:
    def __init__(self):
        self.claim_calls = []
        self.rows = []

    def get_warm_pool_segment(self, warm_pool_key):
        return {"template_id": "python:3.11"}

    def get_sandbox_template(self, template_id):
        return {
            "template_id": template_id,
            "warm_snapshot_image": "warm-image:tag",
            "registry_image_ref": "",
        }

    def list_warm_pool_sandboxes(self, *, warm_pool_key=None):
        return list(self.rows)

    def claim_warm_pool_sandbox(self, **kwargs):
        self.claim_calls.append(kwargs)
        return self.rows[0] if self.rows else None


class FakeGatewaySelector(SandboxGatewayOpsMixin):
    def __init__(self):
        self._config = SimpleNamespace()
        self.db = FakeDB()
        self.execution = FakeExecution()
        self.events = []
        self.lost = []
        self.targets = [
            GatewayTarget("runtime-gateway-0", "http://gateway-0", "http://gateway-0"),
            GatewayTarget("runtime-gateway-1", "http://gateway-1", "http://gateway-1"),
        ]

    def _gateway_targets(self):
        return list(self.targets)

    def _gateway_can_accept_new_usage(self, target, *, force_refresh=False):
        return target.instance_id == "runtime-gateway-1"

    def _mark_sandbox_lost(self, sandbox_id, detail):
        self.lost.append((sandbox_id, detail))
        return True

    def _record_observability_event(self, **kwargs):
        self.events.append(kwargs)


def test_template_owner_gateway_is_preferred_even_when_registry_ref_exists():
    manager = FakeGatewaySelector()

    target = manager._select_gateway_target_for_pool(
        template_id="tpl-fast",
        cpu_limit="1",
        memory_limit="512m",
        timeout=3600,
        template_row={
            "warm_snapshot_image": "local-template:tag",
            "registry_image_ref": "registry.local/templates/tpl-fast:tag",
            "materialized_gateway_instance_id": "runtime-gateway-1",
        },
    )

    assert target is not None
    assert target.instance_id == "runtime-gateway-1"
    assert manager.events[-1]["metadata"]["reason"] == "template_owner_cached"


def test_runtime_gateway_acquire_does_not_claim_filtered_dead_gateway_rows():
    manager = FakeGatewaySelector()
    manager.db.rows = [
        {
            "sandbox_id": "sb-dead",
            "state": "running",
            "is_warm_pool": True,
            "warm_pool_key": "python:3.11|1|512m|3600",
            "gateway_instance_id": "runtime-gateway-9",
            "metadata": {"warm_pool_snapshot_image": "warm-image:tag"},
        }
    ]

    claimed = manager.acquire_warm_pool_sandbox(
        template_id="python:3.11",
        cpu_limit="1",
        memory_limit="512m",
        timeout=3600,
        owner_client_id="client-a",
        owner_api_key_id="key-a",
    )

    assert claimed is None
    assert manager.db.claim_calls == []
    assert manager.lost[0][0] == "sb-dead"
