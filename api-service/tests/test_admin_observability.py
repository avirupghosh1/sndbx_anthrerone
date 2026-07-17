import os

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_KEY", "client-key")
os.environ.setdefault("INTERNAL_API_KEY", "internal-key")
os.environ.setdefault("ADMIN_API_KEY", "admin-key")

from handlers import admin_observability  # noqa: E402
from orchestrator import SandboxManager  # noqa: E402


class FakeDB:
    def __init__(self):
        self.last_event_filters = None

    def list_sandboxes(self, limit=100, offset=0, owner_client_id=None):
        return [
            {
                "sandbox_id": "sb-running",
                "state": "running",
                "is_warm_pool": False,
                "gateway_instance_id": "runtime-gateway-0",
            },
            {
                "sandbox_id": "sb-warm",
                "state": "running",
                "is_warm_pool": True,
                "warm_pool_key": "python:3.11|1|512m",
                "lease_expires_at": "2026-07-16T12:00:00Z",
                "created_at": "2026-07-16T11:00:00Z",
                "gateway_instance_id": "runtime-gateway-0",
            },
            {"sandbox_id": "sb-lost", "state": "lost", "is_warm_pool": False},
        ]

    def list_warm_pool_sandboxes(self, warm_pool_key=None):
        rows = [row for row in self.list_sandboxes() if row.get("is_warm_pool")]
        if warm_pool_key:
            rows = [row for row in rows if row.get("warm_pool_key") == warm_pool_key]
        return rows

    def list_all_sandbox_templates(self, limit=None):
        return [
            {
                "template_id": "tpl-ok",
                "template_alias": "python",
                "base_image": "python:3.11",
                "warm_snapshot_image": "tpl-ok:latest",
                "registry_image_ref": "registry/templates/tpl-ok:latest",
                "materialized_gateway_instance_id": "runtime-gateway-0",
                "build_error": "",
                "updated_at": "2026-07-16T10:00:00Z",
            }
        ]

    def list_observability_metric_samples(self, **kwargs):
        return []

    def list_observability_events(self, **kwargs):
        self.last_event_filters = kwargs
        return [
            {
                "event_id": "evt-1",
                "timestamp": "2026-07-16T12:00:00Z",
                "severity": kwargs.get("severity") or "info",
                "category": kwargs.get("category") or "scheduler",
                "action": "gateway_selected",
                "entity_type": "gateway",
                "entity_id": "runtime-gateway-0",
                "gateway_instance_id": "runtime-gateway-0",
                "template_id": "",
                "sandbox_id": "",
                "message": "selected",
                "metadata": {},
            }
        ]

    def get_sandbox(self, sandbox_id):
        for row in self.list_sandboxes():
            if row["sandbox_id"] == sandbox_id:
                return row
        return None

    def get_command_history(self, sandbox_id, limit=50):
        return []


class FakeManager:
    def __init__(self):
        self.db = FakeDB()

    def runtime_gateway_diagnostics(self):
        return [
            {
                "gateway_instance_id": "runtime-gateway-0",
                "reachable": True,
                "disk_total_bytes": 1000,
                "disk_used_bytes": 250,
                "disk_free_bytes": 750,
                "disk_used_ratio": 0.25,
                "running_sandbox_count": 2,
                "warm_sandbox_count": 1,
            }
        ]

    def warm_pool_segment_diagnostics(self):
        return [
            {
                "warm_pool_key": "python:3.11|1|512m",
                "template_id": "python:3.11",
                "desired_size": 2,
                "ready_count": 1,
                "inflight_count": 0,
                "ready_by_gateway": {"runtime-gateway-0": 1},
                "preferred_gateway_instance_id": "runtime-gateway-0",
                "last_error": None,
            }
        ]

    def get_execution_kind(self):
        return "docker"

    def describe_docker_workload_blocker(self):
        return None

    def _gateway_targets(self):
        return []

    def _registry_image_exists_from_gateway(self, target, image_ref):
        return True


def test_observability_summary_and_detail_shapes(monkeypatch):
    fake = FakeManager()
    monkeypatch.setattr(SandboxManager, "instance", fake)

    summary = admin_observability.get_summary_payload()
    assert summary["gateways"]["total"] == 1
    assert summary["sandboxes"]["active"] == 2
    assert summary["sandboxes"]["lost"] == 1
    assert summary["warm_pools"]["total_deficit"] == 1

    gateways = admin_observability.get_gateways_payload()
    assert gateways["gateways"][0]["gateway_instance_id"] == "runtime-gateway-0"
    assert gateways["gateways"][0]["deletion_cost"] > 0
    assert gateways["gateways"][0]["history"]

    warm = admin_observability.get_warm_pools_payload()
    assert warm["warm_pools"][0]["oldest_warm_lease"] == "2026-07-16T12:00:00Z"

    images = admin_observability.get_templates_images_payload()
    assert images["templates"][0]["status"] == "healthy"


def test_observability_events_pass_filters(monkeypatch):
    fake = FakeManager()
    monkeypatch.setattr(SandboxManager, "instance", fake)

    payload = admin_observability.get_events_payload(
        severity="error",
        category="scheduler",
        gateway_instance_id="runtime-gateway-0",
        limit=25,
        offset=5,
    )
    assert payload["limit"] == 25
    assert payload["offset"] == 5
    assert payload["events"][0]["severity"] == "error"
    assert fake.db.last_event_filters["severity"] == "error"
    assert fake.db.last_event_filters["gateway_instance_id"] == "runtime-gateway-0"
