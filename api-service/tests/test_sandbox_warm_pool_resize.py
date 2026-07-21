import asyncio
import os

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_KEY", "client-key")
os.environ.setdefault("INTERNAL_API_KEY", "internal-key")
os.environ.setdefault("ADMIN_API_KEY", "admin-key")

from handlers.sandboxes import resize_sandbox_warm_pool  # noqa: E402
from middleware import ApiKeyPrincipal  # noqa: E402
from models import ResizeWarmPoolRequest  # noqa: E402


def _principal() -> ApiKeyPrincipal:
    return ApiKeyPrincipal(
        client_id="client-a",
        key_id="key-a",
        key_name="test",
        key_prefix="test",
        email="user@example.com",
        display_name="User",
        is_active=True,
    )


class FakeResizeDB:
    def __init__(self):
        self.segments = {
            "materialized-template|1|512m": {
                "warm_pool_key": "materialized-template|1|512m",
                "template_id": "materialized-template",
                "cpu_limit": "1",
                "memory_limit": "512m",
                "timeout": 3600,
                "desired_size": 4,
                "preferred_gateway_instance_id": "runtime-gateway-0",
            }
        }

    def get_warm_pool_segment(self, warm_pool_key):
        return self.segments.get(warm_pool_key)

    def get_sandbox_template(self, template_id):
        return {"template_id": template_id, "warm_snapshot_image": "warm-image"}


class FakeResizeManager:
    def __init__(self):
        self.db = FakeResizeDB()
        self.warm_pool = object()
        self.trim_calls = []
        self.apply_calls = []
        self._config = type(
            "Config",
            (),
            {
                "DEFAULT_CPU_LIMIT": "1",
                "DEFAULT_MEMORY_LIMIT": "512m",
                "DEFAULT_TIMEOUT": 3600,
            },
        )()
        self.sandbox = {
            "sandbox_id": "sb-1",
            "owner_client_id": "client-a",
            "template_id": "alias-template",
            "cpu_limit": "1",
            "memory_limit": "512m",
            "timeout": 3600,
            "gateway_instance_id": "runtime-gateway-0",
            "metadata": {
                "sandbox_allocation_pool_key": "materialized-template|1|512m",
            },
        }

    def get_sandbox(self, sandbox_id):
        return self.sandbox if sandbox_id == "sb-1" else None

    def warm_pool_key(self, template_id, cpu_limit, memory_limit, timeout):
        return f"{template_id}|{cpu_limit}|{memory_limit}"

    def _apply_requested_warm_pool_size(self, pool, **kwargs):
        self.apply_calls.append(kwargs)
        self.note_warm_pool_segment(**kwargs)

    def note_warm_pool_segment(
        self,
        *,
        template_id,
        cpu_limit,
        memory_limit,
        timeout,
        desired_size,
        preferred_gateway_instance_id=None,
        **_kwargs,
    ):
        key = self.warm_pool_key(template_id, cpu_limit, memory_limit, timeout)
        self.db.segments[key] = {
            "warm_pool_key": key,
            "template_id": template_id,
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "timeout": timeout,
            "desired_size": desired_size,
            "preferred_gateway_instance_id": preferred_gateway_instance_id,
        }
        return self.db.segments[key]

    def trim_warm_pool_to_size(self, warm_pool_key, desired_size):
        self.trim_calls.append((warm_pool_key, desired_size))
        return 4

    def warm_pool_ready_count(self, warm_pool_key):
        return 0


def test_resize_warm_pool_to_zero_uses_allocation_pool_key():
    manager = FakeResizeManager()

    response = asyncio.run(
        resize_sandbox_warm_pool(
            "sb-1",
            ResizeWarmPoolRequest(warmpool_size=0),
            _principal(),
            manager,
        )
    )

    assert response.warm_pool_key == "materialized-template|1|512m"
    assert response.template_id == "materialized-template"
    assert response.previous_desired_size == 4
    assert response.desired_size == 0
    assert manager.db.segments["materialized-template|1|512m"]["desired_size"] == 0
    assert manager.trim_calls == [("materialized-template|1|512m", 0)]
