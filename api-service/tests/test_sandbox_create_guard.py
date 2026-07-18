import asyncio
import os
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_KEY", "client-key")
os.environ.setdefault("INTERNAL_API_KEY", "internal-key")
os.environ.setdefault("ADMIN_API_KEY", "admin-key")

from handlers import sandboxes  # noqa: E402
from models import CreateSandboxRequest  # noqa: E402


class FakeDB:
    def get_sandbox_snapshot(self, *_args, **_kwargs):
        return None

    def get_sandbox_template_by_alias(self, *_args, **_kwargs):
        return None

    def get_best_sandbox_template_by_alias(self, *_args, **_kwargs):
        return None


class FakeSandboxManager:
    def __init__(self):
        self._config = SimpleNamespace(
            SANDBOX_CREATE_QUEUE_TIMEOUT_SEC=0.01,
            SANDBOX_CREATE_REQUEST_TIMEOUT_SEC=0.0,
        )
        self.db = FakeDB()

    def create_sandbox(self, *_args, **_kwargs):
        raise AssertionError("create_sandbox should not run when the worker never starts")

    def describe_docker_workload_blocker(self):
        return None


def test_create_sandbox_row_times_out_when_worker_never_starts(monkeypatch):
    async def blocked_run_io(_fn, *_args, **_kwargs):
        await asyncio.sleep(60)

    monkeypatch.setattr(sandboxes, "run_io", blocked_run_io)
    request = CreateSandboxRequest(template_id="python:3.11", timeout=600, warmpool_size=0)
    principal = SimpleNamespace(client_id="client-a", key_id="key-a")

    with pytest.raises(HTTPException) as exc:
        asyncio.run(sandboxes.create_sandbox_row(request, principal, FakeSandboxManager()))

    assert exc.value.status_code == 503
    assert "capacity" in exc.value.detail
