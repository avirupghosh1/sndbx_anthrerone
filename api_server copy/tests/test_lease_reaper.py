from __future__ import annotations

from pathlib import Path
import sys
import types

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

docker_mod = types.ModuleType("docker")
docker_utils = types.ModuleType("docker.utils")
docker_utils_socket = types.ModuleType("docker.utils.socket")
docker_utils_socket.SocketError = Exception
docker_utils_socket.frames_iter_no_tty = lambda *args, **kwargs: iter(())
docker_mod.utils = docker_utils
docker_mod.errors = types.SimpleNamespace(ImageNotFound=Exception)
docker_mod.from_env = lambda *args, **kwargs: None
sys.modules.setdefault("docker", docker_mod)
sys.modules.setdefault("docker.utils", docker_utils)
sys.modules.setdefault("docker.utils.socket", docker_utils_socket)

k8s_mod = types.ModuleType("kubernetes")
k8s_client = types.ModuleType("kubernetes.client")
k8s_config = types.ModuleType("kubernetes.config")
k8s_config.ConfigException = Exception
k8s_config.load_incluster_config = lambda: None
k8s_config.load_kube_config = lambda: None
k8s_rest = types.ModuleType("kubernetes.client.rest")
k8s_rest.ApiException = Exception
k8s_stream = types.ModuleType("kubernetes.stream")
k8s_stream.stream = lambda *args, **kwargs: None
k8s_mod.client = k8s_client
k8s_mod.config = k8s_config
sys.modules.setdefault("kubernetes", k8s_mod)
sys.modules.setdefault("kubernetes.client", k8s_client)
sys.modules.setdefault("kubernetes.config", k8s_config)
sys.modules.setdefault("kubernetes.client.rest", k8s_rest)
sys.modules.setdefault("kubernetes.stream", k8s_stream)

from database.store import Database
from orchestrator.sandbox_manager import SandboxManager


class _FakeExecution:
    def get_backend_kind(self) -> str:
        return "k8s"

    def is_container_running(self, _container_id: str) -> bool:
        return True


def test_database_timeout_refresh_sets_absolute_lease(tmp_path):
    db = Database(str(tmp_path / "sandboxes.db"))
    db.create_sandbox(
        sandbox_id="sb-1",
        container_id="pod-1",
        template_id="python:3.11",
        timeout=600,
        runtime="k8s",
    )

    row1 = db.get_sandbox("sb-1")
    assert row1 is not None
    first_expiry = row1.get("lease_expires_at")
    assert isinstance(first_expiry, str) and first_expiry

    assert db.update_sandbox_timeout("sb-1", 1200) is True
    row2 = db.get_sandbox("sb-1")
    assert row2 is not None
    assert row2["timeout"] == 1200
    assert isinstance(row2.get("lease_expires_at"), str)
    assert row2["lease_expires_at"] >= first_expiry


def test_database_lists_expired_sandboxes(tmp_path):
    db = Database(str(tmp_path / "sandboxes.db"))
    db.create_sandbox(
        sandbox_id="sb-expired",
        container_id="pod-expired",
        template_id="python:3.11",
        timeout=600,
        runtime="k8s",
    )
    db.create_sandbox(
        sandbox_id="sb-live",
        container_id="pod-live",
        template_id="python:3.11",
        timeout=600,
        runtime="k8s",
    )

    expired = db.list_expired_sandboxes(now_iso="9999-01-01T00:00:00Z")
    expired_ids = {row["sandbox_id"] for row in expired}
    assert {"sb-expired", "sb-live"} <= expired_ids


def test_manager_reaps_expired_sandboxes(monkeypatch):
    db = types.SimpleNamespace(
        list_expired_sandboxes=lambda limit=100: [
            {"sandbox_id": "sb-old-1", "lease_expires_at": "2026-06-23T00:00:00Z"},
            {"sandbox_id": "sb-old-2", "lease_expires_at": "2026-06-23T00:00:01Z"},
        ]
    )
    manager = SandboxManager(db, execution=_FakeExecution())
    manager.stop_background_work()
    killed: list[str] = []

    def _kill(sandbox_id: str, force: bool = True) -> bool:
        killed.append(sandbox_id)
        return True

    monkeypatch.setattr(manager, "kill_sandbox", _kill)

    assert manager.reap_expired_sandboxes(limit=10) == 2
    assert killed == ["sb-old-1", "sb-old-2"]
