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

from orchestrator.sandbox_manager import SandboxManager
from orchestrator.k8s_pod_manager import K8sPodManager


class _FakeExecution:
    def __init__(self) -> None:
        self.put_archive_calls: list[tuple[str, str, bytes]] = []
        self.run_calls: list[tuple[str, str, float | None, str | None]] = []

    def get_backend_kind(self) -> str:
        return "k8s"

    def put_archive_to_container(self, container_id: str, path: str, data: bytes) -> bool:
        self.put_archive_calls.append((container_id, path, data))
        return True

    def run_command(
        self,
        container_id: str,
        command: str,
        cwd=None,
        env=None,
        timeout: float | None = None,
        user: str | None = None,
    ):
        self.run_calls.append((container_id, command, timeout, user))
        if "__ENVD_BAKED__" in command or "__ENVD_NOT_BAKED__" in command:
            return {"exit_code": 0, "stdout": "__ENVD_NOT_BAKED__\n", "stderr": "", "pid": 0}
        return {"exit_code": 0, "stdout": "", "stderr": "", "pid": 0}

    def image_default_user(self, image_ref: str) -> str:
        return "root"

    def kill_container(self, container_id: str, force: bool = True) -> bool:
        return True


class _FakeDb:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def get_sandbox_template(self, template_id: str):
        return {
            "template_id": template_id,
            "start_cmd": "python /app/server.py",
            "env": {"PORT": "8765"},
            "warm_snapshot_image": "img:test",
            "base_image": "img:test",
        }

    def get_sandbox(self, sandbox_id: str):
        return self.rows.get(sandbox_id)

    def create_sandbox(self, **kwargs):
        sandbox_id = kwargs["sandbox_id"]
        self.rows[sandbox_id] = {
            "sandbox_id": sandbox_id,
            "container_id": kwargs["container_id"],
            "template_id": kwargs.get("template_id"),
            "metadata": kwargs.get("metadata") or {},
        }

    def merge_sandbox_metadata(self, sandbox_id: str, metadata):
        row = self.rows.setdefault(sandbox_id, {"metadata": {}})
        row["metadata"] = {**(row.get("metadata") or {}), **metadata}

    def update_sandbox_state(self, sandbox_id: str, state: str):
        row = self.rows.setdefault(sandbox_id, {})
        row["state"] = state

    def delete_sandbox(self, sandbox_id: str):
        self.rows.pop(sandbox_id, None)


def _build_manager(monkeypatch):
    execution = _FakeExecution()
    db = _FakeDb()
    manager = SandboxManager(db, execution=execution)
    manager.warm_pool = None
    monkeypatch.setattr("orchestrator.sandbox_manager.is_container_like_execution", lambda _: True)
    monkeypatch.setattr("orchestrator.sandbox_manager.is_k8s_execution", lambda _: True)
    monkeypatch.setattr(
        "orchestrator.sandbox_manager.data_plane_enabled_for_config",
        lambda cfg: True,
        raising=False,
    )
    manager._config.K8S_COMBINED_GUEST_BOOTSTRAP = True
    manager._config.ENVD_AUTO_START = True
    manager._config.ENVD_ALWAYS_ON = True
    manager._config.ENVD_BOOTSTRAP_PIP_TIMEOUT_SEC = 30.0
    manager._config.SANDBOX_WARM_POOL_SIZE = 0
    return manager, execution, db


def test_k8s_combined_bootstrap_bakes_envd_when_image_is_unbaked(monkeypatch):
    manager, execution, _db = _build_manager(monkeypatch)

    ok = manager._bootstrap_guest_services_k8s_combined(
        "sb-123",
        "pod-123",
        "tpl-1",
        start_envd=True,
        envd_port=49983,
    )

    assert ok is True
    assert execution.put_archive_calls
    combined_script = execution.run_calls[-1][1]
    assert "uvicorn envd_guest.server:app" in combined_script
    assert "127.0.0.1,49983" in combined_script or "/dev/tcp/127.0.0.1/49983" in combined_script


def test_envd_bake_runs_privileged_steps_as_root(monkeypatch):
    manager, execution, _db = _build_manager(monkeypatch)

    ok = manager._ensure_envd_baked("sb-123", "pod-123", pip_timeout=30.0)

    assert ok is True
    commands = execution.run_calls
    assert any(cmd == "root" for _cid, _command, _timeout, cmd in commands[1:])


def test_create_sandbox_fresh_fails_closed_when_guest_bootstrap_fails(monkeypatch):
    manager, execution, db = _build_manager(monkeypatch)

    def _create_container(name, config):
        return "pod-123"

    execution.create_container = _create_container
    killed: list[tuple[str, bool]] = []

    def _kill_sandbox(sandbox_id: str, force: bool = True) -> bool:
        killed.append((sandbox_id, force))
        return True

    manager.kill_sandbox = _kill_sandbox  # type: ignore[method-assign]
    manager._bootstrap_guest_services = lambda sandbox_id, container_id, template_id: False  # type: ignore[method-assign]

    sid = manager._create_sandbox_fresh(template_id="tpl-1", metadata={}, timeout=600)

    assert sid is None
    assert killed
    assert db.rows


def test_k8s_run_command_root_preserves_environment(monkeypatch):
    manager = K8sPodManager()
    monkeypatch.setattr(manager, "_ensure_k8s_client", lambda: True)
    captured = {}

    class _ApiClient:
        def close(self):
            return None

    class _CoreV1Api:
        def __init__(self, api_client=None):
            self.api_client = api_client or _ApiClient()
            self.connect_get_namespaced_pod_exec = object()

    class _Resp:
        returncode = 0

        def is_open(self):
            return False

    def _fake_stream(*args, **kwargs):
        captured["command"] = kwargs["command"]
        return _Resp()

    monkeypatch.setattr("orchestrator.k8s_pod_manager.client.ApiClient", _ApiClient, raising=False)
    monkeypatch.setattr("orchestrator.k8s_pod_manager.client.CoreV1Api", _CoreV1Api, raising=False)
    monkeypatch.setattr("orchestrator.k8s_pod_manager.stream", _fake_stream)

    result = manager.run_command("pod-1", "echo hi", user="root")

    assert result["exit_code"] == 0
    assert "sudo -E -n" in captured["command"][-1]


def test_k8s_run_command_same_user_avoids_su_prompt(monkeypatch):
    manager = K8sPodManager()
    monkeypatch.setattr(manager, "_ensure_k8s_client", lambda: True)
    captured = {}

    class _ApiClient:
        def close(self):
            return None

    class _CoreV1Api:
        def __init__(self, api_client=None):
            self.api_client = api_client or _ApiClient()
            self.connect_get_namespaced_pod_exec = object()

    class _Resp:
        returncode = 0

        def is_open(self):
            return False

    def _fake_stream(*args, **kwargs):
        captured["command"] = kwargs["command"]
        return _Resp()

    monkeypatch.setattr("orchestrator.k8s_pod_manager.client.ApiClient", _ApiClient, raising=False)
    monkeypatch.setattr("orchestrator.k8s_pod_manager.client.CoreV1Api", _CoreV1Api, raising=False)
    monkeypatch.setattr("orchestrator.k8s_pod_manager.stream", _fake_stream)

    result = manager.run_command("pod-1", "echo hi", user="ubuntu")

    assert result["exit_code"] == 0
    assert 'if [ "$(id -un)" = ubuntu ]' in captured["command"][-1]
