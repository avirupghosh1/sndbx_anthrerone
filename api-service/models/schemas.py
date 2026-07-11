"""Request schemas (Pydantic models)."""

from pydantic import AliasChoices, BaseModel, Field, ConfigDict, model_validator
from typing import Optional, Dict, Any, List
from enum import Enum


class SandboxState(str, Enum):
    """Sandbox state."""
    RUNNING = "running"
    PAUSED = "paused"
    KILLED = "killed"
    FAILED = "failed"


class EntryType(str, Enum):
    """File entry type."""
    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class CreateSandboxRequest(BaseModel):
    """Create sandbox request."""
    # Must be a pull-able Docker image ref unless the orchestrator maps it (see SandboxManager).
    template_id: Optional[str] = Field(
        default="python:3.11",
        validation_alias=AliasChoices("template_id", "templateID"),
        description="Container image (e.g. python:3.11) or a known template alias",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default={},
        description=(
            "Custom metadata. Optional ``guest_ports``: list of TCP ports your workload exposes "
            "(e.g. ``[8080, 49983]``) for Pod/Service and data-plane routing."
        ),
    )
    env_vars: Optional[Dict[str, Any]] = Field(
        default=None,
        validation_alias=AliasChoices("env_vars", "envVars"),
        description="Environment variables injected into the guest container.",
    )
    cpu_limit: Optional[str] = Field(default="1", description="CPU limit (e.g., '1', '0.5')")
    memory_limit: Optional[str] = Field(default="512m", description="Memory limit (e.g., '512m', '1g')")
    timeout: Optional[int] = Field(default=3600, description="Sandbox timeout in seconds")
    from_snapshot_image: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("from_snapshot_image", "fromSnapshotImage"),
        description=(
            "Docker image ref returned by POST /sandboxes/{id}/snapshot (filesystem capture). "
            "When set, ``template_id`` is ignored for image selection; warm pool is skipped."
        ),
    )

    model_config = ConfigDict(populate_by_name=True, json_schema_extra={
            "example": {
                "template_id": "python:3.11",
                "metadata": {"purpose": "testing"},
                "cpu_limit": "2",
                "memory_limit": "1g",
                "timeout": 7200
            }
        })


class CreateSnapshotRequest(BaseModel):
    """Optional label for a filesystem snapshot (Docker ``docker commit``)."""

    label: Optional[str] = Field(default=None, max_length=200, description="Human-readable label stored with the snapshot")


class RefreshSandboxTimeoutRequest(BaseModel):
    """Lease refresh: updates the stored sandbox timeout."""

    timeout_seconds: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("timeout_seconds", "timeout"),
        ge=60,
        le=604800,
        description="New lease length in seconds (clamped server-side for Docker sandboxes).",
    )

    @model_validator(mode="after")
    def _require_timeout(self) -> "RefreshSandboxTimeoutRequest":
        if self.timeout_seconds is None:
            raise ValueError("timeout_seconds or timeout is required")
        return self

    model_config = ConfigDict(populate_by_name=True)


class RegisterTemplateRequest(BaseModel):
    """Register a logical ``template_id`` for Docker (base image + env + ``start_cmd``).

    First ``POST /sandboxes`` with this ``template_id`` performs a **one-time** build:
    run container from ``base_image``, apply ``env`` at create time, run ``start_cmd``,
    wait ``settle_seconds``, ``docker commit`` -> warm snapshot; subsequent sandboxes reuse
    that image (and optional warm pool). See ``docs/CUSTOM_TEMPLATES.md``.
    """

    template_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Logical id (letters, digits, ``._-`` only; no ``/``; not a raw image ref).",
    )
    base_image: str = Field(..., min_length=1, description="Docker image to pull for the build container")
    env: Optional[Dict[str, str]] = Field(default=None, description="Container environment during build and run")
    start_cmd: str = Field(
        default="",
        max_length=8000,
        description="Shell command run once inside the build container before settle + commit",
    )
    settle_seconds: int = Field(
        default=20,
        ge=0,
        le=600,
        description="Sleep after ``start_cmd`` before ``docker commit`` (lets background installs finish)",
    )
    ready_cmd: str = Field(
        default="",
        max_length=8000,
        description=(
            "Optional shell probe: after ``settle_seconds``, run repeatedly "
            "until exit code 0 or ``TEMPLATE_READY_TIMEOUT_SEC``. Empty = skip."
        ),
    )
    warm_snapshot_image: Optional[str] = Field(
        default=None,
        max_length=300,
        description=(
            "When set, skip the one-time warm snapshot build and use this OCI image ref directly "
            "(e.g. host ``docker build`` + minikube docker-env for Mac dev)."
        ),
    )


class RegisterTemplateFromDockerfileRequest(BaseModel):
    """Register a template from a Dockerfile.

    **Default** (``TEMPLATE_DOCKERFILE_BUILD_MODE=parsed``): the API parses the Dockerfile with
    ``dockerfile-parse``, runs ``RUN`` / ``COPY`` / ``ADD`` inside a throwaway build container (same
    idea as E2B's step runner), then ``docker commit``. Docker / gVisor sandboxes store the OCI
    tag in ``warm_snapshot_image``.

    **Recommended for remote runtimes** (``TEMPLATE_DOCKERFILE_BUILD_MODE=docker_cli`` with
    ``TEMPLATE_BUILD_VIA_RUNTIME_GATEWAY=true``): the API forwards the build request to
    runtime-gateway, which owns Docker access, registry credentials, and the produced tag.
    """

    template_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Logical id (same rules as ``POST /templates``).",
    )
    dockerfile: str = Field(
        ...,
        min_length=1,
        max_length=512_000,
        description="Full Dockerfile contents (UTF-8).",
    )
    image_tag: Optional[str] = Field(
        default=None,
        max_length=200,
        description="Docker tag for the built image (e.g. ``myregistry.io/org/app:v1``). Auto-generated if omitted.",
    )
    build_args: Optional[Dict[str, str]] = Field(
        default=None,
        description="Passed as ``docker build --build-arg KEY=VALUE``.",
    )
    context_tar_gzip_base64: Optional[str] = Field(
        default=None,
        description="Optional base64-encoded **gzip** tar of build context (extracted before build).",
    )
    env: Optional[Dict[str, str]] = Field(default=None, description="Container env for warm snapshot build/run (same as ``POST /templates``).")
    start_cmd: str = Field(
        default="",
        max_length=8000,
        description="Optional shell run inside the **built** image before settle + ``docker commit``.",
    )
    ready_cmd: str = Field(default="", max_length=8000, description="Optional readiness probe after settle (see ``RegisterTemplateRequest.ready_cmd``).")
    settle_seconds: int = Field(default=20, ge=0, le=600, description="Sleep after ``start_cmd`` before ``ready_cmd`` / commit.")


class RunCommandRequest(BaseModel):
    """Run command request."""
    command: str = Field(..., description="Command to execute")
    cwd: Optional[str] = Field(default="/", description="Working directory")
    env: Optional[Dict[str, str]] = Field(default={}, description="Environment variables")
    timeout: Optional[float] = Field(default=30, description="Command timeout in seconds")
    user: Optional[str] = Field(default=None, description="User to run command as")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "command": "python script.py",
                "cwd": "/app",
                "env": {"DEBUG": "true"},
                "timeout": 60,
                "user": "root"
            }
        })


class WriteFileRequest(BaseModel):
    """Write file request."""
    path: str = Field(..., description="File path")
    content: str = Field(..., description="File content")
    encoding: Optional[str] = Field(default="utf-8", description="File encoding")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp/test.txt",
                "content": "Hello, World!",
                "encoding": "utf-8"
            }
        })


class DeleteFileRequest(BaseModel):
    """Delete file request."""
    path: str = Field(..., description="File path")
    recursive: Optional[bool] = Field(default=False, description="Delete recursively")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp/dir",
                "recursive": True
            }
        })


class CreateDirectoryRequest(BaseModel):
    """Create directory request."""
    path: str = Field(..., description="Directory path")
    mode: Optional[int] = Field(default=0o755, description="Directory permissions")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp/newdir",
                "mode": 493  # 0o755
            }
        })


class ListFilesRequest(BaseModel):
    """List files request."""
    path: Optional[str] = Field(default="/", description="Directory path")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp"
            }
        })


class SpawnAgentRequest(BaseModel):
    """Spawn agent request."""
    agent_name: str = Field(..., description="Agent name/type")
    agent_code: Optional[str] = Field(default=None, description="Agent code (Python)")
    config: Optional[Dict[str, Any]] = Field(default={}, description="Agent configuration")
    auto_start: Optional[bool] = Field(default=True, description="Auto-start agent")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "agent_name": "build_loop_demo",
                "agent_code": "print('Hello from agent')",
                "config": {
                    "single_run": True,
                    "timeout": 300,
                    "exec_interval_sec": 1.0,
                },
                "auto_start": True,
            }
        })


class KillAgentRequest(BaseModel):
    """Kill agent request."""
    agent_id: str = Field(..., description="Agent ID")
    force: Optional[bool] = Field(default=False, description="Force kill")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "agent_id": "agent-123",
                "force": False
            }
        })


class AgentMessage(BaseModel):
    """Agent message."""
    agent_id: str = Field(..., description="Agent ID")
    message_type: str = Field(..., description="Message type")
    content: Dict[str, Any] = Field(..., description="Message content")
    timestamp: Optional[str] = Field(default=None, description="Timestamp")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "agent_id": "agent-123",
                "message_type": "task",
                "content": {"task": "analyze", "data": "..."},
                "timestamp": "2024-01-01T00:00:00Z"
            }
        })
