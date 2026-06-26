"""Response schemas (Pydantic models)."""

from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, Dict, Any, List
from datetime import datetime


class SandboxResponse(BaseModel):
    """Sandbox info response."""
    sandbox_id: str = Field(..., description="Sandbox ID")
    state: str = Field(..., description="Sandbox state")
    created_at: str = Field(..., description="Creation timestamp")
    updated_at: str = Field(..., description="Update timestamp")
    lease_expires_at: Optional[str] = Field(
        default=None,
        description="Absolute UTC lease expiry timestamp used by the timeout reaper.",
    )
    metadata: Optional[Dict[str, Any]] = Field(default={}, description="Custom metadata")
    container_id: Optional[str] = Field(default=None, description="Container ID")
    runtime: str = Field(
        default="docker",
        description="Engine label: ``docker``, ``gvisor`` (Docker + ``runsc``), or ``firecracker`` (KVM microVM).",
    )
    sandbox_domain: str = Field(
        default="localhost",
        description="Domain suffix for ``get_host(port)`` → ``{port}-{sandbox_id}.{sandbox_domain}``",
    )
    envd_port: int = Field(default=49983, description="In-guest envd HTTP port (data plane)")
    envd_access_token: Optional[str] = Field(
        default=None,
        description="Layer-2 token for SDK→sandbox data plane (returned on create; use ``GET …/envd-connection`` later)",
    )
    traffic_access_token: Optional[str] = Field(
        default=None,
        description="Layer-3 token for private ingress (one per sandbox, all guest ports)",
    )
    allow_public_traffic: bool = Field(
        default=False,
        description="When false, Layer-3 ``e2b-traffic-access-token`` is required at ingress",
    )

    @model_validator(mode="before")
    @classmethod
    def _strip_secret_metadata(cls, data: Any) -> Any:
        if isinstance(data, dict):
            md = data.get("metadata")
            if isinstance(md, dict) and (
                "envd_access_token" in md or "traffic_access_token" in md
            ):
                md = {
                    k: v
                    for k, v in md.items()
                    if k not in ("envd_access_token", "traffic_access_token")
                }
                data = {**data, "metadata": md}
        return data

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "sandbox_id": "sb-abc123",
                "state": "running",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
                "metadata": {"purpose": "testing"},
                "container_id": "abc123...",
            }
        })


class SandboxLifecycleResponse(BaseModel):
    """Lightweight liveness: DB row state + runtime probe."""

    sandbox_id: str
    state: str
    running: bool
    timeout_seconds: Optional[int] = Field(
        default=None,
        description="Recorded lease / wall-clock budget from create (refreshed via POST …/timeout).",
    )
    lease_expires_at: Optional[str] = Field(
        default=None,
        description="Absolute UTC lease expiry timestamp used by the timeout reaper.",
    )


class SandboxTimeoutRefreshResponse(BaseModel):
    """Ack after refreshing stored sandbox timeout (E2B ``set_timeout`` parity)."""

    sandbox_id: str
    timeout_seconds: int
    refreshed: bool = Field(..., description="False if sandbox missing or not running")


class SnapshotRecordResponse(BaseModel):
    """One row from ``docker commit`` + SQLite ``sandbox_snapshots``."""

    snapshot_id: str
    source_sandbox_id: str
    image_ref: str
    label: str
    created_at: str


class TemplateDefinitionResponse(BaseModel):
    """Registered logical template (custom Docker warm path)."""

    template_id: str
    base_image: str
    env: Dict[str, str] = Field(default_factory=dict)
    start_cmd: str
    settle_seconds: int
    warm_snapshot_image: Optional[str] = None
    build_error: Optional[str] = None
    ready_cmd: str = Field(default="", description="Readiness probe shell (E2B-style); empty if unused.")
    created_at: str
    updated_at: str


class SandboxGuestConnectionResponse(BaseModel):
    """Data-plane URL for an arbitrary guest port (WebSocket or HTTP)."""

    sandbox_id: str
    guest_port: int = Field(..., description="Guest TCP port inside the sandbox")
    scheme: str = Field(..., description="``ws`` or ``http``")
    url: str = Field(..., description="Client URL via runtime-gateway + ingress")
    data_plane_host: str = Field(
        ...,
        description="Host authority for ``{port}-{sandbox_id}.{domain}`` routing",
    )
    traffic_access_token: str = Field(
        ...,
        description="Header ``e2b-traffic-access-token`` when ``allow_public_traffic`` is false",
    )


class SandboxE2bConnectionResponse(BaseModel):
    """Legacy SDK shape; ``agent_port`` is the requested guest port (not a server default)."""

    sandbox_id: str = Field(..., description="Logical sandbox id")
    agent_port: int = Field(..., description="Guest WebSocket port (from request, not a default)")
    ws_url: str = Field(..., description="Data-plane WebSocket URL via runtime-gateway")
    traffic_access_token: str = Field(..., description="Send as header ``e2b-traffic-access-token``")
    e2b_style_host: str = Field(..., description="Host authority for ingress routing")


class SandboxEnvdConnectionResponse(BaseModel):
    """Direct HTTP access to the in-guest envd-style daemon via sandbox ingress hostname."""

    sandbox_id: str
    sandbox_domain: str = Field(default="localhost")
    envd_port: int = Field(default=49983, description="In-container TCP port")
    http_base_url: str = Field(
        ...,
        description="Base URL (no trailing slash) via ``{envd_port}-{sandbox_id}.{sandbox_domain}`` ingress",
    )
    access_token: str = Field(..., description="Layer-2 header ``X-Access-Token`` for guest envd")
    traffic_access_token: Optional[str] = Field(
        default=None,
        description="Layer-3 header when ``allow_public_traffic`` is false",
    )


class CommandResponse(BaseModel):
    """Command execution response."""
    exit_code: int = Field(..., description="Exit code")
    stdout: str = Field(..., description="Standard output")
    stderr: str = Field(..., description="Standard error")
    pid: int = Field(..., description="Process ID")
    execution_time: float = Field(..., description="Execution time in seconds")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "exit_code": 0,
                "stdout": "Hello, World!",
                "stderr": "",
                "pid": 1234,
                "execution_time": 0.123
            }
        })


class FileEntryResponse(BaseModel):
    """File entry info."""
    path: str = Field(..., description="Full path")
    name: str = Field(..., description="File name")
    type: str = Field(..., description="Entry type (file/directory)")
    size: int = Field(..., description="File size in bytes")
    permissions: str = Field(..., description="Symbolic mode from ls (e.g. drwxr-xr-x)")
    modified_at: str = Field(default="", description="mtime columns from ls when available")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp/file.txt",
                "name": "file.txt",
                "type": "file",
                "size": 1024,
                "permissions": "-rw-r--r--",
                "modified_at": "Jun 4 12:00",
            }
        })


class ListFilesResponse(BaseModel):
    """List files response."""
    path: str = Field(..., description="Directory path")
    entries: List[FileEntryResponse] = Field(..., description="File entries")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp",
                "entries": []
            }
        })


class WriteFileResponse(BaseModel):
    """Write file response."""
    path: str = Field(..., description="File path")
    bytes_written: int = Field(..., description="Bytes written")
    success: bool = Field(..., description="Success status")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "path": "/tmp/file.txt",
                "bytes_written": 1024,
                "success": True
            }
        })


class AgentResponse(BaseModel):
    """Agent info response."""
    agent_id: str = Field(..., description="Agent ID")
    agent_name: str = Field(..., description="Agent name")
    state: str = Field(..., description="Agent state")
    created_at: str = Field(..., description="Creation timestamp")
    config: Optional[Dict[str, Any]] = Field(default={}, description="Agent config")
    last_heartbeat: Optional[str] = Field(default=None, description="Last heartbeat")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "agent_id": "agent-123",
                "agent_name": "echo_agent",
                "state": "running",
                "created_at": "2024-01-01T00:00:00Z",
                "config": {"debug": True},
                "last_heartbeat": "2024-01-01T00:05:00Z"
            }
        })


class AgentMessageResponse(BaseModel):
    """Agent message response."""
    agent_id: str = Field(..., description="Agent ID")
    message_id: str = Field(..., description="Message ID")
    message_type: str = Field(..., description="Message type")
    content: Dict[str, Any] = Field(..., description="Message content")
    timestamp: str = Field(..., description="Timestamp")
    processed: bool = Field(..., description="Processed status")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "agent_id": "agent-123",
                "message_id": "msg-456",
                "message_type": "task",
                "content": {"task": "analyze"},
                "timestamp": "2024-01-01T00:00:00Z",
                "processed": True
            }
        })


class ErrorResponse(BaseModel):
    """Error response."""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    status_code: int = Field(..., description="HTTP status code")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional details")

    model_config = ConfigDict(json_schema_extra={
            "example": {
                "error": "SandboxNotFoundException",
                "message": "Sandbox not found",
                "status_code": 404,
                "details": {"sandbox_id": "sb-123"}
            }
        })
