"""Detailed API reference definitions for the docs portal."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any


BASE_URL = "http://localhost:8000"


def field(
    name: str,
    type_: str,
    required: bool,
    description: str,
    *,
    default: str = "",
) -> dict[str, Any]:
    return {
        "name": name,
        "type": type_,
        "required": bool(required),
        "description": description,
        "default": default,
    }


def _schema_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        if not value:
            return "array"
        return f"array<{_schema_type(value[0])}>"
    if isinstance(value, dict):
        return "object"
    if value is None:
        return "null"
    return "any"


def _auto_response_fields(schema: Any) -> list[dict[str, Any]]:
    source = schema
    if isinstance(schema, list) and schema and isinstance(schema[0], dict):
        source = schema[0]
    if not isinstance(source, dict):
        return []
    return [
        field(name, _schema_type(value), True, f"Returned `{name}` value.")
        for name, value in source.items()
    ]


def response(
    code: str,
    title: str,
    description: str,
    schema: Any,
    fields: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "code": str(code),
        "title": title,
        "description": description,
        "schema": json.dumps(schema, indent=2, ensure_ascii=True),
        "fields": fields if fields is not None else _auto_response_fields(schema),
    }


CLIENT_AUTH = [
    field("X-API-Key", "string", True, "API key generated from the portal. Sent as a request header."),
    field(
        "Authorization",
        "string",
        False,
        "Bearer token issued by `/auth/token`. Bearer API keys also work when enabled by configuration.",
        default="Bearer <token>",
    ),
]

ADMIN_AUTH = [
    field("X-Admin-API-Key", "string", True, "Admin key configured on the API service. Sent as a request header."),
    field("Authorization", "string", False, "Admin bearer value accepted by admin observability endpoints.", default="Bearer <admin-key>"),
]

NO_AUTH: list[dict[str, Any]] = []

SANDBOX_ID = field("sandbox_id", "string", True, "Sandbox identifier returned by `POST /sandboxes`.")
AGENT_ID = field("agent_id", "string", True, "Agent identifier returned by `POST /sandboxes/{sandbox_id}/agents/spawn`.")
TEMPLATE_ID = field("template_id", "string", True, "Template id or alias visible to the authenticated client.")
SESSION_ID = field("session_id", "string", True, "Persistent process or PTY session id.")
COMMAND_ID = field("command_id", "string", True, "Command id returned by a session command operation.")
PORT = field("port", "integer", True, "Guest TCP port inside the sandbox.")
TOKEN = field("token", "string", True, "Signed preview or SSH token value.")


ERROR_FIELDS = [
    field("detail", "string | object | array", False, "FastAPI-style error detail. Validation errors may be arrays."),
    field("message", "string", False, "Normalized message when returned by compatibility helpers."),
    field("code", "integer", False, "Optional compatibility error code."),
]


def error_schema(detail: str) -> dict[str, Any]:
    return {"detail": detail}


def standard_errors(
    *,
    auth: bool = True,
    not_found: bool = True,
    bad_request: bool = True,
    forbidden: bool = False,
    unavailable: bool = False,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if bad_request:
        out.append(response("400", "Bad request", "The request body, path, query parameter, or runtime precondition is invalid.", error_schema("Invalid request"), ERROR_FIELDS))
    if auth:
        out.append(response("401", "Unauthorized", "The request is missing a valid API key or bearer token.", error_schema("X-API-Key or Authorization Bearer token required"), ERROR_FIELDS))
    if forbidden:
        out.append(response("403", "Forbidden", "The credential is valid but is not allowed to access the target resource.", error_schema("Client is disabled or access denied"), ERROR_FIELDS))
    if not_found:
        out.append(response("404", "Not found", "The sandbox, template, agent, file, session, or command does not exist for this client.", error_schema("Resource not found"), ERROR_FIELDS))
    if unavailable:
        out.append(response("503", "Unavailable", "The execution plane, data plane, envd, or runtime gateway is unavailable.", error_schema("Runtime service unavailable"), ERROR_FIELDS))
    out.append(response("500", "Server error", "Unexpected API-service or runtime failure.", error_schema("Internal server error"), ERROR_FIELDS))
    return out


SANDBOX_SCHEMA = {
    "sandbox_id": "sb-abc123",
    "state": "running",
    "created_at": "2026-07-16T17:30:00Z",
    "updated_at": "2026-07-16T17:30:00Z",
    "lease_expires_at": "2026-07-16T18:30:00Z",
    "metadata": {"purpose": "testing"},
    "container_id": "container-id",
    "gateway_instance_id": "runtime-gateway-0",
    "runtime": "docker",
    "sandbox_domain": "localhost",
    "envd_port": 49983,
    "envd_access_token": "envd-token-on-create",
    "traffic_access_token": "traffic-token-on-create",
    "allow_public_traffic": False,
}

SANDBOX_FIELDS = [
    field("sandbox_id", "string", True, "Unique sandbox id."),
    field("state", "string", True, "Current lifecycle state: running, paused, killed, or failed."),
    field("created_at", "string", True, "Creation timestamp."),
    field("updated_at", "string", True, "Last update timestamp."),
    field("lease_expires_at", "string | null", False, "Absolute UTC timeout/reaper deadline."),
    field("metadata", "object", False, "User metadata with internal secret keys stripped."),
    field("container_id", "string | null", False, "Runtime container id when available."),
    field("gateway_instance_id", "string | null", False, "Runtime-gateway shard owning the workload."),
    field("runtime", "string", True, "Runtime backend label such as docker or gvisor."),
    field("sandbox_domain", "string", True, "Domain suffix used for port hostnames."),
    field("envd_port", "integer", True, "In-guest envd HTTP port."),
    field("envd_access_token", "string | null", False, "Layer-2 envd token. Returned on create and envd connection calls."),
    field("traffic_access_token", "string | null", False, "Layer-3 ingress token for private sandbox traffic."),
    field("allow_public_traffic", "boolean", True, "Whether ingress traffic can skip the private token."),
]

TEMPLATE_SCHEMA = {
    "template_id": "fastapi-dev",
    "base_image": "python:3.11",
    "env": {"PYTHONUNBUFFERED": "1"},
    "start_cmd": "uvicorn app:app --host 0.0.0.0 --port 8000",
    "settle_seconds": 20,
    "ready_cmd": "python - <<'PY'\nPY",
    "warm_snapshot_image": "registry.local/fastapi-dev:latest",
    "registry_image_ref": "registry.example.com/templates/fastapi-dev:latest",
    "build_error": None,
    "created_at": "2026-07-16T17:30:00Z",
    "updated_at": "2026-07-16T17:30:00Z",
}

TEMPLATE_FIELDS = [
    field("template_id", "string", True, "Public template id or alias."),
    field("base_image", "string", True, "Base image or built image used for future sandboxes."),
    field("env", "object", True, "Default environment applied to template sandboxes."),
    field("start_cmd", "string", True, "Runtime start command."),
    field("settle_seconds", "integer", True, "Seconds to wait before committing a warm snapshot."),
    field("ready_cmd", "string", False, "Readiness probe shell command."),
    field("warm_snapshot_image", "string | null", False, "Local/runtime image used by warm pools."),
    field("registry_image_ref", "string | null", False, "Published registry image when configured."),
    field("build_error", "string | null", False, "Last build error if materialization failed."),
    field("created_at", "string", True, "Creation timestamp."),
    field("updated_at", "string", True, "Last update timestamp."),
]

COMMAND_SCHEMA = {
    "exit_code": 0,
    "stdout": "hello\n",
    "stderr": "",
    "pid": 1234,
    "execution_time": 0.12,
}

COMMAND_FIELDS = [
    field("exit_code", "integer", True, "Process exit code."),
    field("stdout", "string", True, "Captured stdout."),
    field("stderr", "string", True, "Captured stderr."),
    field("pid", "integer", True, "Runtime process id."),
    field("execution_time", "number", True, "Wall-clock execution time in seconds."),
]

OK_SCHEMA = {"success": True}
OK_FIELDS = [field("success", "boolean", True, "True when the operation completed.")]


def _body_json(body_example: Any | None) -> str:
    if body_example is None:
        return ""
    return json.dumps(body_example, indent=2, ensure_ascii=True)


def _curl(method: str, path: str, auth: list[dict[str, Any]], body_example: Any | None = None, query_example: str = "") -> str:
    url = f"{BASE_URL}{path}"
    if query_example:
        url = f"{url}?{query_example}"
    if method == "WS":
        parts = ["curl --include --no-buffer \\", f"  --url {url} \\", "  --header 'Connection: Upgrade' \\", "  --header 'Upgrade: websocket' \\"]
        if auth:
            parts.append("  --header 'X-API-Key: <api-key>'")
        else:
            parts[-1] = parts[-1].rstrip(" \\")
        return "\n".join(parts)
    parts = [f"curl --request {method} \\", f"  --url {url} \\"]
    header_name = "X-API-Key"
    if auth is ADMIN_AUTH:
        header_name = "X-Admin-API-Key"
    if auth:
        parts.append(f"  --header '{header_name}: <api-key>' \\")
    if body_example is not None:
        parts.append("  --header 'Content-Type: application/json' \\")
        parts.append("  --data '" + _body_json(body_example) + "'")
    else:
        parts[-1] = parts[-1].rstrip(" \\")
    return "\n".join(parts)


def api(
    group: str,
    slug: str,
    method: str,
    path: str,
    title: str,
    description: str,
    *,
    auth: list[dict[str, Any]] = CLIENT_AUTH,
    path_params: list[dict[str, Any]] | None = None,
    query_params: list[dict[str, Any]] | None = None,
    body_params: list[dict[str, Any]] | None = None,
    body_example: Any | None = None,
    query_example: str = "",
    success: dict[str, Any] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    success_response = success or response("200", "OK", "Request completed successfully.", OK_SCHEMA, OK_FIELDS)
    return {
        "group": group,
        "slug": slug,
        "dom_id": slug.replace("/", "-"),
        "method": method,
        "path": path,
        "title": title,
        "description": description,
        "auth": deepcopy(auth),
        "path_params": path_params or [],
        "query_params": query_params or [],
        "body_params": body_params or [],
        "body_example": _body_json(body_example),
        "curl": _curl(method, path, auth, body_example, query_example),
        "responses": [success_response, *(errors if errors is not None else standard_errors(auth=bool(auth)))],
        "href": f"/docs/api-reference/{slug}",
    }


CREATE_SANDBOX_BODY = [
    field("template_id", "string", False, "Container image ref or known template alias.", default="python:3.11"),
    field("metadata", "object", False, "Custom metadata. Use `guest_ports` to declare exposed guest ports."),
    field("env_vars", "object", False, "Environment variables injected into the sandbox container."),
    field("cpu_limit", "string", False, "CPU limit such as `1`, `0.5`, or `2`.", default="1"),
    field("memory_limit", "string", False, "Memory limit such as `512m` or `1g`.", default="512m"),
    field("timeout", "integer", False, "Sandbox timeout in seconds.", default="3600"),
    field("warmpool_size", "integer", False, "Desired warm-pool size for this template/resource segment."),
    field("from_snapshot_image", "string", False, "Snapshot image ref returned by the snapshot API. Skips warm-pool selection."),
]

RUN_COMMAND_BODY = [
    field("command", "string", True, "Command string to execute."),
    field("cwd", "string", False, "Working directory inside the sandbox.", default="/"),
    field("env", "object", False, "Environment variables for the process."),
    field("timeout", "number", False, "Command timeout in seconds.", default="30"),
    field("user", "string", False, "Optional user to run as."),
]

FILE_ENTRY_FIELDS = [
    field("path", "string", True, "Full path."),
    field("name", "string", True, "Base file name."),
    field("type", "string", True, "Entry type: file, directory, or symlink."),
    field("size", "integer", True, "File size in bytes."),
    field("permissions", "string", True, "Symbolic permissions."),
    field("modified_at", "string", False, "Modified time when available."),
]


MAIN_API_ENDPOINTS: list[dict[str, Any]] = [
    api(
        "System",
        "system/root",
        "GET",
        "/",
        "API root",
        "Return basic API service metadata and links to docs and OpenAPI.",
        auth=NO_AUTH,
        success=response("200", "Root metadata", "API service metadata.", {"message": "Sandbox API Server", "version": "0.1.0", "docs": "/docs", "openapi": "/openapi.json"}),
        errors=standard_errors(auth=False, bad_request=False, not_found=False),
    ),
    api(
        "System",
        "system/health",
        "GET",
        "/health",
        "Health check",
        "Cheap liveness check for probes. It does not depend on Docker, runtime-gateway, or database-heavy diagnostics.",
        auth=NO_AUTH,
        success=response("200", "Healthy", "The API process is alive.", {"status": "ok", "version": "0.1.0", "api_service_role": "control"}, [
            field("status", "string", True, "`ok` when the process is alive."),
            field("version", "string", True, "API version."),
            field("api_service_role", "string", True, "Configured API service role."),
        ]),
        errors=standard_errors(auth=False, bad_request=False, not_found=False),
    ),
    api(
        "System",
        "system/ready",
        "GET",
        "/ready",
        "Readiness check",
        "Cheap readiness check for service endpoints.",
        auth=NO_AUTH,
        success=response("200", "Ready", "The API process is ready.", {"status": "ready", "version": "0.1.0", "api_service_role": "control"}),
        errors=standard_errors(auth=False, bad_request=False, not_found=False),
    ),
    api(
        "System",
        "system/diagnostic-health",
        "GET",
        "/diagnostics/health",
        "Diagnostic health",
        "Human-oriented diagnostic payload for execution plane, warm-pool, and gateway state.",
        auth=NO_AUTH,
        success=response("200", "Diagnostic payload", "Detailed health and runtime diagnostics.", {
            "status": "ok",
            "sandbox_runtime": "docker",
            "execution_plane_ok": True,
            "warm_pool": {"enabled": True},
            "runtime_gateways": {"items": []},
        }),
        errors=standard_errors(auth=False, bad_request=False, not_found=False),
    ),
    api(
        "Auth",
        "auth/create-access-token",
        "POST",
        "/auth/token",
        "Create access token",
        "Exchange a valid API key or existing JWT for a short-lived JWT access token.",
        body_params=[field("ttl_seconds", "integer", False, "Requested token lifetime in seconds. Clamped by server config."), field("expires_in", "integer", False, "Alias for `ttl_seconds`.")],
        body_example={"ttl_seconds": 3600},
        success=response("200", "Token issued", "JWT access token response.", {
            "access_token": "eyJ...",
            "token_type": "Bearer",
            "expires_in": 3600,
            "expires_at": 1784220000,
            "issued_at": 1784216400,
            "jti": "jwt-id",
            "client_id": "client-id",
            "key_id": "key-id",
            "auth_type": "api_key",
        }),
    ),
    api(
        "Auth",
        "auth/get-current-principal",
        "GET",
        "/auth/me",
        "Get current principal",
        "Return the authenticated client and key identity.",
        success=response("200", "Current principal", "Authenticated credential metadata.", {
            "client_id": "client-id",
            "key_id": "key-id",
            "key_name": "local",
            "key_prefix": "sndbx_12",
            "email": "team@example.com",
            "display_name": "Team",
            "auth_type": "api_key",
            "token_id": "",
            "expires_at": None,
        }),
    ),
    api(
        "Sandboxes",
        "sandboxes/create-sandbox",
        "POST",
        "/sandboxes",
        "Create sandbox",
        "Create a sandbox from a template alias, image ref, or snapshot image.",
        body_params=CREATE_SANDBOX_BODY,
        body_example={"template_id": "python:3.11", "metadata": {"guest_ports": [8000]}, "env_vars": {"PYTHONUNBUFFERED": "1"}, "cpu_limit": "1", "memory_limit": "512m", "timeout": 3600, "warmpool_size": 1},
        success=response("201", "Created", "Sandbox was created successfully.", SANDBOX_SCHEMA, SANDBOX_FIELDS),
        errors=standard_errors(unavailable=True),
    ),
    api(
        "Sandboxes",
        "sandboxes/list-sandboxes",
        "GET",
        "/sandboxes",
        "List sandboxes",
        "List sandboxes visible to the authenticated client.",
        query_params=[field("limit", "integer", False, "Maximum number of rows.", default="100"), field("offset", "integer", False, "Rows to skip.", default="0")],
        query_example="limit=100&offset=0",
        success=response("200", "Sandbox list", "Array of sandbox rows.", [SANDBOX_SCHEMA], SANDBOX_FIELDS),
    ),
    api("Sandboxes", "sandboxes/get-sandbox", "GET", "/sandboxes/{sandbox_id}", "Get sandbox", "Return one sandbox row without create-only secret fields.", path_params=[SANDBOX_ID], success=response("200", "Sandbox", "Sandbox details.", SANDBOX_SCHEMA, SANDBOX_FIELDS)),
    api("Sandboxes", "sandboxes/get-sandbox-status", "GET", "/sandboxes/{sandbox_id}/status", "Get sandbox status", "Return DB lifecycle state plus runtime liveness.", path_params=[SANDBOX_ID], success=response("200", "Lifecycle", "Lifecycle state response.", {"sandbox_id": "sb-abc123", "state": "running", "running": True, "timeout_seconds": 3600, "lease_expires_at": "2026-07-16T18:30:00Z"})),
    api("Sandboxes", "sandboxes/set-sandbox-timeout", "POST", "/sandboxes/{sandbox_id}/timeout", "Set sandbox timeout", "Refresh the stored sandbox lease.", path_params=[SANDBOX_ID], body_params=[field("timeout_seconds", "integer", True, "New lease length in seconds. Alias: `timeout`."), field("timeout", "integer", False, "Alias for `timeout_seconds`.")], body_example={"timeout_seconds": 7200}, success=response("200", "Timeout refreshed", "Timeout refresh acknowledgement.", {"sandbox_id": "sb-abc123", "timeout_seconds": 7200, "refreshed": True})),
    api("Sandboxes", "sandboxes/set-warm-pool-size", "POST", "/sandboxes/{sandbox_id}/warm-pool/size", "Set warm-pool size", "Set desired warm-pool size for the template/cpu/memory segment derived from this sandbox.", path_params=[SANDBOX_ID], body_params=[field("warmpool_size", "integer", True, "Desired segment size. Aliases: `warm_pool_size`, `warmPoolSize`.")], body_example={"warmpool_size": 1}, success=response("200", "Warm pool resized", "Warm-pool desired size update acknowledgement.", {"sandbox_id": "sb-abc123", "warm_pool_key": "python:3.11|1|512m", "previous_desired_size": 2, "desired_size": 1, "ready_count": 1, "updated": True})),
    api("Sandboxes", "sandboxes/kill-sandbox", "POST", "/sandboxes/{sandbox_id}/kill", "Kill sandbox", "Terminate and mark a sandbox killed.", path_params=[SANDBOX_ID], success=response("200", "Killed", "Sandbox was killed.", {"success": True, "sandbox_id": "sb-abc123"})),
    api("Sandboxes", "sandboxes/pause-sandbox", "POST", "/sandboxes/{sandbox_id}/pause", "Pause sandbox", "Pause a running sandbox workload.", path_params=[SANDBOX_ID], success=response("200", "Paused", "Sandbox was paused.", {"success": True, "sandbox_id": "sb-abc123"})),
    api("Sandboxes", "sandboxes/resume-sandbox", "POST", "/sandboxes/{sandbox_id}/resume", "Resume sandbox", "Resume a paused sandbox workload.", path_params=[SANDBOX_ID], success=response("200", "Resumed", "Sandbox was resumed.", {"success": True, "sandbox_id": "sb-abc123"})),
    api("Sandboxes", "sandboxes/get-sandbox-metrics", "GET", "/sandboxes/{sandbox_id}/metrics", "Get sandbox metrics", "Return runtime metrics for one sandbox.", path_params=[SANDBOX_ID], success=response("200", "Metrics", "Sandbox metrics payload.", {"cpu_percent": 3.2, "memory_bytes": 104857600, "memory_limit_bytes": 536870912, "runtime": "docker"})),
    api("Sandboxes", "sandboxes/create-snapshot", "POST", "/sandboxes/{sandbox_id}/snapshot", "Create snapshot", "Persist the sandbox writable layer with Docker commit.", path_params=[SANDBOX_ID], body_params=[field("label", "string", False, "Human-readable snapshot label.")], body_example={"label": "after-install"}, success=response("200", "Snapshot created", "Filesystem snapshot record.", {"snapshot_id": "snap-123", "source_sandbox_id": "sb-abc123", "image_ref": "snapshot-image:tag", "label": "after-install", "created_at": "2026-07-16T17:30:00Z"})),
    api("Sandboxes", "sandboxes/list-snapshots", "GET", "/sandboxes/{sandbox_id}/snapshots", "List snapshots", "List filesystem snapshots recorded for this sandbox.", path_params=[SANDBOX_ID], query_params=[field("limit", "integer", False, "Maximum number of snapshots.", default="50")], query_example="limit=50", success=response("200", "Snapshot list", "Snapshot records.", [{"snapshot_id": "snap-123", "source_sandbox_id": "sb-abc123", "image_ref": "snapshot-image:tag", "label": "after-install", "created_at": "2026-07-16T17:30:00Z"}])),
    api("Commands", "commands/run-command", "POST", "/sandboxes/{sandbox_id}/commands/run", "Run command", "Run a command inside the sandbox and wait for completion.", path_params=[SANDBOX_ID], body_params=RUN_COMMAND_BODY, body_example={"command": "python --version", "cwd": "/", "env": {}, "timeout": 30}, success=response("200", "Command result", "Completed command result.", COMMAND_SCHEMA, COMMAND_FIELDS)),
    api("Commands", "commands/stream-command", "POST", "/sandboxes/{sandbox_id}/commands/run/stream", "Stream command", "Run a command and stream Server-Sent Events for stdout, stderr, errors, and final exit.", path_params=[SANDBOX_ID], body_params=RUN_COMMAND_BODY, body_example={"command": "for i in 1 2 3; do echo $i; sleep 1; done", "timeout": 30}, success=response("200", "SSE stream", "Text/event-stream response.", {"type": "stdout", "chunk": "hello\n"})),
    api("Commands", "commands/list-commands", "GET", "/sandboxes/{sandbox_id}/commands", "List commands", "Return command history for a sandbox.", path_params=[SANDBOX_ID], query_params=[field("limit", "integer", False, "Maximum history rows.", default="100")], query_example="limit=100", success=response("200", "Command history", "Command history payload.", {"commands": [COMMAND_SCHEMA]})),
    api("Filesystem", "filesystem/list-files", "GET", "/sandboxes/{sandbox_id}/files", "List files", "List entries in a sandbox directory.", path_params=[SANDBOX_ID], query_params=[field("path", "string", False, "Directory path.", default="/")], query_example="path=/tmp", success=response("200", "Directory listing", "Directory entries.", {"path": "/tmp", "entries": [{"path": "/tmp/app.py", "name": "app.py", "type": "file", "size": 32, "permissions": "-rw-r--r--", "modified_at": "Jul 16 17:30"}]}, FILE_ENTRY_FIELDS)),
    api("Filesystem", "filesystem/read-file", "GET", "/sandboxes/{sandbox_id}/files/read", "Read file", "Read a text file from the sandbox.", path_params=[SANDBOX_ID], query_params=[field("path", "string", True, "File path to read.")], query_example="path=/tmp/app.py", success=response("200", "File content", "Text file content.", {"path": "/tmp/app.py", "content": "print('hello')"})),
    api("Filesystem", "filesystem/write-file", "POST", "/sandboxes/{sandbox_id}/files/write", "Write file", "Write text content to a sandbox file.", path_params=[SANDBOX_ID], body_params=[field("path", "string", True, "Target path."), field("content", "string", True, "File content."), field("encoding", "string", False, "Content encoding.", default="utf-8")], body_example={"path": "/tmp/app.py", "content": "print('hello')", "encoding": "utf-8"}, success=response("200", "File written", "Write acknowledgement.", {"path": "/tmp/app.py", "bytes_written": 14, "success": True})),
    api("Filesystem", "filesystem/delete-file", "POST", "/sandboxes/{sandbox_id}/files/delete", "Delete file", "Delete a file or directory.", path_params=[SANDBOX_ID], body_params=[field("path", "string", True, "Path to delete."), field("recursive", "boolean", False, "Delete recursively.", default="false")], body_example={"path": "/tmp/app.py", "recursive": False}, success=response("200", "Deleted", "Delete acknowledgement.", {"success": True, "path": "/tmp/app.py"})),
    api("Filesystem", "filesystem/create-directory", "POST", "/sandboxes/{sandbox_id}/files/mkdir", "Create directory", "Create a directory inside the sandbox.", path_params=[SANDBOX_ID], body_params=[field("path", "string", True, "Directory path."), field("mode", "integer", False, "Unix mode as decimal integer.", default="493")], body_example={"path": "/tmp/work", "mode": 493}, success=response("200", "Directory created", "Directory creation acknowledgement.", {"success": True, "path": "/tmp/work"})),
]


def _add_filesystem_extensions() -> None:
    specs = [
        ("filesystem/get-file-info", "GET", "/sandboxes/{sandbox_id}/files/info", "Get file info", "Return metadata for one sandbox path.", [field("path", "string", True, "Path to inspect.")], None, "path=/tmp/app.py", {"path": "/tmp/app.py", "type": "file", "size": 14, "permissions": "-rw-r--r--"}),
        ("filesystem/download-file", "GET", "/sandboxes/{sandbox_id}/files/download", "Download file", "Download raw file bytes.", [field("path", "string", True, "Path to download.")], None, "path=/tmp/app.py", {"binary": "<file bytes>"}),
        ("filesystem/upload-file", "POST", "/sandboxes/{sandbox_id}/files/upload", "Upload file", "Upload raw or multipart file content.", [field("path", "string", True, "Target path.")], {"path": "/tmp/upload.bin", "content_base64": "<base64>"}, "", {"path": "/tmp/upload.bin", "bytes_written": 123, "success": True}),
        ("filesystem/bulk-download", "POST", "/sandboxes/{sandbox_id}/files/bulk-download", "Bulk download", "Download several files in one operation.", [], {"paths": ["/tmp/a.txt", "/tmp/b.txt"]}, "", {"archive_base64": "<base64-tar-gzip>"}),
        ("filesystem/bulk-upload", "POST", "/sandboxes/{sandbox_id}/files/bulk-upload", "Bulk upload", "Upload several files in one operation.", [], {"files": {"/tmp/a.txt": "hello"}}, "", {"success": True, "files_written": 1}),
        ("filesystem/move-file", "POST", "/sandboxes/{sandbox_id}/files/move", "Move file", "Move or rename a sandbox path.", [], {"source": "/tmp/a.txt", "destination": "/tmp/b.txt"}, "", {"success": True}),
        ("filesystem/set-permissions", "POST", "/sandboxes/{sandbox_id}/files/permissions", "Set permissions", "Set Unix permissions on a path.", [], {"path": "/tmp/app.py", "mode": "0644"}, "", {"success": True}),
        ("filesystem/search-files", "GET", "/sandboxes/{sandbox_id}/files/search", "Search files", "Find paths by filename pattern.", [field("path", "string", True, "Root path."), field("pattern", "string", True, "Search pattern.")], None, "path=/workspace&pattern=*.py", {"matches": ["/workspace/app.py"]}),
        ("filesystem/find-in-files", "GET", "/sandboxes/{sandbox_id}/files/find", "Find in files", "Find text matches inside files.", [field("path", "string", True, "Root path."), field("pattern", "string", True, "Text or regex pattern.")], None, "path=/workspace&pattern=TODO", {"matches": [{"path": "/workspace/app.py", "line": 10, "text": "TODO"}]}),
        ("filesystem/replace-in-files", "POST", "/sandboxes/{sandbox_id}/files/replace", "Replace in files", "Replace text across selected files.", [], {"files": ["/workspace/app.py"], "pattern": "old", "new_value": "new"}, "", {"results": [{"path": "/workspace/app.py", "replacements": 2}]}),
    ]
    for slug, method, path, title, desc, query, body, query_example, schema in specs:
        MAIN_API_ENDPOINTS.append(
            api(
                "Filesystem",
                slug,
                method,
                path,
                title,
                desc,
                path_params=[SANDBOX_ID],
                query_params=query if method == "GET" else [],
                body_params=[] if body is None else [field(k, "any", True, f"`{k}` request value.") for k in body.keys()],
                body_example=body,
                query_example=query_example,
                success=response("200", "OK", "Operation-specific filesystem response.", schema),
            )
        )


_add_filesystem_extensions()


MAIN_API_ENDPOINTS.extend(
    [
        api("Templates", "templates/create-template", "POST", "/templates", "Create template", "Register or update a logical template.", body_params=[
            field("template_id", "string", True, "Logical id/alias for the template."),
            field("base_image", "string", True, "Base Docker image."),
            field("env", "object", False, "Default environment."),
            field("start_cmd", "string", False, "Build/start command."),
            field("settle_seconds", "integer", False, "Seconds to settle before commit.", default="20"),
            field("ready_cmd", "string", False, "Readiness probe command."),
            field("warm_snapshot_image", "string", False, "Prebuilt image ref to use directly."),
        ], body_example={"template_id": "fastapi-dev", "base_image": "python:3.11", "env": {"PYTHONUNBUFFERED": "1"}, "start_cmd": "pip install fastapi", "settle_seconds": 20}, success=response("200", "Template", "Registered template definition.", TEMPLATE_SCHEMA, TEMPLATE_FIELDS)),
        api("Templates", "templates/create-template-from-dockerfile", "POST", "/templates/from-dockerfile", "Create template from Dockerfile", "Build/register a template from Dockerfile text.", body_params=[
            field("template_id", "string", True, "Logical id/alias for the template."),
            field("dockerfile", "string", True, "Full Dockerfile content."),
            field("image_tag", "string", False, "Requested image tag."),
            field("build_args", "object", False, "Docker build args."),
            field("context_tar_gzip_base64", "string", False, "Base64 gzip tar build context."),
            field("env", "object", False, "Runtime environment."),
            field("start_cmd", "string", False, "Post-build start command."),
            field("ready_cmd", "string", False, "Readiness probe command."),
            field("settle_seconds", "integer", False, "Settle seconds.", default="20"),
        ], body_example={"template_id": "fastapi-dev", "dockerfile": "FROM python:3.11\nRUN pip install fastapi", "settle_seconds": 20}, success=response("200", "Template", "Built template definition.", TEMPLATE_SCHEMA, TEMPLATE_FIELDS), errors=standard_errors(unavailable=True)),
        api("Templates", "templates/stream-template-from-dockerfile", "POST", "/templates/from-dockerfile/stream", "Stream template build", "Build/register a Dockerfile template and stream build events over SSE.", body_params=[field("template_id", "string", True, "Logical id/alias."), field("dockerfile", "string", True, "Dockerfile content.")], body_example={"template_id": "fastapi-dev", "dockerfile": "FROM python:3.11"}, success=response("200", "SSE stream", "Build log/status events.", {"type": "log", "line": "Step 1/5 ..."}), errors=standard_errors(unavailable=True)),
        api("Templates", "templates/list-templates", "GET", "/templates", "List templates", "List templates visible to the authenticated client.", success=response("200", "Template list", "Array of template definitions.", [TEMPLATE_SCHEMA], TEMPLATE_FIELDS)),
        api("Templates", "templates/get-template", "GET", "/templates/{template_id}", "Get template", "Get one template by id or alias.", path_params=[TEMPLATE_ID], success=response("200", "Template", "Template definition.", TEMPLATE_SCHEMA, TEMPLATE_FIELDS)),
    ]
)


CONNECTION_SUCCESS = response("200", "Connection info", "Data-plane connection information.", {
    "sandbox_id": "sb-abc123",
    "guest_port": 8000,
    "scheme": "http",
    "url": "https://8000-sb-abc123.localhost",
    "data_plane_host": "8000-sb-abc123.localhost",
    "traffic_access_token": "traffic-token",
})

MAIN_API_ENDPOINTS.extend(
    [
        api("Connections", "connections/get-guest-connection", "GET", "/sandboxes/{sandbox_id}/connection", "Get guest connection", "Return data-plane URL and token for any declared guest port.", path_params=[SANDBOX_ID], query_params=[field("port", "integer", True, "Guest TCP port.", default="8000"), field("scheme", "string", False, "`ws` or `http`.", default="ws")], query_example="port=8000&scheme=http", success=CONNECTION_SUCCESS, errors=standard_errors(unavailable=True)),
        api("Connections", "connections/get-e2b-connection", "GET", "/sandboxes/{sandbox_id}/e2b-connection", "Get E2B connection", "Return legacy E2B-shaped WebSocket connection metadata.", path_params=[SANDBOX_ID], query_params=[field("port", "integer", True, "Guest WebSocket port.")], query_example="port=8765", success=response("200", "E2B connection", "E2B-compatible connection payload.", {"sandbox_id": "sb-abc123", "agent_port": 8765, "ws_url": "wss://8765-sb-abc123.localhost/", "traffic_access_token": "traffic-token", "e2b_style_host": "8765-sb-abc123.localhost"}), errors=standard_errors(unavailable=True)),
        api("Connections", "connections/get-envd-connection", "GET", "/sandboxes/{sandbox_id}/envd-connection", "Get envd connection", "Return envd HTTP base URL and access tokens.", path_params=[SANDBOX_ID], success=response("200", "Envd connection", "Direct envd HTTP connection metadata.", {"sandbox_id": "sb-abc123", "sandbox_domain": "localhost", "envd_port": 49983, "http_base_url": "https://49983-sb-abc123.localhost", "access_token": "envd-token", "traffic_access_token": "traffic-token"}), errors=standard_errors(unavailable=True)),
        api("Connections", "connections/set-labels", "PUT", "/sandboxes/{sandbox_id}/labels", "Set labels", "Replace labels for a sandbox.", path_params=[SANDBOX_ID], body_params=[field("labels", "object", True, "Label key/value object.")], body_example={"labels": {"env": "dev"}}, success=response("200", "Labels", "Updated labels payload.", {"sandbox_id": "sb-abc123", "labels": {"env": "dev"}})),
        api("Connections", "connections/update-network-settings", "POST", "/sandboxes/{sandbox_id}/network-settings", "Update network settings", "Update sandbox networking options.", path_params=[SANDBOX_ID], body_params=[field("settings", "object", True, "Network settings object.")], body_example={"allow_public_traffic": False}, success=response("200", "Network settings", "Updated network settings.", {"sandbox_id": "sb-abc123", "network_settings": {"allow_public_traffic": False}})),
        api("Connections", "connections/set-public-access", "POST", "/sandboxes/{sandbox_id}/public/{is_public}", "Set public access", "Toggle public ingress traffic for a sandbox.", path_params=[SANDBOX_ID, field("is_public", "boolean", True, "True to allow public traffic.")], success=response("200", "Public access", "Public access state.", {"sandbox_id": "sb-abc123", "allow_public_traffic": True})),
        api("Connections", "connections/get-preview-url", "GET", "/sandboxes/{sandbox_id}/ports/{port}/preview-url", "Get preview URL", "Return a browser preview URL for a guest port.", path_params=[SANDBOX_ID, PORT], success=response("200", "Preview URL", "Preview URL payload.", {"url": "https://8000-sb-abc123.localhost"})),
        api("Connections", "connections/get-signed-preview-url", "GET", "/sandboxes/{sandbox_id}/ports/{port}/signed-preview-url", "Get signed preview URL", "Return a tokenized preview URL.", path_params=[SANDBOX_ID, PORT], query_params=[field("expiresInSeconds", "integer", False, "Signed URL lifetime.")], query_example="expiresInSeconds=3600", success=response("200", "Signed URL", "Signed preview URL payload.", {"url": "https://8000-sb-abc123.localhost?token=...", "token": "signed-token", "expires_at": "2026-07-16T18:30:00Z"})),
        api("Connections", "connections/expire-signed-preview-url", "POST", "/sandboxes/{sandbox_id}/ports/{port}/signed-preview-url/{token}/expire", "Expire signed preview URL", "Expire one signed preview token.", path_params=[SANDBOX_ID, PORT, TOKEN], success=response("200", "Expired", "Token expiration acknowledgement.", {"success": True})),
        api("Connections", "connections/create-ssh-access", "POST", "/sandboxes/{sandbox_id}/ssh-access", "Create SSH access", "Create SSH access credentials for a sandbox.", path_params=[SANDBOX_ID], query_params=[field("expiresInMinutes", "number", False, "Credential lifetime in minutes.")], query_example="expiresInMinutes=60", success=response("200", "SSH access", "SSH access payload.", {"token": "ssh-token", "command": "ssh ...", "expires_at": "2026-07-16T18:30:00Z"})),
        api("Connections", "connections/revoke-ssh-access", "DELETE", "/sandboxes/{sandbox_id}/ssh-access", "Revoke SSH access", "Revoke SSH access credentials.", path_params=[SANDBOX_ID], query_params=[field("token", "string", False, "Specific token to revoke.")], query_example="token=ssh-token", success=response("200", "Revoked", "Revocation acknowledgement.", {"success": True})),
        api("Connections", "connections/validate-ssh-access", "GET", "/sandboxes/ssh-access/validate", "Validate SSH access", "Validate an SSH token.", query_params=[field("token", "string", True, "SSH token to validate.")], query_example="token=ssh-token", success=response("200", "Validation", "SSH token validation result.", {"valid": True, "sandbox_id": "sb-abc123"})),
    ]
)


MAIN_API_ENDPOINTS.extend(
    [
        api("Agents", "agents/spawn-agent", "POST", "/sandboxes/{sandbox_id}/agents/spawn", "Spawn agent", "Spawn an agent process associated with a sandbox.", path_params=[SANDBOX_ID], body_params=[field("agent_name", "string", True, "Agent name/type."), field("agent_code", "string", False, "Python agent code."), field("config", "object", False, "Agent configuration."), field("auto_start", "boolean", False, "Whether to start immediately.", default="true")], body_example={"agent_name": "build_loop_demo", "config": {"single_run": True}, "auto_start": True}, success=response("200", "Agent", "Created agent.", {"agent_id": "agent-123", "agent_name": "build_loop_demo", "state": "running", "created_at": "2026-07-16T17:30:00Z", "config": {"single_run": True}})),
        api("Agents", "agents/list-agents", "GET", "/sandboxes/{sandbox_id}/agents", "List agents", "List agents attached to a sandbox.", path_params=[SANDBOX_ID], success=response("200", "Agent list", "Agents payload.", {"agents": [{"agent_id": "agent-123", "agent_name": "build_loop_demo", "state": "running"}]})),
        api("Agents", "agents/get-agent", "GET", "/sandboxes/{sandbox_id}/agents/{agent_id}", "Get agent", "Return one agent status.", path_params=[SANDBOX_ID, AGENT_ID], success=response("200", "Agent", "Agent status.", {"agent_id": "agent-123", "agent_name": "build_loop_demo", "state": "running", "created_at": "", "config": {}})),
        api("Agents", "agents/kill-agent", "POST", "/sandboxes/{sandbox_id}/agents/{agent_id}/kill", "Kill agent", "Stop an agent.", path_params=[SANDBOX_ID, AGENT_ID], body_params=[field("force", "boolean", False, "Force kill.", default="false")], body_example={"force": False}, success=response("200", "Killed", "Agent kill acknowledgement.", {"success": True, "agent_id": "agent-123"})),
        api("Agents", "agents/send-agent-message", "POST", "/sandboxes/{sandbox_id}/agents/{agent_id}/messages", "Send agent message", "Send a message to an agent.", path_params=[SANDBOX_ID, AGENT_ID], body_params=[field("message_type", "string", True, "Message type."), field("content", "string | object", True, "Message content.")], body_example={"message_type": "user", "content": "start"}, success=response("200", "Sent", "Message send acknowledgement.", {"success": True, "agent_id": "agent-123"})),
        api("Agents", "agents/list-agent-messages", "GET", "/sandboxes/{sandbox_id}/agents/{agent_id}/messages", "List agent messages", "Return recent agent messages.", path_params=[SANDBOX_ID, AGENT_ID], query_params=[field("limit", "integer", False, "Maximum messages.", default="100")], query_example="limit=100", success=response("200", "Messages", "Agent message history.", {"agent_id": "agent-123", "messages": [{"message_type": "user", "content": "start"}]})),
    ]
)


def _toolbox_body(names: list[str]) -> list[dict[str, Any]]:
    return [field(name, "any", True, f"`{name}` request value.") for name in names]


TOOLBOX_SPECS = [
    ("Toolbox Process", "toolbox/process-execute", "POST", "/sandboxes/{sandbox_id}/process/execute", "Execute process", ["command", "cwd", "env", "timeout"], {"command": "python --version"}, {"exit_code": 0, "stdout": "Python 3.11\n", "stderr": ""}),
    ("Toolbox Process", "toolbox/process-code-run", "POST", "/sandboxes/{sandbox_id}/process/code-run", "Run code", ["code", "language"], {"code": "print('hi')", "language": "python"}, {"exit_code": 0, "stdout": "hi\n"}),
    ("Toolbox Process", "toolbox/create-process-session", "POST", "/sandboxes/{sandbox_id}/process/sessions", "Create process session", ["command", "cwd", "env"], {"command": "bash", "cwd": "/workspace"}, {"session_id": "sess-123", "state": "running"}),
    ("Toolbox Process", "toolbox/list-process-sessions", "GET", "/sandboxes/{sandbox_id}/process/sessions", "List process sessions", [], None, {"sessions": []}),
    ("Toolbox Process", "toolbox/get-entrypoint-session", "GET", "/sandboxes/{sandbox_id}/process/entrypoint", "Get entrypoint session", [], None, {"session_id": "entrypoint", "state": "running"}),
    ("Toolbox Process", "toolbox/get-entrypoint-logs", "GET", "/sandboxes/{sandbox_id}/process/entrypoint/logs", "Get entrypoint logs", [], None, {"stdout": "", "stderr": ""}),
    ("Toolbox Process", "toolbox/get-process-session", "GET", "/sandboxes/{sandbox_id}/process/sessions/{session_id}", "Get process session", [], None, {"session_id": "sess-123", "state": "running"}),
    ("Toolbox Process", "toolbox/delete-process-session", "DELETE", "/sandboxes/{sandbox_id}/process/sessions/{session_id}", "Delete process session", [], None, {"success": True}),
    ("Toolbox Process", "toolbox/execute-session-command", "POST", "/sandboxes/{sandbox_id}/process/sessions/{session_id}/commands", "Execute session command", ["command", "timeout"], {"command": "ls -la"}, {"command_id": "cmd-123", "state": "running"}),
    ("Toolbox Process", "toolbox/get-session-command", "GET", "/sandboxes/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}", "Get session command", [], None, {"command_id": "cmd-123", "state": "completed", "exit_code": 0}),
    ("Toolbox Process", "toolbox/get-session-command-logs", "GET", "/sandboxes/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}/logs", "Get session command logs", [], None, {"stdout": "output", "stderr": ""}),
    ("Toolbox Process", "toolbox/send-session-command-input", "POST", "/sandboxes/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}/input", "Send session command input", ["data"], {"data": "y\n"}, {"success": True}),
    ("Toolbox PTY", "toolbox/create-pty-session", "POST", "/sandboxes/{sandbox_id}/pty/sessions", "Create PTY session", ["command", "cwd", "rows", "cols"], {"command": "bash", "rows": 24, "cols": 80}, {"session_id": "pty-123", "state": "running"}),
    ("Toolbox PTY", "toolbox/list-pty-sessions", "GET", "/sandboxes/{sandbox_id}/pty/sessions", "List PTY sessions", [], None, {"sessions": []}),
    ("Toolbox PTY", "toolbox/get-pty-session", "GET", "/sandboxes/{sandbox_id}/pty/sessions/{session_id}", "Get PTY session", [], None, {"session_id": "pty-123", "state": "running"}),
    ("Toolbox PTY", "toolbox/delete-pty-session", "DELETE", "/sandboxes/{sandbox_id}/pty/sessions/{session_id}", "Delete PTY session", [], None, {"success": True}),
    ("Toolbox PTY", "toolbox/resize-pty-session", "POST", "/sandboxes/{sandbox_id}/pty/sessions/{session_id}/resize", "Resize PTY session", ["rows", "cols"], {"rows": 30, "cols": 120}, {"session_id": "pty-123", "rows": 30, "cols": 120}),
    ("Toolbox PTY", "toolbox/connect-pty-session", "WS", "/sandboxes/{sandbox_id}/pty/sessions/{session_id}/connect", "Connect PTY session", [], None, {"event": "websocket stream"}),
]

for group, slug, method, path, title, body_names, body, schema in TOOLBOX_SPECS:
    params = [SANDBOX_ID]
    if "{session_id}" in path:
        params.append(SESSION_ID)
    if "{command_id}" in path:
        params.append(COMMAND_ID)
    MAIN_API_ENDPOINTS.append(
        api(
            group,
            slug,
            method,
            path,
            title,
            f"{title} through the generic sandbox toolbox API.",
            path_params=params,
            body_params=_toolbox_body(body_names) if body else [],
            body_example=body,
            success=response("101" if method == "WS" else ("201" if "create-" in slug else "200"), "OK", "Toolbox operation response.", schema),
        )
    )


GIT_SPECS = [
    ("git/init", "POST", "Initialize git repository", ["path", "bare", "initial_branch"], {"path": "/workspace"}),
    ("git/clone", "POST", "Clone repository", ["url", "path", "branch"], {"url": "https://github.com/org/repo.git", "path": "/workspace"}),
    ("git/status", "GET", "Get git status", [], None),
    ("git/branches", "GET", "List git branches", [], None),
    ("git/branches", "POST", "Create git branch", ["path", "name"], {"path": "/workspace", "name": "feature"}),
    ("git/branches", "DELETE", "Delete git branch", ["path", "name"], {"path": "/workspace", "name": "feature"}),
    ("git/checkout", "POST", "Checkout git branch", ["path", "branch"], {"path": "/workspace", "branch": "main"}),
    ("git/add", "POST", "Git add", ["path", "files"], {"path": "/workspace", "files": ["app.py"]}),
    ("git/commit", "POST", "Git commit", ["path", "message", "author_name", "author_email"], {"path": "/workspace", "message": "update"}),
    ("git/pull", "POST", "Git pull", ["path", "remote", "branch"], {"path": "/workspace", "remote": "origin", "branch": "main"}),
    ("git/push", "POST", "Git push", ["path", "remote", "branch"], {"path": "/workspace", "remote": "origin", "branch": "main"}),
    ("git/reset", "POST", "Git reset", ["path", "mode", "target"], {"path": "/workspace", "mode": "hard", "target": "HEAD"}),
    ("git/restore", "POST", "Git restore", ["path", "files"], {"path": "/workspace", "files": ["app.py"]}),
    ("git/remotes", "GET", "List git remotes", [], None),
    ("git/remotes", "POST", "Add git remote", ["path", "name", "url"], {"path": "/workspace", "name": "origin", "url": "https://github.com/org/repo.git"}),
    ("git/config", "GET", "Get git config", [], None),
    ("git/config", "POST", "Set git config", ["key", "value", "scope", "path"], {"key": "user.name", "value": "SNDBX"}),
    ("git/config/user", "POST", "Configure git user", ["name", "email", "scope", "path"], {"name": "SNDBX", "email": "team@example.com"}),
    ("git/credentials", "POST", "Set git credentials", ["username", "password", "host"], {"username": "token", "password": "<secret>", "host": "github.com"}),
    ("git/history", "GET", "Get git history", [], None),
]

for route, method, title, body_names, body in GIT_SPECS:
    route_slug = title.lower().replace(" ", "-")
    MAIN_API_ENDPOINTS.append(
        api(
            "Toolbox Git",
            f"toolbox/{route_slug}",
            method,
            f"/sandboxes/{{sandbox_id}}/{route}",
            title,
            f"{title} inside a sandbox repository.",
            path_params=[SANDBOX_ID],
            body_params=_toolbox_body(body_names) if body else [],
            body_example=body,
            success=response("200", "Git response", "Operation-specific git result.", {"success": True, "result": {}}),
        )
    )

MAIN_API_ENDPOINTS.extend(
    [
        api("Toolbox System", "toolbox/list-ports", "GET", "/sandboxes/{sandbox_id}/ports", "List ports", "List detected/listening sandbox ports.", path_params=[SANDBOX_ID], success=response("200", "Ports", "Port list payload.", {"ports": [{"port": 8000, "in_use": True}]})),
        api("Toolbox System", "toolbox/check-port-in-use", "GET", "/sandboxes/{sandbox_id}/ports/{port}/in-use", "Check port in use", "Check whether a guest port is listening.", path_params=[SANDBOX_ID, PORT], success=response("200", "Port status", "Port status payload.", {"port": 8000, "in_use": True})),
        api("Toolbox System", "toolbox/get-system-metrics", "GET", "/sandboxes/{sandbox_id}/system/metrics", "Get system metrics", "Return sandbox system metrics.", path_params=[SANDBOX_ID], success=response("200", "System metrics", "System metrics payload.", {"cpu_percent": 4.5, "memory": {"used_bytes": 104857600}})),
    ]
)


ADMIN_EVENT_QUERY = [
    field("limit", "integer", False, "Maximum events.", default="100"),
    field("offset", "integer", False, "Rows to skip.", default="0"),
    field("severity", "string", False, "Filter by severity."),
    field("category", "string", False, "Filter by category."),
    field("action", "string", False, "Filter by action."),
    field("entity_type", "string", False, "Filter by entity type."),
    field("entity_id", "string", False, "Filter by entity id."),
    field("gateway_instance_id", "string", False, "Filter by gateway."),
    field("template_id", "string", False, "Filter by template."),
    field("sandbox_id", "string", False, "Filter by sandbox."),
    field("since", "string", False, "Lower time bound."),
    field("until", "string", False, "Upper time bound."),
]

MAIN_API_ENDPOINTS.extend(
    [
        api("Admin Observability", "observability/get-summary", "GET", "/admin/observability/summary", "Get observability summary", "Return global control-plane and runtime health summary.", auth=ADMIN_AUTH, success=response("200", "Summary", "Observability summary payload.", {"status": "healthy", "gateways": {}, "warm_pools": {}}), errors=standard_errors(auth=True, bad_request=False, not_found=False)),
        api("Admin Observability", "observability/list-gateways", "GET", "/admin/observability/gateways", "List gateways", "Return runtime-gateway pod diagnostics and sandbox placement.", auth=ADMIN_AUTH, success=response("200", "Gateways", "Gateway diagnostics.", {"gateways": []}), errors=standard_errors(auth=True, bad_request=False, not_found=False)),
        api("Admin Observability", "observability/list-warm-pools", "GET", "/admin/observability/warm-pools", "List warm pools", "Return warm-pool segment state.", auth=ADMIN_AUTH, success=response("200", "Warm pools", "Warm-pool payload.", {"warm_pools": []}), errors=standard_errors(auth=True, bad_request=False, not_found=False)),
        api("Admin Observability", "observability/list-template-images", "GET", "/admin/observability/templates/images", "List template images", "Return template image availability and repair hints.", auth=ADMIN_AUTH, success=response("200", "Template images", "Template image status payload.", {"images": []}), errors=standard_errors(auth=True, bad_request=False, not_found=False)),
        api("Admin Observability", "observability/list-events", "GET", "/admin/observability/events", "List observability events", "Return filtered observability event rows.", auth=ADMIN_AUTH, query_params=ADMIN_EVENT_QUERY, query_example="limit=100&offset=0", success=response("200", "Events", "Observability events.", {"events": [], "limit": 100, "offset": 0}), errors=standard_errors(auth=True, not_found=False)),
        api("Admin Observability", "observability/get-sandbox-timeline", "GET", "/admin/observability/sandboxes/{sandbox_id}/timeline", "Get sandbox timeline", "Return timeline events for one sandbox.", auth=ADMIN_AUTH, path_params=[SANDBOX_ID], query_params=[field("limit", "integer", False, "Maximum timeline events.", default="200")], query_example="limit=200", success=response("200", "Timeline", "Sandbox timeline payload.", {"sandbox_id": "sb-abc123", "events": []}), errors=standard_errors(auth=True)),
    ]
)
