"""Generic sandbox extension endpoints.

These are the API-owned routes for capabilities that were first needed by
provider compatibility layers: persistent sessions, PTY, git helpers, raw file
transfer, SSH access, preview URLs, and port/system inspection.

Provider adapters may keep their own wire-compatible URI shape, but local
clients should call these ``/sandboxes/...`` routes.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from handlers import daytona_compat as impl
from middleware import ApiKeyPrincipal, validate_api_key
from orchestrator import SandboxManager

router = APIRouter(prefix="/sandboxes", tags=["sandbox-extensions"])


def _manager_dep() -> SandboxManager:
    return SandboxManager.__dict__.get("instance")


# Advanced filesystem routes.
router.add_api_route("/{sandbox_id}/files/info", impl.toolbox_get_file_info, methods=["GET"])
router.add_api_route("/{sandbox_id}/files/download", impl.toolbox_download_file, methods=["GET"])
router.add_api_route("/{sandbox_id}/files/bulk-download", impl.toolbox_bulk_download, methods=["POST"])
router.add_api_route("/{sandbox_id}/files/upload", impl.toolbox_upload_file, methods=["POST"])
router.add_api_route("/{sandbox_id}/files/bulk-upload", impl.toolbox_bulk_upload, methods=["POST"])
router.add_api_route("/{sandbox_id}/files/move", impl.toolbox_move_file, methods=["POST"])
router.add_api_route("/{sandbox_id}/files/permissions", impl.toolbox_set_permissions, methods=["POST"])
router.add_api_route("/{sandbox_id}/files/search", impl.toolbox_search_files, methods=["GET"])
router.add_api_route("/{sandbox_id}/files/find", impl.toolbox_find_in_files, methods=["GET"])
router.add_api_route("/{sandbox_id}/files/replace", impl.toolbox_replace_in_files, methods=["POST"])

# Process and persistent shell session routes.
router.add_api_route("/{sandbox_id}/process/execute", impl.toolbox_execute_command, methods=["POST"])
router.add_api_route("/{sandbox_id}/process/code-run", impl.toolbox_code_run, methods=["POST"])
router.add_api_route("/{sandbox_id}/process/sessions", impl.toolbox_create_process_session, methods=["POST"], status_code=201)
router.add_api_route("/{sandbox_id}/process/sessions", impl.toolbox_list_process_sessions, methods=["GET"])
router.add_api_route("/{sandbox_id}/process/entrypoint", impl.toolbox_get_entrypoint_session, methods=["GET"])
router.add_api_route("/{sandbox_id}/process/entrypoint/logs", impl.toolbox_get_entrypoint_logs, methods=["GET"])
router.add_api_route("/{sandbox_id}/process/sessions/{session_id}", impl.toolbox_get_process_session, methods=["GET"])
router.add_api_route("/{sandbox_id}/process/sessions/{session_id}", impl.toolbox_delete_process_session, methods=["DELETE"])
router.add_api_route("/{sandbox_id}/process/sessions/{session_id}/commands", impl.toolbox_execute_process_session_command, methods=["POST"])
router.add_api_route(
    "/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}",
    impl.toolbox_get_process_session_command,
    methods=["GET"],
)
router.add_api_route(
    "/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}/logs",
    impl.toolbox_get_process_session_command_logs,
    methods=["GET"],
)
router.add_api_websocket_route(
    "/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}/logs",
    impl.toolbox_process_command_logs_ws,
)
router.add_api_route(
    "/{sandbox_id}/process/sessions/{session_id}/commands/{command_id}/input",
    impl.toolbox_send_process_session_command_input,
    methods=["POST"],
)
router.add_api_websocket_route("/{sandbox_id}/process/entrypoint/logs", impl.toolbox_entrypoint_logs_ws)

# PTY routes.
router.add_api_route("/{sandbox_id}/pty/sessions", impl.toolbox_create_pty_session, methods=["POST"], status_code=201)
router.add_api_route("/{sandbox_id}/pty/sessions", impl.toolbox_list_pty_sessions, methods=["GET"])
router.add_api_route("/{sandbox_id}/pty/sessions/{session_id}", impl.toolbox_get_pty_session, methods=["GET"])
router.add_api_route("/{sandbox_id}/pty/sessions/{session_id}", impl.toolbox_delete_pty_session, methods=["DELETE"])
router.add_api_route("/{sandbox_id}/pty/sessions/{session_id}/resize", impl.toolbox_resize_pty_session, methods=["POST"])
router.add_api_websocket_route("/{sandbox_id}/pty/sessions/{session_id}/connect", impl.toolbox_connect_pty_session_ws)

# Git routes.
router.add_api_route("/{sandbox_id}/git/add", impl.toolbox_git_add, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/branches", impl.toolbox_git_branches, methods=["GET"])
router.add_api_route("/{sandbox_id}/git/branches", impl.toolbox_git_simple_body_command, methods=["POST"], name="generic_git_create_branch")
router.add_api_route("/{sandbox_id}/git/branches", impl.toolbox_git_simple_body_command, methods=["DELETE"], name="generic_git_delete_branch")
router.add_api_route("/{sandbox_id}/git/checkout", impl.toolbox_git_simple_body_command, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/clone", impl.toolbox_git_clone, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/commit", impl.toolbox_git_commit, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/status", impl.toolbox_git_status, methods=["GET"])
router.add_api_route("/{sandbox_id}/git/init", impl.toolbox_git_init, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/pull", impl.toolbox_git_simple_body_command, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/push", impl.toolbox_git_simple_body_command, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/reset", impl.toolbox_git_reset, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/restore", impl.toolbox_git_restore, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/remotes", impl.toolbox_git_remotes, methods=["GET"])
router.add_api_route("/{sandbox_id}/git/remotes", impl.toolbox_git_remote_add, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/config", impl.toolbox_git_get_config, methods=["GET"])
router.add_api_route("/{sandbox_id}/git/config", impl.toolbox_git_set_config, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/config/user", impl.toolbox_git_configure_user, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/credentials", impl.toolbox_git_authenticate, methods=["POST"])
router.add_api_route("/{sandbox_id}/git/history", impl.toolbox_git_history, methods=["GET"])

# Port and system inspection.
router.add_api_route("/{sandbox_id}/ports", impl.toolbox_ports, methods=["GET"])
router.add_api_route("/{sandbox_id}/ports/{port}/in-use", impl.toolbox_port_in_use, methods=["GET"])
router.add_api_route("/{sandbox_id}/system/metrics", impl.toolbox_system_metrics, methods=["GET"])


@router.put("/{sandbox_id}/labels")
async def replace_labels(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.replace_labels(sandbox_id, request, principal, sandbox_manager)


@router.post("/{sandbox_id}/network-settings")
async def update_network_settings(
    sandbox_id: str,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.update_network_settings(sandbox_id, request, principal, sandbox_manager)


@router.post("/{sandbox_id}/public/{is_public}")
async def set_public_access(
    sandbox_id: str,
    is_public: bool,
    request: Request,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.set_public_access(sandbox_id, is_public, request, principal, sandbox_manager)


@router.get("/{sandbox_id}/ports/{port}/preview-url")
async def get_port_preview_url(
    sandbox_id: str,
    port: int,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.get_port_preview_url(sandbox_id, port, principal, sandbox_manager)


@router.get("/{sandbox_id}/ports/{port}/signed-preview-url")
async def get_signed_port_preview_url(
    sandbox_id: str,
    port: int,
    expires_in_seconds: Optional[int] = Query(default=None, alias="expiresInSeconds"),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.get_signed_port_preview_url(sandbox_id, port, expires_in_seconds, principal, sandbox_manager)


@router.post("/{sandbox_id}/ports/{port}/signed-preview-url/{token}/expire")
async def expire_signed_port_preview_url(sandbox_id: str, port: int, token: str) -> Response:
    return await impl.expire_signed_port_preview_url(sandbox_id, port, token)


@router.post("/{sandbox_id}/ssh-access")
async def create_ssh_access(
    sandbox_id: str,
    request: Request,
    expires_in_minutes: Optional[float] = Query(default=None, alias="expiresInMinutes"),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.create_ssh_access(sandbox_id, request, expires_in_minutes, principal, sandbox_manager)


@router.delete("/{sandbox_id}/ssh-access")
async def revoke_ssh_access(
    sandbox_id: str,
    request: Request,
    token: Optional[str] = Query(default=None),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.revoke_ssh_access(sandbox_id, request, token, principal, sandbox_manager)


@router.get("/ssh-access/validate")
async def validate_ssh_access(
    token: str = Query(...),
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(_manager_dep),
) -> dict[str, Any]:
    return await impl.validate_ssh_access(token, principal, sandbox_manager)
