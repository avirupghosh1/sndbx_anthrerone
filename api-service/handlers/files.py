"""File operation endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from typing import Optional

from async_runner import run_io
from models import (
    WriteFileRequest,
    DeleteFileRequest,
    CreateDirectoryRequest,
    ListFilesRequest,
    ListFilesResponse,
    FileEntryResponse,
    WriteFileResponse,
)
from middleware import (
    ApiKeyPrincipal,
    ensure_sandbox_access,
    validate_api_key,
    SandboxNotFoundException,
    SandboxRuntimeLostException,
    FileNotFoundException,
)
from orchestrator import SandboxManager

router = APIRouter(prefix="/sandboxes", tags=["files"])


def _owned_sandbox_or_404(sandbox_manager: SandboxManager, principal: ApiKeyPrincipal, sandbox_id: str) -> dict:
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)
    return ensure_sandbox_access(principal, sandbox, sandbox_id)


def _ensure_live_sandbox(sandbox_manager: SandboxManager, sandbox_id: str) -> None:
    reason = sandbox_manager.get_sandbox_runtime_failure(sandbox_id)
    if reason:
        raise SandboxRuntimeLostException(sandbox_id, reason)


@router.get("/{sandbox_id}/files", response_model=ListFilesResponse)
async def list_files(
    sandbox_id: str,
    path: str = "/",
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List files in directory."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # List files
    entries = await run_io(sandbox_manager.list_files, sandbox_id, path)
    if entries is None:
        raise FileNotFoundException(path)

    return ListFilesResponse(
        path=path,
        entries=[FileEntryResponse(**entry) for entry in entries],
    )


@router.get("/{sandbox_id}/files/read")
async def read_file(
    sandbox_id: str,
    path: str,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Read file content."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Read file
    content = await run_io(sandbox_manager.read_file, sandbox_id, path)
    if content is None:
        raise FileNotFoundException(path)

    return {"path": path, "content": content}


@router.post("/{sandbox_id}/files/write", response_model=WriteFileResponse)
async def write_file(
    sandbox_id: str,
    request: WriteFileRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Write file content."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Write file
    success = await run_io(
        sandbox_manager.write_file,
        sandbox_id,
        request.path,
        request.content,
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to write file")

    return WriteFileResponse(
        path=request.path,
        bytes_written=len(request.content),
        success=True,
    )


@router.post("/{sandbox_id}/files/delete")
async def delete_file(
    sandbox_id: str,
    request: DeleteFileRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Delete file."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Delete file
    success = await run_io(
        sandbox_manager.delete_file,
        sandbox_id,
        request.path,
        request.recursive,
    )

    if not success:
        raise FileNotFoundException(request.path)

    return {"success": True, "path": request.path}


@router.post("/{sandbox_id}/files/mkdir")
async def create_directory(
    sandbox_id: str,
    request: CreateDirectoryRequest,
    principal: ApiKeyPrincipal = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Create directory."""
    # Check if sandbox exists
    _owned_sandbox_or_404(sandbox_manager, principal, sandbox_id)
    _ensure_live_sandbox(sandbox_manager, sandbox_id)

    # Create directory
    success = await run_io(
        sandbox_manager.create_directory,
        sandbox_id,
        request.path,
        request.mode,
    )

    if not success:
        raise HTTPException(status_code=500, detail="Failed to create directory")

    return {"success": True, "path": request.path}
