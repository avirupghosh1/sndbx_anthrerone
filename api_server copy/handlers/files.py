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
    validate_api_key,
    SandboxNotFoundException,
    FileNotFoundException,
)
from orchestrator import SandboxManager

router = APIRouter(prefix="/sandboxes", tags=["files"])


@router.get("/{sandbox_id}/files", response_model=ListFilesResponse)
async def list_files(
    sandbox_id: str,
    path: str = "/",
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """List files in directory."""
    # Check if sandbox exists
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)

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
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Read file content."""
    # Check if sandbox exists
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)

    # Read file
    content = await run_io(sandbox_manager.read_file, sandbox_id, path)
    if content is None:
        raise FileNotFoundException(path)

    return {"path": path, "content": content}


@router.post("/{sandbox_id}/files/write", response_model=WriteFileResponse)
async def write_file(
    sandbox_id: str,
    request: WriteFileRequest,
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Write file content."""
    # Check if sandbox exists
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)

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
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Delete file."""
    # Check if sandbox exists
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)

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
    api_key: str = Depends(validate_api_key),
    sandbox_manager: SandboxManager = Depends(lambda: SandboxManager.__dict__.get("instance")),
):
    """Create directory."""
    # Check if sandbox exists
    sandbox = sandbox_manager.get_sandbox(sandbox_id)
    if not sandbox:
        raise SandboxNotFoundException(sandbox_id)

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
