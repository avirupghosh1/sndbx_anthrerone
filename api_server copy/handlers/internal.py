"""Internal handlers for API orchestration."""

import os
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from middleware import validate_api_key

router = APIRouter(prefix="/internal", tags=["internal"])

CONTEXTS_DIR = "/var/lib/api/contexts"

@router.get("/contexts/{job_id}")
async def get_context(job_id: str, _api_key: str = Depends(validate_api_key)):
    """Serve a Kaniko build context tarball."""
    if not job_id.replace('-', '').isalnum():
        raise HTTPException(status_code=400, detail="Invalid job ID")
    
    file_path = os.path.join(CONTEXTS_DIR, f"{job_id}.tar.gz")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Context not found")
        
    return FileResponse(file_path, media_type="application/gzip")
