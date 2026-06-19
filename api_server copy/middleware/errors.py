"""Error handling middleware."""

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import logging

logger = logging.getLogger(__name__)


class APIException(Exception):
    """Base API exception."""

    def __init__(self, message: str, status_code: int = 500, details: dict = None):
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        super().__init__(self.message)


class SandboxNotFoundException(APIException):
    """Sandbox not found."""

    def __init__(self, sandbox_id: str):
        super().__init__(
            f"Sandbox not found: {sandbox_id}",
            status_code=404,
            details={"sandbox_id": sandbox_id},
        )


class AgentNotFoundException(APIException):
    """Agent not found."""

    def __init__(self, agent_id: str):
        super().__init__(
            f"Agent not found: {agent_id}",
            status_code=404,
            details={"agent_id": agent_id},
        )


class FileNotFoundException(APIException):
    """File not found."""

    def __init__(self, path: str):
        super().__init__(
            f"File not found: {path}",
            status_code=404,
            details={"path": path},
        )


class InsufficientResourcesException(APIException):
    """Insufficient resources."""

    def __init__(self, message: str):
        super().__init__(message, status_code=507)


class InvalidArgumentException(APIException):
    """Invalid argument."""

    def __init__(self, message: str):
        super().__init__(message, status_code=400)


class TimeoutException(APIException):
    """Operation timeout."""

    def __init__(self, message: str):
        super().__init__(message, status_code=504)


async def api_exception_handler(request: Request, exc: APIException):
    """Handle API exceptions."""
    logger.error(f"API Error: {exc.message}")

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.__class__.__name__,
            "message": exc.message,
            "status_code": exc.status_code,
            "details": exc.details,
        },
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation exceptions."""
    logger.error(f"Validation Error: {exc.errors()}")

    return JSONResponse(
        status_code=422,
        content={
            "error": "ValidationError",
            "message": "Request validation failed",
            "status_code": 422,
            "details": {"errors": exc.errors()},
        },
    )


async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unexpected Error: {str(exc)}", exc_info=True)

    return JSONResponse(
        status_code=500,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "status_code": 500,
            "details": {"error": str(exc)},
        },
    )
