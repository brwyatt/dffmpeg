from fastapi import APIRouter, Depends
from pydantic import BaseModel

from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.api.auth import optional_hmac_auth

router = APIRouter()


class PingRequest(BaseModel):
    """
    Request model for the ping endpoint.

    Attributes:
        client_id: The ID of the client sending the ping.
        message: A message to be echoed back.
    """

    client_id: str
    message: str


@router.get("/health")
async def health():
    """
    Unauthenticated health check endpoint.

    Returns:
        dict: A dictionary indicating the service status (e.g., {"status": "online"}).
    """
    return {"status": "online"}


@router.post("/ping")
async def ping(payload: PingRequest, identity: AuthenticatedIdentity = Depends(optional_hmac_auth)):
    """
    Authenticated test endpoint to verify HMAC signature.

    Args:
        payload (PingRequest): The request payload containing client_id and message.
        identity (AuthenticatedIdentity): The authenticated user identity (injected by dependency).

    Returns:
        dict: A dictionary containing the status, echoed message, and identity details.
    """
    return {
        "status": "received",
        "echo": payload.message,
        "identity": identity,
    }
