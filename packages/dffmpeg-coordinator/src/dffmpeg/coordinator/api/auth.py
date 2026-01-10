from fastapi import Depends, Request, Header, HTTPException
from logging import getLogger
from pydantic import BaseModel
from typing import Optional

from dffmpeg.common.auth.request_signer import RequestSigner


logger = getLogger(__name__)


class HMACIdentity(BaseModel):
    authenticated: bool
    client_id: str


# Temporary client key lookup
# This would be bad for real, but we don't have a database yet...
def get_client_key(client_id: str) -> Optional[str]:
    return {
        "test-client": "vWkaG7UUQTXXUdQBMV+45OeOlNhV2mY9n+woIUCQqqs="
    }.get(client_id)


async def _get_verified_identity_from_request(
    request: Request,
    request_client_id: Optional[str],
    request_signature: Optional[str],
    request_timestamp: Optional[str],
) -> Optional[HMACIdentity]:
    if not request_client_id and not request_signature and not request_timestamp:
        # No auth headers provided
        logger.info("Request contained no auth headers")
        return None

    if not request_client_id or not request_signature or not request_timestamp:
        logger.warning("\n".join([
            "Request contained partial auth headers.",
            f"  client-id: {request_client_id}",
            f"  signature: {request_signature}",
            f"  timestamp: {request_timestamp}",
        ]))
        raise HTTPException(status_code=401, detail="Incomplete HMAC authentication provided")
    
    client_key = get_client_key(request_client_id)
    if not client_key:
        logger.warning(f"Unable to find a key for client {request_client_id}")
        raise HTTPException(status_code=401, detail="Invalid user")

    body = await request.body()
    signer = RequestSigner(client_key)

    if signer.verify(
        method=request.method,
        path=request.url.path,
        timestamp=request_timestamp,
        signature=request_signature,
        payload=body
    ):
        logger.info(f"Request signature verified for client {request_client_id} to {request.url.path} via {request.method}")
        return HMACIdentity(authenticated=True, client_id=request_client_id)

    logger.warning(f"Request signature verification failed for client {request_client_id} to {request.url.path} via {request.method}")
    raise HTTPException(status_code=401, detail="Invalid HMAC signature")


async def optional_hmac_auth(
    request: Request,
    request_client_id: Optional[str] = Header(None, alias="x-dffmpeg-client-id"),
    request_timestamp: Optional[str] = Header(None, alias="x-dffmpeg-timestamp"),
    request_signature: Optional[str] = Header(None, alias="x-dffmpeg-signature"),
) -> Optional[HMACIdentity]:
    return await _get_verified_identity_from_request(request, request_client_id, request_signature, request_timestamp)


async def required_hmac_auth(
    identity: Optional[HMACIdentity] = Depends(optional_hmac_auth),
) -> HMACIdentity:
    if not identity:
        raise HTTPException(status_code=401, detail="Authentication required")
    return identity
