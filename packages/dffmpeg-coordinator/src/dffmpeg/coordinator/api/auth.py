import ipaddress
from logging import getLogger
from typing import Optional

from fastapi import Depends, Header, HTTPException, Request

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.models import AuthenticatedIdentity
from dffmpeg.coordinator.db.auth import AuthRepository

logger = getLogger(__name__)


def get_auth_repo(request: Request) -> AuthRepository:
    return request.app.state.db.auth


async def _get_verified_identity_from_request(
    request: Request,
    request_client_id: Optional[str],
    request_signature: Optional[str],
    request_timestamp: Optional[str],
    auth_repo: AuthRepository,
) -> Optional[AuthenticatedIdentity]:
    if not request_client_id and not request_signature and not request_timestamp:
        # No auth headers provided
        logger.info("Request contained no auth headers")
        return None

    if not request_client_id or not request_signature or not request_timestamp:
        logger.warning(
            "\n".join(
                [
                    "Request contained partial auth headers.",
                    f"  client-id: {request_client_id}",
                    f"  signature: {request_signature}",
                    f"  timestamp: {request_timestamp}",
                ]
            )
        )
        raise HTTPException(status_code=401, detail="Incomplete HMAC authentication provided")

    client_identity = await auth_repo.get_identity(request_client_id, include_hmac_key=True)
    if not client_identity or not client_identity.hmac_key:
        logger.warning(f"Unable to find a key for client {request_client_id}")
        raise HTTPException(status_code=401, detail="Invalid user")

    # Validate IP against allowed CIDRs
    if request.client and request.client.host:
        try:
            client_ip = ipaddress.ip_address(request.client.host)
            allowed = False
            for cidr in client_identity.allowed_cidrs:
                if client_ip in cidr:
                    allowed = True
                    break

            if not allowed:
                logger.warning(
                    f"Client {request_client_id} blocked from IP {request.client.host} "
                    f"(Allowed: {client_identity.allowed_cidrs})"
                )
                raise HTTPException(status_code=401, detail="Client IP not allowed")
        except ValueError:
            logger.warning(f"Invalid client IP address: {request.client.host}")
            raise HTTPException(status_code=401, detail="Invalid client IP")

    body = await request.body()
    signer = RequestSigner(client_identity.hmac_key)

    if signer.verify(
        method=request.method,
        path=request.url.path,
        timestamp=request_timestamp,
        signature=request_signature,
        payload=body,
    ):
        logger.info(
            f"Request signature verified for client {request_client_id} to {request.url.path} via {request.method}"
        )
        return client_identity.model_copy(
            update={
                "hmac_key": None,
                "timestamp": request_timestamp,
                "authenticated": True,
            }
        )

    logger.warning(
        f"Request signature verification failed for client {request_client_id} "
        f"to {request.url.path} via {request.method}"
    )
    raise HTTPException(status_code=401, detail="Invalid HMAC signature")


async def optional_hmac_auth(
    request: Request,
    request_client_id: Optional[str] = Header(None, alias="x-dffmpeg-client-id"),
    request_timestamp: Optional[str] = Header(None, alias="x-dffmpeg-timestamp"),
    request_signature: Optional[str] = Header(None, alias="x-dffmpeg-signature"),
    auth_repo: AuthRepository = Depends(get_auth_repo),
) -> Optional[AuthenticatedIdentity]:
    return await _get_verified_identity_from_request(
        request, request_client_id, request_signature, request_timestamp, auth_repo=auth_repo
    )


async def required_hmac_auth(
    identity: Optional[AuthenticatedIdentity] = Depends(optional_hmac_auth),
) -> AuthenticatedIdentity:
    if not identity:
        raise HTTPException(status_code=401, detail="Authentication required")
    return identity
