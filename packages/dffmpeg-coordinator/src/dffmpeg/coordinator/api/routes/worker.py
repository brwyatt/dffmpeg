from logging import getLogger
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    TransportRecord,
    WorkerRegistration,
)
from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.api.dependencies import get_transports, get_worker_repo
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository
from dffmpeg.coordinator.transports import Transports

router = APIRouter()

logger = getLogger(__name__)


def get_negotiated_transport(client_transports: List[str], server_transports: List[str]):
    """
    Finds the first transport method supported by both client and server.

    Args:
        client_transports: List of transports supported by the client.
        server_transports: List of transports supported by the server.

    Returns:
        str: The name of the negotiated transport.

    Raises:
        ValueError: If no common transport is found.
    """
    for client_transport in client_transports:
        if client_transport in server_transports:
            return client_transport
    raise ValueError("Cannot find supported transport!")


@router.post("/worker/register")
async def worker_register(
    payload: WorkerRegistration,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: Transports = Depends(get_transports),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
):
    """
    Registers a worker with the coordinator.

    Args:
        payload (WorkerRegistration): The worker registration details (capabilities, etc.).
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (Transports): Transport manager.
        worker_repo (WorkerRepository): Repository for worker storage.

    Returns:
        TransportRecord: The negotiated transport configuration for the worker to use.

    Raises:
        HTTPException: If authentication fails, worker ID mismatches, or transport negotiation fails.
    """
    if identity.client_id != payload.worker_id:
        raise HTTPException(status_code=403, detail="WorkerID does not match authenticated ClientID")

    try:
        negotiated_transport = get_negotiated_transport(payload.supported_transports, transports.transport_names)
    except ValueError:
        logger.error(f"Client requested unsupported transports: {', '.join(payload.supported_transports)}")
        raise HTTPException(
            status_code=400,
            detail=f"No supported transports in: {', '.join(payload.supported_transports)}",
        )

    record = WorkerRecord(
        **payload.model_dump(mode="python", exclude={"supported_transports"}),
        status="online",
        transport=negotiated_transport,
        transport_metadata=transports[negotiated_transport].get_metadata(payload.worker_id),
    )

    await worker_repo.add_or_update(record)

    return TransportRecord(transport=record.transport, transport_metadata=record.transport_metadata)
