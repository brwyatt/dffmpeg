from logging import getLogger
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    TransportRecord,
    Worker,
    WorkerDeregistration,
    WorkerRegistration,
)
from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.api.dependencies import get_config, get_transports, get_worker_repo
from dffmpeg.coordinator.api.utils import get_negotiated_transport
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository
from dffmpeg.coordinator.transports import TransportManager

router = APIRouter()

logger = getLogger(__name__)


@router.post("/worker/register")
async def worker_register(
    payload: WorkerRegistration,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
    config: CoordinatorConfig = Depends(get_config),
):
    """
    Registers a worker with the coordinator.

    Args:
        payload (WorkerRegistration): The worker registration details (capabilities, etc.).
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (Transports): Transport manager.
        worker_repo (WorkerRepository): Repository for worker storage.
        config (CoordinatorConfig): Coordinator configuration.

    Returns:
        TransportRecord: The negotiated transport configuration for the worker to use.

    Raises:
        HTTPException: If authentication fails, worker ID mismatches, or transport negotiation fails.
    """
    if identity.client_id != payload.worker_id:
        raise HTTPException(status_code=403, detail="WorkerID does not match authenticated ClientID")

    try:
        healthy_transports = await transports.get_healthy_transports()
        negotiated_transport = get_negotiated_transport(payload.supported_transports, healthy_transports)
    except ValueError:
        logger.error(f"Client requested unsupported transports: {', '.join(payload.supported_transports)}")
        raise HTTPException(
            status_code=400,
            detail=f"No supported transports in: {', '.join(payload.supported_transports)}",
        )

    # Filter reported binaries against allowed binaries (intersection)
    filtered_binaries = list(set(config.allowed_binaries).intersection(payload.binaries))

    payload_dict = payload.model_dump(mode="python", exclude={"supported_transports"})
    payload_dict["binaries"] = filtered_binaries

    record = WorkerRecord(
        **payload_dict,
        status="online",
        transport=negotiated_transport,
        transport_metadata=transports[negotiated_transport].get_metadata(payload.worker_id),
    )

    await worker_repo.add_or_update(record)

    return TransportRecord(transport=record.transport, transport_metadata=record.transport_metadata)


@router.post("/worker/deregister")
async def worker_deregister(
    payload: WorkerDeregistration,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
):
    """
    Deregisters a worker from the coordinator (marks as offline).

    Args:
        payload (WorkerDeregistration): Deregistration details.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        worker_repo (WorkerRepository): Repository for worker storage.

    Returns:
        dict: Status OK.

    Raises:
        HTTPException: If authentication fails or worker ID mismatches.
    """
    if identity.client_id != payload.worker_id:
        raise HTTPException(status_code=403, detail="WorkerID does not match authenticated ClientID")

    # Mark offline by updating with minimal info
    record = WorkerRecord(
        worker_id=payload.worker_id,
        status="offline",
        # Use existing transport for now or empty, doesn't matter much if offline
        transport="none",
        transport_metadata={},
        capabilities=[],
        binaries=[],
        paths=[],
        registration_interval=0,
    )

    await worker_repo.add_or_update(record)
    return {"status": "ok"}


@router.get("/workers", response_model=List[Worker])
async def list_workers(
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
    window: int = 3600 * 24,
):
    """
    Lists all known workers (online and offline).

    Args:
        identity (AuthenticatedIdentity): The authenticated identity (optional).
        worker_repo (WorkerRepository): Repository for worker storage.

    Returns:
        List[Worker]: A list of worker details.
    """
    # While the dashboard is unauthenticated, this API returns a lot more information

    online = await worker_repo.get_workers_by_status("online")
    # For offline, maybe limit to recent ones? Or all?
    # Admin CLI limits to 24h. Let's do the same for API to avoid massive lists.
    offline = await worker_repo.get_workers_by_status("offline", since_seconds=window)

    workers = online + offline
    # Sort by status (online first), then last seen (desc), then ID
    workers.sort(key=lambda w: (w.status != "online", -(w.last_seen.timestamp() if w.last_seen else 0), w.worker_id))

    return workers


@router.get("/workers/{worker_id}", response_model=Worker)
async def get_worker(
    worker_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
):
    """
    Gets details for a specific worker.

    Args:
        worker_id (str): The ID of the worker.
        identity (AuthenticatedIdentity): The authenticated identity (optional).
        worker_repo (WorkerRepository): Repository for worker storage.

    Returns:
        Worker: The worker details.

    Raises:
        HTTPException: If worker not found.
    """
    # While the dashboard is unauthenticated, this API returns a lot more information

    worker = await worker_repo.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    return worker
