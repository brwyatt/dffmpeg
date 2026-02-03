import random
from datetime import datetime, timezone
from logging import getLogger
from typing import List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from ulid import ULID

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    JobLogsMessage,
    JobLogsPayload,
    JobLogsResponse,
    JobRequest,
    JobRequestMessage,
    JobRequestPayload,
    JobStatusMessage,
    JobStatusPayload,
    JobStatusUpdate,
    LogEntry,
)
from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.api.dependencies import (
    get_config,
    get_job_repo,
    get_message_repo,
    get_transports,
    get_worker_repo,
)
from dffmpeg.coordinator.api.utils import get_negotiated_transport
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db.jobs import JobRecord, JobRepository
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.transports import TransportManager

router = APIRouter()

logger = getLogger(__name__)


async def process_job_assignment(
    job_id: ULID,
    job_repo: JobRepository,
    worker_repo: WorkerRepository,
    transports: TransportManager,
):
    """
    Background task to find a suitable worker for a pending job and assign it.

    Args:
        job_id (ULID): The ID of the pending job.
        job_repo (JobRepository): Repository for accessing job data.
        worker_repo (WorkerRepository): Repository for accessing worker data.
        transports (TransportManager): Transport manager for sending notifications.
        sender_id (str | None): The ID of the user triggering the assignment.
    """
    try:
        job = await job_repo.get_job(job_id)
        if not job or job.status != "pending":
            return

        workers = await worker_repo.get_online_workers()
        if not workers:
            logger.warning(f"No online workers found for job {job_id}")
            return

        # Filter by binary
        candidates = [w for w in workers if job.binary_name in w.binaries]

        # Filter by paths
        candidates = [w for w in candidates if set(job.paths).issubset(set(w.paths))]

        if not candidates:
            logger.warning(f"No workers match requirements for job {job_id}")
            return

        # Get load
        worker_load = await job_repo.get_worker_load()

        # Sort candidates
        # 1. Load (asc)
        # 2. Last seen (desc, rounded to minute)
        # 3. Random (shuffle first)

        random.shuffle(candidates)
        candidates.sort(key=lambda w: w.last_seen.replace(second=0, microsecond=0), reverse=True)
        candidates.sort(key=lambda w: worker_load.get(w.worker_id, 0))

        selected_worker = candidates[0]

        timestamp = datetime.now(timezone.utc)

        # Assign
        await job_repo.update_status(job_id, "assigned", selected_worker.worker_id, timestamp=timestamp)

        # Notify Worker
        await transports.send_message(
            JobRequestMessage(
                recipient_id=selected_worker.worker_id,
                job_id=job_id,
                payload=JobRequestPayload(
                    job_id=str(job_id),
                    binary_name=job.binary_name,
                    arguments=job.arguments,
                    paths=job.paths,
                    heartbeat_interval=job.heartbeat_interval,
                ),
            )
        )

        # Notify Client
        await transports.send_message(
            JobStatusMessage(
                recipient_id=job.requester_id,
                job_id=job_id,
                payload=JobStatusPayload(status="assigned", last_update=timestamp),
            )
        )

        logger.info(f"Assigned job {job_id} to worker {selected_worker.worker_id}")

    except Exception as e:
        logger.error(f"Error processing assignment for job {job_id}: {e}")


@router.post("/jobs/submit")
async def job_submit(
    payload: JobRequest,
    background_tasks: BackgroundTasks,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
    config: CoordinatorConfig = Depends(get_config),
):
    """
    Submits a new job to the system.

    Args:
        payload (JobRequest): The job details (binary, arguments, paths).
        background_tasks (BackgroundTasks): FastAPI background tasks handler.
        identity (AuthenticatedIdentity): The authenticated client identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.
        worker_repo (WorkerRepository): Worker repository.
        config (CoordinatorConfig): Coordinator configuration.

    Returns:
        JobRecord: The created job record.

    Raises:
        HTTPException: If no supported transports are available.
    """
    try:
        negotiated_transport = get_negotiated_transport(payload.supported_transports, transports.transport_names)
    except ValueError:
        raise HTTPException(status_code=400, detail="No supported transports")

    job_id = ULID()
    job_record = JobRecord(
        job_id=job_id,
        requester_id=identity.client_id,
        binary_name=payload.binary_name,
        arguments=payload.arguments,
        paths=payload.paths,
        status="pending",
        transport=negotiated_transport,
        transport_metadata=transports[negotiated_transport].get_metadata(identity.client_id, job_id),
        heartbeat_interval=config.job_heartbeat_interval,
    )

    await job_repo.create_job(job_record)

    # Trigger assignment in background
    background_tasks.add_task(
        process_job_assignment,
        job_id,
        job_repo,
        worker_repo,
        transports,
    )

    return job_record


@router.post("/jobs/{job_id}/accept")
async def job_accept(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Endpoint for a worker to accept an assigned job.

    Args:
        job_id (str): The ID of the job to accept.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        dict: Status OK if successful.

    Raises:
        HTTPException: If job not found or not assigned to this worker.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    timestamp = datetime.now(timezone.utc)

    await job_repo.update_status(j_id, "running", identity.client_id, timestamp=timestamp)

    await transports.send_message(
        JobStatusMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=JobStatusPayload(status="running", last_update=timestamp),
        )
    )

    return {"status": "ok"}


@router.post("/jobs/{job_id}/cancel")
async def job_cancel(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Cancels a job. Can be called by the requester or an admin.
    If the job is assigned, it requests cancellation from the worker first.

    Args:
        job_id (str): The ID of the job to cancel.
        identity (AuthenticatedIdentity): The authenticated user identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        dict: Status OK if successful.

    Raises:
        HTTPException: If job not found or user lacks permission.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.requester_id != identity.client_id and identity.role != "admin":
        raise HTTPException(status_code=403, detail="No permission to cancel job")

    if job.status in ["completed", "failed", "canceled"]:
        return {"status": "ok", "detail": "Job already finished"}

    timestamp = datetime.now(timezone.utc)

    if job.worker_id is not None:
        # Update status to canceling
        await job_repo.update_status(j_id, "canceling", job.worker_id, timestamp=timestamp)

        # Tell the client
        await transports.send_message(
            JobStatusMessage(
                recipient_id=job.requester_id,
                job_id=j_id,
                sender_id=identity.client_id,
                payload=JobStatusPayload(status="canceling", last_update=timestamp),
            )
        )

        # And tell the assigned worker to cancel
        await transports.send_message(
            JobStatusMessage(
                recipient_id=job.worker_id,
                job_id=j_id,
                sender_id=identity.client_id,
                payload=JobStatusPayload(status="canceling", last_update=timestamp),
            )
        )
    else:
        # Update status to canceled
        await job_repo.update_status(j_id, "canceled", timestamp=timestamp)

        # Tell the client
        await transports.send_message(
            JobStatusMessage(
                recipient_id=job.requester_id,
                job_id=j_id,
                sender_id=identity.client_id,
                payload=JobStatusPayload(status="canceled", last_update=timestamp),
            )
        )

    return {"status": "ok"}


@router.get("/jobs/{job_id}/status")
async def job_status(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Retrieves the current status of a job.

    Args:
        job_id (str): The ID of the job.
        identity (AuthenticatedIdentity): The authenticated user identity.
        job_repo (JobRepository): Job repository.

    Returns:
        JobRecord: The job details.

    Raises:
        HTTPException: If job not found or user lacks permission.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if identity.client_id not in [job.worker_id, job.requester_id] and identity.role != "admin":
        raise HTTPException(status_code=403, detail="No permission to job")

    return job


@router.post("/jobs/{job_id}/status")
async def job_status_update(
    job_id: str,
    payload: JobStatusUpdate,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Updates the status of a job (e.g., to completed or failed).
    Called by the assigned worker.

    Args:
        job_id (str): The ID of the job.
        payload (JobStatusUpdate): The new status details.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        dict: Status OK if successful.

    Raises:
        HTTPException: If job not found or not assigned to this worker.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    timestamp = datetime.now(timezone.utc)

    # Update status
    # We pass worker_id to ensure we are the owner (already checked above, but good for consistency)
    await job_repo.update_status(j_id, payload.status, identity.client_id, timestamp=timestamp)

    await transports.send_message(
        JobStatusMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=JobStatusPayload(status=payload.status, last_update=timestamp),
        )
    )

    return {"status": "ok"}


@router.post("/jobs/{job_id}/heartbeat")
async def job_heartbeat(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Updates the last_update timestamp for a job to indicate the worker is still active.

    Args:
        job_id (str): The ID of the job.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        dict: Status OK if successful.

    Raises:
        HTTPException: If job not found or not assigned to this worker.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    timestamp = datetime.now(timezone.utc)

    await job_repo.update_heartbeat(j_id, timestamp=timestamp)

    await transports.send_message(
        JobStatusMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=JobStatusPayload(status="running", last_update=timestamp),
        )
    )

    return {"status": "ok"}


@router.post("/jobs/{job_id}/logs")
async def job_logs_submit(
    job_id: str,
    payload: JobLogsPayload,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Endpoint for a worker to submit a batch of logs for an assigned job.
    Relays the logs to the job's requester.

    Args:
        job_id (str): The ID of the job.
        payload (JobLogsPayload): The batch of logs.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        dict: Status OK if successful.

    Raises:
        HTTPException: If job not found or not assigned to this worker.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    # Relay the logs to the requester
    await transports.send_message(
        JobLogsMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=payload,
        )
    )

    return {"status": "ok"}


@router.get("/jobs/{job_id}/logs")
async def job_logs_get(
    job_id: str,
    since_message_id: str | None = None,
    limit: int | None = None,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
    message_repo: MessageRepository = Depends(get_message_repo),
) -> JobLogsResponse:
    """
    Endpoint for a client or admin to retrieve logs for a job.

    Args:
        job_id (str): The ID of the job.
        since_message_id (Optional[str]): Only retrieve logs newer than this message ID.
        limit (Optional[int]): Limit the number of messages to retrieve.
        identity (AuthenticatedIdentity): The authenticated user identity.
        job_repo (JobRepository): Job repository.
        message_repo (MessageRepository): Message repository.

    Returns:
        JobLogsResponse: The aggregated logs and the last message ID.

    Raises:
        HTTPException: If job not found or user lacks permission.
    """
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    since_id = None
    if since_message_id:
        try:
            since_id = ULID.from_str(since_message_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid since_message_id")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.requester_id != identity.client_id and identity.role != "admin":
        raise HTTPException(status_code=403, detail="No permission to view job logs")

    messages = await message_repo.get_job_messages(
        j_id, message_type="job_logs", since_message_id=since_id, limit=limit
    )

    all_entries: List[LogEntry] = []
    last_msg_id = None

    for msg in messages:
        last_msg_id = msg.message_id
        if isinstance(msg, JobLogsMessage):
            all_entries.extend(msg.payload.logs)

    return JobLogsResponse(logs=all_entries, last_message_id=last_msg_id)
