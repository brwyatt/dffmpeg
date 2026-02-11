from datetime import datetime, timezone
from logging import getLogger
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from ulid import ULID

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    CommandResponse,
    JobLogsMessage,
    JobLogsPayload,
    JobLogsResponse,
    JobRequest,
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
from dffmpeg.coordinator.scheduler import process_job_assignment
from dffmpeg.coordinator.transports import TransportManager

router = APIRouter()

logger = getLogger(__name__)


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
        healthy_transports = await transports.get_healthy_transports()
        negotiated_transport = get_negotiated_transport(payload.supported_transports, healthy_transports)
    except ValueError:
        raise HTTPException(status_code=400, detail="No supported transports")

    job_id = ULID()
    now = datetime.now(timezone.utc)
    job_record = JobRecord(
        job_id=job_id,
        requester_id=identity.client_id,
        binary_name=payload.binary_name,
        arguments=payload.arguments,
        paths=payload.paths,
        status="pending",
        transport=negotiated_transport,
        transport_metadata=transports[negotiated_transport].get_metadata(identity.client_id, job_id),
        heartbeat_interval=payload.heartbeat_interval or config.default_job_heartbeat_interval,
        monitor=payload.monitor,
        client_last_seen=now if payload.monitor else None,
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


@router.get("/jobs")
async def job_list(
    limit: int = 20,
    since_id: Optional[str] = None,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
):
    """
    Lists jobs for the authenticated client.
    Shows active jobs and recently finished jobs (last 1 hour).

    Args:
        limit (int): Max number of jobs to return.
        since_id (Optional[str]): Cursor for pagination (job_id).
        identity (AuthenticatedIdentity): The authenticated client identity.
        job_repo (JobRepository): Job repository.

    Returns:
        List[JobRecord]: List of matching jobs.
    """
    s_id = None
    if since_id:
        try:
            s_id = ULID.from_str(since_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid since_id")

    jobs = await job_repo.get_dashboard_jobs(
        requester_id=identity.client_id,
        limit=limit,
        since_id=s_id,
    )

    return jobs


@router.post("/jobs/{job_id}/accept")
async def job_accept(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
) -> CommandResponse:
    """
    Endpoint for a worker to accept an assigned job.

    Args:
        job_id (str): The ID of the job to accept.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        CommandResponse: Status OK if successful.

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

    await job_repo.update_status(j_id, "running", worker_id=identity.client_id, timestamp=timestamp)

    await transports.send_message(
        JobStatusMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=JobStatusPayload(status="running", last_update=timestamp),
        )
    )

    return CommandResponse(status="ok")


@router.post("/jobs/{job_id}/cancel")
async def job_cancel(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
) -> CommandResponse:
    """
    Cancels a job. Can be called by the requester or an admin.
    If the job is assigned, it requests cancellation from the worker first.

    Args:
        job_id (str): The ID of the job to cancel.
        identity (AuthenticatedIdentity): The authenticated user identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        CommandResponse: Status OK if successful.

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
        return CommandResponse(status="ok", detail="Job already finished")

    timestamp = datetime.now(timezone.utc)

    if job.worker_id is not None:
        # Update status to canceling
        await job_repo.update_status(j_id, "canceling", worker_id=job.worker_id, timestamp=timestamp)

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

    return CommandResponse(status="ok")


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
) -> CommandResponse:
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
        CommandResponse: Status OK if successful.

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
    await job_repo.update_status(
        j_id, payload.status, exit_code=payload.exit_code, worker_id=identity.client_id, timestamp=timestamp
    )

    await transports.send_message(
        JobStatusMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=JobStatusPayload(status=payload.status, exit_code=payload.exit_code, last_update=timestamp),
        )
    )

    return CommandResponse(status="ok")


@router.post("/jobs/{job_id}/worker_heartbeat")
async def job_worker_heartbeat(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
) -> CommandResponse:
    """
    Updates the worker_last_seen timestamp for a job to indicate the worker is still active.

    Args:
        job_id (str): The ID of the job.
        identity (AuthenticatedIdentity): The authenticated worker identity.
        transports (TransportManager): Transport manager.
        job_repo (JobRepository): Job repository.

    Returns:
        CommandResponse: Status OK if successful.

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

    await job_repo.update_worker_heartbeat(j_id, timestamp=timestamp)

    await transports.send_message(
        JobStatusMessage(
            recipient_id=job.requester_id,
            job_id=j_id,
            sender_id=identity.client_id,
            payload=JobStatusPayload(status="running", last_update=timestamp),
        )
    )

    return CommandResponse(status="ok")


@router.post("/jobs/{job_id}/client_heartbeat")
async def job_client_heartbeat(
    job_id: str,
    monitor: Optional[bool] = None,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
) -> CommandResponse:
    """
    Updates the client_last_seen timestamp for a job to indicate the requester is still monitoring.

    Args:
        job_id (str): The ID of the job.
        monitor (Optional[bool]): Optionally update the monitor flag (e.g., when attaching).
        identity (AuthenticatedIdentity): The authenticated requester identity.
        job_repo (JobRepository): Job repository.

    Returns:
        CommandResponse: Status OK if successful.

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
        raise HTTPException(status_code=403, detail="No permission to heartbeat for this job")

    # If job is already terminal, just return OK (no need to update DB)
    if job.status in ["completed", "failed", "canceled"]:
        return CommandResponse(status="ok", detail="Job already finished")

    timestamp = datetime.now(timezone.utc)

    await job_repo.update_client_heartbeat(j_id, timestamp=timestamp, monitor=monitor)

    return CommandResponse(status="ok")


@router.post("/jobs/{job_id}/logs")
async def job_logs_submit(
    job_id: str,
    payload: JobLogsPayload,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
) -> CommandResponse:
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
        CommandResponse: Status OK if successful.

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

    return CommandResponse(status="ok")


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
