import random
from datetime import datetime, timezone
from logging import getLogger

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from ulid import ULID

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    JobRequest,
    JobStatusUpdate,
    Message,
)
from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.api.dependencies import (
    get_job_repo,
    get_message_repo,
    get_transports,
    get_worker_repo,
)
from dffmpeg.coordinator.api.utils import get_negotiated_transport
from dffmpeg.coordinator.db.jobs import JobRecord, JobRepository
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.transports import TransportManager

router = APIRouter()

logger = getLogger(__name__)


async def process_job_assignment(
    job_id: ULID, job_repo: JobRepository, worker_repo: WorkerRepository, message_repo: MessageRepository
):
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

        # Assign
        await job_repo.update_status(job_id, "assigned", selected_worker.worker_id)

        # Notify Worker
        await message_repo.add_message(
            Message(
                recipient_id=selected_worker.worker_id,
                job_id=job_id,
                message_type="job_request",
                payload={
                    "job_id": str(job_id),
                    "binary_name": job.binary_name,
                    "arguments": job.arguments,
                    "paths": job.paths,
                },
            )
        )

        # Notify Client
        await message_repo.add_message(
            Message(
                recipient_id=job.requester_id, job_id=job_id, message_type="job_status", payload={"status": "assigned"}
            )
        )

        logger.info(f"Assigned job {job_id} to worker {selected_worker.worker_id}")

    except Exception as e:
        logger.error(f"Error processing assignment for job {job_id}: {e}")


@router.post("/job/submit")
async def job_submit(
    payload: JobRequest,
    background_tasks: BackgroundTasks,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    transports: TransportManager = Depends(get_transports),
    job_repo: JobRepository = Depends(get_job_repo),
    worker_repo: WorkerRepository = Depends(get_worker_repo),
    message_repo: MessageRepository = Depends(get_message_repo),
):
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
    )

    await job_repo.create_job(job_record)

    # Trigger assignment in background
    background_tasks.add_task(process_job_assignment, job_id, job_repo, worker_repo, message_repo)

    return job_record


@router.post("/job/{job_id}/accept")
async def job_accept(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
    message_repo: MessageRepository = Depends(get_message_repo),
):
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    await job_repo.update_status(j_id, "running", identity.client_id)

    await message_repo.add_message(
        Message(recipient_id=job.requester_id, job_id=j_id, message_type="job_status", payload={"status": "running"})
    )

    return {"status": "ok"}


@router.post("/job/{job_id}/status")
async def job_status_update(
    job_id: str,
    payload: JobStatusUpdate,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
    message_repo: MessageRepository = Depends(get_message_repo),
):
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    # Update status
    # We pass worker_id to ensure we are the owner (already checked above, but good for consistency)
    await job_repo.update_status(j_id, payload.status, identity.client_id)

    await message_repo.add_message(
        Message(
            recipient_id=job.requester_id, job_id=j_id, message_type="job_status", payload={"status": payload.status}
        )
    )

    return {"status": "ok"}


@router.post("/job/{job_id}/heartbeat")
async def job_heartbeat(
    job_id: str,
    identity: AuthenticatedIdentity = Depends(required_hmac_auth),
    job_repo: JobRepository = Depends(get_job_repo),
    message_repo: MessageRepository = Depends(get_message_repo),
):
    try:
        j_id = ULID.from_str(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    job = await job_repo.get_job(j_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.worker_id != identity.client_id:
        raise HTTPException(status_code=403, detail="Not assigned to this job")

    await job_repo.update_heartbeat(j_id)

    await message_repo.add_message(
        Message(
            recipient_id=job.requester_id,
            job_id=j_id,
            message_type="job_status",
            payload={"status": "running", "last_update": str(datetime.now(timezone.utc))},
        )
    )

    return {"status": "ok"}
