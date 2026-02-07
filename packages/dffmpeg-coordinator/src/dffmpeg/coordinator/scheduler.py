import random
from datetime import datetime, timezone
from logging import getLogger

from ulid import ULID

from dffmpeg.common.models import (
    JobRequestMessage,
    JobRequestPayload,
    JobStatusMessage,
    JobStatusPayload,
)
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.transports import TransportManager

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
    """
    try:
        job = await job_repo.get_job(job_id)
        if not job or job.status != "pending":
            return

        workers = await worker_repo.get_workers_by_status("online")

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
