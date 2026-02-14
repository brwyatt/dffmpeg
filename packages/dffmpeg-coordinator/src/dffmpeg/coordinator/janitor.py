import asyncio
import random
from datetime import datetime, timezone
from logging import getLogger

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.coordinator.config import JanitorConfig
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.scheduler import process_job_assignment
from dffmpeg.coordinator.transports import TransportManager

logger = getLogger(__name__)


class Janitor:
    def __init__(
        self,
        worker_repo: WorkerRepository,
        job_repo: JobRepository,
        transports: TransportManager,
        config: JanitorConfig,
    ):
        self.worker_repo = worker_repo
        self.job_repo = job_repo
        self.transports = transports
        self.config = config
        self.running = False

    async def start(self):
        """
        Starts the Janitor loop.
        """
        self.running = True
        logger.info("Janitor service started.")
        jitter_bound = min(0.5 * self.config.interval, self.config.jitter)
        while self.running:
            try:
                await self.reap_workers()
                await self.reap_running_jobs()
                await self.reap_assigned_jobs()
                await self.reap_pending_jobs()
                await self.reap_abandoned_monitored_jobs()
            except asyncio.CancelledError:
                logger.info("Janitor service cancelled.")
                self.running = False
                break
            except Exception:
                logger.exception("Error in Janitor loop")

            try:
                jitter = random.uniform(-jitter_bound, jitter_bound)
                await asyncio.sleep(max(1, self.config.interval + jitter))
            except asyncio.CancelledError:
                logger.info("Janitor service cancelled during sleep.")
                self.running = False
                break
        logger.info("Janitor service stopped.")

    async def reap_workers(self):
        """
        Finds and marks stale workers as offline.
        """
        stale_workers = await self.worker_repo.get_stale_workers(threshold_factor=self.config.worker_threshold_factor)
        for worker in stale_workers:
            logger.warning(f"Worker {worker.worker_id} is stale. Marking as offline.")
            worker.status = "offline"
            # Clear capabilities and transport info, similar to deregister
            worker.capabilities = []
            worker.binaries = []
            worker.paths = []
            worker.transport = "none"
            worker.transport_metadata = {}
            worker.registration_interval = 0
            await self.worker_repo.add_or_update(worker)

    async def reap_running_jobs(self):
        """
        Finds running jobs that have timed out and marks them as failed.
        """
        stale_jobs = await self.job_repo.get_stale_running_jobs(
            threshold_factor=self.config.job_heartbeat_threshold_factor
        )
        for job in stale_jobs:
            timestamp = datetime.now(timezone.utc)
            success = await self.job_repo.update_status(
                job.job_id,
                "failed",
                previous_status="running",
                timestamp=timestamp,
            )
            if success:
                logger.warning(f"Job {job.job_id} timed out. Marked as failed.")

                # Notify Client
                msg = JobStatusMessage(
                    recipient_id=job.requester_id,
                    job_id=job.job_id,
                    payload=JobStatusPayload(status="failed", last_update=timestamp),
                )
                await self.transports.send_message(msg)

                # Notify Worker (if possible/connected)
                if job.worker_id:
                    msg_worker = JobStatusMessage(
                        recipient_id=job.worker_id,
                        job_id=job.job_id,
                        payload=JobStatusPayload(status="failed", last_update=timestamp),
                    )
                    await self.transports.send_message(msg_worker)

    async def reap_assigned_jobs(self):
        """
        Finds assigned jobs that have not been accepted in time and re-queues them.
        """
        stale_jobs = await self.job_repo.get_stale_assigned_jobs(timeout_seconds=self.config.job_assignment_timeout)
        for job in stale_jobs:
            timestamp = datetime.now(timezone.utc)
            success = await self.job_repo.update_status(
                job.job_id,
                "pending",
                previous_status="assigned",
                timestamp=timestamp,
            )
            if success:
                logger.warning(f"Job {job.job_id} assignment timed out. Re-queueing as pending.")

                # Notify Worker of cancellation
                if job.worker_id:
                    msg = JobStatusMessage(
                        recipient_id=job.worker_id,
                        job_id=job.job_id,
                        payload=JobStatusPayload(
                            status="canceled", last_update=timestamp
                        ),  # Or canceling? "canceled" is terminal.
                    )
                    # If we set job status to 'pending', effectively we canceled the assignment.
                    # Letting the worker know it's canceled is good, just in case.
                    await self.transports.send_message(msg)

    async def reap_pending_jobs(self):
        """
        Manages pending jobs:
        1. Retries assignment for jobs aged 5s-30s.
        2. Fails jobs aged > 30s.
        """
        # Retry Phase
        retry_jobs = await self.job_repo.get_stale_pending_jobs(
            min_seconds=self.config.job_pending_retry_delay,
            max_seconds=self.config.job_pending_timeout,
        )
        for job in retry_jobs:
            await process_job_assignment(
                job.job_id,
                self.job_repo,
                self.worker_repo,
                self.transports,
            )

        # Fail Phase
        fail_jobs = await self.job_repo.get_stale_pending_jobs(min_seconds=self.config.job_pending_timeout)
        for job in fail_jobs:
            timestamp = datetime.now(timezone.utc)
            success = await self.job_repo.update_status(
                job.job_id,
                "failed",
                previous_status="pending",
                timestamp=timestamp,
            )
            if success:
                logger.warning(f"Job {job.job_id} pending timeout. Marked as failed.")

                # Notify Client
                msg = JobStatusMessage(
                    recipient_id=job.requester_id,
                    job_id=job.job_id,
                    payload=JobStatusPayload(status="failed", last_update=timestamp),
                )
                await self.transports.send_message(msg)

    async def reap_abandoned_monitored_jobs(self):
        """
        Finds monitored jobs whose client has stopped heartbeating and cancels them.
        """
        abandoned_jobs = await self.job_repo.get_stale_monitored_jobs(
            threshold_factor=self.config.job_heartbeat_threshold_factor
        )
        for job in abandoned_jobs:
            logger.warning(f"Job {job.job_id} client heartbeat timeout. Canceling.")
            timestamp = datetime.now(timezone.utc)

            # Mark as canceled (Terminal state)
            success = await self.job_repo.update_status(
                job.job_id,
                "canceling",
                timestamp=timestamp,
            )

            if success:
                # Notify Client (they might be gone, but good for logs/transports)
                msg_client = JobStatusMessage(
                    recipient_id=job.requester_id,
                    job_id=job.job_id,
                    payload=JobStatusPayload(status="canceling", last_update=timestamp),
                )
                await self.transports.send_message(msg_client)

                # Notify Worker to stop
                if job.worker_id:
                    msg_worker = JobStatusMessage(
                        recipient_id=job.worker_id,
                        job_id=job.job_id,
                        payload=JobStatusPayload(status="canceling", last_update=timestamp),
                    )
                    await self.transports.send_message(msg_worker)
