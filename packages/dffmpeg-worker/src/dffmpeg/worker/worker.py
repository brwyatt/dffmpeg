import asyncio
import logging
import random
from typing import Dict, Optional

import httpx
from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.models import (
    JobRequestMessage,
    JobStatusMessage,
    JobStatusUpdate,
    JobStatusUpdateStatus,
    WorkerDeregistration,
    WorkerRegistration,
)
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.executor import SubprocessJobExecutor
from dffmpeg.worker.job import JobRunner
from dffmpeg.worker.transport import WorkerTransportManager

logger = logging.getLogger(__name__)


class Worker:
    """
    Main worker coordinator class.
    """

    def __init__(
        self,
        config: WorkerConfig,
        http_client_cls: type[httpx.AsyncClient] = httpx.AsyncClient,
    ):
        self.config = config
        self.client_id = config.client_id
        self.base_url = (
            f"{config.coordinator.scheme}://{config.coordinator.host}:{config.coordinator.port}"
            f"{'' if config.coordinator.path_base.startswith('/') else '/'}{config.coordinator.path_base}"
        )

        logger.info(f"BASE URL: {self.base_url}")

        self.client = AuthenticatedAsyncClient(
            base_url=self.base_url,
            client_id=self.client_id,
            hmac_key=config.hmac_key or "",
            http_client_cls=http_client_cls,
        )
        self.transport_manager = WorkerTransportManager(config.transports)

        logger.info(f"ClientID: {config.client_id} HMAC: {config.hmac_key}")

        self._running = False
        self._registration_task: Optional[asyncio.Task] = None
        self._transport_task: Optional[asyncio.Task] = None

        self.coordinator_paths = {
            "register": "/worker/register",
            "deregister": "/worker/deregister",
        }

        self._active_jobs: Dict[ULID, JobRunner] = {}

    async def start(self):
        """Starts the worker operations."""
        logger.info(f"[{self.client_id}] Starting worker...")
        self._running = True
        self._registration_task = asyncio.create_task(self._registration_loop())

        # Keep running until stopped
        while self._running:
            await asyncio.sleep(1)

    async def stop(self):
        """Stops the worker gracefully."""
        logger.info(f"[{self.client_id}] Stopping worker...")
        self._running = False

        # Cancel registration
        if self._registration_task:
            self._registration_task.cancel()

        # Cancel transport
        await self._stop_transport()

        # Cancel all jobs
        for job_runner in list(self._active_jobs.values()):
            await job_runner.cancel()

        # De-register
        try:
            logger.info(f"[{self.client_id}] Deregistering...")
            payload_model = WorkerDeregistration(worker_id=self.client_id)
            path = self.coordinator_paths["deregister"]
            body = payload_model.model_dump(mode="json")

            await self.client.post(path, json=body)
        except Exception as e:
            logger.warning(f"Failed to deregister: {e}")

        await self.client.aclose()

    async def _registration_loop(self):
        """Periodically registers with the coordinator."""
        jitter_bound = min(0.5 * self.config.registration_interval, self.config.jitter)
        while self._running:
            try:
                # Prepare payload
                payload_model = WorkerRegistration(
                    worker_id=self.client_id,
                    capabilities=[],  # TODO: Retrieve actual capabilities dynamically
                    binaries=list(self.config.binaries.keys()),
                    paths=list(self.config.paths.keys()),
                    supported_transports=self.transport_manager.transport_names,
                )

                path = self.coordinator_paths["register"]
                body = payload_model.model_dump(mode="json")

                resp = await self.client.post(path, json=body)

                if resp.status_code == 200:
                    data = resp.json()
                    new_transport = data.get("transport")
                    metadata = data.get("transport_metadata", {})

                    if new_transport != self.transport_manager.current_transport_name:
                        logger.info(
                            f"Transport changed from {self.transport_manager.current_transport_name} to {new_transport}"
                        )
                        await self._update_transport(new_transport, metadata)
                else:
                    logger.warning(f"Registration failed: {resp.status_code} - {resp.text}")

            except asyncio.CancelledError:
                break
            except httpx.RequestError as e:
                logger.warning(f"Registration request failed (network error): {e}")
            except Exception as e:
                logger.error(f"Error in registration loop: {e}")

            # Sleep with jitter
            jitter = random.uniform(-jitter_bound, jitter_bound)
            await asyncio.sleep(max(1, self.config.registration_interval + jitter))

    async def _update_transport(self, transport_name: str, metadata: dict):
        """
        Switches the active transport.

        Args:
            transport_name (str): The name of the new transport to switch to.
            metadata (dict): Metadata required for the transport connection.
        """
        await self._stop_transport()

        try:
            await self.transport_manager.connect(transport_name, metadata)
            self._transport_task = asyncio.create_task(self._listen_loop())

        except Exception as e:
            logger.error(f"Failed to start transport {transport_name}: {e}")

    async def _stop_transport(self):
        """Stops the current transport."""
        if self._transport_task:
            self._transport_task.cancel()
            try:
                await self._transport_task
            except asyncio.CancelledError:
                pass
            self._transport_task = None

        await self.transport_manager.disconnect()

    async def _listen_loop(self):
        """Listens for messages from the transport."""
        logger.info(f"Listening on transport {self.transport_manager.current_transport_name}...")
        try:
            async for message in self.transport_manager.listen():
                if isinstance(message, JobRequestMessage):
                    await self._handle_job_request(message)
                elif isinstance(message, JobStatusMessage):
                    await self._handle_job_status(message)
                else:
                    logger.debug(f"Ignored message type: {message.message_type}")
        except Exception as e:
            logger.error(f"Transport listener error: {e}")

    async def _handle_job_request(self, message: JobRequestMessage):
        """
        Handles a new job request.

        Args:
            message (JobRequestMessage): The job request message received.
        """
        job_id_str = message.payload.job_id
        try:
            job_id = ULID.from_str(job_id_str)
        except ValueError:
            logger.error(f"Invalid job ID received: {job_id_str}")
            return

        if job_id in self._active_jobs:
            logger.warning(f"Job {job_id} already exists, ignoring duplicate request.")
            return

        # Validation
        binary_name = message.payload.binary_name
        if binary_name not in self.config.binaries:
            logger.error(f"Job {job_id} requires binary '{binary_name}' which is not configured.")
            await self._report_job_failure(job_id, "failed")
            return

        for path_var in message.payload.paths:
            if path_var not in self.config.paths:
                logger.error(f"Job {job_id} requires path variable '{path_var}' which is not configured.")
                await self._report_job_failure(job_id, "failed")
                return

        try:
            # Prepare Executor
            binary_path = self.config.binaries[binary_name]

            # Filter applicable paths (optimization, though config.paths is likely small enough)
            path_map = {k: v for k, v in self.config.paths.items() if k in message.payload.paths}

            executor = SubprocessJobExecutor(
                job_id=str(job_id),
                binary_path=binary_path,
                arguments=message.payload.arguments,
                path_map=path_map,
            )

            runner = JobRunner(
                config=self.config,
                client=self.client,
                job_id=job_id,
                job_payload=message.payload.model_dump(),
                cleanup_callback=self._cleanup_job,
                executor=executor,
            )
            self._active_jobs[job_id] = runner
            await runner.start()

        except Exception as e:
            logger.error(f"Failed to initialize job {job_id}: {e}")
            await self._report_job_failure(job_id, "failed")

    async def _report_job_failure(self, job_id: ULID, status: JobStatusUpdateStatus):
        """Helper to report failure for a job that couldn't be started."""
        try:
            payload_model = JobStatusUpdate(status=status)
            path = f"/jobs/{job_id}/status"
            body = payload_model.model_dump(mode="json")
            await self.client.post(path, json=body)
        except Exception as e:
            logger.error(f"Failed to report failure for job {job_id}: {e}")

    async def _handle_job_status(self, message: JobStatusMessage):
        """
        Handles a job status update (e.g., cancel).

        Args:
            message (JobStatusMessage): The job status message received.
        """
        if not message.job_id:
            return

        job_id = message.job_id
        status = message.payload.status

        if job_id in self._active_jobs:
            if status == "canceling":
                logger.info(f"Received cancellation request for {job_id}")
                await self._active_jobs[job_id].cancel()
        else:
            logger.debug(f"Received status update for unknown job {job_id}")

    def _cleanup_job(self, job_id: ULID):
        """
        Callback to remove job from active registry.

        Args:
            job_id (ULID): The ID of the job to remove.
        """
        if job_id in self._active_jobs:
            del self._active_jobs[job_id]
