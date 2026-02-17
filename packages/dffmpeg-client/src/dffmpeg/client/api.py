import asyncio
import logging
import random
from typing import Any, AsyncIterator, Dict, List, Optional, Union

from ulid import ULID

from dffmpeg.client.config import ClientConfig
from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.models import (
    CommandResponse,
    JobLogsMessage,
    JobLogsResponse,
    JobRecord,
    JobRequest,
    JobStatusMessage,
)
from dffmpeg.common.transports import TransportManager

logger = logging.getLogger(__name__)


class DFFmpegClient:
    def __init__(self, config: ClientConfig):
        self.config = config

        coord = config.coordinator
        base_url = (
            f"{coord.scheme}://{coord.host}:{coord.port}"
            f"{'' if coord.path_base.startswith('/') else '/'}{coord.path_base}"
        )

        self.client = AuthenticatedAsyncClient(
            base_url=base_url,
            client_id=config.client_id,
            hmac_key=str(config.hmac_key),
        )

        # Setup Transport Manager
        # Transport settings are already injected by load_config helper
        self.transport_manager = TransportManager(config.transports)
        self.active_transport = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def submit_job(
        self,
        binary_name: str,
        arguments: List[str],
        paths: List[str],
        monitor: bool = False,
        heartbeat_interval: Optional[int] = None,
    ) -> JobRecord:
        """
        Submits a job to the coordinator.

        Returns:
            JobRecord: The created job record.
        """
        path = "/jobs/submit"

        # Determine available transports to advertise
        supported_transports = self.transport_manager.transport_names
        if not supported_transports:
            # Fallback if discovery failed, though it shouldn't if common is installed
            supported_transports = ["http_polling"]

        payload = JobRequest(
            binary_name=binary_name,
            arguments=arguments,
            paths=paths,
            supported_transports=supported_transports,
            monitor=monitor,
            heartbeat_interval=heartbeat_interval,
        )

        resp = await self.client.post(path, json=payload.model_dump(mode="json"))
        resp.raise_for_status()

        return JobRecord.model_validate(resp.json())

    async def get_job_status(self, job_id: str) -> JobRecord:
        """Retrieves the current status of a job."""
        path = f"/jobs/{job_id}/status"
        resp = await self.client.get(path)
        resp.raise_for_status()
        return JobRecord.model_validate(resp.json())

    async def cancel_job(self, job_id: str) -> CommandResponse:
        """Cancels a job."""
        path = f"/jobs/{job_id}/cancel"
        resp = await self.client.post(path)
        resp.raise_for_status()
        return CommandResponse.model_validate(resp.json())

    async def list_jobs(self, limit: int = 20, since_id: str | None = None) -> List[JobRecord]:
        """Lists active and recently finished jobs."""
        params: Dict[str, Any] = {"limit": limit}
        if since_id:
            params["since_id"] = since_id

        path = "/jobs"
        resp = await self.client.get(path, params=params)
        resp.raise_for_status()
        return [JobRecord.model_validate(j) for j in resp.json()]

    async def get_job_logs(
        self, job_id: str, since_message_id: str | None = None, limit: int | None = None
    ) -> JobLogsResponse:
        """Retrieves logs for a job."""
        params: Dict[str, Any] = {}
        if since_message_id:
            params["since_message_id"] = since_message_id
        if limit:
            params["limit"] = limit

        path = f"/jobs/{job_id}/logs"
        resp = await self.client.get(path, params=params)
        resp.raise_for_status()
        return JobLogsResponse.model_validate(resp.json())

    async def start_monitoring(self, job_id: str, monitor: bool = True):
        """
        Updates the monitoring status of a job and starts the heartbeat loop if needed.
        """
        path = f"/jobs/{job_id}/client_heartbeat"
        params = {"monitor": monitor}
        resp = await self.client.post(path, params=params)
        resp.raise_for_status()

        if monitor:
            # Fetch the job to get the negotiated heartbeat interval
            job = await self.get_job_status(job_id)
            await self._start_heartbeat_loop(job_id, job.heartbeat_interval)
        else:
            await self._stop_heartbeat_loop()

    async def _start_heartbeat_loop(self, job_id: str, interval: int):
        """Starts the background heartbeat loop."""
        await self._stop_heartbeat_loop()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(job_id, interval))

    async def _stop_heartbeat_loop(self):
        """Stops the background heartbeat loop."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

    async def _heartbeat_loop(self, job_id: str, interval: int):
        """Periodically sends client heartbeats to the coordinator."""
        # Use jitter logic similar to worker
        jitter_bound = min(0.5 * interval, 0.5)  # Max 0.5s jitter for client
        while True:
            try:
                path = f"/jobs/{job_id}/client_heartbeat"
                resp = await self.client.post(path)
                if resp.status_code != 200:
                    logger.warning(f"Client heartbeat failed: {resp.status_code} - {resp.text}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in client heartbeat loop: {e}")

            # Sleep with jitter
            jitter = random.uniform(-jitter_bound, jitter_bound)
            await asyncio.sleep(max(1, interval + jitter))

    async def stream_job(
        self, job_id: str, transport_name: str, transport_metadata: Dict[str, Any]
    ) -> AsyncIterator[Union[JobStatusMessage, JobLogsMessage]]:
        """
        Connects to the specified transport and yields status and log messages for the job.
        """
        if transport_name not in self.transport_manager.transport_names:
            raise ValueError(f"Unsupported transport: {transport_name}")

        transport = self.transport_manager[transport_name]

        await transport.connect(transport_metadata)
        self.active_transport = transport

        try:
            async for message in transport.listen():
                # Filter for messages related to our job
                if message.job_id != ULID.from_str(job_id):
                    continue

                if isinstance(message, (JobStatusMessage, JobLogsMessage)):
                    yield message

                    # If job is terminal, we can stop listening
                    if isinstance(message, JobStatusMessage):
                        status = message.payload.status
                        if status in ["completed", "failed", "canceled"]:
                            break

        finally:
            await transport.disconnect()
            self.active_transport = None

    async def close(self):
        """Closes the client and any active transports."""
        await self._stop_heartbeat_loop()
        if self.active_transport:
            await self.active_transport.disconnect()
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
