import logging
from typing import Any, AsyncIterator, Dict, List, Union

from ulid import ULID

from dffmpeg.client.config import ClientConfig
from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.models import (
    CommandResponse,
    JobLogsMessage,
    JobRecord,
    JobRequest,
    JobStatusMessage,
    SupportedBinaries,
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

    async def submit_job(self, binary_name: SupportedBinaries, arguments: List[str], paths: List[str]) -> JobRecord:
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
            binary_name=binary_name, arguments=arguments, paths=paths, supported_transports=supported_transports
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
        if self.active_transport:
            await self.active_transport.disconnect()
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
