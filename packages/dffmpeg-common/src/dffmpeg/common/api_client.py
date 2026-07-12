import logging
from typing import Any, Dict, List

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.models import (
    CommandResponse,
    JobLogsResponse,
    JobRecord,
    Worker,
)

logger = logging.getLogger(__name__)


class DFFmpegAPIClient:
    """
    Base HTTP API Client for dffmpeg, containing shared query methods
    used by both client and admin interfaces.
    """

    def __init__(self, base_url: str, client_id: str, hmac_key: str):
        self.client_id = client_id
        self.client = AuthenticatedAsyncClient(
            base_url=base_url,
            client_id=client_id,
            hmac_key=hmac_key,
        )

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

    async def list_jobs(self, window: int = 3600, since_id: str | None = None) -> List[JobRecord]:
        """Lists active and recently finished jobs."""
        params: Dict[str, Any] = {"window": window}
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

    async def list_workers(self, window: int = 3600 * 24) -> List[Worker]:
        """Lists all known workers."""
        path = "/workers"
        params = {"window": window}
        resp = await self.client.get(path, params=params)
        resp.raise_for_status()
        return [Worker.model_validate(w) for w in resp.json()]

    async def get_worker(self, worker_id: str) -> Worker:
        """Gets details for a specific worker."""
        path = f"/workers/{worker_id}"
        resp = await self.client.get(path)
        resp.raise_for_status()
        return Worker.model_validate(resp.json())

    async def trigger_janitor(self, action: str) -> Dict[str, Any]:
        """Triggers a background janitor action."""
        path = "/admin/janitor"
        resp = await self.client.post(path, json={"action": action})
        resp.raise_for_status()
        return resp.json()

    async def close(self):
        """Closes the client connection."""
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
