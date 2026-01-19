from typing import Any, Dict, Optional

from fastapi import FastAPI
from dffmpeg.coordinator.transports.base import BaseServerTransport


class HTTPPollingTransport(BaseServerTransport):
    def __init__(self, *args, base_path: str = "/poll", **kwargs):
        self.base_path = base_path
        self.job_path = f"{base_path}/jobs"
        self.worker_path = f"{base_path}/worker"

    async def setup(self, app: FastAPI):
        # TODO: setup polling paths and register handlers for querying the Message DB
        # Need paths for clients to monitor work status and for the workers to listen for requests
        # Handler needs:
        # * Verify client is authenticated and is a "client"
        # * check DB for messages and send any that are found
        # * if none found, either return a 404, or if long-poll, start a job to check for messages
        # * if client times out or disconnects, cancel the job/timer
        # * any returned messages are updated as sent
        pass

    async def send_message(self, message) -> bool:
        # HTTP polling doesn't actually "send"
        return True

    def get_metadata(self, client_id: str, job_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "path": self.job_path if job_id else self.worker_path,
        }
