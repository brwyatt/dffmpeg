import asyncio
from logging import getLogger
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI
from ulid import ULID

from dffmpeg.common.models import AuthenticatedIdentity, Message, TransportMetadata

from dffmpeg.coordinator.api.auth import required_hmac_auth
from dffmpeg.coordinator.db.messages import MessageRepository
from dffmpeg.coordinator.transports.base import BaseServerTransport


logger = getLogger(__name__)


class HTTPPollingTransport(BaseServerTransport):
    def __init__(self, *args, app: FastAPI, base_path: str = "/poll", **kwargs):
        self.app = app
        self.base_path = base_path
        self.job_path = f"{base_path}/jobs/{{job_id}}"
        self.worker_path = f"{base_path}/worker"
        self._message_condition = asyncio.Condition()

    async def setup(self):
        self.app.add_api_route(self.job_path, self.handle_job_poll, methods=["GET"])
        self.app.add_api_route(self.worker_path, self.handle_worker_poll, methods=["GET"])

    async def _poll_loop(self, identity: AuthenticatedIdentity, last_message_id: Optional[ULID] = None, wait_seconds: int = 20, job_id: Optional[ULID] = None):
        """The abstracted core logic used by both endpoints."""
        repo: MessageRepository = self.app.state.db.messages
        end_time = asyncio.get_event_loop().time() + wait_seconds

        try:
            while True:
                # Fetch logic: logic changes slightly if a job_id is provided
                messages = await repo.retrieve_messages(
                    recipient_id=identity.client_id,
                    last_message_id=last_message_id,
                    job_id=job_id,
                )

                if messages:
                    return {"messages": messages}

                if asyncio.get_event_loop().time() >= end_time:
                    return {"messages": []}

                # Wait for a "poke" from the send_message call or a system-wide Janitor event
                async with self._message_condition:
                    wait_timeout = min(5, max(0, end_time - asyncio.get_event_loop().time()))
                    try:
                        await asyncio.wait_for(self._message_condition.wait(), timeout=wait_timeout)
                    except asyncio.TimeoutError:
                        continue # Regular interval check
        except asyncio.CancelledError:
            # Handle sudden disconnects
            logger.info(f"Connection closed by {identity.client_id}")
            raise

    async def handle_job_poll(self, job_id: ULID, last_message_id: Optional[ULID] = None, wait: int = 20, identity = Depends(required_hmac_auth)):
        return await self._poll_loop(identity, last_message_id=last_message_id, wait=wait, job_id=job_id)

    async def handle_worker_poll(self, last_message_id: Optional[ULID] = None, wait: int = 20, identity = Depends(required_hmac_auth)):
        return await self._poll_loop(identity, last_message_id=last_message_id, wait=wait)

    async def send_message(self, message: Message, transport_metadata: Optional[TransportMetadata] = None) -> bool:
        # HTTP polling doesn't actually "send", but we can at least tell connected clients to check
        async with self._message_condition:
            self._message_condition.notify_all()
        return True

    def get_metadata(self, client_id: str, job_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "path": self.job_path if job_id else self.worker_path,
        }
