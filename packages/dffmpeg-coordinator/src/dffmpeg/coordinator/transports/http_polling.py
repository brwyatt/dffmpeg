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
    """
    Transport implementation using HTTP Polling.
    Clients poll specific endpoints to receive messages.
    """

    def __init__(self, *args, app: FastAPI, base_path: str = "/poll", **kwargs):
        self.app = app
        self.base_path = base_path
        self.job_path = f"{base_path}/jobs/{{job_id}}"
        self.worker_path = f"{base_path}/worker"
        self._message_condition = asyncio.Condition()

    async def setup(self):
        """
        Sets up the polling endpoints on the FastAPI application.
        """
        self.app.add_api_route(self.job_path, self.handle_job_poll, methods=["GET"])
        self.app.add_api_route(self.worker_path, self.handle_worker_poll, methods=["GET"])

    async def _poll_loop(
        self,
        identity: AuthenticatedIdentity,
        last_message_id: Optional[ULID] = None,
        wait: int = 20,
        job_id: Optional[ULID] = None,
    ):
        """
        The abstracted core logic used by both polling endpoints.
        Waits for new messages or a timeout.

        Args:
            identity (AuthenticatedIdentity): The authenticated user.
            last_message_id (Optional[ULID]): Filter for messages newer than this ID.
            wait (int): Timeout in seconds.
            job_id (Optional[ULID]): Filter for job-specific messages.

        Returns:
            dict: A dictionary containing the list of messages.
        """
        repo: MessageRepository = self.app.state.db.messages
        end_time = asyncio.get_event_loop().time() + wait

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
                        continue  # Regular interval check
        except asyncio.CancelledError:
            # Handle sudden disconnects
            logger.info(f"Connection closed by {identity.client_id}")
            raise

    async def handle_job_poll(
        self, job_id: ULID, last_message_id: Optional[ULID] = None, wait: int = 20, identity=Depends(required_hmac_auth)
    ):
        """
        Endpoint handler for job-specific polling.
        """
        return await self._poll_loop(identity, last_message_id=last_message_id, wait=wait, job_id=job_id)

    async def handle_worker_poll(
        self, last_message_id: Optional[ULID] = None, wait: int = 20, identity=Depends(required_hmac_auth)
    ):
        """
        Endpoint handler for worker polling (general messages).
        """
        return await self._poll_loop(identity, last_message_id=last_message_id, wait=wait)

    async def send_message(self, message: Message, transport_metadata: Optional[TransportMetadata] = None) -> bool:
        """
        Notifies polling clients that a new message might be available.
        Does not actually 'send' the message payload directly, but triggers a poll check.

        Args:
            message (Message): The message object (already saved to DB).
            transport_metadata (Optional[TransportMetadata]): Metadata for the transport.

        Returns:
            bool: Always True (notification sent).
        """
        # HTTP polling doesn't actually "send", but we can at least tell connected clients to check
        async with self._message_condition:
            self._message_condition.notify_all()
        return True

    def get_metadata(self, client_id: str, job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns metadata needed for a client to connect/poll (e.g. the path).

        Args:
            client_id (str): The client ID.
            job_id (Optional[str]): The job ID if applicable.

        Returns:
            Dict[str, Any]: Metadata containing the path.
        """
        return {
            "path": self.job_path if job_id else self.worker_path,
        }
