from typing import Any, Dict, Optional

from fastapi import FastAPI
from ulid import ULID

from dffmpeg.common.models import BaseMessage, TransportMetadata


class BaseServerTransport:
    """
    Abstract base class for server-side transport implementations.
    Transports handle the mechanism of delivering messages to clients/workers.
    """

    def __init__(self, *args, app: FastAPI, **kwargs):
        self.app = app

    async def setup(self):
        """
        Perform any necessary setup for the transport (e.g., registering routes,
        starting background tasks).
        """
        raise NotImplementedError()

    async def send_message(self, message: BaseMessage, transport_metadata: Optional[TransportMetadata] = None) -> bool:
        """
        Send a message to a recipient.

        Args:
            message (Message): The message object to send.
            transport_metadata (Optional[TransportMetadata]): Transport-specific metadata
                for the recipient (e.g., connection ID, polling path).

        Returns:
            bool: True if the message was successfully sent (or queued/notified), False otherwise.
        """
        raise NotImplementedError()

    def get_metadata(self, client_id: str, job_id: Optional[ULID] = None) -> Dict[str, Any]:
        """
        Generate transport-specific metadata for a client/worker.
        This metadata is sent to the client during registration or job submission
        to tell them how to connect/listen.

        Args:
            client_id (str): The ID of the client/worker.
            job_id (Optional[ULID]): The associated Job ID, if applicable (e.g. for job-specific channels).

        Returns:
            Dict[str, Any]: A dictionary of metadata (e.g., {"path": "/poll/..."}).
        """
        raise NotImplementedError()
