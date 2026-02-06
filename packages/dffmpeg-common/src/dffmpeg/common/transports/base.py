from typing import Any, AsyncIterator, Dict, cast

from dffmpeg.common.models import BaseMessage


class BaseClientTransport:
    """
    Abstract base class for client-side transport implementations.
    Transports handle the mechanism of receiving messages from the coordinator.
    """

    async def connect(self, metadata: Dict[str, Any]):
        """
        Connect to the transport using the provided metadata.

        Args:
            metadata (Dict[str, Any]): Transport-specific metadata from the coordinator.
        """
        raise NotImplementedError()

    async def disconnect(self):
        """
        Disconnect from the transport.
        """
        raise NotImplementedError()

    async def listen(self) -> AsyncIterator[BaseMessage]:
        """
        Listen for messages from the coordinator.

        Yields:
            BaseMessage: Messages received from the coordinator.
        """
        if False:
            yield cast(BaseMessage, None)
        raise NotImplementedError()
