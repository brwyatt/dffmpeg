import logging
from typing import AsyncIterator, Dict, List, Optional

from dffmpeg.common.models import BaseMessage
from dffmpeg.common.transports import ClientTransportConfig, TransportManager
from dffmpeg.common.transports.base import BaseClientTransport

logger = logging.getLogger(__name__)


class WorkerTransportManager:
    """
    Manages the active transport connection for the Worker.
    """

    def __init__(self, config: ClientTransportConfig):
        self.transports = TransportManager(config)
        self._current_transport: Optional[BaseClientTransport] = None
        self._current_transport_name: Optional[str] = None

    @property
    def transport_names(self) -> List[str]:
        return self.transports.transport_names

    @property
    def current_transport_name(self) -> Optional[str]:
        return self._current_transport_name

    async def connect(self, transport_name: str, metadata: Dict):
        """
        Connects to a specified transport.
        Disconnects the current transport if one is active.

        Args:
            transport_name (str): Name of the transport to connect to.
            metadata (Dict): Metadata for connection.
        """
        await self.disconnect()

        try:
            # Get transport instance from common TransportManager
            # Note: TransportManager.__getitem__ returns an instance
            self._current_transport = self.transports[transport_name]
            self._current_transport_name = transport_name

            await self._current_transport.connect(metadata)
            logger.info(f"Connected to transport: {transport_name}")
        except Exception as e:
            logger.error(f"Failed to connect to transport {transport_name}: {e}")
            self._current_transport = None
            self._current_transport_name = None
            raise

    async def disconnect(self):
        """Disconnects the current transport."""
        if self._current_transport:
            try:
                await self._current_transport.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting transport {self._current_transport_name}: {e}")
            finally:
                self._current_transport = None
                self._current_transport_name = None

    async def listen(self) -> AsyncIterator[BaseMessage]:
        """
        Listens for messages on the current transport.

        Yields:
            BaseMessage: Messages received from the transport.
        """
        if not self._current_transport:
            return

        async for message in self._current_transport.listen():
            yield message
