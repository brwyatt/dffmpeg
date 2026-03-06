import asyncio
import json
import logging
from typing import Any, AsyncIterator, Dict, Optional

import aio_pika
from pydantic import TypeAdapter

from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.common.transports.base import BaseClientTransport
from dffmpeg.common.transports.utils.rabbitmq import RabbitMQConnectionManager

logger = logging.getLogger(__name__)


class RabbitMQClientTransport(BaseClientTransport):
    """
    RabbitMQ transport implementation for workers and clients.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        use_tls: bool = False,
        use_srv: bool = False,
        verify_ssl: bool = True,
        vhost: str = "/",
        **kwargs,
    ):
        self.default_vhost = vhost

        self._manager = RabbitMQConnectionManager(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=use_tls,
            use_srv=use_srv,
            verify_ssl=verify_ssl,
        )

        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._message_queue: asyncio.Queue[BaseMessage] = asyncio.Queue()
        self._listen_task: Optional[asyncio.Task] = None

    async def connect(self, metadata: Dict[str, Any]):
        """
        Connect to RabbitMQ and setup the consumer.

        Args:
            metadata (Dict[str, Any]): Metadata from the coordinator containing:
                - vhost: The virtual host to use (overrides local default if provided)
                - exchange: The exchange to bind to
                - routing_key: The routing key to bind with
                - queue_name: The name of the queue to declare
                - durable: Whether the queue should be durable
                - auto_delete: Whether the queue should auto-delete
        """
        required = ["exchange", "routing_key", "queue_name"]
        missing = [k for k in required if k not in metadata]
        if missing:
            raise ValueError(f"Missing required RabbitMQ metadata: {', '.join(missing)}")

        # Use vhost from metadata if provided, else local config
        vhost = metadata.get("vhost", self.default_vhost)

        logger.info(
            f"Starting RabbitMQ transport for queue {metadata['queue_name']} "
            f"on exchange {metadata['exchange']} (vhost: {vhost})"
        )

        self._listen_task = asyncio.create_task(self._connection_task(metadata, vhost))

    async def _on_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """Callback for incoming messages."""
        adapter = TypeAdapter(Message)
        async with message.process():
            try:
                payload_str = message.body.decode()
                data = json.loads(payload_str)
                msg = adapter.validate_python(data)
                await self._message_queue.put(msg)
            except Exception as e:
                logger.error(f"Error parsing RabbitMQ message: {e}")

    async def _connection_task(self, metadata: Dict[str, Any], vhost: str):
        """
        Setup robust connection, channel, and queue, then await indefinitely.
        """
        exchange_name = metadata["exchange"]
        routing_key = metadata["routing_key"]
        queue_name = metadata["queue_name"]
        durable = metadata.get("durable", False)
        auto_delete = metadata.get("auto_delete", True)

        try:
            connection = await self._manager.connect(vhost=vhost)

            self._channel = await connection.channel()
            # Set QoS to ensure fair distribution and not overload
            await self._channel.set_qos(prefetch_count=10)

            queue = await self._channel.declare_queue(
                queue_name,
                durable=durable,
                auto_delete=auto_delete,
            )

            await queue.bind(exchange_name, routing_key=routing_key)
            logger.info(f"Bound queue {queue_name} to {exchange_name} with key {routing_key}")

            # Register consumer callback (aio_pika will auto-recreate this consumer if the channel drops!)
            await queue.consume(self._on_message)

            # Keep task alive infinitely while aio_pika manages the background I/O
            await asyncio.Future()

        except asyncio.CancelledError:
            logger.info("RabbitMQ client loop cancelled.")
        except Exception as e:
            logger.error(f"RabbitMQ client setup failed: {e}")
            raise
        finally:
            await self._manager.close()
            self._channel = None

    async def disconnect(self):
        """
        Disconnect from RabbitMQ.
        """
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        await self._manager.close()

    async def listen(self) -> AsyncIterator[BaseMessage]:
        """
        Yield messages from the internal queue.
        """
        while True:
            msg = await self._message_queue.get()
            yield msg
