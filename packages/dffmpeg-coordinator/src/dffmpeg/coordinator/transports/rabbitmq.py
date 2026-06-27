import asyncio
import json
import logging
import uuid
from typing import Any, Dict, Optional

import aio_pika
from pydantic import TypeAdapter
from ulid import ULID

from dffmpeg.common.models import BaseMessage, ComponentHealth, Message
from dffmpeg.common.transports.base import BaseClientTransport
from dffmpeg.common.transports.utils.rabbitmq import RabbitMQConnectionManager
from dffmpeg.coordinator.transports.base import BaseServerTransport

logger = logging.getLogger(__name__)


class RabbitMQServerTransport(BaseServerTransport):
    """
    RabbitMQ transport implementation for the coordinator.
    Delivers messages to workers and clients via RabbitMQ exchanges.
    """

    def __init__(
        self,
        *args,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        use_tls: bool = False,
        use_srv: bool = False,
        verify_ssl: bool = True,
        vhost: str = "/",
        workers_exchange: str = "dffmpeg.workers",
        jobs_exchange: str = "dffmpeg.jobs",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.vhost = vhost
        self.workers_exchange_name = workers_exchange
        self.jobs_exchange_name = jobs_exchange

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
        self._workers_exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._jobs_exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._loop_task: Optional[asyncio.Task] = None
        self._coordinator_queue: Optional[aio_pika.abc.AbstractQueue] = None
        self._coordinator_queue_name: Optional[str] = None
        self._multiplex_clients: Dict[str, Any] = {}

    async def setup(self):
        """
        Connect to RabbitMQ and declare exchanges.
        """
        self._loop_task = asyncio.create_task(self._connection_task())

    async def _on_coordinator_message(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        """
        Receives messages from RabbitMQ, finds the target multiplexed client,
        and places the message into its in-memory queue.
        """
        async with message.process():
            try:
                routing_key = message.routing_key
                queue_name = None
                if routing_key.startswith("worker."):
                    client_id = routing_key.split(".", 1)[1]
                    queue_name = f"dffmpeg.worker.{client_id}"
                elif routing_key.startswith("job."):
                    parts = routing_key.split(".", 2)
                    if len(parts) >= 3:
                        client_id = parts[1]
                        job_id = parts[2]
                        queue_name = f"dffmpeg.job.{client_id}.{job_id}"

                if queue_name and queue_name in self._multiplex_clients:
                    client = self._multiplex_clients[queue_name]
                    adapter = TypeAdapter(Message)
                    payload_str = message.body.decode()
                    data = json.loads(payload_str)
                    msg = adapter.validate_python(data)
                    await client._message_queue.put(msg)
                    logger.debug(f"Successfully routed message {msg.message_id} to multiplexed client {queue_name}")
            except Exception as e:
                logger.error(f"Error processing coordinator multiplex message: {e}")

    async def register_multiplex_client(self, client: Any):
        metadata = client._metadata
        routing_key = metadata["routing_key"]
        exchange_name = metadata["exchange"]
        queue_name = metadata["queue_name"]

        self._multiplex_clients[queue_name] = client

        if self._coordinator_queue:
            await self._coordinator_queue.bind(exchange_name, routing_key=routing_key)
            logger.info(f"Registered multiplexed client and bound {routing_key} to coordinator queue")

    async def unregister_multiplex_client(self, client: Any):
        metadata = client._metadata
        routing_key = metadata["routing_key"]
        exchange_name = metadata["exchange"]
        queue_name = metadata["queue_name"]

        self._multiplex_clients.pop(queue_name, None)

        if self._coordinator_queue and self._manager.is_connected.is_set():
            try:
                await self._coordinator_queue.unbind(exchange_name, routing_key=routing_key)
                logger.info(f"Unregistered multiplexed client and unbound {routing_key} from coordinator queue")
            except Exception as e:
                logger.debug(f"Failed to unbind {routing_key} from coordinator queue (likely already cleaned up): {e}")

    async def _connection_task(self):
        """
        Background task to set up robust connection and await indefinitely.
        """
        try:
            connection = await self._manager.connect(vhost=self.vhost)

            self._channel = await connection.channel()

            # Declare Exchanges
            self._workers_exchange = await self._channel.declare_exchange(
                self.workers_exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )
            self._jobs_exchange = await self._channel.declare_exchange(
                self.jobs_exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )

            logger.info(
                f"Connected to RabbitMQ and declared exchanges: "
                f"{self.workers_exchange_name}, {self.jobs_exchange_name}"
            )

            # Declare Coordinator-wide queue
            self._coordinator_queue_name = f"dffmpeg.coordinator.{uuid.uuid4().hex}"
            self._coordinator_queue = await self._channel.declare_queue(
                self._coordinator_queue_name,
                durable=False,
                exclusive=True,
                auto_delete=True,
            )
            await self._coordinator_queue.consume(self._on_coordinator_message)
            logger.info(f"Declared persistent coordinator queue: {self._coordinator_queue_name}")

            # Keep task alive infinitely while aio_pika manages background I/O
            await asyncio.Future()

        except asyncio.CancelledError:
            logger.info("RabbitMQ server loop cancelled.")
        except Exception as e:
            logger.error(f"RabbitMQ server setup failed: {e}")
            raise
        finally:
            await self._manager.close()
            self._channel = None
            self._workers_exchange = None
            self._jobs_exchange = None
            self._coordinator_queue = None

    async def send_message(
        self,
        message: BaseMessage,
        transport_metadata: Optional[Dict[str, Any]] = None,
        mark_sent: bool = True,
    ) -> bool:
        """
        Publish a message to a RabbitMQ exchange.
        """
        if not self._channel or not self._manager.is_connected.is_set():
            logger.warning(f"RabbitMQ not connected, cannot send message {message.message_id}.")
            return False

        if not transport_metadata:
            logger.error(f"No transport_metadata provided for message {message.message_id}")
            return False

        exchange_name = transport_metadata.get("exchange")
        routing_key = transport_metadata.get("routing_key")

        if not exchange_name or not routing_key:
            logger.error(f"Missing exchange or routing_key in metadata for message {message.message_id}")
            return False

        try:
            exchange = self._workers_exchange if exchange_name == self.workers_exchange_name else self._jobs_exchange

            if not exchange:
                # Should typically not happen if connected, but fallback just in case names mismatch or init issue
                # We can try to get it from channel, or log error
                # For robustness, we can publish to the channel's default exchange with the exchange name?
                # No, standard is to use the exchange object.
                logger.error(f"Exchange object for {exchange_name} not ready.")
                return False

            payload = message.model_dump_json().encode()

            await exchange.publish(
                aio_pika.Message(body=payload, delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=routing_key
            )

            logger.debug(f"Published message {message.message_id} to {exchange_name}::{routing_key}")
            if mark_sent:
                await self._messages.update_message_sent_at(str(message.message_id))
            return True

        except Exception as e:
            logger.error(f"Error publishing RabbitMQ message {message.message_id}: {e}")
            return False

    def get_metadata(self, client_id: str, job_id: Optional[ULID] = None) -> Dict[str, Any]:
        """
        Generate RabbitMQ metadata for a client or worker.
        """
        if job_id:
            # Client Job Update
            return {
                "vhost": self.vhost,
                "exchange": self.jobs_exchange_name,
                "routing_key": f"job.{client_id}.{job_id}",
                "queue_name": f"dffmpeg.job.{client_id}.{job_id}",
                "durable": False,
                "auto_delete": True,
            }
        else:
            # Worker Command
            return {
                "vhost": self.vhost,
                "exchange": self.workers_exchange_name,
                "routing_key": f"worker.{client_id}",
                "queue_name": f"dffmpeg.worker.{client_id}",
                "durable": True,
                "auto_delete": False,
            }

    async def health_check(self) -> ComponentHealth:
        """
        Check the health of the RabbitMQ connection.
        """
        if self._manager.is_connected.is_set():
            return ComponentHealth(status="online")
        else:
            return ComponentHealth(status="unhealthy", detail="Not connected to RabbitMQ")

    def create_client_transport(self) -> BaseClientTransport:
        """
        Creates a RabbitMQ client transport instance.
        Always returns a multiplexed proxy transport.
        """
        return RabbitMQProxyClientTransport(self)


class RabbitMQProxyClientTransport(BaseClientTransport):
    """
    A lightweight, in-memory proxy client transport.
    Instead of establishing its own TCP connection, it registers with the
    RabbitMQServerTransport and receives routed messages in-memory, backed
    by dynamic queue binding on the persistent coordinator connection.
    """

    def __init__(self, server_transport: RabbitMQServerTransport):
        self._server_transport = server_transport
        self._message_queue: asyncio.Queue[BaseMessage] = asyncio.Queue()
        self._metadata: Optional[Dict[str, Any]] = None

    async def connect(self, metadata: Dict[str, Any]):
        required = ["exchange", "routing_key", "queue_name"]
        missing = [k for k in required if k not in metadata]
        if missing:
            raise ValueError(f"Missing required RabbitMQ metadata: {', '.join(missing)}")

        self._metadata = metadata
        await self._server_transport.register_multiplex_client(self)

    async def disconnect(self):
        if self._metadata:
            await self._server_transport.unregister_multiplex_client(self)

    async def receive(self) -> BaseMessage:
        return await self._message_queue.get()

    def receive_nowait(self) -> BaseMessage:
        return self._message_queue.get_nowait()
