import asyncio
import json
import logging
import ssl
from typing import Any, AsyncIterator, Dict, Optional, Tuple

import aio_pika
import dns.asyncresolver
from pydantic import TypeAdapter

from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.common.transports.base import BaseClientTransport

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
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.use_srv = use_srv
        self.verify_ssl = verify_ssl
        self.default_vhost = vhost

        self._connection: Optional[aio_pika.abc.AbstractConnection] = None
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

        self._listen_task = asyncio.create_task(self._run_client(metadata, vhost))

    async def _resolve_srv(self, host: str, use_tls: bool) -> Tuple[str, int]:
        """
        Resolve SRV record to get the actual host and port.
        """
        prefix = "_amqps._tcp" if use_tls else "_amqp._tcp"
        srv_name = f"{prefix}.{host}"

        try:
            # Use async resolver directly
            answers = await dns.asyncresolver.resolve(srv_name, "SRV")

            # Sort by priority and weight, pick the best one
            # SRV records are (priority, weight, port, target)
            # Lower priority is better. Higher weight is better (for same priority).
            # We'll just pick the first one for simplicity, or handle basic load balancing if needed.
            # dns.resolver returns objects with these attributes.

            # Simple strategy: take the first one (lowest priority, highest weight usually sorted by resolver?)
            # Actually resolver returns a set, we should sort.
            sorted_answers = sorted(answers, key=lambda x: (x.priority, -x.weight))
            best = sorted_answers[0]

            target = str(best.target).rstrip(".")
            logger.debug(f"Resolved SRV {srv_name} to {target}:{best.port}")
            return target, best.port

        except Exception as e:
            logger.error(f"Failed to resolve SRV record {srv_name}: {e}")
            # Fallback to configured host/port
            return host, self.port

    async def _run_client(self, metadata: Dict[str, Any], vhost: str):
        """
        Main loop for maintaining connection and consuming messages.
        """
        adapter = TypeAdapter(Message)
        exchange_name = metadata["exchange"]
        routing_key = metadata["routing_key"]
        queue_name = metadata["queue_name"]
        durable = metadata.get("durable", False)
        auto_delete = metadata.get("auto_delete", True)

        while True:
            try:
                # Resolve host/port if using SRV
                connect_host = self.host
                connect_port = self.port

                if self.use_srv:
                    connect_host, connect_port = await self._resolve_srv(self.host, self.use_tls)

                logger.info(
                    f"Connecting to RabbitMQ at {connect_host}:{connect_port} "
                    f"(vhost: {vhost}, TLS: {self.use_tls}, Verify: {self.verify_ssl})"
                )

                ssl_context = None
                if self.use_tls:
                    if self.verify_ssl:
                        ssl_context = ssl.create_default_context()
                    else:
                        ssl_context = ssl._create_unverified_context()

                self._connection = await aio_pika.connect_robust(
                    host=connect_host,
                    port=connect_port,
                    login=self.username,
                    password=self.password,
                    ssl=self.use_tls,
                    virtualhost=vhost,
                    ssl_context=ssl_context,
                )

                if not self._connection:
                    logger.error("Failed to establish RabbitMQ connection.")
                    continue

                async with self._connection:
                    self._channel = await self._connection.channel()

                    if not self._channel:
                        logger.error("Failed to create RabbitMQ channel.")
                        continue

                    # Set QoS to ensure fair distribution and not overload
                    await self._channel.set_qos(prefetch_count=10)

                    # Declare the queue
                    # We don't declare the exchange here, assuming Coordinator did it.
                    # But, if we try to bind to a non-existent exchange, it will fail.
                    # Let's assume Coordinator setup is correct.

                    queue = await self._channel.declare_queue(
                        queue_name,
                        durable=durable,
                        auto_delete=auto_delete,
                    )

                    # Bind the queue
                    await queue.bind(exchange_name, routing_key=routing_key)
                    logger.info(f"Bound queue {queue_name} to {exchange_name} with key {routing_key}")

                    # Start consuming
                    async with queue.iterator() as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                try:
                                    payload_str = message.body.decode()
                                    data = json.loads(payload_str)
                                    msg = adapter.validate_python(data)
                                    await self._message_queue.put(msg)
                                except Exception as e:
                                    logger.error(f"Error parsing RabbitMQ message: {e}")

            except asyncio.CancelledError:
                logger.info("RabbitMQ client loop cancelled.")
                break
            except Exception as e:
                logger.error(f"RabbitMQ connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

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

        if self._connection:
            await self._connection.close()
            self._connection = None

    async def listen(self) -> AsyncIterator[BaseMessage]:
        """
        Yield messages from the internal queue.
        """
        while True:
            msg = await self._message_queue.get()
            yield msg
