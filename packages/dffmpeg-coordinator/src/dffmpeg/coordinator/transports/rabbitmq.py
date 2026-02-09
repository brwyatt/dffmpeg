import asyncio
import logging
import ssl
from typing import Any, Dict, Optional, Tuple

import aio_pika
import dns.asyncresolver
from ulid import ULID

from dffmpeg.common.models import BaseMessage, ComponentHealth
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
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.use_srv = use_srv
        self.verify_ssl = verify_ssl
        self.vhost = vhost
        self.workers_exchange_name = workers_exchange
        self.jobs_exchange_name = jobs_exchange

        self._connection: Optional[aio_pika.abc.AbstractConnection] = None
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._workers_exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._jobs_exchange: Optional[aio_pika.abc.AbstractExchange] = None
        self._connect_event = asyncio.Event()
        self._loop_task: Optional[asyncio.Task] = None

    async def setup(self):
        """
        Connect to RabbitMQ and declare exchanges.
        """
        self._loop_task = asyncio.create_task(self._server_loop())

    async def _resolve_srv(self, host: str, use_tls: bool) -> Tuple[str, int]:
        """
        Resolve SRV record to get the actual host and port.
        """
        prefix = "_amqps._tcp" if use_tls else "_amqp._tcp"
        srv_name = f"{prefix}.{host}"

        try:
            # Use async resolver
            answers = await dns.asyncresolver.resolve(srv_name, "SRV")

            sorted_answers = sorted(answers, key=lambda x: (x.priority, -x.weight))
            best = sorted_answers[0]
            target = str(best.target).rstrip(".")
            logger.debug(f"Resolved SRV {srv_name} to {target}:{best.port}")
            return target, best.port
        except Exception as e:
            logger.error(f"Failed to resolve SRV record {srv_name}: {e}")
            return host, self.port

    async def _server_loop(self):
        """
        Background task to maintain the RabbitMQ connection.
        """
        while True:
            try:
                connect_host = self.host
                connect_port = self.port

                if self.use_srv:
                    connect_host, connect_port = await self._resolve_srv(self.host, self.use_tls)

                logger.info(
                    f"Connecting to RabbitMQ at {connect_host}:{connect_port} "
                    f"(vhost: {self.vhost}, TLS: {self.use_tls}, Verify: {self.verify_ssl})"
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
                    virtualhost=self.vhost,
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

                    # Declare Exchanges
                    self._workers_exchange = await self._channel.declare_exchange(
                        self.workers_exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
                    )
                    self._jobs_exchange = await self._channel.declare_exchange(
                        self.jobs_exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
                    )

                    self._connect_event.set()
                    logger.info(
                        f"Connected to RabbitMQ and declared exchanges: "
                        f"{self.workers_exchange_name}, {self.jobs_exchange_name}"
                    )

                    # Keep connection alive
                    while True:
                        await asyncio.sleep(1)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connection = None
                self._channel = None
                self._workers_exchange = None
                self._jobs_exchange = None
                self._connect_event.clear()
                logger.error(f"RabbitMQ server error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    async def send_message(self, message: BaseMessage, transport_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Publish a message to a RabbitMQ exchange.
        """
        if not self._channel or not self._connect_event.is_set():
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
        if self._connection and self._connect_event.is_set() and not self._connection.is_closed:
            return ComponentHealth(status="online")
        else:
            return ComponentHealth(status="unhealthy", detail="Not connected to RabbitMQ")
