import asyncio
import logging
from typing import Any, Dict, Optional

import aio_pika
from ulid import ULID

from dffmpeg.common.models import BaseMessage, ComponentHealth
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

    async def setup(self):
        """
        Connect to RabbitMQ and declare exchanges.
        """
        self._loop_task = asyncio.create_task(self._connection_task())

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

    async def send_message(self, message: BaseMessage, transport_metadata: Optional[Dict[str, Any]] = None) -> bool:
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
