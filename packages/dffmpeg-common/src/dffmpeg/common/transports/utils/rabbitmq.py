import asyncio
import logging
import ssl
from typing import Any, Optional, Tuple

import aio_pika
import dns.asyncresolver

logger = logging.getLogger(__name__)


class RabbitMQConnectionManager:
    """
    Manages robust RabbitMQ connections including SRV resolution, TLS config,
    and handling reconnect events automatically.
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
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.use_srv = use_srv
        self.verify_ssl = verify_ssl

        self.connection: Optional[aio_pika.abc.AbstractConnection] = None
        self.is_connected = asyncio.Event()
        self.is_connected.clear()
        self._closing = False

    async def _resolve_srv(self) -> Tuple[str, int]:
        """
        Resolve SRV record to get the actual host and port.
        """
        prefix = "_amqps._tcp" if self.use_tls else "_amqp._tcp"
        srv_name = f"{prefix}.{self.host}"

        try:
            answers = await dns.asyncresolver.resolve(srv_name, "SRV")

            sorted_answers = sorted(answers, key=lambda x: (x.priority, -x.weight))
            best = sorted_answers[0]

            target = str(best.target).rstrip(".")
            logger.debug(f"Resolved SRV {srv_name} to {target}:{best.port}")
            return target, best.port

        except Exception as e:
            logger.error(f"Failed to resolve SRV record {srv_name}: {e}")
            # Fallback to configured host/port
            return self.host, self.port

    def _on_connection_close(self, sender: Any, exc: Optional[BaseException] = None, *args, **kwargs) -> None:
        """Callback triggered when the RabbitMQ connection is closed/dropped."""
        self.is_connected.clear()
        if self._closing:
            return  # Suppress logs during intended shutdown

        if exc:
            logger.warning(f"RabbitMQ connection closed unexpectedly: {exc}")
        else:
            logger.info("RabbitMQ connection closed cleanly.")

    def _on_connection_reconnect(
        self, sender: Any, connection: Optional[aio_pika.abc.AbstractConnection] = None, *args, **kwargs
    ) -> None:
        """Callback triggered when aio_pika successfully reconnects."""
        if connection:
            logger.info(f"RabbitMQ successfully reconnected to {connection.url}")
        else:
            logger.info("RabbitMQ successfully reconnected.")
        self.is_connected.set()

    async def connect(self, vhost: str = "/") -> aio_pika.abc.AbstractConnection:
        """
        Setup robust connection and hook up callbacks.
        """
        connect_host = self.host
        connect_port = self.port

        if self.use_srv:
            connect_host, connect_port = await self._resolve_srv()

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

        # Using connect_robust handles reconnects, channel restoration, queue/binding re-declaration automatically!
        self.connection = await aio_pika.connect_robust(
            host=connect_host,
            port=connect_port,
            login=self.username,
            password=self.password,
            ssl=self.use_tls,
            virtualhost=vhost,
            ssl_context=ssl_context,
        )

        # Register connection lifecycle callbacks for observability
        self.connection.close_callbacks.add(self._on_connection_close)
        self.connection.reconnect_callbacks.add(self._on_connection_reconnect)

        self.is_connected.set()
        return self.connection

    async def close(self):
        """
        Cleanly close the RabbitMQ connection.
        """
        self._closing = True
        self.is_connected.clear()

        if self.connection and not self.connection.is_closed:
            try:
                await self.connection.close()
            except Exception as close_err:
                logger.error(f"Error closing RabbitMQ connection: {close_err}")

        self.connection = None
