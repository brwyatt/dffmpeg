import asyncio
import json
import logging
from typing import Any, AsyncIterator, Dict, Optional

import aiomqtt
from pydantic import TypeAdapter

from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.common.transports.base import BaseClientTransport

logger = logging.getLogger(__name__)


class MQTTClientTransport(BaseClientTransport):
    """
    MQTT transport implementation for workers and clients.
    Listens for messages on a specific MQTT topic.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = False,
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls

        self._client: Optional[aiomqtt.Client] = None
        self._message_queue: asyncio.Queue[BaseMessage] = asyncio.Queue()
        self._listen_task: Optional[asyncio.Task] = None

    async def connect(self, metadata: Dict[str, Any]):
        """
        Connect to the MQTT broker and subscribe to the topic.
        """
        topic = metadata.get("topic")
        if not topic:
            raise ValueError("No topic provided in MQTT metadata")

        logger.info(f"Connecting to MQTT broker at {self.host}:{self.port}, subscribing to {topic}")

        # Start background listener
        self._listen_task = asyncio.create_task(self._run_client(topic))

    async def _run_client(self, topic: str):
        """
        Maintains the MQTT connection and puts messages into the queue.
        """
        adapter = TypeAdapter(Message)
        while True:
            try:
                logger.info(
                    f"Connecting to MQTT broker at {self.host}:{self.port} (TLS: {self.use_tls}, "
                    f"User: {self.username}), topic: {topic}"
                )
                async with aiomqtt.Client(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    tls_params=None if not self.use_tls else aiomqtt.TLSParameters(),
                ) as client:
                    self._client = client
                    logger.info(f"Subscribing to MQTT topic: {topic}")
                    await client.subscribe(topic, qos=1)
                    logger.info(f"Successfully subscribed to MQTT topic: {topic}")

                    async for message in client.messages:
                        try:
                            payload_str = message.payload.decode()
                            data = json.loads(payload_str)
                            # Use Pydantic to validate and tag the message
                            msg = adapter.validate_python(data)
                            await self._message_queue.put(msg)
                        except Exception as e:
                            logger.error(f"Error parsing MQTT message payload on topic {topic}: {e}")

            except aiomqtt.MqttError as e:
                logger.error(f"MQTT client error for topic {topic}: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.info(f"MQTT client loop for topic {topic} cancelled.")
                break
            except Exception as e:
                logger.error(f"Unexpected error in MQTT client loop for topic {topic}: {e}. Retrying in 5 seconds...")
                logger.exception(e)
                await asyncio.sleep(5)

    async def disconnect(self):
        """
        Disconnect from the MQTT broker.
        """
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None
        self._client = None

    async def listen(self) -> AsyncIterator[BaseMessage]:
        """
        Yield messages from the internal queue.
        """
        while True:
            msg = await self._message_queue.get()
            yield msg
