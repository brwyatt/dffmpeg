import asyncio
import logging
from typing import Any, Dict, Optional

import aiomqtt
from ulid import ULID

from dffmpeg.common.models import BaseMessage, ComponentHealth
from dffmpeg.coordinator.transports.base import BaseServerTransport

logger = logging.getLogger(__name__)


class MQTTServerTransport(BaseServerTransport):
    """
    MQTT transport implementation for the coordinator.
    Delivers messages to workers and clients via MQTT topics.
    """

    def __init__(
        self,
        *args,
        host: str = "localhost",
        port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = False,
        topic_prefix: str = "dffmpeg",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.topic_prefix = topic_prefix.strip("/")

        self._client: Optional[aiomqtt.Client] = None
        self._connect_event = asyncio.Event()

    async def setup(self):
        """
        Connect to the MQTT broker.
        """
        asyncio.create_task(self._client_loop())

    async def _client_loop(self):
        """
        Background task to maintain the MQTT connection.
        """
        while True:
            try:
                logger.info(
                    f"Connecting to MQTT broker at {self.host}:{self.port} (TLS: {self.use_tls}, User: {self.username})"
                )
                async with aiomqtt.Client(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    tls_params=None if not self.use_tls else aiomqtt.TLSParameters(),
                ) as client:
                    self._client = client
                    self._connect_event.set()
                    logger.info(f"Connected to MQTT broker at {self.host}:{self.port}")
                    # Keep the connection alive
                    while True:
                        await asyncio.sleep(1)
            except aiomqtt.MqttError as e:
                self._client = None
                self._connect_event.clear()
                logger.error(f"MQTT error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._client = None
                self._connect_event.clear()
                logger.error(f"Unexpected error in MQTT loop: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    async def send_message(self, message: BaseMessage, transport_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Publish a message to an MQTT topic.
        """
        if not self._client:
            logger.warning(f"MQTT client not connected, cannot send message {message.message_id}.")
            return False

        topic = transport_metadata.get("topic") if transport_metadata else None
        if not topic:
            logger.error(f"No topic provided in transport_metadata for message {message.message_id}")
            return False

        try:
            payload = message.model_dump_json()
            logger.info(f"Publishing message {message.message_id} to topic {topic}")
            await self._client.publish(topic, payload, qos=1)
            logger.debug(f"Published message {message.message_id} to topic {topic}")
            return True
        except aiomqtt.MqttError as e:
            logger.error(f"MQTT publish error for message {message.message_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing MQTT message {message.message_id}: {e}")
            return False

    def get_metadata(self, client_id: str, job_id: Optional[ULID] = None) -> Dict[str, Any]:
        """
        Generate the MQTT topic for a client or worker.
        """
        if job_id:
            # Topic for job status updates to client
            # The client_id here should be the requester_id
            topic = f"{self.topic_prefix}/jobs/{client_id}/{job_id}"
        else:
            # Topic for worker commands
            topic = f"{self.topic_prefix}/workers/{client_id}"

        return {
            "topic": topic,
        }

    async def health_check(self) -> ComponentHealth:
        """
        Check the health of the MQTT connection.
        """
        if self._client and self._connect_event.is_set():
            return ComponentHealth(status="online")
        else:
            return ComponentHealth(status="unhealthy", detail="Not connected to MQTT broker")
