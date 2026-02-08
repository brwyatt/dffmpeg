from unittest.mock import AsyncMock, MagicMock

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.coordinator.transports.mqtt import MQTTServerTransport


@pytest.mark.asyncio
async def test_mqtt_server_get_metadata():
    app = MagicMock()
    transport = MQTTServerTransport(app=app, topic_prefix="/dffmpeg")

    # Worker metadata
    metadata = transport.get_metadata(client_id="worker1")
    assert metadata == {"topic": "dffmpeg/workers/worker1"}

    # Job metadata
    job_id = ULID()
    metadata = transport.get_metadata(client_id="client1", job_id=job_id)
    assert metadata == {"topic": f"dffmpeg/jobs/client1/{job_id}"}


@pytest.mark.asyncio
async def test_mqtt_server_health_check():
    app = MagicMock()
    transport = MQTTServerTransport(app=app)

    # Initial state
    health = await transport.health_check()
    assert health.status == "unhealthy"

    # Mock connected state
    transport._client = MagicMock()
    transport._connect_event.set()
    health = await transport.health_check()
    assert health.status == "online"

    # Mock disconnected state
    transport._connect_event.clear()
    health = await transport.health_check()
    assert health.status == "unhealthy"


@pytest.mark.asyncio
async def test_mqtt_server_send_message():
    app = MagicMock()
    transport = MQTTServerTransport(app=app)
    mock_client = AsyncMock()
    transport._client = mock_client

    message = JobStatusMessage(recipient_id="client1", job_id=ULID(), payload=JobStatusPayload(status="running"))

    # No topic in metadata
    result = await transport.send_message(message, transport_metadata={})
    assert result is False

    # Successful send
    result = await transport.send_message(message, transport_metadata={"topic": "/test/topic"})
    assert result is True
    mock_client.publish.assert_called_once_with("/test/topic", message.model_dump_json(), qos=1)


@pytest.mark.asyncio
async def test_mqtt_server_send_message_no_client():
    app = MagicMock()
    transport = MQTTServerTransport(app=app)
    transport._client = None

    message = JobStatusMessage(recipient_id="client1", job_id=ULID(), payload=JobStatusPayload(status="running"))

    result = await transport.send_message(message, transport_metadata={"topic": "/test/topic"})
    assert result is False
