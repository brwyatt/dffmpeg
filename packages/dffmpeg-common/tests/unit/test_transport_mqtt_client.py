import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.common.transports.mqtt import MQTTClientTransport


@pytest.mark.asyncio
async def test_mqtt_client_connect_and_listen():
    transport = MQTTClientTransport(host="localhost", port=1883)

    # Mock _run_client to avoid actual connection
    with patch.object(transport, "_run_client", return_value=AsyncMock()) as mock_run:
        await transport.connect({"topic": "/test/topic"})
        assert transport._listen_task is not None
        mock_run.assert_called_once_with("/test/topic")

    # Simulate a message arriving in the queue
    message = JobStatusMessage(recipient_id="client1", job_id=ULID(), payload=JobStatusPayload(status="running"))
    await transport._message_queue.put(message)

    # Test listen
    async for msg in transport.listen():
        assert msg == message
        break  # Only one message in queue

    # Test disconnect
    await transport.disconnect()
    assert transport._listen_task is None


@pytest.mark.asyncio
async def test_mqtt_client_run_client_logic():
    transport = MQTTClientTransport(host="localhost", port=1883)
    topic = "/test/topic"

    mock_client = AsyncMock()
    mock_client.messages = AsyncMock()

    # Create a mock message for the iterator
    mock_mqtt_message = MagicMock()
    job_id = ULID()
    message_data = {
        "message_id": str(ULID()),
        "recipient_id": "client1",
        "job_id": str(job_id),
        "message_type": "job_status",
        "payload": {"status": "running"},
        "timestamp": "2026-02-08T19:00:00Z",
    }
    mock_mqtt_message.payload = json.dumps(message_data).encode()

    # Setup the messages iterator to yield one message then raise CancelledError
    async def mock_messages_gen():
        yield mock_mqtt_message
        raise asyncio.CancelledError()

    mock_client.messages.__aiter__.side_effect = mock_messages_gen

    with patch("aiomqtt.Client", return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_client))):
        try:
            await transport._run_client(topic)
        except asyncio.CancelledError:
            pass

    # Verify the message reached the queue
    queued_msg = await transport._message_queue.get()
    assert str(queued_msg.job_id) == str(job_id)
    assert queued_msg.payload.status == "running"
