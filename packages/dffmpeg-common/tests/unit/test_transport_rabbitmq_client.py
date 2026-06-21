import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.common.transports.rabbitmq import RabbitMQClientTransport, RabbitMQMultiplexedClientTransport


@pytest.mark.asyncio
async def test_rabbitmq_multiplexed_client_connect_and_disconnect():
    mock_connection = AsyncMock()
    mock_channel = AsyncMock()
    mock_connection.channel.return_value = mock_channel

    transport = RabbitMQMultiplexedClientTransport(shared_connection=mock_connection)

    metadata = {
        "exchange": "dffmpeg.workers",
        "routing_key": "worker.1",
        "queue_name": "dffmpeg.worker.1",
    }

    # Connect should open channel, declare queue, bind, and consume
    await transport.connect(metadata)
    assert transport._listen_task is not None

    # Give the task a brief moment to run _connection_task
    await asyncio.sleep(0.01)

    mock_connection.channel.assert_called_once()
    mock_channel.set_qos.assert_called_once_with(prefetch_count=10)
    mock_channel.declare_queue.assert_called_once_with("dffmpeg.worker.1", durable=False, auto_delete=True)

    queue = mock_channel.declare_queue.return_value
    queue.bind.assert_called_once_with("dffmpeg.workers", routing_key="worker.1")
    queue.consume.assert_called_once_with(transport._on_message)

    # Disconnect should cancel listen task and close logical channel only
    mock_channel.is_closed = False
    await transport.disconnect()

    assert transport._listen_task is None
    mock_channel.close.assert_called_once()
    mock_connection.close.assert_not_called()


@pytest.mark.asyncio
async def test_rabbitmq_client_connect_and_listen():
    transport = RabbitMQClientTransport(host="localhost", port=5672)

    metadata = {
        "vhost": "/dffmpeg",
        "exchange": "dffmpeg.workers",
        "routing_key": "worker.1",
        "queue_name": "dffmpeg.worker.1",
    }

    # Mock _connection_task to avoid actual connection
    with patch.object(transport, "_connection_task", return_value=AsyncMock()) as mock_run:
        await transport.connect(metadata)
        assert transport._listen_task is not None
        mock_run.assert_called_once_with(metadata, "/dffmpeg")

    # Simulate a message arriving in the internal queue
    message = JobStatusMessage(recipient_id="client1", job_id=ULID(), payload=JobStatusPayload(status="running"))
    await transport._message_queue.put(message)

    # Test listen
    async for msg in transport.listen():
        assert msg == message
        break  # Only one message in queue

    # Test disconnect
    await transport.disconnect()
    assert transport._listen_task is None
