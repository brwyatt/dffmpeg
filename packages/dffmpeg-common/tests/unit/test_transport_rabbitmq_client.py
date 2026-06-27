from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.common.transports.rabbitmq import RabbitMQClientTransport, RabbitMQMultiplexedClientTransport


@pytest.mark.asyncio
async def test_rabbitmq_multiplexed_client_connect_and_disconnect():
    mock_server_transport = AsyncMock()
    transport = RabbitMQMultiplexedClientTransport(server_transport=mock_server_transport)

    metadata = {
        "exchange": "dffmpeg.workers",
        "routing_key": "worker.1",
        "queue_name": "dffmpeg.worker.1",
    }

    # Connect should register multiplexed client
    await transport.connect(metadata)
    mock_server_transport.register_multiplex_client.assert_called_once_with(transport)

    # Disconnect should unregister multiplexed client
    await transport.disconnect()
    mock_server_transport.unregister_multiplex_client.assert_called_once_with(transport)


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
