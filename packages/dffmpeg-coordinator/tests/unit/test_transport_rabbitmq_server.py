from unittest.mock import AsyncMock, MagicMock

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.coordinator.transports.rabbitmq import RabbitMQServerTransport


@pytest.mark.asyncio
async def test_rabbitmq_server_get_metadata():
    app = MagicMock()
    transport = RabbitMQServerTransport(app=app, vhost="/dffmpeg")

    # Worker metadata
    metadata = transport.get_metadata(client_id="worker1")
    assert metadata["exchange"] == "dffmpeg.workers"
    assert metadata["routing_key"] == "worker.worker1"
    assert metadata["vhost"] == "/dffmpeg"

    # Job metadata
    job_id = ULID()
    metadata = transport.get_metadata(client_id="client1", job_id=job_id)
    assert metadata["exchange"] == "dffmpeg.jobs"
    assert metadata["routing_key"] == f"job.client1.{job_id}"


@pytest.mark.asyncio
async def test_rabbitmq_server_health_check():
    app = MagicMock()
    transport = RabbitMQServerTransport(app=app)

    # Initial state
    health = await transport.health_check()
    assert health.status == "unhealthy"

    # Mock connected state
    transport._connection = MagicMock()
    transport._connection.is_closed = False
    transport._connect_event.set()
    health = await transport.health_check()
    assert health.status == "online"

    # Mock disconnected state
    transport._connect_event.clear()
    health = await transport.health_check()
    assert health.status == "unhealthy"


@pytest.mark.asyncio
async def test_rabbitmq_server_send_message():
    app = MagicMock()
    transport = RabbitMQServerTransport(app=app)

    mock_exchange = AsyncMock()
    transport._workers_exchange = mock_exchange
    transport._connect_event.set()
    transport._channel = MagicMock()

    message = JobStatusMessage(recipient_id="client1", job_id=ULID(), payload=JobStatusPayload(status="running"))

    # Successful send
    metadata = {"exchange": "dffmpeg.workers", "routing_key": "worker.1"}
    result = await transport.send_message(message, transport_metadata=metadata)
    assert result is True

    mock_exchange.publish.assert_called_once()
    call_args = mock_exchange.publish.call_args
    assert call_args[1]["routing_key"] == "worker.1"
    # Verify body is bytes
    assert isinstance(call_args[0][0].body, bytes)
