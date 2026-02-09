from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.common.transports.rabbitmq import RabbitMQClientTransport


@pytest.mark.asyncio
async def test_rabbitmq_client_connect_and_listen():
    transport = RabbitMQClientTransport(host="localhost", port=5672)

    metadata = {
        "vhost": "/dffmpeg",
        "exchange": "dffmpeg.workers",
        "routing_key": "worker.1",
        "queue_name": "dffmpeg.worker.1",
    }

    # Mock _run_client to avoid actual connection
    with patch.object(transport, "_run_client", return_value=AsyncMock()) as mock_run:
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


@pytest.mark.asyncio
async def test_rabbitmq_client_resolve_srv():
    transport = RabbitMQClientTransport(host="rabbitmq.example.com", use_srv=True)

    # Mock SRV response
    srv_record = MagicMock()
    srv_record.target = "node1.rabbitmq.example.com."
    srv_record.port = 5671
    srv_record.priority = 10
    srv_record.weight = 10

    with patch("dns.asyncresolver.resolve", new_callable=AsyncMock) as mock_resolve:
        mock_resolve.return_value = [srv_record]

        target, port = await transport._resolve_srv("rabbitmq.example.com", use_tls=True)

        assert target == "node1.rabbitmq.example.com"
        assert port == 5671
        mock_resolve.assert_called_once_with("_amqps._tcp.rabbitmq.example.com", "SRV")
