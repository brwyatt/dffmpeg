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
    transport._manager.is_connected.set()
    health = await transport.health_check()
    assert health.status == "online"

    # Mock disconnected state
    transport._manager.is_connected.clear()
    health = await transport.health_check()
    assert health.status == "unhealthy"


@pytest.mark.asyncio
async def test_rabbitmq_server_send_message():
    app = MagicMock()
    app.state.db.messages.update_message_sent_at = AsyncMock()
    transport = RabbitMQServerTransport(app=app)

    mock_exchange = AsyncMock()
    transport._workers_exchange = mock_exchange
    transport._manager.is_connected.set()
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

    # Verify update_message_sent_at called
    app.state.db.messages.update_message_sent_at.assert_called_once_with(str(message.message_id))


@pytest.mark.asyncio
async def test_rabbitmq_server_send_message_no_mark_sent():
    app = MagicMock()
    app.state.db.messages.update_message_sent_at = AsyncMock()
    transport = RabbitMQServerTransport(app=app)

    mock_exchange = AsyncMock()
    transport._workers_exchange = mock_exchange
    transport._manager.is_connected.set()
    transport._channel = MagicMock()

    message = JobStatusMessage(recipient_id="client1", job_id=ULID(), payload=JobStatusPayload(status="running"))

    metadata = {"exchange": "dffmpeg.workers", "routing_key": "worker.1"}
    result = await transport.send_message(message, transport_metadata=metadata, mark_sent=False)
    assert result is True

    # Verify update_message_sent_at was NOT called when mark_sent=False
    app.state.db.messages.update_message_sent_at.assert_not_called()


@pytest.mark.asyncio
async def test_rabbitmq_server_create_client_transport_active_connection():
    app = MagicMock()
    transport = RabbitMQServerTransport(app=app)

    # Setup active connection
    mock_conn = AsyncMock()
    transport._manager.connection = mock_conn
    transport._channel = AsyncMock()
    transport._manager.is_connected.set()

    client = transport.create_client_transport()

    from dffmpeg.coordinator.transports.rabbitmq import RabbitMQProxyClientTransport

    assert isinstance(client, RabbitMQProxyClientTransport)
    assert client._server_transport == transport


@pytest.mark.asyncio
async def test_rabbitmq_server_create_client_transport_active_connection_disabled_muxing():
    app = MagicMock()
    transport = RabbitMQServerTransport(app=app, enable_multiplexing=False)

    # Setup active connection
    mock_conn = AsyncMock()
    transport._manager.connection = mock_conn
    transport._manager.is_connected.set()

    client = transport.create_client_transport()

    from dffmpeg.common.transports.rabbitmq import RabbitMQClientTransport
    from dffmpeg.coordinator.transports.rabbitmq import RabbitMQProxyClientTransport

    assert isinstance(client, RabbitMQClientTransport)
    assert not isinstance(client, RabbitMQProxyClientTransport)


@pytest.mark.asyncio
async def test_rabbitmq_server_create_client_transport_inactive_connection():
    app = MagicMock()
    transport = RabbitMQServerTransport(app=app)

    # Ensure connection is cleared
    transport._manager.connection = None
    transport._manager.is_connected.clear()

    client = transport.create_client_transport()

    from dffmpeg.common.transports.rabbitmq import RabbitMQClientTransport
    from dffmpeg.coordinator.transports.rabbitmq import RabbitMQProxyClientTransport

    assert isinstance(client, RabbitMQClientTransport)
    assert not isinstance(client, RabbitMQProxyClientTransport)


@pytest.mark.asyncio
async def test_rabbitmq_proxy_client_connect_and_disconnect():
    mock_server_transport = AsyncMock()
    from dffmpeg.coordinator.transports.rabbitmq import RabbitMQProxyClientTransport

    transport = RabbitMQProxyClientTransport(server_transport=mock_server_transport)

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
