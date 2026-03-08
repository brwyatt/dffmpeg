from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dffmpeg.common.transports.utils.rabbitmq import RabbitMQConnectionManager


@pytest.mark.asyncio
async def test_rabbitmq_connection_manager_callbacks():
    manager = RabbitMQConnectionManager()

    # Test reconnect callback
    manager.is_connected.clear()

    mock_conn = MagicMock()
    mock_conn.url = "amqp://test"

    # It should accept arbitrary positional and keyword args from aio_pika
    manager._on_connection_reconnect(None, connection=mock_conn, extra_arg="test")
    assert manager.is_connected.is_set()

    # Test close callback (unexpected)
    manager._on_connection_close(None, exc=Exception("test error"), extra_arg="test")
    assert not manager.is_connected.is_set()


@pytest.mark.asyncio
async def test_rabbitmq_connection_manager_shutdown_suppression():
    manager = RabbitMQConnectionManager()

    manager.is_connected.set()
    manager._closing = True

    # Simulate a close with an exception
    with patch("dffmpeg.common.transports.utils.rabbitmq.logger.warning") as mock_warn:
        manager._on_connection_close(None, exc=Exception("test error"))

        # It shouldn't log a warning because _closing is True
        mock_warn.assert_not_called()

    assert not manager.is_connected.is_set()


@pytest.mark.asyncio
async def test_rabbitmq_connection_manager_resolve_srv():
    manager = RabbitMQConnectionManager(host="rabbitmq.example.com", use_srv=True, use_tls=True)

    # Mock SRV response
    srv_record = MagicMock()
    srv_record.target = "node1.rabbitmq.example.com."
    srv_record.port = 5671
    srv_record.priority = 10
    srv_record.weight = 10

    with patch("dns.asyncresolver.resolve", new_callable=AsyncMock) as mock_resolve:
        mock_resolve.return_value = [srv_record]

        target, port = await manager._resolve_srv()

        assert target == "node1.rabbitmq.example.com"
        assert port == 5671
        mock_resolve.assert_called_once_with("_amqps._tcp.rabbitmq.example.com", "SRV")


@pytest.mark.asyncio
async def test_rabbitmq_connection_manager_close():
    manager = RabbitMQConnectionManager()
    manager.is_connected.set()

    mock_conn = AsyncMock()
    mock_conn.is_closed = False
    manager.connection = mock_conn

    await manager.close()

    assert manager._closing is True
    assert not manager.is_connected.is_set()
    mock_conn.close.assert_awaited_once()
    assert manager.connection is None
