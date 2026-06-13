import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.models import AuthenticatedIdentity, ComponentHealth, JobRequestMessage, JobRequestPayload
from dffmpeg.coordinator.transports.http_polling import HTTPPollingTransport


@pytest.fixture
def mock_app():
    app = FastAPI()
    app.state.shutting_down = False
    app.state.db = MagicMock()
    app.state.db.messages = AsyncMock()
    app.state.db.messages.retrieve_messages.return_value = []
    app.state.db.workers = AsyncMock()
    app.state.db.workers.get_worker.return_value = None
    app.state.db.jobs = AsyncMock()
    app.state.db.jobs.get_job.return_value = None
    # Mock config
    mock_config = MagicMock()
    mock_config.transports.get_transport_config.return_value = {}
    app.state.config = mock_config
    return app


@pytest.fixture
def transport(mock_app):
    return HTTPPollingTransport(app=mock_app)


@pytest.fixture
def identity():
    return AuthenticatedIdentity(client_id="test_client", role="worker", hmac_key=RequestSigner.generate_key())


@pytest.mark.asyncio
async def test_poll_loop_timeout(transport, identity):
    """
    Test that _poll_loop correctly times out and returns empty messages when no messages arrive.
    """
    # Set wait to a very small amount
    result = await transport._poll_loop(identity, wait=0.1)

    assert result == {"messages": []}
    assert identity.client_id not in transport._recipient_waiters


@pytest.mark.asyncio
async def test_poll_loop_immediate_return(transport, identity, mock_app):
    """
    Test that _poll_loop returns immediately if messages are already in DB.
    """
    msg = JobRequestMessage(
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_app.state.db.messages.retrieve_messages.return_value = [msg]

    result = await transport._poll_loop(identity, wait=5)

    assert result == {"messages": [msg]}
    assert identity.client_id not in transport._recipient_waiters


@pytest.mark.asyncio
async def test_stream_loop_keepalive(transport, identity):
    """
    Test that _stream_loop yields a keep-alive line when timeout occurs.
    """
    generator = transport._stream_loop(identity, wait=0.1)

    # We expect the first iteration to yield a newline after 0.1s
    line = await generator.__anext__()
    assert line == "\n"

    # Force shutdown so the generator closes
    await transport.drain()

    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_stream_loop_messages(transport, identity, mock_app):
    """
    Test that _stream_loop yields valid JSON when messages arrive.
    """
    msg = JobRequestMessage(
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    # Set up retrieve_messages to return the message on the first call, and empty on subsequent calls
    mock_app.state.db.messages.retrieve_messages.side_effect = [[msg], []]

    generator = transport._stream_loop(identity, wait=0.1)

    # The first yield should be the message JSON
    line = await generator.__anext__()

    data = json.loads(line)
    assert "messages" in data
    assert len(data["messages"]) == 1
    assert data["messages"][0]["message_id"] == str(msg.message_id)

    # The next yield should be a keepalive after 0.1s
    line2 = await generator.__anext__()
    assert line2 == "\n"

    await transport.drain()
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_send_message_wakes_poll(transport, identity, mock_app):
    """
    Test that send_message triggers the event and wakes up a waiting poll.
    """
    # Start the poll task in the background
    poll_task = asyncio.create_task(transport._poll_loop(identity, wait=5.0))

    # Give the task a tiny bit of time to register the event
    await asyncio.sleep(0.01)

    assert identity.client_id in transport._recipient_waiters
    assert len(transport._recipient_waiters[identity.client_id]) == 1

    # Now simulate a message arriving in the DB
    msg = JobRequestMessage(
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_app.state.db.messages.retrieve_messages.return_value = [msg]

    # Notify via send_message
    await transport.send_message(msg)

    # The poll loop should wake up, call retrieve_messages, and return the msg
    result = await asyncio.wait_for(poll_task, timeout=1.0)

    assert result == {"messages": [msg]}
    assert identity.client_id not in transport._recipient_waiters


@pytest.mark.asyncio
async def test_drain_wakes_all(transport, identity):
    """
    Test that drain() sets all waiting events so they can exit.
    """
    # Start a poll loop that will wait for 5 seconds
    poll_task = asyncio.create_task(transport._poll_loop(identity, wait=5.0))
    await asyncio.sleep(0.01)

    # Simulate a shutdown
    await transport.drain()

    # The poll loop should wake up and return empty messages immediately because _draining=True
    result = await asyncio.wait_for(poll_task, timeout=1.0)
    assert result == {"messages": []}


@pytest.mark.asyncio
async def test_poll_loop_proxy_rabbitmq(identity, mock_app):
    """
    Test that _poll_loop cleanly proxies through RabbitMQClientTransport if backend_transport is set.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")

    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)

    # Mock backend server transport and client instance
    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {"routing_key": "test_rk"}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"rabbitmq": mock_backend}

    msg = JobRequestMessage(
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_client.receive.return_value = msg

    result = await transport._poll_loop(identity, wait=1)

    assert result == {"messages": [msg]}
    mock_client_cls.assert_called_once()
    mock_client.connect.assert_called_once_with({"routing_key": "test_rk"})
    mock_client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_stream_loop_proxy_mqtt(identity, mock_app):
    """
    Test that _stream_loop cleanly proxies through MQTTClientTransport if backend_transport is set.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="mqtt")

    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)

    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {"topic": "test_topic"}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"mqtt": mock_backend}

    msg = JobRequestMessage(
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_client.receive.return_value = msg

    generator = transport._stream_loop(identity, wait=1)

    # First yield should be the proxied message
    line = await generator.__anext__()
    data = json.loads(line)
    assert len(data["messages"]) == 1
    assert data["messages"][0]["message_id"] == str(msg.message_id)

    # Drain should trigger exit
    await transport.drain()

    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()

    mock_client_cls.assert_called_once()
    mock_client.connect.assert_called_once_with({"topic": "test_topic"})
    mock_client.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_send_message_proxy(mock_app):
    """
    Test that send_message directly proxies to the backend transport if configured.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")

    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {"routing_key": "test"}
    mock_backend.send_message = AsyncMock()
    mock_app.state.transports = {"rabbitmq": mock_backend}

    msg = JobRequestMessage(
        recipient_id="test",
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    await transport.send_message(msg)

    mock_backend.send_message.assert_called_once_with(msg, transport_metadata={"routing_key": "test"})


@pytest.mark.asyncio
async def test_health_check_proxy(mock_app):
    """
    Test that health_check delegates to the backend transport if configured.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")

    mock_backend = AsyncMock()
    mock_backend.health_check.return_value = ComponentHealth(status="unhealthy", detail="Broker offline")
    mock_app.state.transports = {"rabbitmq": mock_backend}

    health = await transport.health_check()
    assert health.status == "unhealthy"
    assert "Backed by rabbitmq: Broker offline" in health.detail
