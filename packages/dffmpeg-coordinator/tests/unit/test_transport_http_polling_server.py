# pyright: reportPrivateUsage = false

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

    mock_backend.send_message.assert_called_once_with(msg, transport_metadata={"routing_key": "test"}, mark_sent=False)


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
    assert "Backed by rabbitmq: Broker offline" == health.detail


@pytest.mark.asyncio
async def test_setup_raises_on_missing_backend(mock_app):
    """
    Test that setup() raises a ValueError when the backend transport is configured but missing.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")

    # Mock transports manager on state
    mock_transports_mgr = MagicMock()
    mock_transports_mgr.loaded_transports = {}  # Empty loaded transports
    mock_app.state.transports = mock_transports_mgr

    with pytest.raises(ValueError, match="http_polling configured to use backend transport"):
        await transport.setup()


@pytest.mark.asyncio
async def test_setup_succeeds_with_loaded_backend(mock_app):
    """
    Test that setup() succeeds when the backend transport is configured and loaded.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")

    # Mock transports manager on state
    mock_transports_mgr = MagicMock()
    mock_transports_mgr.loaded_transports = {"rabbitmq": MagicMock()}
    mock_app.state.transports = mock_transports_mgr

    # Should not raise ValueError
    await transport.setup()


@pytest.mark.asyncio
async def test_hybrid_poll_no_messages(identity, mock_app):
    """
    Scenario 1: No Messages anywhere.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")
    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)
    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"rabbitmq": mock_backend}

    mock_app.state.db.messages.retrieve_messages.return_value = []

    # Define an async function that sleeps to trigger the wait timeout
    async def mock_sleep(*args, **kwargs):
        await asyncio.sleep(10)

    mock_client.receive.side_effect = mock_sleep

    result = await transport._poll_loop(identity, last_message_id=ULID(), wait=0.1)
    assert result == {"messages": []}


@pytest.mark.asyncio
async def test_hybrid_poll_db_gaps_only(identity, mock_app):
    """
    Scenario 2: DB contains a message, but the broker queue is currently empty.
    The coordinator should fetch Y from the DB and ignore the broker queue entirely for this poll.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")
    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)
    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"rabbitmq": mock_backend}

    last_id = ULID()
    msg_y = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_app.state.db.messages.retrieve_messages.return_value = [msg_y]

    result = await transport._poll_loop(identity, last_message_id=last_id, wait=1)
    assert result == {"messages": [msg_y]}
    # _backend_client should not have been connected because we successfully drained DB history
    mock_client_cls.assert_not_called()


@pytest.mark.asyncio
async def test_hybrid_stream_duplicate_overlap(identity, mock_app):
    """
    Scenario 3: DB has message Y, and broker queue also delivers message Y.
    The stream delivers Y from the DB history, and when the broker queue subsequently delivers Y,
    it is discarded as a duplicate inside the same stream connection.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")
    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)
    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"rabbitmq": mock_backend}

    last_id = ULID()
    msg_y = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    # DB has msg_y
    mock_app.state.db.messages.retrieve_messages.side_effect = [[msg_y], []]

    # Configure mock client receive to yield msg_y, then hang (sleep) to simulate idle state
    msg_queue = asyncio.Queue()
    await msg_queue.put(msg_y)

    async def mock_receive():
        try:
            return await msg_queue.get()
        except asyncio.CancelledError:
            raise

    mock_client.receive.side_effect = mock_receive

    generator = transport._stream_loop(identity, last_message_id=last_id, wait=0.1)

    # 1. The first yield should be the DB drained message Y
    line1 = await generator.__anext__()
    data1 = json.loads(line1)
    assert len(data1["messages"]) == 1
    assert data1["messages"][0]["message_id"] == str(msg_y.message_id)

    # 2. Since msg_y is already drained and tracker is updated,
    # the broker-delivered copy of msg_y is discarded.
    # The next yield should be a keepalive timeout newline instead of msg_y again.
    line2 = await generator.__anext__()
    assert line2 == "\n"

    await transport.drain()
    with pytest.raises(StopAsyncIteration):
        await generator.__anext__()


@pytest.mark.asyncio
async def test_hybrid_poll_interleaved(identity, mock_app):
    """
    Scenario 4: DB has Y, broker queue has Z (where Z > Y).
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")
    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)
    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"rabbitmq": mock_backend}

    last_id = ULID()
    msg_y = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    # Ensure Z > Y by generating Z after Y
    msg_z = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    # First poll drains Y
    mock_app.state.db.messages.retrieve_messages.side_effect = [[msg_y], []]
    result1 = await transport._poll_loop(identity, last_message_id=last_id, wait=1)
    assert result1 == {"messages": [msg_y]}

    # Second poll gets Z from the broker (DB is empty)
    mock_client.receive.return_value = msg_z
    result2 = await transport._poll_loop(identity, last_message_id=last_id, wait=1)
    assert result2 == {"messages": [msg_z]}


@pytest.mark.asyncio
async def test_hybrid_poll_no_last_message_id(identity, mock_app):
    """
    Scenario 5: No last_message_id passed.
    The coordinator should query the DB for unsent messages (retrieve_messages(last_message_id=None)),
    and since none are found, retrieve msg_z from broker queue.
    """
    transport = HTTPPollingTransport(app=mock_app, backend_transport="rabbitmq")
    mock_client = AsyncMock()
    mock_client_cls = MagicMock(return_value=mock_client)
    mock_backend = MagicMock()
    mock_backend.get_metadata.return_value = {}
    mock_backend.get_client_transport_class.return_value = mock_client_cls
    mock_app.state.transports = {"rabbitmq": mock_backend}

    msg_z = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_client.receive.return_value = msg_z

    result = await transport._poll_loop(identity, last_message_id=None, wait=1)
    assert result == {"messages": [msg_z]}

    # DB retrieve_messages should have been called with last_message_id=None to check for unsent messages
    mock_app.state.db.messages.retrieve_messages.assert_called_once_with(
        recipient_id=identity.client_id,
        last_message_id=None,
        job_id=None,
    )


@pytest.mark.asyncio
async def test_drain_db_history_none_last_id_with_unsent_messages(identity, mock_app):
    """
    Test that _drain_db_history queries the DB with last_message_id=None and returns unsent messages if found.
    """
    transport = HTTPPollingTransport(app=mock_app)
    msg1 = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    mock_app.state.db.messages.retrieve_messages.return_value = [msg1]

    max_id, messages = await transport._drain_db_history(mock_app.state.db.messages, identity, last_message_id=None)
    assert max_id == msg1.message_id
    assert messages == [msg1]
    mock_app.state.db.messages.retrieve_messages.assert_called_once_with(
        recipient_id=identity.client_id,
        last_message_id=None,
        job_id=None,
    )


@pytest.mark.asyncio
async def test_drain_db_history_empty_db(identity, mock_app):
    """
    Test that _drain_db_history returns last_message_id and empty list if DB has no messages.
    """
    transport = HTTPPollingTransport(app=mock_app)
    last_id = ULID()
    mock_app.state.db.messages.retrieve_messages.return_value = []

    max_id, messages = await transport._drain_db_history(mock_app.state.db.messages, identity, last_message_id=last_id)
    assert max_id == last_id
    assert messages == []
    mock_app.state.db.messages.retrieve_messages.assert_called_once_with(
        recipient_id=identity.client_id,
        last_message_id=last_id,
        job_id=None,
    )


@pytest.mark.asyncio
async def test_drain_db_history_with_messages(identity, mock_app):
    """
    Test that _drain_db_history returns the maximum message_id and the messages.
    """
    transport = HTTPPollingTransport(app=mock_app)
    last_id = ULID()
    msg1 = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )
    msg2 = JobRequestMessage(
        message_id=ULID(),
        recipient_id=identity.client_id,
        job_id=ULID(),
        payload=JobRequestPayload(job_id=str(ULID()), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    # Ensure msg2 > msg1
    assert msg2.message_id > msg1.message_id

    mock_app.state.db.messages.retrieve_messages.return_value = [msg1, msg2]

    max_id, messages = await transport._drain_db_history(mock_app.state.db.messages, identity, last_message_id=last_id)
    assert max_id == msg2.message_id
    assert messages == [msg1, msg2]
