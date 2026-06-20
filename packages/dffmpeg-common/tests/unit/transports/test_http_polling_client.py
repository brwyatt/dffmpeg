import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.transports.http_polling import HTTPPollingClientTransport


@pytest.mark.asyncio
@patch("dffmpeg.common.transports.http_polling.httpx.AsyncClient")
async def test_http_polling_client_streaming(mock_client_cls):
    """
    Test that the HTTPPollingClientTransport properly handles a streaming (NDJSON) response.
    """
    mock_client = AsyncMock()
    mock_client_cls.return_value = mock_client

    # Mock the stream context manager
    mock_stream_ctx = AsyncMock()
    mock_client.stream = MagicMock(return_value=mock_stream_ctx)

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/x-ndjson"}

    # Simulate a streaming response with 2 lines, plus a keep-alive line
    msg1 = {
        "message_id": str(ULID()),
        "recipient_id": "test",
        "message_type": "job_status",
        "job_id": str(ULID()),
        "payload": {"status": "running"},
    }
    msg2 = {
        "message_id": str(ULID()),
        "recipient_id": "test",
        "message_type": "job_status",
        "job_id": str(ULID()),
        "payload": {"status": "completed"},
    }

    async def mock_aiter_lines():
        yield json.dumps({"messages": [msg1]}) + "\n"
        yield "\n"  # keep-alive
        yield json.dumps({"messages": [msg2]}) + "\n"

        # Block forever to simulate the open stream
        await asyncio.Event().wait()

    mock_response.aiter_lines = mock_aiter_lines
    mock_stream_ctx.__aenter__.return_value = mock_response

    hmac_key = RequestSigner.generate_key()
    transport = HTTPPollingClientTransport(
        client_id="test_client",
        hmac_key=hmac_key,
        coordinator_url="http://test_coordinator",
        poll_wait=5,
        streaming=True,
    )

    await transport.connect({"path": "/poll/test"})

    # Wait for the messages to be processed
    received_msg1 = await asyncio.wait_for(transport.receive(), timeout=1.0)
    received_msg2 = await asyncio.wait_for(transport.receive(), timeout=1.0)

    assert received_msg1.message_id == msg1["message_id"]
    assert received_msg2.message_id == msg2["message_id"]

    await transport.disconnect()

    # Verify Accept header includes ndjson
    mock_client.stream.assert_called()
    call_kwargs = mock_client.stream.call_args[1]
    headers = call_kwargs.get("headers", {})
    assert "application/x-ndjson" in headers.get("Accept", "")


@pytest.mark.asyncio
@patch("dffmpeg.common.transports.http_polling.httpx.AsyncClient")
async def test_http_polling_client_fallback_standard_json(mock_client_cls):
    """
    Test that the HTTPPollingClientTransport gracefully falls back to standard JSON
    if the server returns application/json.
    """
    mock_client = AsyncMock()
    mock_client_cls.return_value = mock_client

    mock_stream_ctx = AsyncMock()
    mock_client.stream = MagicMock(return_value=mock_stream_ctx)

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}

    msg1 = {
        "message_id": str(ULID()),
        "recipient_id": "test",
        "message_type": "job_status",
        "job_id": str(ULID()),
        "payload": {"status": "running"},
    }

    # Standard JSON body reading
    async def mock_aread():
        await asyncio.sleep(0.01)  # Yield to event loop

    mock_response.aread = AsyncMock(side_effect=mock_aread)
    mock_response.json = MagicMock(return_value={"messages": [msg1]})

    mock_stream_ctx.__aenter__.return_value = mock_response

    hmac_key = RequestSigner.generate_key()
    transport = HTTPPollingClientTransport(
        client_id="test_client",
        hmac_key=hmac_key,
        coordinator_url="http://test_coordinator",
        poll_wait=5,
        streaming=True,
    )

    await transport.connect({"path": "/poll/test"})

    received_msg1 = await asyncio.wait_for(transport.receive(), timeout=1.0)
    assert received_msg1.message_id == msg1["message_id"]

    await transport.disconnect()

    # Ensure it called aread and json
    mock_response.aread.assert_awaited()
    mock_response.json.assert_called()


@pytest.mark.asyncio
@patch("dffmpeg.common.transports.http_polling.httpx.AsyncClient")
async def test_http_polling_client_non_streaming_config(mock_client_cls):
    """
    Test that if streaming=False is passed, it only asks for application/json.
    """
    mock_client = AsyncMock()
    mock_client_cls.return_value = mock_client

    mock_stream_ctx = AsyncMock()
    mock_client.stream = MagicMock(return_value=mock_stream_ctx)

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.headers = {"Content-Type": "application/json"}

    async def mock_aread_empty():
        await asyncio.sleep(0.01)  # Yield to event loop

    mock_response.aread = AsyncMock(side_effect=mock_aread_empty)
    mock_response.json = MagicMock(return_value={"messages": []})

    mock_stream_ctx.__aenter__.return_value = mock_response

    hmac_key = RequestSigner.generate_key()
    transport = HTTPPollingClientTransport(
        client_id="test_client",
        hmac_key=hmac_key,
        coordinator_url="http://test_coordinator",
        poll_wait=5,
        streaming=False,
    )

    await transport.connect({"path": "/poll/test"})
    await asyncio.sleep(0.1)  # Let the loop run
    await transport.disconnect()

    mock_client.stream.assert_called()
    call_kwargs = mock_client.stream.call_args[1]
    headers = call_kwargs.get("headers", {})
    assert headers.get("Accept") == "application/json"
