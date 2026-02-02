from unittest.mock import AsyncMock, MagicMock

import pytest

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.http_client import AuthenticatedAsyncClient


@pytest.mark.asyncio
async def test_authenticated_client_request():
    # Setup
    mock_httpx_client = AsyncMock()
    # mock_httpx_client.request is async, so AsyncMock is correct.
    # We pass a factory/class that returns this mock.
    mock_httpx_cls = MagicMock(return_value=mock_httpx_client)

    client_id = "test-client"
    hmac_key = RequestSigner.generate_key()
    base_url = "http://test"

    client = AuthenticatedAsyncClient(base_url, client_id, hmac_key, http_client_cls=mock_httpx_cls)

    # Test request directly
    path = "/test/path"
    body = {"foo": "bar"}
    method = "POST"

    await client.request(method, path, json=body)

    # Verify
    mock_httpx_client.request.assert_called_once()
    call_args = mock_httpx_client.request.call_args
    assert call_args[0][0] == method
    assert call_args[0][1] == path

    kwargs = call_args[1]
    # AuthenticatedAsyncClient converts json to content bytes
    assert "content" in kwargs
    assert "json" not in kwargs
    # We rely on RequestSigner correctness, but we can check if content is populated
    assert kwargs["content"] is not None
    assert "headers" in kwargs

    # Check headers contain signature
    headers = kwargs["headers"]
    assert "x-dffmpeg-client-id" in headers
    assert headers["x-dffmpeg-client-id"] == client_id
    assert "x-dffmpeg-timestamp" in headers
    assert "x-dffmpeg-signature" in headers

    # Verify signature validity (optional, but good sanity check)
    signer = RequestSigner(hmac_key)
    # Note: verify expects bytes/str payload, not dict
    assert (
        signer.verify("POST", path, headers["x-dffmpeg-timestamp"], headers["x-dffmpeg-signature"], kwargs["content"])
        is True
    )


@pytest.mark.asyncio
async def test_authenticated_client_get():
    mock_httpx_client = AsyncMock()
    mock_httpx_cls = MagicMock(return_value=mock_httpx_client)

    key = RequestSigner.generate_key()
    client = AuthenticatedAsyncClient("http://test", "id", key, http_client_cls=mock_httpx_cls)

    await client.get("/foo")

    mock_httpx_client.request.assert_called_once()
    assert mock_httpx_client.request.call_args[0][0] == "GET"
    assert mock_httpx_client.request.call_args[0][1] == "/foo"


@pytest.mark.asyncio
async def test_authenticated_client_post():
    mock_httpx_client = AsyncMock()
    mock_httpx_cls = MagicMock(return_value=mock_httpx_client)

    key = RequestSigner.generate_key()
    client = AuthenticatedAsyncClient("http://test", "id", key, http_client_cls=mock_httpx_cls)

    body = {"foo": "bar"}
    await client.post("/foo", json=body)

    mock_httpx_client.request.assert_called_once()
    assert mock_httpx_client.request.call_args[0][0] == "POST"
    assert mock_httpx_client.request.call_args[0][1] == "/foo"
    # Content check to verify json passed through
    assert "content" in mock_httpx_client.request.call_args[1]
