from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dffmpeg.common.models import VerifyRegistrationMessage, VerifyRegistrationPayload
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.worker import Worker


@pytest.fixture
def mock_config():
    config = WorkerConfig(client_id="worker_01", hmac_key="testkey", registration_interval=10)
    config.coordinator.host = "127.0.0.1"
    config.coordinator.port = 8000
    config.coordinator.scheme = "http"
    return config


@pytest.fixture
def worker(mock_config):
    with (
        patch("dffmpeg.worker.worker.AuthenticatedAsyncClient") as mock_client_cls,
        patch("dffmpeg.worker.worker.WorkerTransportManager") as mock_tm_cls,
    ):

        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client

        mock_tm = AsyncMock()
        mock_tm.current_transport_name = "http_polling"
        mock_tm_cls.return_value = mock_tm

        worker = Worker(config=mock_config)
        # Prevent the registration loop from actually running
        worker._running = False

        yield worker


@pytest.mark.anyio
async def test_handle_verify_registration_success(worker):
    token = "secret_token_abc"
    msg = VerifyRegistrationMessage(
        recipient_id=worker.client_id, payload=VerifyRegistrationPayload(registration_token=token)
    )

    # Mock a successful post to /verify
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    worker.client.post.return_value = mock_resp

    # Ensure the event starts cleared
    assert not worker._verified_event.is_set()

    # Trigger the handler
    await worker._handle_verify_registration(msg)

    # Assert API was called correctly
    worker.client.post.assert_called_once()
    call_args = worker.client.post.call_args
    assert call_args[0][0] == worker.coordinator_paths["verify"]
    assert call_args[1]["json"] == {"registration_token": token}

    # Assert event was set
    assert worker._verified_event.is_set()


@pytest.mark.anyio
async def test_verification_timeout_triggers_teardown(worker):
    worker._stop_transport = AsyncMock()

    # Call the timeout method with a very short timeout
    await worker._verification_timeout(timeout=0.01)

    # It should have timed out and called stop_transport
    assert worker._stop_transport.called


@pytest.mark.anyio
async def test_verification_timeout_success_aborts_teardown(worker):
    worker._stop_transport = AsyncMock()

    # We set the event immediately before or during the wait to simulate success
    worker._verified_event.set()

    # Call the timeout method
    await worker._verification_timeout(timeout=0.01)

    # Since it was verified, it should NOT tear down the transport
    assert not worker._stop_transport.called
