from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from dffmpeg.common.models import AuthenticatedIdentity, WorkerRegistration, WorkerVerifyRequest
from dffmpeg.coordinator.api.routes.worker import worker_register, worker_verify
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository
from dffmpeg.coordinator.transports import TransportManager


@pytest.fixture
def mock_config():
    config = CoordinatorConfig()
    config.allowed_binaries = ["ffmpeg"]
    config.handshake_delay_seconds = 1.0
    return config


@pytest.fixture
def mock_deps():
    return {
        "identity": AuthenticatedIdentity(client_id="worker_01", role="worker", hmac_key="a" * 44),
        "transports": AsyncMock(spec=TransportManager),
        "worker_repo": AsyncMock(spec=WorkerRepository),
        "background_tasks": MagicMock(),
    }


@pytest.fixture
def registration_payload():
    return WorkerRegistration(
        worker_id="worker_01",
        capabilities=[],
        binaries=["ffmpeg"],
        paths=[],
        supported_transports=["http_polling"],
        registration_interval=30,
        version="1.0.0",
    )


@pytest.mark.anyio
async def test_worker_register_new_worker(mock_config, mock_deps, registration_payload):
    # Setup mocks
    mock_deps["transports"].get_healthy_transports.return_value = {"http_polling"}
    mock_transport = MagicMock()
    mock_transport.get_metadata.return_value = {}
    mock_deps["transports"].__getitem__.return_value = mock_transport

    # Simulate worker not existing yet
    mock_deps["worker_repo"].get_worker.return_value = None

    # Call the endpoint
    result = await worker_register(
        payload=registration_payload,
        background_tasks=mock_deps["background_tasks"],
        identity=mock_deps["identity"],
        transports=mock_deps["transports"],
        worker_repo=mock_deps["worker_repo"],
        config=mock_config,
    )

    assert result.transport == "http_polling"

    # Verify add_or_update was called correctly
    assert mock_deps["worker_repo"].add_or_update.called
    record: WorkerRecord = mock_deps["worker_repo"].add_or_update.call_args[0][0]

    assert record.worker_id == "worker_01"
    assert record.status == "registering"  # Brand new worker is "registering"
    assert record.registration_token is not None
    assert record.last_registration_attempt is not None

    # Verify that the ping task was scheduled with the expected delay
    assert mock_deps["background_tasks"].add_task.called
    delay_arg = mock_deps["background_tasks"].add_task.call_args[0][3]
    assert delay_arg == mock_config.handshake_delay_seconds


@pytest.mark.anyio
async def test_worker_register_already_online(mock_config, mock_deps, registration_payload):
    # Setup mocks
    mock_deps["transports"].get_healthy_transports.return_value = {"http_polling"}
    mock_transport = MagicMock()
    mock_transport.get_metadata.return_value = {}
    mock_deps["transports"].__getitem__.return_value = mock_transport

    # Simulate an already ONLINE worker
    existing_worker = WorkerRecord(
        worker_id="worker_01",
        status="online",
        capabilities=[],
        binaries=[],
        paths=[],
        registration_interval=30,
        transport="http_polling",
        transport_metadata={},
        last_seen=datetime.now(timezone.utc),
    )
    mock_deps["worker_repo"].get_worker.return_value = existing_worker

    # Call the endpoint
    await worker_register(
        payload=registration_payload,
        background_tasks=mock_deps["background_tasks"],
        identity=mock_deps["identity"],
        transports=mock_deps["transports"],
        worker_repo=mock_deps["worker_repo"],
        config=mock_config,
    )

    # Verify add_or_update
    assert mock_deps["worker_repo"].add_or_update.called
    record: WorkerRecord = mock_deps["worker_repo"].add_or_update.call_args[0][0]

    assert record.status == "online"  # Worker stays online, no flapping!
    assert record.registration_token is not None  # But token is still issued

    # Delay should be 0 since transport didn't change
    delay_arg = mock_deps["background_tasks"].add_task.call_args[0][3]
    assert delay_arg == 0.0


@pytest.mark.anyio
async def test_worker_verify_success(mock_deps):
    # Simulate a registering worker waiting for verification
    pending_worker = WorkerRecord(
        worker_id="worker_01",
        status="registering",
        capabilities=[],
        binaries=[],
        paths=[],
        registration_interval=30,
        transport="http_polling",
        transport_metadata={},
        registration_token="secret_token_123",
        last_registration_attempt=datetime.now(timezone.utc),
        last_seen=datetime.now(timezone.utc),
    )
    mock_deps["worker_repo"].get_worker.return_value = pending_worker

    payload = WorkerVerifyRequest(registration_token="secret_token_123")

    result = await worker_verify(
        worker_id="worker_01",
        payload=payload,
        identity=mock_deps["identity"],
        worker_repo=mock_deps["worker_repo"],
    )

    assert result == {"status": "ok"}

    assert mock_deps["worker_repo"].add_or_update.called
    updated_record: WorkerRecord = mock_deps["worker_repo"].add_or_update.call_args[0][0]

    assert updated_record.status == "online"
    assert updated_record.registration_token is None  # Token cleared


@pytest.mark.anyio
async def test_worker_verify_invalid_token(mock_deps):
    # Simulate a registering worker waiting for verification
    pending_worker = WorkerRecord(
        worker_id="worker_01",
        status="registering",
        capabilities=[],
        binaries=[],
        paths=[],
        registration_interval=30,
        transport="http_polling",
        transport_metadata={},
        registration_token="secret_token_123",
        last_registration_attempt=datetime.now(timezone.utc),
        last_seen=datetime.now(timezone.utc),
    )
    mock_deps["worker_repo"].get_worker.return_value = pending_worker

    # Worker sends WRONG token
    payload = WorkerVerifyRequest(registration_token="wrong_token_456")

    with pytest.raises(HTTPException) as exc:
        await worker_verify(
            worker_id="worker_01",
            payload=payload,
            identity=mock_deps["identity"],
            worker_repo=mock_deps["worker_repo"],
        )

    assert exc.value.status_code == 400
    assert "Invalid registration token" in exc.value.detail


@pytest.mark.anyio
async def test_worker_verify_no_token_pending(mock_deps):
    # Simulate a worker that doesn't have a pending registration
    # e.g., verified twice or Janitor cleared it
    worker = WorkerRecord(
        worker_id="worker_01",
        status="online",
        capabilities=[],
        binaries=[],
        paths=[],
        registration_interval=30,
        transport="http_polling",
        transport_metadata={},
        registration_token=None,
        last_registration_attempt=datetime.now(timezone.utc),
        last_seen=datetime.now(timezone.utc),
    )
    mock_deps["worker_repo"].get_worker.return_value = worker

    payload = WorkerVerifyRequest(registration_token="secret_token_123")

    with pytest.raises(HTTPException) as exc:
        await worker_verify(
            worker_id="worker_01",
            payload=payload,
            identity=mock_deps["identity"],
            worker_repo=mock_deps["worker_repo"],
        )

    assert exc.value.status_code == 400
    assert "No pending registration" in exc.value.detail
