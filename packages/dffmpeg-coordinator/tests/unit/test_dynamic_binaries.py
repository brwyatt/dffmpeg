from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from dffmpeg.common.models import JobRequest, WorkerRegistration
from dffmpeg.coordinator.api.routes.job import job_submit
from dffmpeg.coordinator.api.routes.worker import worker_register
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db.jobs import JobRepository
from dffmpeg.coordinator.db.workers import WorkerRepository
from dffmpeg.coordinator.transports import TransportManager


@pytest.fixture
def mock_config():
    config = CoordinatorConfig()
    config.allowed_binaries = ["allowed_tool"]
    return config


@pytest.fixture
def mock_deps():
    return {
        "identity": MagicMock(client_id="test_client", role="client"),
        "transports": AsyncMock(spec=TransportManager),
        "job_repo": AsyncMock(spec=JobRepository),
        "worker_repo": AsyncMock(spec=WorkerRepository),
        "background_tasks": MagicMock(),
    }


@pytest.mark.anyio
async def test_job_submit_invalid_binary(mock_config, mock_deps):
    payload = JobRequest(binary_name="invalid_tool", arguments=[], paths=[], supported_transports=["http_polling"])

    with pytest.raises(HTTPException) as exc:
        await job_submit(
            payload=payload,
            background_tasks=mock_deps["background_tasks"],
            identity=mock_deps["identity"],
            transports=mock_deps["transports"],
            job_repo=mock_deps["job_repo"],
            worker_repo=mock_deps["worker_repo"],
            config=mock_config,
        )

    assert exc.value.status_code == 400
    assert "Unsupported binary" in exc.value.detail


@pytest.mark.anyio
async def test_job_submit_valid_binary(mock_config, mock_deps):
    mock_deps["transports"].get_healthy_transports.return_value = {"http_polling"}

    mock_transport = MagicMock()
    mock_transport.get_metadata.return_value = {}
    mock_deps["transports"].__getitem__.return_value = mock_transport

    payload = JobRequest(binary_name="allowed_tool", arguments=[], paths=[], supported_transports=["http_polling"])

    # Should not raise exception
    await job_submit(
        payload=payload,
        background_tasks=mock_deps["background_tasks"],
        identity=mock_deps["identity"],
        transports=mock_deps["transports"],
        job_repo=mock_deps["job_repo"],
        worker_repo=mock_deps["worker_repo"],
        config=mock_config,
    )

    assert mock_deps["job_repo"].create_job.called


@pytest.mark.anyio
async def test_worker_register_filtering(mock_config, mock_deps):
    mock_deps["transports"].get_healthy_transports.return_value = {"http_polling"}

    # Mock transports.__getitem__ to return a mock with get_metadata
    mock_transport = MagicMock()
    mock_transport.get_metadata.return_value = {}
    mock_deps["transports"].__getitem__.return_value = mock_transport

    payload = WorkerRegistration(
        worker_id="test_client",
        capabilities=[],
        binaries=["allowed_tool", "forbidden_tool"],
        paths=[],
        supported_transports=["http_polling"],
        registration_interval=10,
    )

    await worker_register(
        payload=payload,
        identity=mock_deps["identity"],
        transports=mock_deps["transports"],
        worker_repo=mock_deps["worker_repo"],
        config=mock_config,
    )

    # Verify what was passed to add_or_update
    assert mock_deps["worker_repo"].add_or_update.called
    call_args = mock_deps["worker_repo"].add_or_update.call_args
    record = call_args[0][0]

    assert "allowed_tool" in record.binaries
    assert "forbidden_tool" not in record.binaries
