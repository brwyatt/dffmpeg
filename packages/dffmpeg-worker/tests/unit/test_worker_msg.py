from unittest.mock import AsyncMock

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.worker.config import CoordinatorConnectionConfig, WorkerConfig
from dffmpeg.worker.worker import Worker


@pytest.fixture
def worker_config():
    return WorkerConfig(client_id="test-worker", hmac_key="x" * 44, coordinator=CoordinatorConnectionConfig(host="localhost", port=8000))


@pytest.fixture
def worker(worker_config):
    return Worker(config=worker_config)


@pytest.mark.asyncio
async def test_handle_job_status_canceling(worker):
    job_id = ULID()
    mock_runner = AsyncMock()
    worker._active_jobs[job_id] = mock_runner

    msg = JobStatusMessage(recipient_id="test-worker", job_id=job_id, payload=JobStatusPayload(status="canceling"))

    await worker._handle_job_status(msg)

    mock_runner.cancel.assert_called_once()
    mock_runner.abort.assert_not_called()


@pytest.mark.asyncio
async def test_handle_job_status_canceled(worker):
    job_id = ULID()
    mock_runner = AsyncMock()
    worker._active_jobs[job_id] = mock_runner

    msg = JobStatusMessage(recipient_id="test-worker", job_id=job_id, payload=JobStatusPayload(status="canceled"))

    await worker._handle_job_status(msg)

    mock_runner.abort.assert_called_once()
    mock_runner.cancel.assert_not_called()


@pytest.mark.asyncio
async def test_handle_job_status_failed(worker):
    job_id = ULID()
    mock_runner = AsyncMock()
    worker._active_jobs[job_id] = mock_runner

    msg = JobStatusMessage(recipient_id="test-worker", job_id=job_id, payload=JobStatusPayload(status="failed"))

    await worker._handle_job_status(msg)

    mock_runner.abort.assert_called_once()
    mock_runner.cancel.assert_not_called()
