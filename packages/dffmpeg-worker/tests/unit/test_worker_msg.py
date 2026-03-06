from unittest.mock import AsyncMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.models import JobStatusMessage, JobStatusPayload
from dffmpeg.worker.config import CoordinatorConnectionConfig, WorkerConfig
from dffmpeg.worker.worker import Worker


@pytest.fixture
def worker_config():
    return WorkerConfig(
        client_id="test-worker", hmac_key="x" * 44, coordinator=CoordinatorConnectionConfig(host="localhost", port=8000)
    )


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
async def test_handle_job_status_canceling_unknown_job(worker):
    job_id = ULID()
    # Job not in _active_jobs
    msg = JobStatusMessage(recipient_id="test-worker", job_id=job_id, payload=JobStatusPayload(status="canceling"))

    with patch.object(worker, "_report_job_failure", new_callable=AsyncMock) as mock_report:
        await worker._handle_job_status(msg)

        # It should ack the cancellation so the coordinator doesn't hang waiting
        mock_report.assert_called_once_with(job_id, "canceled")


@pytest.mark.asyncio
async def test_handle_job_status_failed(worker):
    job_id = ULID()
    mock_runner = AsyncMock()
    worker._active_jobs[job_id] = mock_runner

    msg = JobStatusMessage(recipient_id="test-worker", job_id=job_id, payload=JobStatusPayload(status="failed"))

    await worker._handle_job_status(msg)

    mock_runner.abort.assert_called_once()
    mock_runner.cancel.assert_not_called()
