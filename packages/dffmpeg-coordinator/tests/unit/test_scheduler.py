from unittest.mock import ANY, AsyncMock

import pytest
from ulid import ULID

from dffmpeg.common.models import (
    JobRequestMessage,
    JobStatusMessage,
)
from dffmpeg.coordinator.db.jobs import JobRecord
from dffmpeg.coordinator.db.workers import WorkerRecord
from dffmpeg.coordinator.scheduler import process_job_assignment


@pytest.fixture
def job_repo():
    return AsyncMock()


@pytest.fixture
def worker_repo():
    return AsyncMock()


@pytest.fixture
def transports():
    return AsyncMock()


@pytest.mark.anyio
async def test_process_job_assignment_normal(job_repo, worker_repo, transports):
    job_id = ULID()
    job = JobRecord(
        job_id=job_id,
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        paths=["/data"],
        arguments=["-i", "input.mp4", "output.mkv"],
        transport="http_polling",
        transport_metadata={},
    )
    job_repo.get_job.return_value = job

    worker = WorkerRecord(
        worker_id="w1",
        status="online",
        binaries=["ffmpeg"],
        paths=["/data", "/other"],
        transport="http_polling",
        transport_metadata={},
        registration_interval=60,
    )
    worker_repo.get_workers_by_status.return_value = [worker]
    job_repo.get_worker_load.return_value = {}

    await process_job_assignment(job_id, job_repo, worker_repo, transports)

    job_repo.update_status.assert_called_once_with(job_id, "assigned", worker_id="w1", timestamp=ANY)
    assert transports.send_message.call_count == 2

    # Verify the correct messages are sent
    calls = transports.send_message.call_args_list
    request_msg = calls[0][0][0]
    status_msg = calls[1][0][0]

    assert isinstance(request_msg, JobRequestMessage)
    assert request_msg.recipient_id == "w1"

    assert isinstance(status_msg, JobStatusMessage)
    assert status_msg.recipient_id == "client1"


@pytest.mark.anyio
async def test_process_job_assignment_exclusion(job_repo, worker_repo, transports):
    """Test that a re-assigned job excludes the previously assigned worker if >1 workers are available."""
    job_id = ULID()
    job = JobRecord(
        job_id=job_id,
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        paths=["/data"],
        worker_id="w1",  # Previously assigned to w1, but failed/timed out and is back to pending
        transport="http_polling",
        transport_metadata={},
    )
    job_repo.get_job.return_value = job

    worker1 = WorkerRecord(
        worker_id="w1",
        status="online",
        binaries=["ffmpeg"],
        paths=["/data"],
        transport="http_polling",
        transport_metadata={},
        registration_interval=60,
    )
    worker2 = WorkerRecord(
        worker_id="w2",
        status="online",
        binaries=["ffmpeg"],
        paths=["/data"],
        transport="http_polling",
        transport_metadata={},
        registration_interval=60,
    )

    worker_repo.get_workers_by_status.return_value = [worker1, worker2]
    job_repo.get_worker_load.return_value = {}

    await process_job_assignment(job_id, job_repo, worker_repo, transports)

    # It should pick w2, ignoring w1
    job_repo.update_status.assert_called_once_with(job_id, "assigned", worker_id="w2", timestamp=ANY)
    assert transports.send_message.call_count == 2
    assert transports.send_message.call_args_list[0][0][0].recipient_id == "w2"


@pytest.mark.anyio
async def test_process_job_assignment_no_exclusion_fallback(job_repo, worker_repo, transports):
    """Test that if there's only 1 eligible worker, it doesn't exclude it even if it was previously assigned."""
    job_id = ULID()
    job = JobRecord(
        job_id=job_id,
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        paths=["/data"],
        worker_id="w1",  # Previously assigned to w1
        transport="http_polling",
        transport_metadata={},
    )
    job_repo.get_job.return_value = job

    worker1 = WorkerRecord(
        worker_id="w1",
        status="online",
        binaries=["ffmpeg"],
        paths=["/data"],
        transport="http_polling",
        transport_metadata={},
        registration_interval=60,
    )

    # w2 is online but doesn't meet path requirements, so it gets filtered out
    worker2 = WorkerRecord(
        worker_id="w2",
        status="online",
        binaries=["ffmpeg"],
        paths=["/wrong_path"],
        transport="http_polling",
        transport_metadata={},
        registration_interval=60,
    )

    worker_repo.get_workers_by_status.return_value = [worker1, worker2]
    job_repo.get_worker_load.return_value = {}

    await process_job_assignment(job_id, job_repo, worker_repo, transports)

    # It should fall back to w1 since w2 is filtered out and w1 is the only valid candidate
    job_repo.update_status.assert_called_once_with(job_id, "assigned", worker_id="w1", timestamp=ANY)
    assert transports.send_message.call_count == 2
    assert transports.send_message.call_args_list[0][0][0].recipient_id == "w1"
