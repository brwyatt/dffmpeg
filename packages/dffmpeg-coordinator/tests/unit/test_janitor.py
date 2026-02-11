from unittest.mock import ANY, AsyncMock, patch

import pytest
from ulid import ULID

from dffmpeg.coordinator.config import JanitorConfig
from dffmpeg.coordinator.db.jobs import JobRecord
from dffmpeg.coordinator.db.workers import WorkerRecord
from dffmpeg.coordinator.janitor import Janitor


@pytest.fixture
def worker_repo():
    return AsyncMock()


@pytest.fixture
def job_repo():
    return AsyncMock()


@pytest.fixture
def transports():
    return AsyncMock()


@pytest.fixture
def config():
    return JanitorConfig()


@pytest.fixture
def janitor(worker_repo, job_repo, transports, config):
    return Janitor(worker_repo, job_repo, transports, config)


@pytest.mark.anyio
async def test_reap_workers(janitor, worker_repo):
    worker = WorkerRecord(
        worker_id="w1", status="online", registration_interval=10, transport="http", transport_metadata={}
    )
    worker_repo.get_stale_workers.return_value = [worker]

    await janitor.reap_workers()

    # Check if worker status was updated to offline
    assert worker.status == "offline"
    worker_repo.add_or_update.assert_called_once_with(worker)


@pytest.mark.anyio
async def test_reap_running_jobs(janitor, job_repo, transports):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        worker_id="worker1",
        binary_name="ffmpeg",
        status="running",
        transport="http",
        transport_metadata={},
    )
    job_repo.get_stale_running_jobs.return_value = [job]
    job_repo.update_status.return_value = True

    await janitor.reap_running_jobs()

    job_repo.update_status.assert_called_once_with(job.job_id, "failed", previous_status="running", timestamp=ANY)

    # Check notifications
    assert transports.send_message.call_count == 2
    # First one to client, second to worker (order in code)
    # Actually order depends on implementation, but both should be called.


@pytest.mark.anyio
async def test_reap_running_jobs_update_fails(janitor, job_repo, transports):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        worker_id="worker1",
        binary_name="ffmpeg",
        status="running",
        transport="http",
        transport_metadata={},
    )
    job_repo.get_stale_running_jobs.return_value = [job]
    job_repo.update_status.return_value = False  # Simulate race condition failure

    await janitor.reap_running_jobs()

    # Should NOT send notifications
    transports.send_message.assert_not_called()


@pytest.mark.anyio
async def test_reap_assigned_jobs(janitor, job_repo, transports):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        worker_id="worker1",
        binary_name="ffmpeg",
        status="assigned",
        transport="http",
        transport_metadata={},
    )
    job_repo.get_stale_assigned_jobs.return_value = [job]
    job_repo.update_status.return_value = True

    await janitor.reap_assigned_jobs()

    job_repo.update_status.assert_called_once_with(job.job_id, "pending", previous_status="assigned", timestamp=ANY)

    # Check notifications (only worker)
    assert transports.send_message.call_count == 1


@pytest.mark.anyio
async def test_reap_pending_jobs(janitor, job_repo, transports):
    with patch("dffmpeg.coordinator.janitor.process_job_assignment", new_callable=AsyncMock) as mock_process:
        # Job 1: Retry (10s old)
        job1 = JobRecord(
            job_id=ULID(),
            requester_id="c1",
            binary_name="ffmpeg",
            status="pending",
            transport="http",
            transport_metadata={},
        )

        # Job 2: Fail (40s old)
        job2 = JobRecord(
            job_id=ULID(),
            requester_id="c1",
            binary_name="ffmpeg",
            status="pending",
            transport="http",
            transport_metadata={},
        )

        # Setup job repo returns
        # get_stale_pending_jobs called twice:
        # 1. min=5, max=30 -> returns [job1]
        # 2. min=30 -> returns [job2]
        job_repo.get_stale_pending_jobs.side_effect = [[job1], [job2]]

        job_repo.update_status.return_value = True

        await janitor.reap_pending_jobs()

        # Verify process_job_assignment called for job1
        mock_process.assert_called_once()
        assert mock_process.call_args[0][0] == job1.job_id

        # Verify fail logic for job2
        job_repo.update_status.assert_called_once_with(job2.job_id, "failed", previous_status="pending", timestamp=ANY)

        # Verify notification for job2
        assert transports.send_message.call_count == 1
        assert transports.send_message.call_args[0][0].job_id == job2.job_id


@pytest.mark.anyio
async def test_reap_abandoned_monitored_jobs(janitor, job_repo, transports):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        worker_id="worker1",
        binary_name="ffmpeg",
        status="running",
        monitor=True,
        transport="http",
        transport_metadata={},
    )
    job_repo.get_stale_monitored_jobs.return_value = [job]
    job_repo.update_status.return_value = True

    await janitor.reap_abandoned_monitored_jobs()

    job_repo.update_status.assert_called_once_with(job.job_id, "canceling", timestamp=ANY)

    # Check notifications: 1 to client, 1 to worker
    assert transports.send_message.call_count == 2
    # Verify both recipients got a message
    recipients = [call.args[0].recipient_id for call in transports.send_message.call_args_list]
    assert "client1" in recipients
    assert "worker1" in recipients
