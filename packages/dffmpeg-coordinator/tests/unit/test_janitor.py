import pytest
from unittest.mock import AsyncMock, MagicMock, call
from dffmpeg.coordinator.janitor import Janitor
from dffmpeg.coordinator.db.workers import WorkerRecord
from dffmpeg.coordinator.db.jobs import JobRecord
from ulid import ULID

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
def janitor(worker_repo, job_repo, transports):
    return Janitor(worker_repo, job_repo, transports)

@pytest.mark.anyio
async def test_reap_workers(janitor, worker_repo):
    worker = WorkerRecord(
        worker_id="w1",
        status="online",
        registration_interval=10,
        transport="http",
        transport_metadata={}
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
        transport_metadata={}
    )
    job_repo.get_stale_running_jobs.return_value = [job]
    job_repo.update_status.return_value = True
    
    await janitor.reap_running_jobs()
    
    job_repo.update_status.assert_called_once_with(
        job.job_id, "failed", previous_status="running"
    )
    
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
        transport_metadata={}
    )
    job_repo.get_stale_running_jobs.return_value = [job]
    job_repo.update_status.return_value = False # Simulate race condition failure
    
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
        transport_metadata={}
    )
    job_repo.get_stale_assigned_jobs.return_value = [job]
    job_repo.update_status.return_value = True
    
    await janitor.reap_assigned_jobs()
    
    job_repo.update_status.assert_called_once_with(
        job.job_id, "pending", previous_status="assigned"
    )
    
    # Check notifications (only worker)
    assert transports.send_message.call_count == 1
