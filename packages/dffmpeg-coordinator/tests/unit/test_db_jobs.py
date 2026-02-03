import pytest
from datetime import datetime, timedelta, timezone
from ulid import ULID
from dffmpeg.coordinator.db.jobs.sqlite import SQLiteJobRepository
from dffmpeg.coordinator.db.jobs import JobRecord

@pytest.fixture
async def job_repo(tmp_path):
    db_path = tmp_path / "test_jobs.db"
    repo = SQLiteJobRepository(engine="sqlite", path=str(db_path))
    await repo.setup()
    return repo

@pytest.mark.anyio
async def test_get_stale_running_jobs(job_repo):
    now = datetime.now(timezone.utc)
    
    # Job 1: Running, Stale (last_update 20s ago, interval 10s, threshold 1.5 -> cutoff 15s)
    job1 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        last_update=now - timedelta(seconds=20),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={}
    )
    
    # Job 2: Running, Active (last_update 10s ago)
    job2 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        last_update=now - timedelta(seconds=10),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={}
    )

    # Job 3: Pending, Old (should be ignored)
    job3 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        last_update=now - timedelta(seconds=100),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={}
    )
    
    await job_repo.create_job(job1)
    await job_repo.create_job(job2)
    await job_repo.create_job(job3)
    
    stale = await job_repo.get_stale_running_jobs(threshold_factor=1.5, timestamp=now)
    assert len(stale) == 1
    assert stale[0].job_id == job1.job_id

@pytest.mark.anyio
async def test_get_stale_assigned_jobs(job_repo):
    now = datetime.now(timezone.utc)
    
    # Job 1: Assigned, Stale (last_update 40s ago, timeout 30s)
    job1 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="assigned",
        last_update=now - timedelta(seconds=40),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={}
    )
    
    # Job 2: Assigned, Active (last_update 10s ago)
    job2 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="assigned",
        last_update=now - timedelta(seconds=10),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={}
    )
    
    await job_repo.create_job(job1)
    await job_repo.create_job(job2)
    
    stale = await job_repo.get_stale_assigned_jobs(timeout_seconds=30, timestamp=now)
    assert len(stale) == 1
    assert stale[0].job_id == job1.job_id

@pytest.mark.anyio
async def test_update_status_conditional(job_repo):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        transport="http",
        transport_metadata={}
    )
    await job_repo.create_job(job)
    
    # 1. Successful update (previous matches)
    success = await job_repo.update_status(job.job_id, "failed", previous_status="running")
    assert success is True
    
    updated = await job_repo.get_job(job.job_id)
    assert updated.status == "failed"
    
    # 2. Failed update (previous mismatch)
    success = await job_repo.update_status(job.job_id, "running", previous_status="running")
    assert success is False
    
    updated = await job_repo.get_job(job.job_id)
    assert updated.status == "failed" # Unchanged
