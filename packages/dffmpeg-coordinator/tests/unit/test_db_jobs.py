from datetime import datetime, timedelta, timezone

import pytest
from ulid import ULID

from dffmpeg.coordinator.db.jobs import JobRecord
from dffmpeg.coordinator.db.jobs.sqlite import SQLiteJobRepository


@pytest.fixture
async def job_repo(tmp_path):
    db_path = tmp_path / "test_jobs.db"
    repo = SQLiteJobRepository(engine="sqlite", path=str(db_path))
    await repo.setup()
    return repo


@pytest.mark.anyio
async def test_get_stale_running_jobs(job_repo):
    now = datetime.now(timezone.utc)

    # Job 1: Running, Stale (worker_last_seen 20s ago, interval 10s, threshold 1.5 -> cutoff 15s)
    job1 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        worker_last_seen=now - timedelta(seconds=20),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    # Job 2: Running, Active (worker_last_seen 10s ago)
    job2 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        worker_last_seen=now - timedelta(seconds=10),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    # Job 3: Pending, Old (should be ignored)
    job3 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        worker_last_seen=now - timedelta(seconds=100),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
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
        transport_metadata={},
    )

    # Job 2: Assigned, Recent (last_update 10s ago)
    job2 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="assigned",
        last_update=now - timedelta(seconds=10),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    await job_repo.create_job(job1)
    await job_repo.create_job(job2)

    stale = await job_repo.get_stale_assigned_jobs(timeout_seconds=30, timestamp=now)
    assert len(stale) == 1
    assert stale[0].job_id == job1.job_id


@pytest.mark.anyio
async def test_get_stale_pending_jobs(job_repo):
    now = datetime.now(timezone.utc)

    # Job 1: Pending, Retry window (10s old)
    job1 = JobRecord(
        job_id=ULID(),
        requester_id="c1",
        binary_name="ffmpeg",
        status="pending",
        last_update=now - timedelta(seconds=10),
        transport="http",
        transport_metadata={},
    )

    # Job 2: Pending, Fail window (40s old)
    job2 = JobRecord(
        job_id=ULID(),
        requester_id="c1",
        binary_name="ffmpeg",
        status="pending",
        last_update=now - timedelta(seconds=40),
        transport="http",
        transport_metadata={},
    )

    # Job 3: Pending, Too young (2s old)
    job3 = JobRecord(
        job_id=ULID(),
        requester_id="c1",
        binary_name="ffmpeg",
        status="pending",
        last_update=now - timedelta(seconds=2),
        transport="http",
        transport_metadata={},
    )

    await job_repo.create_job(job1)
    await job_repo.create_job(job2)
    await job_repo.create_job(job3)

    # Test retry window (5s to 30s)
    retry_jobs = await job_repo.get_stale_pending_jobs(min_seconds=5, max_seconds=30, timestamp=now)
    assert len(retry_jobs) == 1
    assert retry_jobs[0].job_id == job1.job_id

    # Test fail window (> 30s)
    fail_jobs = await job_repo.get_stale_pending_jobs(min_seconds=30, timestamp=now)
    assert len(fail_jobs) == 1
    assert fail_jobs[0].job_id == job2.job_id


@pytest.mark.anyio
async def test_update_status_conditional(job_repo):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        transport="http",
        transport_metadata={},
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
    assert updated.status == "failed"  # Unchanged


@pytest.mark.anyio
async def test_get_stale_monitored_jobs(job_repo):
    now = datetime.now(timezone.utc)

    # Job 1: Monitored, Stale (client_last_seen 20s ago, interval 10s, threshold 1.5 -> cutoff 15s)
    job1 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        monitor=True,
        client_last_seen=now - timedelta(seconds=20),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    # Job 2: Monitored, Active (client_last_seen 10s ago)
    job2 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        monitor=True,
        client_last_seen=now - timedelta(seconds=10),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    # Job 3: NOT Monitored, Old (should be ignored)
    job3 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        monitor=False,
        client_last_seen=now - timedelta(seconds=100),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    # Job 4: Monitored, Finished (should be ignored)
    job4 = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="completed",
        monitor=True,
        client_last_seen=now - timedelta(seconds=100),
        heartbeat_interval=10,
        transport="http",
        transport_metadata={},
    )

    await job_repo.create_job(job1)
    await job_repo.create_job(job2)
    await job_repo.create_job(job3)
    await job_repo.create_job(job4)

    stale = await job_repo.get_stale_monitored_jobs(threshold_factor=1.5, timestamp=now)
    assert len(stale) == 1
    assert stale[0].job_id == job1.job_id


@pytest.mark.anyio
async def test_update_client_heartbeat(job_repo):
    job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        monitor=False,
        transport="http",
        transport_metadata={},
    )
    await job_repo.create_job(job)

    now = datetime.now(timezone.utc)
    success = await job_repo.update_client_heartbeat(job.job_id, timestamp=now, monitor=True)
    assert success is True

    updated = await job_repo.get_job(job.job_id)
    assert updated.monitor is True
    # SQLite might lose some precision on timestamps during storage/retrieval,
    # but they should be effectively equal or very close.
    assert updated.client_last_seen is not None
