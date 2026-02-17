from datetime import datetime, timedelta, timezone

import pytest

from dffmpeg.coordinator.db.workers import WorkerRecord
from dffmpeg.coordinator.db.workers.sqlite import SQLiteWorkerRepository


@pytest.fixture
async def worker_repo(tmp_path):
    db_path = tmp_path / "test_workers.db"
    repo = SQLiteWorkerRepository(engine="sqlite", path=str(db_path))
    await repo.setup()
    return repo


@pytest.mark.anyio
async def test_get_stale_workers(worker_repo):
    now = datetime.now(timezone.utc)

    # Worker 1: Stale (last_seen 20s ago, interval 10s, threshold 1.5 -> cutoff 15s ago)
    worker1 = WorkerRecord(
        worker_id="worker1",
        status="online",
        last_seen=now - timedelta(seconds=20),
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
    )

    # Worker 2: Active (last_seen 10s ago)
    worker2 = WorkerRecord(
        worker_id="worker2",
        status="online",
        last_seen=now - timedelta(seconds=10),
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
    )

    # Worker 3: Offline (should be ignored)
    worker3 = WorkerRecord(
        worker_id="worker3",
        status="offline",
        last_seen=now - timedelta(seconds=100),
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
    )

    await worker_repo.add_or_update(worker1)
    await worker_repo.add_or_update(worker2)
    await worker_repo.add_or_update(worker3)

    stale = await worker_repo.get_stale_workers(threshold_factor=1.5, timestamp=now)

    assert len(stale) == 1
    assert stale[0].worker_id == "worker1"


@pytest.mark.anyio
async def test_get_stale_workers_threshold(worker_repo):
    now = datetime.now(timezone.utc)

    # Worker: last_seen 20s ago, interval 10s
    # If threshold 1.5, cutoff 15s -> Stale
    # If threshold 2.5, cutoff 25s -> Not stale

    worker = WorkerRecord(
        worker_id="worker1",
        status="online",
        last_seen=now - timedelta(seconds=20),
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
    )

    await worker_repo.add_or_update(worker)

    stale_low = await worker_repo.get_stale_workers(threshold_factor=1.5, timestamp=now)
    assert len(stale_low) == 1

    stale_high = await worker_repo.get_stale_workers(threshold_factor=2.5, timestamp=now)
    assert len(stale_high) == 0


@pytest.mark.anyio
async def test_worker_version_persistence(worker_repo):
    now = datetime.now(timezone.utc)

    # Create worker with version
    worker_with_version = WorkerRecord(
        worker_id="worker_v1",
        status="online",
        last_seen=now,
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
        version="1.2.3",
    )

    await worker_repo.add_or_update(worker_with_version)

    fetched = await worker_repo.get_worker("worker_v1")
    assert fetched is not None
    assert fetched.version == "1.2.3"


@pytest.mark.anyio
async def test_worker_no_version_persistence(worker_repo):
    now = datetime.now(timezone.utc)

    # Create worker without version
    worker_no_version = WorkerRecord(
        worker_id="worker_v2",
        status="online",
        last_seen=now,
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
        version=None,
    )

    await worker_repo.add_or_update(worker_no_version)

    fetched = await worker_repo.get_worker("worker_v2")
    assert fetched is not None
    assert fetched.version is None


@pytest.mark.anyio
async def test_worker_version_update(worker_repo):
    now = datetime.now(timezone.utc)

    # Create worker without version
    worker = WorkerRecord(
        worker_id="worker_update",
        status="online",
        last_seen=now,
        capabilities=[],
        binaries=[],
        paths=[],
        transport="http",
        transport_metadata={},
        registration_interval=10,
        version="1.0.0",
    )

    await worker_repo.add_or_update(worker)

    # Update with new version
    worker.version = "1.1.0"
    await worker_repo.add_or_update(worker)

    fetched = await worker_repo.get_worker("worker_update")
    assert fetched.version == "1.1.0"
