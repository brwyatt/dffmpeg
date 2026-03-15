import asyncio
from unittest.mock import AsyncMock

import pytest

from dffmpeg.coordinator.config import JanitorConfig
from dffmpeg.coordinator.janitor import Janitor


@pytest.fixture
def mock_worker_repo():
    return AsyncMock()


@pytest.fixture
def mock_job_repo():
    return AsyncMock()


@pytest.fixture
def mock_transports():
    return AsyncMock()


@pytest.fixture
def janitor_config():
    return JanitorConfig(interval=10, jitter=0)


@pytest.fixture
def janitor(mock_worker_repo, mock_job_repo, mock_transports, janitor_config):
    return Janitor(
        worker_repo=mock_worker_repo,
        job_repo=mock_job_repo,
        transports=mock_transports,
        config=janitor_config,
    )


@pytest.mark.asyncio
async def test_schedule_task_no_delay(janitor):
    janitor.reap_workers = AsyncMock()
    await janitor.start(schedule_task=False)
    janitor.schedule_task("clean_workers", delay=0)
    assert janitor._queue.qsize() == 1
    janitor.reap_workers.assert_not_awaited()
    await janitor._queue.join()
    janitor.reap_workers.assert_awaited_once()


@pytest.mark.asyncio
async def test_schedule_task_with_delay(janitor):
    janitor.reap_workers = AsyncMock()
    await janitor.start(schedule_task=False)
    janitor.schedule_task("clean_workers", delay=0.1)
    assert janitor._queue.qsize() == 0
    janitor.reap_workers.assert_not_awaited()
    await asyncio.sleep(0.15)
    assert janitor._queue.qsize() == 0
    janitor.reap_workers.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_loop_clean_workers(janitor):
    janitor.reap_workers = AsyncMock()

    await janitor.start(schedule_task=False)
    assert janitor._queue.qsize() == 0
    janitor.schedule_task("clean_workers", delay=0)
    assert janitor._queue.qsize() == 1

    # Wait for queue to process
    await janitor._queue.join()
    janitor.reap_workers.assert_awaited_once()

    # Stop loop
    await janitor.stop()


@pytest.mark.asyncio
async def test_worker_loop_clean_jobs(janitor):
    janitor.reap_running_jobs = AsyncMock()
    janitor.reap_assigned_jobs = AsyncMock()
    janitor.reap_pending_jobs = AsyncMock()
    janitor.reap_abandoned_monitored_jobs = AsyncMock()

    await janitor.start(schedule_task=False)
    janitor.schedule_task("clean_jobs", delay=0)

    # Wait for queue to process
    await janitor._queue.join()
    janitor.reap_running_jobs.assert_awaited_once()
    janitor.reap_assigned_jobs.assert_awaited_once()
    janitor.reap_pending_jobs.assert_awaited_once()
    janitor.reap_abandoned_monitored_jobs.assert_awaited_once()

    # Stop loop
    await janitor.stop()


@pytest.mark.asyncio
async def test_run_all_and_reschedule(janitor):
    janitor.reap_workers = AsyncMock()
    janitor.reap_running_jobs = AsyncMock()
    janitor.reap_assigned_jobs = AsyncMock()
    janitor.reap_pending_jobs = AsyncMock()
    janitor.reap_abandoned_monitored_jobs = AsyncMock()

    await janitor.start(schedule_task=False)
    janitor.schedule_task("run_all_and_reschedule", delay=0)

    # Wait for first execution to process
    await janitor._queue.join()

    janitor.reap_workers.assert_awaited_once()
    janitor.reap_running_jobs.assert_awaited_once()
    janitor.reap_assigned_jobs.assert_awaited_once()
    janitor.reap_pending_jobs.assert_awaited_once()
    janitor.reap_abandoned_monitored_jobs.assert_awaited_once()

    # It should have rescheduled itself (delay=10, so it shouldn't be in queue yet)
    assert janitor._queue.qsize() == 0

    # Stop loop
    await janitor.stop()
