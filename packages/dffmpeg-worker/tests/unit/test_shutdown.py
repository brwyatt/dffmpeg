import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.job import JobRunner
from dffmpeg.worker.worker import Worker


@pytest.mark.asyncio
async def test_fast_shutdown_behavior():
    # Setup
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)

    # mock_client.post needs to fail to trigger retries
    def post_side_effect(path, *args, **kwargs):
        if "status" in path:
            raise Exception("Network Error")
        return MagicMock()  # Return success for accept, heartbeat, etc.

    mock_client.post.side_effect = post_side_effect

    mock_executor = AsyncMock()

    # Executor hangs to simulate running job
    async def hang(*args, **kwargs):
        await asyncio.sleep(10)

    mock_executor.execute.side_effect = hang

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={"heartbeat_interval": 10},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    # Start the job
    await runner.start()
    await asyncio.sleep(0.1)

    # Verify initial state
    assert not runner._fast_shutdown

    # Cancel with fast_shutdown=True
    # We patch asyncio.sleep to avoid waiting for real time during retries
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await runner.cancel(fast_shutdown=True)

        # Wait for _run to complete
        try:
            if runner._main_task:
                await runner._main_task
        except asyncio.CancelledError:
            pass

        # Verify fast_shutdown flag
        assert runner._fast_shutdown

        # Verify _report_status was called with retries=0
        # We can't easily spy on internal method call args directly unless we mock it,
        # but we can verify behavior: it should have tried ONCE and then stopped.

        # Filter for status calls
        status_calls = [
            call
            for call in mock_client.post.call_args_list
            if str(job_id) in str(call[0][0]) and "/status" in str(call[0][0])
        ]

        # Should be at least 1 call (initial failure)
        # With retries=0, attempts = max(1, 0+1) = 1.
        # So loop always runs once.
        # If it failed, it logged error and returned/broke.
        # mock_sleep should NOT have been called with exponential backoff if loop ran once.

        assert len(status_calls) == 1, "Should attempt status report exactly once"
        mock_sleep.assert_not_called()


@pytest.mark.asyncio
async def test_normal_cancel_behavior():
    # Setup
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)

    def post_side_effect(path, *args, **kwargs):
        if "status" in path:
            raise Exception("Network Error")
        return MagicMock()

    mock_client.post.side_effect = post_side_effect

    mock_executor = AsyncMock()

    async def hang(*args, **kwargs):
        await asyncio.sleep(10)

    mock_executor.execute.side_effect = hang

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={"heartbeat_interval": 10},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    # Start
    await runner.start()
    await asyncio.sleep(0.1)

    # Cancel normally (fast_shutdown=False)
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await runner.cancel(fast_shutdown=False)

        try:
            if runner._main_task:
                await runner._main_task
        except asyncio.CancelledError:
            pass

        assert not runner._fast_shutdown

        # Verify retries
        # Default retries=5. attempts=6.
        # Loops 0..5.
        # Sleeps 5 times.

        status_calls = [
            call
            for call in mock_client.post.call_args_list
            if str(job_id) in str(call[0][0]) and "/status" in str(call[0][0])
        ]

        # It should have tried 6 times (1 initial + 5 retries)
        assert len(status_calls) == 6
        assert mock_sleep.call_count == 5


@pytest.mark.asyncio
async def test_worker_drain_behavior():
    """
    Test Worker.drain() behavior:
    1. Sets status to draining.
    2. Cancels registration task and updates transport.
    3. Waits min_drain_time_seconds.
    4. Deregisters and waits for jobs if active.
    5. Cleans up HTTP client if empty.
    """
    config = WorkerConfig(
        client_id="test-worker", hmac_key="dummy-key", enable_job_draining=True, min_drain_time_seconds=0.1
    )
    worker = Worker(config=config, http_client_cls=AsyncMock)
    worker._registration_task = asyncio.create_task(asyncio.sleep(10))
    worker.transport_manager = AsyncMock()
    worker.transport_manager.transport_names = ["http_polling"]
    worker.client = AsyncMock()

    # Mock active jobs
    mock_runner = AsyncMock(spec=JobRunner)
    # We mock _main_task to be a real, awaiting task so wait() doesn't fail
    mock_runner._main_task = asyncio.create_task(asyncio.sleep(0.2))
    job_id = ULID()
    worker._active_jobs[job_id] = mock_runner

    async def drain_jobs_after_delay():
        await asyncio.sleep(0.1)
        worker._active_jobs.clear()

    asyncio.create_task(drain_jobs_after_delay())

    await worker.drain()

    assert worker._draining is True
    assert not worker.transport_manager.disconnect.called
    assert not worker.client.aclose.called


@pytest.mark.asyncio
async def test_worker_drain_disabled():
    """
    Test Worker.drain() does nothing if disable_job_draining is true.
    """
    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key", enable_job_draining=False)
    worker = Worker(config=config, http_client_cls=AsyncMock)
    worker.transport_manager = MagicMock()
    worker.client = AsyncMock()

    await worker.drain()

    assert worker._draining is False  # Didn't change
    assert not worker.transport_manager.disconnect.called
    assert not worker.client.post.called


@pytest.mark.asyncio
async def test_worker_stop_cancels_jobs():
    """
    Test Worker.stop() calls cancel(fast_shutdown=True) on all jobs.
    """
    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")
    worker = Worker(config=config, http_client_cls=AsyncMock)
    worker._registration_task = asyncio.create_task(asyncio.sleep(10))
    worker.transport_manager = AsyncMock()
    worker.client = AsyncMock()
    worker.client.is_closed = False

    mock_runner = AsyncMock(spec=JobRunner)
    job_id = ULID()
    worker._active_jobs[job_id] = mock_runner

    await worker.stop()

    assert not worker._running
    assert worker.transport_manager.disconnect.called
    assert worker.client.post.call_count > 0  # Should call deregister
    assert worker.client.aclose.called

    # Should call cancel(fast_shutdown=True) on the runner
    mock_runner.cancel.assert_called_once_with(fast_shutdown=True)


@pytest.mark.asyncio
async def test_worker_drain_rejects_job():
    """
    Test that if worker is in draining state, it explicitly rejects new job requests.
    """
    from dffmpeg.common.models import JobRequestMessage, JobRequestPayload

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")
    worker = Worker(config=config, http_client_cls=AsyncMock)
    worker.client = AsyncMock()

    worker._draining = True

    job_id = ULID()
    msg = JobRequestMessage(
        recipient_id="test-worker",
        job_id=job_id,
        payload=JobRequestPayload(job_id=str(job_id), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    await worker._handle_job_request(msg)

    # Should explicitly reject the job via API
    worker.client.post.assert_called_once_with(f"/jobs/{job_id}/reject")
    assert job_id not in worker._active_jobs


@pytest.mark.asyncio
async def test_worker_drain_cancelled_and_stopped():
    """
    Test that if Worker.drain() is cancelled while waiting on jobs,
    and then Worker.stop() is immediately called, everything cleans up gracefully without errors.
    """
    config = WorkerConfig(
        client_id="test-worker",
        hmac_key="dummy-key",
        enable_job_draining=True,
        min_drain_time_seconds=10.0,  # long sleep to ensure we can cancel it
    )
    worker = Worker(config=config, http_client_cls=AsyncMock)
    worker._registration_task = asyncio.create_task(asyncio.sleep(10))
    worker.transport_manager = AsyncMock()
    worker.transport_manager.transport_names = ["http_polling"]
    worker.client = AsyncMock()
    worker.client.is_closed = False

    # Start drain in background
    drain_task = asyncio.create_task(worker.drain())
    await asyncio.sleep(0.1)  # let it set self._draining and sleep

    assert worker._draining is True

    # Simulate signal handler cancelling drain and calling stop
    drain_task.cancel()
    try:
        await drain_task
    except asyncio.CancelledError:
        pass

    await worker.stop()

    # Verify everything cleaned up
    assert not worker._running
    assert worker.transport_manager.disconnect.called
    assert worker.client.post.call_count > 0  # Should call deregister
    assert worker.client.aclose.called
