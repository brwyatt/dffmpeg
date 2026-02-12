import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.job import JobRunner


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
