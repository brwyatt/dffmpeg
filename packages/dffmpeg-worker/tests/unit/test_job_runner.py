# pyright: reportPrivateUsage = false

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.executor import JobExecutor
from dffmpeg.worker.job import JobRunner

# Mock hmac key for config validation
# Just use a dummy key; 32 bytes = 44 base64 chars.
HMAC_KEY = "x" * 44


@pytest.mark.asyncio
async def test_job_runner_success():
    # Setup
    job_id = ULID()
    job_payload = {"binary_name": "ffmpeg", "arguments": [], "paths": []}

    # Mocks
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)
    mock_executor = AsyncMock(spec=JobExecutor)
    mock_executor.execute.return_value = 0

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload=job_payload,
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    # Test
    await runner.start()

    # Wait for the main task to finish
    if runner._main_task:
        await runner._main_task

    # Verify sequence
    # 1. Accept
    mock_client.post.assert_any_call(runner.coordinator_paths["accept"])

    # 2. Execute
    mock_executor.execute.assert_called_once()

    # 3. Status completed
    status_calls = [
        call for call in mock_client.post.call_args_list if call[0][0] == runner.coordinator_paths["status"]
    ]
    assert len(status_calls) == 1
    kwargs = status_calls[0][1]
    assert kwargs["json"]["status"] == "completed"
    assert kwargs["json"]["exit_code"] == 0

    # Verify cleanup
    mock_cleanup.assert_called_with(job_id)


@pytest.mark.asyncio
async def test_job_runner_failure():
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)
    mock_executor = AsyncMock(spec=JobExecutor)

    # Executor fails
    mock_executor.execute.side_effect = Exception("Boom")

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    await runner.start()

    try:
        if runner._main_task:
            await runner._main_task
    except Exception:
        pass

    # Verify status failed
    status_calls = [
        call for call in mock_client.post.call_args_list if call[0][0] == runner.coordinator_paths["status"]
    ]
    assert len(status_calls) == 1
    kwargs = status_calls[0][1]
    assert kwargs["json"]["status"] == "failed"
    assert kwargs["json"]["exit_code"] is None

    mock_cleanup.assert_called_with(job_id)


@pytest.mark.asyncio
async def test_job_runner_cancel():
    # Setup
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)
    mock_executor = AsyncMock(spec=JobExecutor)

    # Mock executor to hang so we can cancel it
    async def hang(*args, **kwargs):
        await asyncio.sleep(10)

    mock_executor.execute.side_effect = hang

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    await runner.start()

    # Give it a moment to start
    await asyncio.sleep(0.1)

    # Cancel
    await runner.cancel()

    # Wait for task to finish
    try:
        if runner._main_task:
            await runner._main_task
    except asyncio.CancelledError:
        pass

    # Verify status canceled
    status_calls = [
        call for call in mock_client.post.call_args_list if call[0][0] == runner.coordinator_paths["status"]
    ]
    # Filter for canceled status
    canceled_calls = [c for c in status_calls if c[1]["json"]["status"] == "canceled"]
    assert len(canceled_calls) == 1


@pytest.mark.asyncio
async def test_job_runner_abort():
    # Setup
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)
    mock_executor = AsyncMock(spec=JobExecutor)

    async def hang(*args, **kwargs):
        await asyncio.sleep(10)

    mock_executor.execute.side_effect = hang

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    await runner.start()
    await asyncio.sleep(0.1)

    # Abort
    await runner.abort()

    try:
        if runner._main_task:
            await runner._main_task
    except asyncio.CancelledError:
        pass

    # Verify status NOT canceled
    status_calls = [
        call for call in mock_client.post.call_args_list if call[0][0] == runner.coordinator_paths["status"]
    ]
    # Filter for canceled status
    canceled_calls = [c for c in status_calls if c[1]["json"]["status"] == "canceled"]
    assert len(canceled_calls) == 0


@pytest.mark.asyncio
async def test_job_runner_empty_binary_stream_eof():
    """Verify that if binary mode is expected but no data is generated, uploader still sends 0-byte EOF."""
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)
    mock_executor = AsyncMock(spec=JobExecutor)

    # Simulated binary execution that finishes instantly without calling _on_binary_data but registers the callback
    async def empty_binary_execute(log_callback, binary_callback=None):
        # Setup _binary_mode_active since a binary callback is registered on the executor
        if binary_callback:
            # We mock the _on_binary_data being triggered internally but yielding 0 bytes
            pass
        return 0

    mock_executor.execute.side_effect = empty_binary_execute

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    # Force binary mode active
    runner._binary_mode_active = True

    await runner.start()

    if runner._main_task:
        await runner._main_task

    # Verify that post was called to upload EOF payload
    eof_calls = [call for call in mock_client.post.call_args_list if call[0][0] == f"/jobs/{job_id}/streams/stdout/eof"]
    assert len(eof_calls) == 1
    kwargs = eof_calls[0][1]
    assert kwargs["json"]["final_sequence"] is None
    assert kwargs["json"]["total_bytes"] == 0


@pytest.mark.asyncio
async def test_job_runner_uploader_fatal_failure_fail_fast():
    """Verify uploader fatal failures instantly cancel the executor task and fail the runner."""
    job_id = ULID()
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)
    mock_executor = AsyncMock(spec=JobExecutor)

    # Mock post_binary to fail fatal on chunk uploads
    mock_client.post_binary.side_effect = Exception("Fatal network error")

    # Mock long running execute that should be cancelled immediately
    executor_cancelled = False

    async def long_execute(log_callback, binary_callback=None):
        nonlocal executor_cancelled
        try:
            if binary_callback:
                await binary_callback(b"chunk data")
            await asyncio.sleep(10)
            return 0
        except asyncio.CancelledError:
            executor_cancelled = True
            raise

    mock_executor.execute.side_effect = long_execute

    config = WorkerConfig(client_id="test-worker", hmac_key="dummy-key")

    runner = JobRunner(
        config=config,
        client=mock_client,
        job_id=job_id,
        job_payload={},
        cleanup_callback=mock_cleanup,
        executor=mock_executor,
    )

    # Patch the _sleep helper directly inside job.py so that the 5 upload retries fail-fast immediately
    # without taking 15+ seconds and triggering the pytest-timeout!
    with patch("dffmpeg.worker.job._sleep", new_callable=AsyncMock) as _mock_sleep:  # noqa: F841
        await runner.start()

        try:
            if runner._main_task:
                await runner._main_task
        except Exception:
            pass

    # Verify uploader fatal failure caused executor subprocess task cancellation (fail-fast)
    assert executor_cancelled is True

    # Verify runner reported job failed status
    status_calls = [
        call for call in mock_client.post.call_args_list if call[0][0] == runner.coordinator_paths["status"]
    ]
    assert len(status_calls) == 1
    kwargs = status_calls[0][1]
    assert kwargs["json"]["status"] == "failed"
