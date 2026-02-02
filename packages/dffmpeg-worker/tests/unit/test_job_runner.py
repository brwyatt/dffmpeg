from unittest.mock import AsyncMock, MagicMock

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

    mock_cleanup.assert_called_with(job_id)
