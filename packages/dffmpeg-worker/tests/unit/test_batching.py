from unittest.mock import AsyncMock, MagicMock

import pytest
from ulid import ULID

from dffmpeg.common.http_client import AuthenticatedAsyncClient
from dffmpeg.common.models import LogEntry
from dffmpeg.worker.config import WorkerConfig
from dffmpeg.worker.executor import JobExecutor
from dffmpeg.worker.job import JobRunner


@pytest.mark.asyncio
async def test_job_runner_log_batching():
    # Setup
    job_id = ULID()
    job_payload = {"binary_name": "ffmpeg", "arguments": [], "paths": []}

    # Mocks
    mock_cleanup = MagicMock()
    mock_client = AsyncMock(spec=AuthenticatedAsyncClient)

    # We want to track calls to the logs endpoint
    log_calls = []

    async def mock_post(path, **kwargs):
        if "/logs" in path:
            log_calls.append(kwargs.get("json", {}))
        return MagicMock()

    mock_client.post.side_effect = mock_post

    mock_executor = AsyncMock(spec=JobExecutor)

    # Executor that sends many logs rapidly
    async def many_logs(log_callback):
        for i in range(50):
            await log_callback(LogEntry(stream="stdout", content=f"log {i}"))
        return 0

    mock_executor.execute.side_effect = many_logs

    # Small delay and large batch size to ensure they get batched
    config = WorkerConfig(client_id="test-worker", hmac_key="x" * 44, log_batch_size=100, log_batch_delay=0.1)

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

    # Verify that logs were batched
    # Since we sent 50 logs and the batch size is 100, and they were sent rapidly,
    # they should ideally arrive in 1 or very few batches.
    # The final flush in JobRunner ensure they are all sent.

    assert len(log_calls) > 0
    total_logs_received = sum(len(batch["logs"]) for batch in log_calls)
    assert total_logs_received == 50

    # If it's working as intended, it should be much fewer than 50 calls
    assert len(log_calls) < 10
