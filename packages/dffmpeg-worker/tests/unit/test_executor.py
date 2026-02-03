import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
from ulid import ULID

from dffmpeg.worker.executor import SubprocessJobExecutor


@pytest.mark.parametrize(
    "arguments, path_map, expected_resolved",
    [
        # Basic passthrough
        (
            ["-i", "input.mkv", "-c", "copy", "output.mkv"],
            {},
            ["-i", "input.mkv", "-c", "copy", "output.mkv"],
        ),
        # Simple substitution
        (
            ["-i", "$Movies/Avatar.mkv", "output.mkv"],
            {"Movies": "/mnt/media/movies", "Temp": "/tmp/transcode"},
            ["-i", "/mnt/media/movies/Avatar.mkv", "output.mkv"],
        ),
        # Exact match substitution
        (
            ["$Movies"],
            {"Movies": "/mnt/media/movies"},
            ["/mnt/media/movies"],
        ),
        # Missing variable (passthrough)
        (
            ["-i", "$Unknown/file.mkv"],
            {"Movies": "/mnt/media/movies"},
            ["-i", "$Unknown/file.mkv"],
        ),
        # Slash handling: Path map has no trailing slash
        (
            ["$NoSlash/file", "$WithSlash/file"],
            {"NoSlash": "/path/to", "WithSlash": "/path/to/"},
            ["/path/to/file", "/path/to/file"],
        ),
    ],
)
def test_executor_resolve_arguments(arguments, path_map, expected_resolved):
    executor = SubprocessJobExecutor(
        job_id=str(ULID()),
        binary_path="/bin/ffmpeg",
        arguments=arguments,
        path_map=path_map,
    )
    assert executor.resolved_arguments == expected_resolved


@pytest.mark.asyncio
async def test_executor_cancellation_terminates_process():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = None

    # Mock streams that wait forever (simulating a running process)
    async def infinite_read():
        await asyncio.sleep(10)
        return b""

    mock_process.stdout = AsyncMock()
    mock_process.stdout.readline.side_effect = infinite_read
    mock_process.stderr = AsyncMock()
    mock_process.stderr.readline.side_effect = infinite_read

    # Mock wait to simulate process running until terminated
    async def mock_wait():
        while mock_process.returncode is None:
            await asyncio.sleep(0.1)
        return mock_process.returncode

    mock_process.wait = AsyncMock(side_effect=mock_wait)

    def terminate_side_effect():
        mock_process.returncode = -15

    mock_process.terminate = Mock(side_effect=terminate_side_effect)
    mock_process.kill = Mock()

    # Create executor
    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    # Patch create_subprocess_exec
    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        # Run execute in a task
        task = asyncio.create_task(executor.execute(AsyncMock()))

        # Give it a moment to start and enter the gather block
        await asyncio.sleep(0.1)

        # Cancel the task
        task.cancel()

        # Helper to set return code when terminate is called (simulating OS behavior)
        # We can't easily side-effect the Mock.terminate because it's synchronous and we're in asyncio loop flow
        # But our mock_wait checks returncode.

        # Wait for task to finish (should raise CancelledError)
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify terminate was called
        # The finally block calls process.terminate()
        mock_process.terminate.assert_called_once()
