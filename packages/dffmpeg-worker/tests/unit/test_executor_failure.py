from unittest.mock import AsyncMock, Mock, patch

import pytest

from dffmpeg.worker.executor import SubprocessJobExecutor


@pytest.mark.asyncio
async def test_executor_stream_read_failure_returns_exit_code():
    """
    Test that if one of the streams throws an exception during reading
    (e.g., UnicodeDecodeError), the executor does not abort but instead
    continues reading the other stream and ultimately returns the exit code.
    """
    mock_process = Mock()
    mock_process.returncode = 0

    # stdout will raise an exception on the first read
    mock_process.stdout = AsyncMock()
    mock_process.stdout.readline.side_effect = Exception("Stream error (stdout)")

    # stderr will return a line and then EOF
    mock_process.stderr = AsyncMock()
    mock_process.stderr.readline.side_effect = [b"Error log line\n", b""]

    # wait() just returns the exit code
    async def mock_wait():
        return 42

    mock_process.wait = AsyncMock(side_effect=mock_wait)
    mock_process.terminate = Mock()
    mock_process.kill = Mock()

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    log_callback = AsyncMock()

    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = mock_process

        # execute should not raise the exception
        return_code = await executor.execute(log_callback)

        # Ensure return code is exactly what mock_wait returned
        assert return_code == 42

        # Ensure stderr was read and log_callback was called
        log_callback.assert_called_once()
        log_entry = log_callback.call_args[0][0]
        assert log_entry.stream == "stderr"
        assert log_entry.content == "Error log line"

        # Ensure process.wait() was called to reap the process
        mock_process.wait.assert_called_once()
