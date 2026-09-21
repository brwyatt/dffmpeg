import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from dffmpeg.worker.executor import SubprocessJobExecutor


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


@pytest.mark.asyncio
async def test_executor_pure_text_stdout():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    mock_process.stdout = AsyncMock()
    mock_process.stdout.readline.side_effect = [b"line 1\n", b"line 2\n", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.readline.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    binary_received = []

    async def binary_callback(data):
        binary_received.append(data)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback, binary_callback)

    assert exit_code == 0
    assert len(logs_received) == 2
    assert logs_received[0].content == "line 1"
    assert logs_received[1].content == "line 2"
    assert len(binary_received) == 0


@pytest.mark.asyncio
async def test_executor_pure_binary_stdout():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    # Mock binary stream data (contains non-UTF-8 bytes like 0x80)
    mock_process.stdout = AsyncMock()
    # readline/read mocks depending on implementation
    mock_process.stdout.readline.side_effect = [b"\x80\x01\x02", b""]
    mock_process.stdout.read = AsyncMock(side_effect=[b""])

    mock_process.stderr = AsyncMock()
    mock_process.stderr.readline.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    binary_received = []

    async def binary_callback(data):
        binary_received.append(data)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback, binary_callback)

    assert exit_code == 0
    assert len(logs_received) == 0
    assert len(binary_received) > 0
    assert b"".join(binary_received) == b"\x80\x01\x02"


@pytest.mark.asyncio
async def test_executor_mixed_text_and_binary_stdout():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    # Mock mixed data: first line text, then raw binary bytes
    mock_process.stdout = AsyncMock()
    mock_process.stdout.readline.side_effect = [b"text header\n", b"\x00\x01\x02", b""]
    mock_process.stdout.read = AsyncMock(side_effect=[b""])

    mock_process.stderr = AsyncMock()
    mock_process.stderr.readline.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    binary_received = []

    async def binary_callback(data):
        binary_received.append(data)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback, binary_callback)

    assert exit_code == 0
    assert len(logs_received) == 1
    assert logs_received[0].content == "text header"
    assert len(binary_received) > 0
    assert b"".join(binary_received) == b"\x00\x01\x02"
