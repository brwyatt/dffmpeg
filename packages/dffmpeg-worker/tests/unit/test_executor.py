import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from dffmpeg.common.models import LogEnding
from dffmpeg.worker.executor import SubprocessJobExecutor


@pytest.mark.asyncio
async def test_executor_cancellation_terminates_process():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = None

    # Mock streams that wait forever (simulating a running process)
    async def infinite_read(*args, **kwargs):
        await asyncio.sleep(10)
        return b""

    mock_process.stdout = AsyncMock()
    mock_process.stdout.readline.side_effect = infinite_read
    mock_process.stdout.read.side_effect = infinite_read
    mock_process.stderr = AsyncMock()
    mock_process.stderr.readline.side_effect = infinite_read
    mock_process.stderr.read.side_effect = infinite_read

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
    # Read will return our text and then empty
    mock_process.stdout.read.side_effect = [b"line 1\n", b"line 2\n", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
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
    mock_process.stdout.read.side_effect = [b"\x80\x01\x02", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
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
    mock_process.stdout.read.side_effect = [b"text header\n", b"\x00\x01\x02", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
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


@pytest.mark.asyncio
async def test_executor_mixed_binary_header_corruption_prevention():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    # Mock a stream that starts with a valid UTF-8 header line but is actually a binary format (e.g. GIF)
    # under our 4KB chunk uploader, the entire block is correctly emitted as binary
    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [b"GIF89a\x01\x02\n", b"\x00\x01\x02", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
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
    # Under Option A, the text header "GIF89a..." is emitted as a text log and NOT included in the binary stream
    assert len(logs_received) == 1
    assert logs_received[0].content == "GIF89a\x01\x02"
    combined_binary = b"".join(binary_received)
    assert combined_binary == b"\x00\x01\x02"


@pytest.mark.asyncio
async def test_executor_large_unbroken_binary_no_overrun():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    # Mock a stream that produces 100KB of binary bytes without any newline
    large_data = b"\x80" * (100 * 1024)
    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [large_data, b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
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
    assert b"".join(binary_received) == large_data


@pytest.mark.asyncio
async def test_executor_universal_endings_lf():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [b"line 1\nline 2\n", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback)

    assert exit_code == 0
    assert len(logs_received) == 2
    assert logs_received[0].content == "line 1"
    assert logs_received[0].ending == LogEnding.LF
    assert logs_received[1].content == "line 2"
    assert logs_received[1].ending == LogEnding.LF


@pytest.mark.asyncio
async def test_executor_universal_endings_crlf():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [b"line 1\r\nline 2\r\n", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback)

    assert exit_code == 0
    assert len(logs_received) == 2
    assert logs_received[0].content == "line 1"
    assert logs_received[0].ending == LogEnding.CRLF
    assert logs_received[1].content == "line 2"
    assert logs_received[1].ending == LogEnding.CRLF


@pytest.mark.asyncio
async def test_executor_universal_endings_cr():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [b"frame= 100\rframe= 200\r", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback)

    assert exit_code == 0
    assert len(logs_received) == 2
    assert logs_received[0].content == "frame= 100"
    assert logs_received[0].ending == LogEnding.CR
    assert logs_received[1].content == "frame= 200"
    assert logs_received[1].ending == LogEnding.CR


@pytest.mark.asyncio
async def test_executor_universal_endings_eof_residual_none():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [b"residual non-newline text", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback)

    assert exit_code == 0
    assert len(logs_received) == 1
    assert logs_received[0].content == "residual non-newline text"
    assert logs_received[0].ending == LogEnding.NONE


@pytest.mark.asyncio
async def test_executor_universal_endings_split_packet_crlf():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    mock_process.stdout = AsyncMock()
    # Packet 1 ends with \r, packet 2 starts with \n
    mock_process.stdout.read.side_effect = [b"line 1\r", b"\nline 2\n", b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback)

    assert exit_code == 0
    assert len(logs_received) == 2
    assert logs_received[0].content == "line 1"
    assert logs_received[0].ending == LogEnding.CRLF
    assert logs_received[1].content == "line 2"
    assert logs_received[1].ending == LogEnding.LF


@pytest.mark.asyncio
async def test_executor_universal_endings_64kb_chunking_none():
    # Setup mock process
    mock_process = Mock()
    mock_process.returncode = 0

    large_data = b"A" * (70 * 1024)  # 70KB unbroken line
    mock_process.stdout = AsyncMock()
    mock_process.stdout.read.side_effect = [large_data, b""]

    mock_process.stderr = AsyncMock()
    mock_process.stderr.read.side_effect = [b""]
    mock_process.wait = AsyncMock(return_value=0)

    executor = SubprocessJobExecutor(job_id="test_job", binary_path="ffmpeg", arguments=[], path_map={})

    logs_received = []

    async def log_callback(entry):
        logs_received.append(entry)

    with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=mock_process)):
        exit_code = await executor.execute(log_callback)

    assert exit_code == 0
    # Expected to be split into 64KB chunk (NONE) and 6KB residual chunk (NONE)
    assert len(logs_received) == 2
    assert len(logs_received[0].content) == 64 * 1024
    assert logs_received[0].ending == LogEnding.NONE
    assert len(logs_received[1].content) == 6 * 1024
    assert logs_received[1].ending == LogEnding.NONE
