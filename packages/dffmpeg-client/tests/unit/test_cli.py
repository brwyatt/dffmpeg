import argparse
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.client.cli import job_logs, job_submit, stream_and_wait
from dffmpeg.client.config import ClientConfig
from dffmpeg.common.models import (
    JobLogsResponse,
    JobRecord,
    JobStatusMessage,
    JobStatusPayload,
    JobStreamModeSwitchMessage,
    JobStreamModeSwitchPayload,
    LogEntry,
)


@pytest.mark.anyio
async def test_job_submit_defaults():
    # Test that run_submit defaults to monitor=True
    job_id = ULID()
    mock_job = JobRecord(
        job_id=job_id,
        requester_id="client1",
        binary_name="ffmpeg",
        arguments=["-i", "in.mp4", "out.mp4"],
        status="pending",
        transport="http",
        transport_metadata={},
        heartbeat_interval=5,
        monitor=True,
    )
    mock_config = ClientConfig(client_id="client1", paths={})

    mock_client = MagicMock()
    mock_client.config = mock_config
    mock_client.submit_job = AsyncMock(return_value=mock_job)
    mock_client._start_heartbeat_loop = AsyncMock()

    args = argparse.Namespace(
        binary="ffmpeg",
        arguments=["-i", "in.mp4", "out.mp4"],
        detach=False,
        heartbeat_interval=None,
    )

    with patch("dffmpeg.client.cli.stream_and_wait") as mock_stream:
        mock_stream.return_value = 0

        result = await job_submit(mock_client, args)

        assert result == 0
        mock_client.submit_job.assert_called_once_with(
            "ffmpeg", ANY, ANY, working_directory=ANY, monitor=True, heartbeat_interval=None
        )
        mock_client._start_heartbeat_loop.assert_called_once_with(str(job_id), 5)


@pytest.mark.anyio
async def test_job_submit_background():
    # Test that background mode sets monitor=False
    job_id = ULID()
    mock_job = JobRecord(
        job_id=job_id,
        requester_id="client1",
        binary_name="ffmpeg",
        arguments=["-i", "in.mp4"],
        status="pending",
        transport="http",
        transport_metadata={},
        heartbeat_interval=5,
        monitor=False,
    )
    mock_config = ClientConfig(client_id="client1", paths={})

    mock_client = MagicMock()
    mock_client.config = mock_config
    mock_client.submit_job = AsyncMock(return_value=mock_job)
    mock_client._start_heartbeat_loop = AsyncMock()

    args = argparse.Namespace(
        binary="ffmpeg",
        arguments=["-i", "in.mp4"],
        detach=True,
        heartbeat_interval=None,
    )

    result = await job_submit(mock_client, args)

    assert result == 0
    mock_client.submit_job.assert_called_once_with(
        "ffmpeg", ANY, ANY, working_directory=ANY, monitor=False, heartbeat_interval=None
    )
    mock_client._start_heartbeat_loop.assert_not_called()


@pytest.mark.anyio
async def test_job_logs_basic():
    job_id = str(ULID())
    msg_id = ULID()
    mock_logs = JobLogsResponse(
        logs=[
            LogEntry(stream="stdout", content="log line 1"),
            LogEntry(stream="stderr", content="error line 1"),
        ],
        last_message_id=msg_id,
    )
    # Empty response to signal end of logs
    mock_empty = JobLogsResponse(logs=[], last_message_id=msg_id)

    mock_client = MagicMock()
    # 1. First fetch -> mock_logs
    # 2. Second fetch -> mock_empty
    # 3. Third fetch (terminal check) -> mock_empty
    mock_client.get_job_logs = AsyncMock(side_effect=[mock_logs, mock_empty, mock_empty])

    args = argparse.Namespace(job_id=job_id, follow=False)

    result = await job_logs(mock_client, args)

    assert result == 0
    assert mock_client.get_job_logs.call_count == 3


@pytest.mark.anyio
async def test_job_logs_follow():
    job_id = str(ULID())

    msg_id1 = ULID()
    mock_logs1 = JobLogsResponse(
        logs=[LogEntry(stream="stdout", content="line 1")],
        last_message_id=msg_id1,
    )

    # Initial logs
    msg_id2 = ULID()
    mock_logs2 = JobLogsResponse(
        logs=[LogEntry(stream="stdout", content="line 2")],
        last_message_id=msg_id2,
    )

    # Empty poll logs
    mock_logs3 = JobLogsResponse(logs=[], last_message_id=msg_id2)

    # Job status: running then completed
    mock_job_running = JobRecord(
        job_id=ULID.from_str(job_id),
        requester_id="client1",
        binary_name="ffmpeg",
        status="running",
        transport="http",
        transport_metadata={},
    )
    mock_job_completed = JobRecord(
        job_id=ULID.from_str(job_id),
        requester_id="client1",
        binary_name="ffmpeg",
        status="completed",
        transport="http",
        transport_metadata={},
    )

    mock_client = MagicMock()
    # 1. Iter 1: get_job_logs -> mock_logs1, follow=True, get_job_status -> running, sleep
    # 2. Iter 2: get_job_logs -> mock_logs2, follow=True, get_job_status -> completed, BREAK
    # 3. Terminal fetch: get_job_logs -> mock_logs3
    mock_client.get_job_logs = AsyncMock(side_effect=[mock_logs1, mock_logs2, mock_logs3])
    mock_client.get_job_status = AsyncMock(side_effect=[mock_job_running, mock_job_completed])

    args = argparse.Namespace(job_id=job_id, follow=True)

    with patch("asyncio.sleep", return_value=None):
        result = await job_logs(mock_client, args)

        assert result == 0
        assert mock_client.get_job_logs.call_count == 3
        assert mock_client.get_job_status.call_count == 2

        # Check that it uses the cursor
        mock_client.get_job_logs.assert_any_call(job_id, since_message_id=None)
        mock_client.get_job_logs.assert_any_call(job_id, since_message_id=str(msg_id1))
        # Note: the terminal fetch uses the last seen message ID from mock_logs2
        mock_client.get_job_logs.assert_any_call(job_id, since_message_id=str(msg_id2))


@pytest.mark.anyio
async def test_stream_and_wait_mode_switch_success():
    # Setup mocks
    mock_client = MagicMock()
    mock_client.total_bytes_read = 0
    job_id = ULID()

    switch_msg = JobStreamModeSwitchMessage(
        recipient_id="client1",
        job_id=job_id,
        payload=JobStreamModeSwitchPayload(
            stream="stdout",
            mode="binary",
            endpoint_path="/jobs/123/streams/stdout",
        ),
    )
    status_msg = JobStatusMessage(
        recipient_id="client1",
        job_id=job_id,
        payload=JobStatusPayload(status="completed", exit_code=0),
    )

    async def mock_stream_job(*args, **kwargs):
        yield switch_msg
        yield status_msg

    mock_client.stream_job.side_effect = mock_stream_job

    # Mock stream_binary generator
    async def mock_stream_binary(*args, **kwargs):
        yield b"chunk 1 data"
        yield b"chunk 2 data"

    mock_client.stream_binary.side_effect = mock_stream_binary

    # Mock sys.stdout
    mock_stdout = MagicMock()
    mock_buffer = MagicMock()
    mock_stdout.buffer = mock_buffer

    with patch("sys.stdout", mock_stdout):
        exit_code = await stream_and_wait(mock_client, str(job_id), "http", {})

    assert exit_code == 0
    # Bytes should be written directly to sys.stdout.buffer unconditionally (Unix trust user philosophy)
    mock_buffer.write.assert_any_call(b"chunk 1 data")
    mock_buffer.write.assert_any_call(b"chunk 2 data")
    # Verify that total_bytes_read was updated on the client
    assert mock_client.total_bytes_read == 24  # len(chunk 1) + len(chunk 2)
