import argparse
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from ulid import ULID

from dffmpeg.client.cli import job_logs, job_submit, process_arguments
from dffmpeg.client.config import ClientConfig
from dffmpeg.common.models import JobLogsResponse, JobRecord, LogEntry


def test_process_arguments_basic():
    path_map = {"Movies": "/mnt/media/movies"}
    raw_args = ["-i", "/mnt/media/movies/input.mkv", "output.mp4"]

    processed, used = process_arguments(raw_args, path_map)

    assert processed == ["-i", "$Movies/input.mkv", "output.mp4"]
    assert used == ["Movies"]


def test_process_arguments_nested():
    path_map = {"Movies": "/mnt/media/movies", "All": "/mnt/media"}
    raw_args = ["/mnt/media/movies/file.mkv", "/mnt/media/other/file.mkv"]

    processed, used = process_arguments(raw_args, path_map)

    # "Movies" is longer than "All", so first arg should use Movies
    assert processed[0] == "$Movies/file.mkv"
    # Second arg doesn't match Movies, but matches All
    assert processed[1] == "$All/other/file.mkv"
    assert set(used) == {"Movies", "All"}


def test_process_arguments_boundary():
    path_map = {"Movies": "/mnt/media/movies"}
    # Should NOT match /mnt/media/movies-animated
    raw_args = ["/mnt/media/movies-animated/file.mkv"]

    processed, used = process_arguments(raw_args, path_map)

    assert processed == raw_args
    assert used == []


def test_process_arguments_exact():
    path_map = {"Movies": "/mnt/media/movies"}
    raw_args = ["/mnt/media/movies"]

    processed, used = process_arguments(raw_args, path_map)

    assert processed == ["$Movies"]
    assert used == ["Movies"]


def test_process_arguments_file_prefix():
    path_map = {"Movies": "/mnt/media/movies"}
    raw_args = ["-i", "file:/mnt/media/movies/input.mkv", "file:/mnt/media/movies"]

    processed, used = process_arguments(raw_args, path_map)

    assert processed == ["-i", "file:$Movies/input.mkv", "file:$Movies"]
    assert set(used) == {"Movies"}


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
        mock_client.submit_job.assert_called_once_with("ffmpeg", ANY, ANY, monitor=True, heartbeat_interval=None)
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
    mock_client.submit_job.assert_called_once_with("ffmpeg", ANY, ANY, monitor=False, heartbeat_interval=None)
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
