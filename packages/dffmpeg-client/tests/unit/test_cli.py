from unittest.mock import ANY, patch

import pytest
from ulid import ULID

from dffmpeg.client.cli import process_arguments, run_submit
from dffmpeg.client.config import ClientConfig
from dffmpeg.common.models import JobRecord


def test_process_arguments_basic():
    path_map = {"Movies": "/mnt/media/movies"}
    # Use os.sep to be cross-platform compatible in expectation if needed,
    # but the logic assumes input strings.
    # We simulate absolute paths.
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


@pytest.mark.anyio
async def test_run_submit_defaults():
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
    mock_config = ClientConfig(
        client_id="client1",
    )

    with (
        patch("dffmpeg.client.cli.load_config") as mock_load_config,
        patch("dffmpeg.client.cli.DFFmpegClient") as mock_client_cls,
        patch("dffmpeg.client.cli.stream_and_wait") as mock_stream,
    ):
        mock_load_config.return_value = mock_config
        mock_client = mock_client_cls.return_value.__aenter__.return_value
        mock_client.submit_job.return_value = mock_job
        mock_stream.return_value = 0

        result = await run_submit("ffmpeg", ["-i", "in.mp4", "out.mp4"], monitor=True)

        assert result == 0
        mock_client.submit_job.assert_called_once_with("ffmpeg", ANY, ANY, monitor=True, heartbeat_interval=None)
        mock_client._start_heartbeat_loop.assert_called_once_with(str(job_id), 5)


@pytest.mark.anyio
async def test_run_submit_background():
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
    mock_config = ClientConfig(
        client_id="client1",
    )

    with (
        patch("dffmpeg.client.cli.load_config") as mock_load_config,
        patch("dffmpeg.client.cli.DFFmpegClient") as mock_client_cls,
    ):
        mock_load_config.return_value = mock_config
        mock_client = mock_client_cls.return_value.__aenter__.return_value
        mock_client.submit_job.return_value = mock_job

        result = await run_submit("ffmpeg", ["-i", "in.mp4"], monitor=False)

        assert result == 0
        mock_client.submit_job.assert_called_once_with("ffmpeg", ANY, ANY, monitor=False, heartbeat_interval=None)
        mock_client._start_heartbeat_loop.assert_not_called()
