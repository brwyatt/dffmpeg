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
