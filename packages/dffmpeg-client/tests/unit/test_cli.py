from dffmpeg.client.cli import process_arguments


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
