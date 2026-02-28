from dffmpeg.common.paths import map_arguments, map_path, resolve_arguments, resolve_path


def test_map_path_basic():
    path_map = {"Movies": "/mnt/media/movies"}
    assert map_path("/mnt/media/movies/input.mkv", path_map) == ("$Movies/input.mkv", "Movies")
    assert map_path("/mnt/media/movies", path_map) == ("$Movies", "Movies")


def test_map_path_nested():
    path_map = {"Movies": "/mnt/media/movies", "All": "/mnt/media"}
    assert map_path("/mnt/media/movies/file.mkv", path_map) == ("$Movies/file.mkv", "Movies")
    assert map_path("/mnt/media/other/file.mkv", path_map) == ("$All/other/file.mkv", "All")


def test_map_path_boundary():
    path_map = {"Movies": "/mnt/media/movies"}
    # Should NOT match /mnt/media/movies-animated
    assert map_path("/mnt/media/movies-animated/file.mkv", path_map) == ("/mnt/media/movies-animated/file.mkv", None)


def test_map_path_file_prefix():
    path_map = {"Movies": "/mnt/media/movies"}
    assert map_path("file:/mnt/media/movies/input.mkv", path_map) == ("file:$Movies/input.mkv", "Movies")


def test_map_arguments():
    path_map = {"Movies": "/mnt/media/movies", "TV": "/mnt/media/tv"}
    raw_args = ["-i", "/mnt/media/movies/input.mkv", "-vf", "scale=1920:1080", "/mnt/media/tv/output.mp4"]
    processed, used = map_arguments(raw_args, path_map)
    assert processed == ["-i", "$Movies/input.mkv", "-vf", "scale=1920:1080", "$TV/output.mp4"]
    assert set(used) == {"Movies", "TV"}


def test_resolve_path_basic():
    path_map = {"Movies": "/mnt/media/movies"}
    assert resolve_path("$Movies/input.mkv", path_map) == "/mnt/media/movies/input.mkv"
    assert resolve_path("$Movies", path_map) == "/mnt/media/movies"


def test_resolve_path_not_found():
    path_map = {"Movies": "/mnt/media/movies"}
    assert resolve_path("$Unknown/input.mkv", path_map) == "$Unknown/input.mkv"


def test_resolve_path_no_double_slash():
    path_map = {"Movies": "/mnt/media/movies/"}  # Base path ends in slash
    assert resolve_path("$Movies/input.mkv", path_map) == "/mnt/media/movies/input.mkv"


def test_resolve_path_file_prefix():
    path_map = {"Movies": "/mnt/media/movies"}
    assert resolve_path("file:$Movies/input.mkv", path_map) == "file:/mnt/media/movies/input.mkv"


def test_resolve_arguments():
    path_map = {"Movies": "/mnt/media/movies"}
    args = ["-i", "$Movies/input.mkv", "output.mp4", "file:$Movies/list.txt"]
    resolved = resolve_arguments(args, path_map)
    assert resolved == ["-i", "/mnt/media/movies/input.mkv", "output.mp4", "file:/mnt/media/movies/list.txt"]
