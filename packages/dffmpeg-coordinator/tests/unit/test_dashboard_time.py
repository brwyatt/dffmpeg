from datetime import datetime, timezone

from dffmpeg.coordinator.api.routes.dashboard import format_utc


def test_format_utc_naive():
    # Naive datetime (e.g. from SQLite without timezone)
    dt = datetime(2023, 10, 27, 10, 0, 0)
    formatted = format_utc(dt)
    # Should append +00:00 or Z
    assert formatted == "2023-10-27T10:00:00+00:00"


def test_format_utc_aware():
    # Aware datetime (already UTC)
    dt = datetime(2023, 10, 27, 10, 0, 0, tzinfo=timezone.utc)
    formatted = format_utc(dt)
    assert formatted == "2023-10-27T10:00:00+00:00"


def test_format_utc_none():
    assert format_utc(None) is None
