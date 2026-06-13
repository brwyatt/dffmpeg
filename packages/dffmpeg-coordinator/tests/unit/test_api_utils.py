from dffmpeg.coordinator.api.utils import sanitize_transport_metadata


def test_sanitize_transport_metadata_removes_private_keys():
    metadata = {
        "path": "/poll/worker",
        "_backend_metadata": {"queue_name": "dffmpeg.worker.worker01", "routing_key": "worker.worker01"},
        "something_else": "public",
    }

    sanitized = sanitize_transport_metadata(metadata)

    assert "path" in sanitized
    assert "something_else" in sanitized
    assert sanitized["path"] == "/poll/worker"
    assert sanitized["something_else"] == "public"
    assert "_backend_metadata" not in sanitized


def test_sanitize_transport_metadata_non_dict():
    assert sanitize_transport_metadata(None) is None
    assert sanitize_transport_metadata("some_string") == "some_string"
