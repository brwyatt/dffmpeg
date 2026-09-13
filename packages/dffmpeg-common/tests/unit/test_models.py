from typing import Any, Dict

import pytest
from pydantic import TypeAdapter, ValidationError
from ulid import ULID

from dffmpeg.common.models import (
    ClientHeartbeatPayload,
    Job,
    JobRequest,
    JobRequestPayload,
    JobStreamModeSwitchMessage,
    JobStreamModeSwitchPayload,
    Message,
)


def test_job_stream_mode_switch_message_serialization():
    job_id = ULID()
    message_id = ULID()

    payload = JobStreamModeSwitchPayload(
        stream="stdout",
        mode="binary",
        endpoint_path="/jobs/123/streams/stdout",
    )

    msg = JobStreamModeSwitchMessage(
        message_id=message_id,
        recipient_id="client123",
        job_id=job_id,
        payload=payload,
    )

    # Assert fields are accessible
    assert msg.message_type == "job_stream_mode_switch"
    assert msg.payload.stream == "stdout"
    assert msg.payload.mode == "binary"
    assert msg.payload.endpoint_path == "/jobs/123/streams/stdout"

    # Serialize and deserialize via standard message TypeAdapter
    adapter = TypeAdapter(Message)
    serialized = adapter.dump_python(msg)

    # Ensure correct structure for union discriminator
    assert serialized["message_type"] == "job_stream_mode_switch"
    assert serialized["payload"]["stream"] == "stdout"

    # Round-trip check
    deserialized = adapter.validate_python(serialized)
    assert isinstance(deserialized, JobStreamModeSwitchMessage)
    assert deserialized.message_id == message_id
    assert deserialized.payload.stream == "stdout"


def test_job_stream_mode_switch_validation_invalid():
    # Attempting to use a non-supported stream name like "stderr" or "stdin" (for now)
    with pytest.raises(ValidationError):
        JobStreamModeSwitchPayload(
            stream="stderr",  # pyright: ignore[reportArgumentType]
            mode="binary",
            endpoint_path="/jobs/123/streams/stderr",
        )


def test_client_heartbeat_payload_validation():
    # Test without streams_progress
    payload_dict: Dict[str, Any] = {}
    payload = ClientHeartbeatPayload.model_validate(payload_dict)
    assert payload.streams_progress is None

    # Test with empty streams_progress
    payload_dict = {"streams_progress": {}}
    payload = ClientHeartbeatPayload.model_validate(payload_dict)
    assert payload.streams_progress == {}

    # Test with fully populated streams_progress using bytes_read
    payload_dict = {
        "streams_progress": {
            "stdout": {"bytes_read": 1048576},
        },
    }
    payload = ClientHeartbeatPayload.model_validate(payload_dict)
    assert payload.streams_progress is not None
    assert payload.streams_progress["stdout"].bytes_read == 1048576

    # Validation fails if stream name is invalid (e.g. "stderr")
    with pytest.raises(ValidationError):
        ClientHeartbeatPayload.model_validate(
            {
                "streams_progress": {
                    "stderr": {"bytes_read": 100},
                }
            }
        )


def test_job_request_supported_features_backward_compatibility():
    # Omitting supported_features should default to empty list on JobRequest
    req_dict = {
        "binary_name": "ffmpeg",
        "arguments": ["-i", "input.mp4", "output.mkv"],
        "paths": ["input.mp4", "output.mkv"],
        "supported_transports": ["http_polling"],
    }
    req = JobRequest.model_validate(req_dict)
    assert req.supported_features == []

    # Including supported_features
    req_dict["supported_features"] = ["binary_stream"]
    req = JobRequest.model_validate(req_dict)
    assert req.supported_features == ["binary_stream"]


def test_job_supported_features_backward_compatibility():
    # Omitting supported_features should default to empty list on Job
    job_dict: Dict[str, Any] = {
        "requester_id": "client123",
        "binary_name": "ffmpeg",
        "status": "pending",
    }
    job = Job.model_validate(job_dict)
    assert job.supported_features == []

    # Including supported_features
    job_dict["supported_features"] = ["binary_stream"]
    job = Job.model_validate(job_dict)
    assert job.supported_features == ["binary_stream"]


def test_job_request_payload_supported_features_backward_compatibility():
    # Omitting supported_features should default to empty list on JobRequestPayload
    payload_dict = {
        "job_id": str(ULID()),
        "binary_name": "ffmpeg",
        "arguments": [],
        "paths": [],
    }
    payload = JobRequestPayload.model_validate(payload_dict)
    assert payload.supported_features == []

    # Including supported_features
    payload_dict["supported_features"] = ["binary_stream"]
    payload = JobRequestPayload.model_validate(payload_dict)
    assert payload.supported_features == ["binary_stream"]
