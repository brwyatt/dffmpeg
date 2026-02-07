from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from ulid import ULID

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    CommandResponse,
    JobLogsMessage,
    JobLogsPayload,
    JobLogsResponse,
    LogEntry,
)
from dffmpeg.coordinator.api.routes.job import job_logs_get, job_logs_submit


@pytest.mark.anyio
async def test_job_logs_submit_isolated():
    job_id = ULID()
    worker_id = "worker01"
    requester_id = "client01"

    # Mock dependencies
    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(worker_id=worker_id, requester_id=requester_id)
    mock_transports = AsyncMock()

    identity = AuthenticatedIdentity(client_id=worker_id, role="worker", authenticated=True, hmac_key="a" * 44)
    payload = JobLogsPayload(logs=[LogEntry(stream="stdout", content="test log")])

    response = await job_logs_submit(
        job_id=str(job_id), payload=payload, identity=identity, transports=mock_transports, job_repo=mock_job_repo
    )

    assert response == CommandResponse(status="ok")
    mock_transports.send_message.assert_called_once()
    sent_msg = mock_transports.send_message.call_args[0][0]
    assert sent_msg.recipient_id == requester_id
    assert sent_msg.message_type == "job_logs"
    assert sent_msg.payload.logs[0].content == "test log"


@pytest.mark.anyio
async def test_job_logs_submit_wrong_worker():
    job_id = ULID()
    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(worker_id="other_worker")
    mock_transports = AsyncMock()

    identity = AuthenticatedIdentity(client_id="worker01", role="worker", authenticated=True, hmac_key="a" * 44)
    payload = JobLogsPayload(logs=[LogEntry(stream="stdout", content="test log")])

    with pytest.raises(HTTPException) as exc:
        await job_logs_submit(str(job_id), payload, identity, mock_transports, mock_job_repo)
    assert exc.value.status_code == 403


@pytest.mark.anyio
async def test_job_logs_get_isolated():
    job_id = ULID()
    client_id = "client01"
    msg_id = ULID()

    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(requester_id=client_id, worker_id="worker01")

    mock_msg_repo = AsyncMock()
    mock_msg_repo.get_job_messages.return_value = [
        JobLogsMessage(
            recipient_id=client_id,
            job_id=job_id,
            payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="line 1")]),
            message_id=msg_id,
        )
    ]

    identity = AuthenticatedIdentity(client_id=client_id, role="client", authenticated=True, hmac_key="a" * 44)

    response = await job_logs_get(
        job_id=str(job_id), identity=identity, job_repo=mock_job_repo, message_repo=mock_msg_repo
    )

    assert isinstance(response, JobLogsResponse)
    assert len(response.logs) == 1
    assert response.logs[0].content == "line 1"
    assert response.last_message_id == msg_id


@pytest.mark.anyio
async def test_job_logs_get_unauthorized():
    job_id = ULID()
    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(requester_id="client01", worker_id="worker01")
    mock_msg_repo = AsyncMock()

    identity = AuthenticatedIdentity(client_id="intruder", role="client", authenticated=True, hmac_key="a" * 44)

    with pytest.raises(HTTPException) as exc:
        await job_logs_get(str(job_id), None, None, identity, mock_job_repo, mock_msg_repo)
    assert exc.value.status_code == 403
