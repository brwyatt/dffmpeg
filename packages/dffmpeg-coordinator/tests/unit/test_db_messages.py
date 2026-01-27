import pytest
from ulid import ULID

from dffmpeg.common.models import JobLogsMessage, JobLogsPayload, JobStatusMessage, JobStatusPayload, LogEntry
from dffmpeg.coordinator.db.messages.sqlite import SQLiteMessageRepository


@pytest.fixture
async def message_repo(tmp_path):
    db_path = tmp_path / "test_messages.db"
    repo = SQLiteMessageRepository(engine="sqlite", path=str(db_path))
    await repo.setup()
    return repo


@pytest.mark.anyio
async def test_get_job_messages_basic(message_repo):
    job_id = ULID()
    msg1 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="log 1")])
    )
    msg2 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="log 2")])
    )
    msg3 = JobLogsMessage(
        recipient_id="client1", job_id=ULID(), payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="other")])
    )

    await message_repo.add_message(msg1)
    await message_repo.add_message(msg2)
    await message_repo.add_message(msg3)

    messages = await message_repo.get_job_messages(job_id)
    assert len(messages) == 2
    assert messages[0].message_id == msg1.message_id
    assert messages[1].message_id == msg2.message_id


@pytest.mark.anyio
async def test_get_job_messages_type_filter(message_repo):
    job_id = ULID()
    msg1 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="log")])
    )
    msg2 = JobStatusMessage(recipient_id="client1", job_id=job_id, payload=JobStatusPayload(status="running"))

    await message_repo.add_message(msg1)
    await message_repo.add_message(msg2)

    messages = await message_repo.get_job_messages(job_id, message_type="job_logs")
    assert len(messages) == 1
    assert messages[0].message_type == "job_logs"
    assert messages[0].message_id == msg1.message_id


@pytest.mark.anyio
async def test_get_job_messages_since_id(message_repo):
    job_id = ULID()
    msg1 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="1")])
    )
    msg2 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="2")])
    )
    msg3 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="3")])
    )

    await message_repo.add_message(msg1)
    await message_repo.add_message(msg2)
    await message_repo.add_message(msg3)

    messages = await message_repo.get_job_messages(job_id, since_message_id=msg1.message_id)
    assert len(messages) == 2
    assert messages[0].message_id == msg2.message_id
    assert messages[1].message_id == msg3.message_id


@pytest.mark.anyio
async def test_get_job_messages_limit(message_repo):
    job_id = ULID()
    msg1 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="1")])
    )
    msg2 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="2")])
    )
    msg3 = JobLogsMessage(
        recipient_id="client1", job_id=job_id, payload=JobLogsPayload(logs=[LogEntry(stream="stdout", content="3")])
    )

    await message_repo.add_message(msg1)
    await message_repo.add_message(msg2)
    await message_repo.add_message(msg3)

    # Limit 2 should give us msg2 and msg3 (most recent)
    messages = await message_repo.get_job_messages(job_id, limit=2)
    assert len(messages) == 2
    assert messages[0].message_id == msg2.message_id
    assert messages[1].message_id == msg3.message_id
