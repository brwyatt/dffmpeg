import asyncio
from unittest.mock import AsyncMock

import pytest
from ulid import ULID

from dffmpeg.common.models import (
    JobRequestMessage,
    JobRequestPayload,
    JobStatusMessage,
    JobStatusPayload,
    VerifyRegistrationMessage,
    VerifyRegistrationPayload,
)
from dffmpeg.common.transports import ClientTransportConfig
from dffmpeg.worker.transport import WorkerTransportManager

# Generate a static sequence of chronological ULIDs to use for test message sorting
M_ULIDS = [ULID.from_timestamp(1700000000.0 + i) for i in range(20)]


@pytest.mark.parametrize(
    "sequence, expected_index",
    [
        # 1. Simple Assign -> Cancel -> Assign flap
        (["assign", "cancel", "assign"], 2),
        # 2. Complex 4-flap Assign -> Cancel -> Assign -> Cancel
        (["assign", "cancel", "assign", "cancel"], 3),
        # 3. Starting with Cancel: Cancel -> Assign -> Cancel
        (["cancel", "assign", "cancel"], 2),
        # 4. Long 6-message flap
        (["cancel", "assign", "cancel", "assign", "cancel", "assign"], 5),
        # 5. Massive queue drain with duplicate status updates
        (["assign", "cancel", "cancel", "cancel", "assign", "assign"], 5),
    ],
)
def test_collapse_batch_filters_flapping(sequence, expected_index):
    job_id = ULID()
    messages = []

    # Build the sequence of mocked messages with chronologically increasing message IDs
    for idx, msg_type in enumerate(sequence):
        msg_id = M_ULIDS[idx]
        if msg_type == "assign":
            messages.append(
                JobRequestMessage(
                    message_id=msg_id,
                    recipient_id="worker1",
                    job_id=job_id,
                    payload=JobRequestPayload(job_id=str(job_id), binary_name="ffmpeg", arguments=[], paths=[]),
                )
            )
        elif msg_type == "cancel":
            messages.append(
                JobStatusMessage(
                    message_id=msg_id,
                    recipient_id="worker1",
                    job_id=job_id,
                    payload=JobStatusPayload(status="canceling"),
                )
            )

    collapsed = WorkerTransportManager.collapse_batch(messages)

    # Assert we collapsed down to exactly 1 message
    assert len(collapsed) == 1
    # Assert that the chosen message is the very last one in our chronological sequence
    assert collapsed[0] == messages[expected_index]


def test_collapse_batch_filters_multiple_jobs():
    job_a = ULID()
    job_b = ULID()

    msg_a1 = JobRequestMessage(
        message_id=M_ULIDS[0],
        recipient_id="worker1",
        job_id=job_a,
        payload=JobRequestPayload(job_id=str(job_a), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    msg_b1 = JobRequestMessage(
        message_id=M_ULIDS[1],
        recipient_id="worker1",
        job_id=job_b,
        payload=JobRequestPayload(job_id=str(job_b), binary_name="ffmpeg", arguments=[], paths=[]),
    )

    msg_a2 = JobStatusMessage(
        message_id=M_ULIDS[2], recipient_id="worker1", job_id=job_a, payload=JobStatusPayload(status="canceling")
    )

    messages = [msg_a1, msg_b1, msg_a2]
    collapsed = WorkerTransportManager.collapse_batch(messages)

    # Should contain msg_b1 and msg_a2
    assert len(collapsed) == 2
    assert msg_b1 in collapsed
    assert msg_a2 in collapsed
    assert msg_a1 not in collapsed


def test_collapse_batch_filters_registration_pings():
    """
    Test that multiple registration verification messages are collapsed
    to the most recent one.
    """
    msg1 = VerifyRegistrationMessage(
        message_id=M_ULIDS[0],
        recipient_id="worker1",
        payload=VerifyRegistrationPayload(registration_token="token1"),
    )
    msg2 = VerifyRegistrationMessage(
        message_id=M_ULIDS[1],
        recipient_id="worker1",
        payload=VerifyRegistrationPayload(registration_token="token2"),
    )
    msg3 = VerifyRegistrationMessage(
        message_id=M_ULIDS[2],
        recipient_id="worker1",
        payload=VerifyRegistrationPayload(registration_token="token3"),
    )

    messages = [msg1, msg2, msg3]
    collapsed = WorkerTransportManager.collapse_batch(messages)

    # We expect them to be collapsed to exactly 1 (the latest).
    assert len(collapsed) == 1
    assert collapsed[0] == msg3


@pytest.mark.asyncio
async def test_listen_batch_integration():
    """
    Brief integration test to ensure listen_batch correctly uses collapse_batch
    and operates as an async generator.
    """
    config = ClientTransportConfig()
    manager = WorkerTransportManager(config)

    mock_transport = AsyncMock()
    manager._current_transport = mock_transport

    job_id = ULID()
    msg1 = JobStatusMessage(
        message_id=M_ULIDS[0],
        recipient_id="worker1",
        job_id=job_id,
        payload=JobStatusPayload(status="canceling"),
    )
    msg2 = JobStatusMessage(
        message_id=M_ULIDS[1],
        recipient_id="worker1",
        job_id=job_id,
        payload=JobStatusPayload(status="canceling"),
    )

    # Setup mock to return msg1 from receive(), then msg2 and QueueEmpty from
    # receive_nowait(). We use a mutable list to ensure mock_receive_nowait()
    # returns msg2 once, then QueueEmpty.
    state = {"yielded_msg2": False}

    async def mock_receive():
        return msg1

    def mock_receive_nowait():
        # Using a state on the mock to return msg2 then raise
        if not state["yielded_msg2"]:
            state["yielded_msg2"] = True
            return msg2
        raise asyncio.QueueEmpty()

    mock_transport.receive = mock_receive
    mock_transport.receive_nowait = mock_receive_nowait

    # Use a small debounce for fast test
    batch_iterator = manager.listen_batch(debounce=0.01)

    batch = await batch_iterator.__anext__()

    # Should be collapsed by collapse_batch
    assert len(batch) == 1
    assert batch[0] == msg2
