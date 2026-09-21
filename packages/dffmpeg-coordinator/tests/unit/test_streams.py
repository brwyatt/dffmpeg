import asyncio
import shutil
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from ulid import ULID

from dffmpeg.common.models import (
    AuthenticatedIdentity,
    ClientHeartbeatPayload,
    CommandResponse,
    EOFPayload,
    StreamProgress,
)
from dffmpeg.coordinator.api.routes.job import (
    job_client_heartbeat,
    job_stream_ack,
    job_stream_download,
    job_stream_write_chunk,
    job_stream_write_eof,
)
from dffmpeg.coordinator.janitor import Janitor
from dffmpeg.coordinator.streams import StreamStorageManager


@pytest.fixture
def temp_storage():
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.mark.anyio
async def test_stream_storage_manager_basic_flow(temp_storage):
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Write chunks
    await manager.write_chunk(job_id, stream_name, 0, b"Hello ")
    await manager.write_chunk(job_id, stream_name, 1, b"World!")
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=1, total_bytes=12))

    # Stream chunks and check content
    chunks = []
    async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=0):
        chunks.append(chunk)

    assert b"".join(chunks) == b"Hello World!"


@pytest.mark.anyio
async def test_stream_storage_manager_resumption(temp_storage):
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Write chunks
    await manager.write_chunk(job_id, stream_name, 0, b"AAAA")
    await manager.write_chunk(job_id, stream_name, 1, b"BBBB")
    await manager.write_chunk(job_id, stream_name, 2, b"CCCC")
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=2, total_bytes=12))

    # Resume at offset 2 (mid-chunk 0)
    chunks = []
    async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=2):
        chunks.append(chunk)
    assert b"".join(chunks) == b"AABBBBCCCC"

    # Resume at offset 5 (mid-chunk 1)
    chunks = []
    async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=5):
        chunks.append(chunk)
    assert b"".join(chunks) == b"BBBCCCC"


@pytest.mark.anyio
async def test_stream_storage_manager_pruning(temp_storage):
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Write chunks
    await manager.write_chunk(job_id, stream_name, 0, b"1234")
    await manager.write_chunk(job_id, stream_name, 1, b"5678")
    await manager.write_chunk(job_id, stream_name, 2, b"abcd")
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=2, total_bytes=12))

    # Prune progress up to 6 bytes, with safety margin of 2 bytes (retains 4 bytes from end)
    # Target prune limit = 6 - 2 = 4 bytes.
    # Chunk 0 is 4 bytes. 0 + 4 <= 4 is True, so Chunk 0 should be deleted.
    # Chunk 1 is 4 bytes. 4 + 4 <= 4 is False, so Chunk 1 is retained.
    await manager.prune_progress(job_id, stream_name, bytes_read=6, safety_margin_bytes=2)

    stream_dir = manager._get_stream_dir(job_id, stream_name)  # pyright: ignore[reportPrivateUsage]
    assert not (stream_dir / "00000000.chunk").exists()
    assert (stream_dir / "00000001.chunk").exists()
    assert (stream_dir / "00000002.chunk").exists()

    # Validate stream continues working when starting from start_offset=4 (right at pruned boundary)
    chunks = []
    async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=4):
        chunks.append(chunk)
    assert b"".join(chunks) == b"5678abcd"


@pytest.mark.anyio
async def test_stream_storage_manager_pruning_multiple_increments(temp_storage):
    """
    Paranoid check: Verifies that multiple incremental heartbeat pruning calls
    accurately update .pruned_bytes and delete ONLY the expected irregular files.
    """
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Write highly irregular chunks: 100B, 300B, 500B, 200B (total 1100B)
    await manager.write_chunk(job_id, stream_name, 0, b"A" * 100)
    await manager.write_chunk(job_id, stream_name, 1, b"B" * 300)
    await manager.write_chunk(job_id, stream_name, 2, b"C" * 500)
    await manager.write_chunk(job_id, stream_name, 3, b"D" * 200)
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=3, total_bytes=1100))

    stream_dir = manager._get_stream_dir(job_id, stream_name)  # pyright: ignore[reportPrivateUsage]

    # 1. Prune up to 450 bytes with safety margin 50 bytes (Target Limit: 400 bytes).
    # Chunk 0 (100B): 0 + 100 <= 400 (True) -> Pruned
    # Chunk 1 (300B): 100 + 300 <= 400 (True) -> Pruned
    # Chunk 2 (500B): 400 + 500 <= 400 (False) -> Retained
    # Expected .pruned_bytes = 400.
    await manager.prune_progress(job_id, stream_name, bytes_read=450, safety_margin_bytes=50)
    assert not (stream_dir / "00000000.chunk").exists()
    assert not (stream_dir / "00000001.chunk").exists()
    assert (stream_dir / "00000002.chunk").exists()
    assert manager.get_pruned_bytes(job_id, stream_name) == 400

    # 2. Duplicate prune call (should be safe and no-op)
    await manager.prune_progress(job_id, stream_name, bytes_read=450, safety_margin_bytes=50)
    assert manager.get_pruned_bytes(job_id, stream_name) == 400

    # 3. Prune up to 950 bytes with safety margin 50 bytes (Target Limit: 900 bytes).
    # Baseline: 400.
    # Chunk 2 (500B): 400 + 500 <= 900 (True) -> Pruned
    # Chunk 3 (200B): 900 + 200 <= 900 (False) -> Retained
    # Expected .pruned_bytes = 900.
    await manager.prune_progress(job_id, stream_name, bytes_read=950, safety_margin_bytes=50)
    assert not (stream_dir / "00000002.chunk").exists()
    assert (stream_dir / "00000003.chunk").exists()
    assert manager.get_pruned_bytes(job_id, stream_name) == 900


@pytest.mark.anyio
async def test_stream_storage_manager_pruned_irregular_resume(temp_storage):
    """
    Paranoid check: Verifies resumption when chunks are of irregular sizes,
    some have been pruned, and client reconnects in the middle of a remaining chunk.
    """
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Write irregular chunks: 100B, 500B, 250B
    await manager.write_chunk(job_id, stream_name, 0, b"A" * 100)
    await manager.write_chunk(job_id, stream_name, 1, b"B" * 500)
    await manager.write_chunk(job_id, stream_name, 2, b"C" * 250)
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=2, total_bytes=850))

    stream_dir = manager._get_stream_dir(job_id, stream_name)  # pyright: ignore[reportPrivateUsage]

    # Prune chunk 0 (100B)
    await manager.prune_progress(job_id, stream_name, bytes_read=300, safety_margin_bytes=200)  # Target: 100B
    assert not (stream_dir / "00000000.chunk").exists()
    assert manager.get_pruned_bytes(job_id, stream_name) == 100

    # Attempt to stream starting at offset 300 (which falls mid-way into chunk 1)
    # Chunk 1 starts at cumulative offset 100 and ends at 600.
    # The starting slice offset is 300 - 100 = 200 bytes inside Chunk 1.
    # So we expect 300 'B's and 250 'C's.
    chunks = []
    async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=300):
        chunks.append(chunk)

    expected = b"B" * 300 + b"C" * 250
    assert b"".join(chunks) == expected


@pytest.mark.anyio
async def test_stream_storage_manager_pruned_invalid_range_error(temp_storage):
    """
    Paranoid check: Requests a range start_offset < pruned_bytes and verifies it raises ValueError.
    """
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    await manager.write_chunk(job_id, stream_name, 0, b"A" * 100)
    await manager.write_chunk(job_id, stream_name, 1, b"B" * 200)
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=1, total_bytes=300))

    # Prune chunk 0 (100B)
    await manager.prune_progress(job_id, stream_name, bytes_read=200, safety_margin_bytes=100)  # Target: 100B

    # Attempting to read at offset 50 (which was already pruned) must raise ValueError
    with pytest.raises(ValueError) as exc:
        async for _ in manager.stream_chunks(job_id, stream_name, start_offset=50):
            pass
    assert "Requested start_offset 50 is less than pruned baseline" in str(exc.value)


@pytest.mark.anyio
async def test_stream_storage_manager_tail_streaming_irregular_speed(temp_storage):
    """
    Paranoid check: Simulates real-time "tail-streaming" where the client/reader is caught up,
    blocks patiently, wakes up immediately when the worker uploads irregular chunks, and finishes gracefully.
    """
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Worker writes chunks asynchronously with a fast sleep delay
    async def worker_writer():
        await asyncio.sleep(0.02)
        await manager.write_chunk(job_id, stream_name, 0, b"Hello ")
        await asyncio.sleep(0.05)
        await manager.write_chunk(job_id, stream_name, 1, b"World!")
        await asyncio.sleep(0.02)
        await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=1, total_bytes=12))

    # Client streams chunks simultaneously
    async def client_reader():
        chunks = []
        async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=0):
            chunks.append(chunk)
        return b"".join(chunks)

    # Run concurrently
    client_task = asyncio.create_task(client_reader())
    writer_task = asyncio.create_task(worker_writer())

    results = await asyncio.gather(client_task, writer_task)
    assert results[0] == b"Hello World!"


@pytest.mark.anyio
async def test_janitor_cleanup(temp_storage):
    # Setup directories
    streams_manager = StreamStorageManager(str(temp_storage))

    active_job_id = str(ULID())
    expired_job_id = str(ULID())

    await streams_manager.write_chunk(active_job_id, "stdout", 0, b"data")
    await streams_manager.write_chunk(expired_job_id, "stdout", 0, b"data")

    # Mock DB/Job repos
    mock_worker_repo = AsyncMock()
    mock_job_repo = AsyncMock()
    mock_transports = AsyncMock()
    mock_config = AsyncMock()

    janitor = Janitor(
        worker_repo=mock_worker_repo,
        job_repo=mock_job_repo,
        transports=mock_transports,
        config=mock_config,
        streams=streams_manager,
        stream_retention_minutes=0,  # Prune immediately for test
    )

    # Mock active jobs in DB
    mock_job_repo.get_active_job_ids.return_value = {active_job_id}

    await janitor.reap_stale_streams()

    # Active stream dir must be retained
    assert (temp_storage / active_job_id).exists()
    # Expired/orphaned stream dir must be deleted
    assert not (temp_storage / expired_job_id).exists()


@pytest.mark.anyio
async def test_job_stream_write_chunk_api():
    job_id = ULID()
    worker_id = "worker01"

    # Mock request body
    mock_request = AsyncMock()
    mock_request.body.return_value = b"test payload data"

    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(worker_id=worker_id)

    mock_streams = AsyncMock()

    identity = AuthenticatedIdentity(client_id=worker_id, role="worker", authenticated=True, hmac_key="a" * 44)

    response = await job_stream_write_chunk(
        job_id=str(job_id),
        stream_name="stdout",
        seq=5,
        request=mock_request,
        identity=identity,
        job_repo=mock_job_repo,
        streams=mock_streams,
    )

    assert response == CommandResponse(status="ok")
    mock_streams.write_chunk.assert_called_once_with(str(job_id), "stdout", 5, b"test payload data")


@pytest.mark.anyio
async def test_job_stream_write_chunk_unauthorized():
    job_id = ULID()
    mock_request = AsyncMock()

    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(worker_id="different_worker")
    mock_streams = AsyncMock()

    identity = AuthenticatedIdentity(client_id="worker01", role="worker", authenticated=True, hmac_key="a" * 44)

    with pytest.raises(HTTPException) as exc:
        await job_stream_write_chunk(
            job_id=str(job_id),
            stream_name="stdout",
            seq=0,
            request=mock_request,
            identity=identity,
            job_repo=mock_job_repo,
            streams=mock_streams,
        )
    assert exc.value.status_code == 403


@pytest.mark.anyio
async def test_job_stream_write_eof_api():
    job_id = ULID()
    worker_id = "worker01"

    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(worker_id=worker_id)
    mock_streams = AsyncMock()

    identity = AuthenticatedIdentity(client_id=worker_id, role="worker", authenticated=True, hmac_key="a" * 44)
    payload = EOFPayload(final_sequence=12, total_bytes=1024)

    response = await job_stream_write_eof(
        job_id=str(job_id),
        stream_name="stdout",
        payload=payload,
        identity=identity,
        job_repo=mock_job_repo,
        streams=mock_streams,
    )

    assert response == CommandResponse(status="ok")
    mock_streams.write_eof.assert_called_once_with(str(job_id), "stdout", payload)


@pytest.mark.anyio
async def test_client_heartbeat_progress_api():
    job_id = ULID()
    client_id = "client01"

    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(requester_id=client_id, status="running")
    mock_transports = AsyncMock()
    mock_streams = AsyncMock()

    identity = AuthenticatedIdentity(client_id=client_id, role="client", authenticated=True, hmac_key="a" * 44)
    payload = ClientHeartbeatPayload(streams_progress={"stdout": StreamProgress(bytes_read=5000)})

    response = await job_client_heartbeat(
        job_id=str(job_id),
        monitor=True,
        payload=payload,
        identity=identity,
        transports=mock_transports,
        job_repo=mock_job_repo,
        streams=mock_streams,
    )

    assert response == CommandResponse(status="ok")
    mock_streams.prune_progress.assert_called_once_with(str(job_id), "stdout", 5000)


@pytest.mark.anyio
async def test_job_stream_download_pruned_error_api(temp_storage):
    """
    Paranoid check: Verifies that downloading a range start_offset < pruned_bytes
    synchronously raises HTTP 416 Range Not Satisfiable at API level.
    """
    job_id = ULID()
    client_id = "client01"

    streams_manager = StreamStorageManager(str(temp_storage))

    # Setup pruned folder
    await streams_manager.write_chunk(str(job_id), "stdout", 0, b"data")
    await streams_manager.prune_progress(str(job_id), "stdout", bytes_read=4, safety_margin_bytes=0)

    mock_job_repo = AsyncMock()
    mock_job_repo.get_job.return_value = Mock(requester_id=client_id)

    identity = AuthenticatedIdentity(client_id=client_id, role="client", authenticated=True, hmac_key="a" * 44)

    # Calling download at start_offset=2 (which was pruned) must raise 416
    with pytest.raises(HTTPException) as exc:
        await job_stream_download(
            job_id=str(job_id),
            stream_name="stdout",
            start_offset=2,
            identity=identity,
            job_repo=mock_job_repo,
            streams=streams_manager,
        )
    assert exc.value.status_code == 416
    assert "data no longer available on disk" in str(exc.value.detail)


@pytest.mark.anyio
async def test_job_stream_ack_immediate_cleanup_api(temp_storage):
    """
    Paranoid check: Verifies that calling /ack on a completed job instantly deletes the streams folder on disk.
    """
    job_id = ULID()
    client_id = "client01"

    streams_manager = StreamStorageManager(str(temp_storage))
    await streams_manager.write_chunk(str(job_id), "stdout", 0, b"some chunk data")

    mock_job_repo = AsyncMock()
    # Mocking terminal completed status
    mock_job_repo.get_job.return_value = Mock(requester_id=client_id, status="completed")

    identity = AuthenticatedIdentity(client_id=client_id, role="client", authenticated=True, hmac_key="a" * 44)

    # Calling ACK must trigger stream deletion because status is terminal (completed)
    response = await job_stream_ack(
        job_id=str(job_id),
        stream_name="stdout",
        identity=identity,
        job_repo=mock_job_repo,
        streams=streams_manager,
    )

    assert response == CommandResponse(status="ok")
    # File must be deleted immediately
    assert not (temp_storage / str(job_id)).exists()


@pytest.mark.anyio
async def test_stream_storage_manager_empty_stream(temp_storage):
    manager = StreamStorageManager(str(temp_storage))
    job_id = str(ULID())
    stream_name = "stdout"

    # Now we can safely write EOF via write_eof using an EOFPayload with None/0
    await manager.write_eof(job_id, stream_name, EOFPayload(final_sequence=None, total_bytes=0))

    # The stream_chunks generator should now detect 0-bytes empty stream EOF immediately
    # and complete without hanging/timeout.
    chunks = []
    async with asyncio.timeout(0.2):
        async for chunk in manager.stream_chunks(job_id, stream_name, start_offset=0):
            chunks.append(chunk)
    assert chunks == []
