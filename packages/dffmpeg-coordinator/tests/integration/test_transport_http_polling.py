import pytest
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.models import JobRequestMessage, JobRequestPayload, JobStatusMessage, JobStatusPayload


@pytest.mark.anyio
async def test_http_polling_transport_worker_send_receive(
    test_app, sign_request, create_auth_identity, create_worker_record
):
    """
    Test the HTTP Polling transport for a worker (internal send -> http poll).
    """
    worker_id = "worker01"
    worker_key = RequestSigner.generate_key()
    signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        # 1. Setup Identity & Record
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        # Ensure worker record exists with http_polling transport
        transport_manager = test_app.state.transports
        transport_metadata = transport_manager["http_polling"].get_metadata(worker_id)
        record = await create_worker_record(
            test_app, worker_id, transport="http_polling", transport_metadata=transport_metadata
        )

        # Get the polling path from metadata
        poll_path = record.transport_metadata.get("path")
        assert poll_path is not None

        # 2. Send Message via Transport Manager
        job_id = ULID()
        msg = JobRequestMessage(
            recipient_id=worker_id,
            job_id=job_id,
            payload=JobRequestPayload(job_id=str(job_id), binary_name="ffmpeg", arguments=[], paths=[]),
        )

        await transport_manager.send_message(msg)

        # 3. Poll for message via HTTP
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = await sign_request(signer, worker_id, "GET", poll_path)
            resp = await client.get(poll_path, headers=headers)
            assert resp.status_code == 200

            data = resp.json()
            messages = data["messages"]
            assert len(messages) == 1
            assert messages[0]["recipient_id"] == worker_id
            assert messages[0]["message_id"] == str(msg.message_id)
            assert messages[0]["message_type"] == "job_request"
            assert messages[0]["job_id"] == str(job_id)
            assert messages[0]["payload"]["job_id"] == str(job_id)


@pytest.mark.anyio
async def test_http_polling_streaming_worker_send_receive(
    test_app, sign_request, create_auth_identity, create_worker_record
):
    """
    Test the HTTP Polling transport streaming for a worker.
    """
    import asyncio
    import json

    worker_id = "worker01_stream"
    worker_key = RequestSigner.generate_key()
    signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        # 1. Setup Identity & Record
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        transport_manager = test_app.state.transports
        transport_metadata = transport_manager["http_polling"].get_metadata(worker_id)
        record = await create_worker_record(
            test_app, worker_id, transport="http_polling", transport_metadata=transport_metadata
        )

        poll_path = record.transport_metadata.get("path")
        assert poll_path is not None

        # 2. Setup streaming client
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = await sign_request(signer, worker_id, "GET", poll_path)
            headers["Accept"] = "application/x-ndjson"

            async def do_poll():
                results = []
                async with client.stream("GET", poll_path, headers=headers) as resp:
                    assert resp.status_code == 200
                    assert resp.headers.get("content-type") == "application/x-ndjson"

                    # Verify streaming headers on chunked NDJSON response
                    assert resp.headers.get("Connection") == "keep-alive"
                    assert resp.headers.get("X-Accel-Buffering") == "no"

                    async for line in resp.aiter_lines():
                        if not line.strip():
                            continue
                        data = json.loads(line)
                        results.append(data)
                        total_so_far = sum(len(r.get("messages", [])) for r in results)
                        if total_so_far >= 2:
                            break
                return results

            poll_task = asyncio.create_task(do_poll())

            # Yield to let the request start
            await asyncio.sleep(0.1)

            # 3. Send Message 1
            job_id1 = ULID()
            msg1 = JobRequestMessage(
                recipient_id=worker_id,
                job_id=job_id1,
                payload=JobRequestPayload(job_id=str(job_id1), binary_name="ffmpeg", arguments=[], paths=[]),
            )
            await transport_manager.send_message(msg1)

            # 4. Send Message 2
            job_id2 = ULID()
            msg2 = JobRequestMessage(
                recipient_id=worker_id,
                job_id=job_id2,
                payload=JobRequestPayload(job_id=str(job_id2), binary_name="ffprobe", arguments=[], paths=[]),
            )
            await transport_manager.send_message(msg2)

            # 5. Wait for both messages to be received
            await asyncio.sleep(0.5)  # Give time for messages to process and stream to yield

            # Force the server loop to exit so ASGITransport flushes
            test_app.state.shutting_down = True
            await transport_manager.drain_all()

            results = []
            try:
                results = await asyncio.wait_for(poll_task, timeout=2.0)
            except asyncio.TimeoutError:
                pass  # let's see what we got

            # We might get 1 line with 2 messages, or 2 lines with 1 message each
            total_messages_received = []
            for r in results:
                total_messages_received.extend(r.get("messages", []))

            assert len(total_messages_received) == 2
            assert total_messages_received[0]["message_id"] == str(msg1.message_id)
            assert total_messages_received[1]["message_id"] == str(msg2.message_id)


@pytest.mark.anyio
async def test_http_polling_transport_client_send_receive(
    test_app, sign_request, create_auth_identity, create_job_record
):
    """
    Test the HTTP Polling transport for a client job (internal send -> http poll).
    """
    client_id = "client01"
    client_key = RequestSigner.generate_key()
    signer = RequestSigner(client_key)

    async with test_app.router.lifespan_context(test_app):
        # 1. Setup Identity & Job
        await create_auth_identity(test_app, client_id, "client", client_key)
        # Ensure job record exists with http_polling transport
        job_id = ULID()
        transport_manager = test_app.state.transports
        transport_metadata = transport_manager["http_polling"].get_metadata(client_id, job_id=job_id)
        job = await create_job_record(
            test_app, job_id, client_id, transport="http_polling", transport_metadata=transport_metadata
        )

        # Get the polling path from metadata
        poll_path = job.transport_metadata.get("path")
        assert poll_path is not None

        # 2. Send Message via Transport Manager
        msg = JobStatusMessage(recipient_id=client_id, job_id=job_id, payload=JobStatusPayload(status="running"))

        await transport_manager.send_message(msg)

        # 3. Poll for message via HTTP
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = await sign_request(signer, client_id, "GET", poll_path)
            resp = await client.get(poll_path, headers=headers)
            assert resp.status_code == 200

            data = resp.json()
            messages = data["messages"]
            assert len(messages) == 1
            assert messages[0]["recipient_id"] == client_id
            assert messages[0]["message_id"] == str(msg.message_id)
            assert messages[0]["message_type"] == "job_status"
            assert messages[0]["job_id"] == str(job_id)
            assert messages[0]["payload"]["status"] == "running"


@pytest.mark.anyio
async def test_http_polling_transport_drain(test_app, sign_request, create_auth_identity, create_worker_record):
    """
    Test that calling drain() on the HTTP Polling transport immediately wakes up
    and terminates long-polling requests when shutting down.
    """
    import asyncio

    worker_id = "worker01"
    worker_key = RequestSigner.generate_key()
    signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        # 1. Setup Identity & Record
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        transport_manager = test_app.state.transports
        transport_metadata = transport_manager["http_polling"].get_metadata(worker_id)
        record = await create_worker_record(
            test_app, worker_id, transport="http_polling", transport_metadata=transport_metadata
        )

        poll_path = record.transport_metadata.get("path")
        assert poll_path is not None

        # 2. Prepare background polling task that waits for 5 seconds
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            headers = await sign_request(signer, worker_id, "GET", poll_path)

            async def do_poll():
                return await client.get(poll_path, headers=headers, params={"wait": 5})

            poll_task = asyncio.create_task(do_poll())

            # Give the server loop a tiny moment to process the request and block
            await asyncio.sleep(0.1)

            # 3. Initiate Drain (simulating shutdown)
            test_app.state.shutting_down = True
            await transport_manager.drain_all()

            # 4. The poll_task should complete almost instantly, well before 5 seconds
            try:
                resp = await asyncio.wait_for(poll_task, timeout=0.5)
            except asyncio.TimeoutError:
                pytest.fail("Poll request was not awoken by drain()")

            assert resp.status_code == 200
            data = resp.json()
            # It should return empty messages because we're shutting down
            assert data["messages"] == []
