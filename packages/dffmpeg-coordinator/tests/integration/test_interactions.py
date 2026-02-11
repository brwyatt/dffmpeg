import json

import pytest
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.coordinator.api.routes.job import process_job_assignment


@pytest.mark.anyio
async def test_job_submission_interaction(test_app, sign_request, create_auth_identity):
    """
    Test that a client can submit a job.
    """
    client_id = "client01"
    client_key = RequestSigner.generate_key()
    client_signer = RequestSigner(client_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", client_key)

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Client submits job
            path = "/jobs/submit"
            body = {
                "binary_name": "ffmpeg",
                "arguments": ["-version"],
                "paths": [],
                "supported_transports": ["http_polling"],
            }
            body_str = json.dumps(body)
            headers = await sign_request(client_signer, client_id, "POST", path, body_str)

            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200

            job_resp = resp.json()
            job_id = ULID.from_str(job_resp["job_id"])

            # Verify: DB Updated
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job is not None
            assert job.status == "pending"
            assert job.requester_id == client_id


@pytest.mark.anyio
async def test_job_assignment_logic(test_app, create_auth_identity, create_worker_record, create_job_record):
    """
    Test the job assignment logic and message generation.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()

    async with test_app.router.lifespan_context(test_app):
        # Setup State
        await create_auth_identity(test_app, client_id, "client", RequestSigner.generate_key())
        await create_auth_identity(test_app, worker_id, "worker", RequestSigner.generate_key())

        # Create worker with matching capabilities
        await create_worker_record(test_app, worker_id)

        # Create pending job
        await create_job_record(test_app, job_id, client_id, status="pending")

        # Action: Run assignment manually
        await process_job_assignment(
            job_id, test_app.state.db.jobs, test_app.state.db.workers, test_app.state.transports
        )

        # Verify: Job Assigned
        job = await test_app.state.db.jobs.get_job(job_id)
        assert job.status == "assigned"
        assert job.worker_id == worker_id

        # Verify: Worker Notification
        messages = await test_app.state.db.messages.get_messages(worker_id)
        assert any(
            m.recipient_id == worker_id
            and m.sender_id is None
            and m.message_type == "job_request"
            and m.job_id == job_id
            and m.payload.job_id == str(job_id)
            for m in messages
        )

        # Verify: Client Notification
        messages = await test_app.state.db.messages.get_messages(client_id, job_id=job_id)
        assert any(
            m.recipient_id == client_id
            and m.sender_id is None
            and m.message_type == "job_status"
            and m.job_id == job_id
            and m.payload.status == "assigned"
            for m in messages
        )


@pytest.mark.anyio
async def test_job_acceptance_interaction(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test that a worker can accept an assigned job.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        # Setup State
        await create_auth_identity(test_app, client_id, "client", RequestSigner.generate_key())
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        # Create job in 'assigned' state
        await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="assigned")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Worker accepts job
            path = f"/jobs/{job_id}/accept"
            headers = await sign_request(worker_signer, worker_id, "POST", path)
            resp = await client.post(path, headers=headers)
            assert resp.status_code == 200

            # Verify: DB Updated
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.status == "running"

            # Verify: Client Notification
            messages = await test_app.state.db.messages.get_messages(client_id, job_id=job_id)
            assert any(
                m.recipient_id == client_id
                and m.sender_id == worker_id
                and m.message_type == "job_status"
                and m.job_id == job_id
                and m.payload.status == "running"
                for m in messages
            )


@pytest.mark.anyio
async def test_job_completion_interaction(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test that a worker can mark a job as completed.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        # Setup State
        await create_auth_identity(test_app, client_id, "client", RequestSigner.generate_key())
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="running")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Worker completes job
            path = f"/jobs/{job_id}/status"
            body = {"status": "completed"}
            body_str = json.dumps(body)
            headers = await sign_request(worker_signer, worker_id, "POST", path, body_str)

            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200

            # Verify: DB Updated
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.status == "completed"

            # Verify: Client Notification
            messages = await test_app.state.db.messages.get_messages(client_id, job_id=job_id)
            assert any(
                m.recipient_id == client_id
                and m.sender_id == worker_id
                and m.message_type == "job_status"
                and m.job_id == job_id
                and m.payload.status == "completed"
                for m in messages
            )


@pytest.mark.anyio
async def test_job_failure_interaction(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test that a worker can mark a job as failed.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", RequestSigner.generate_key())
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="running")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Worker fails job
            path = f"/jobs/{job_id}/status"
            body = {"status": "failed"}
            body_str = json.dumps(body)
            headers = await sign_request(worker_signer, worker_id, "POST", path, body_str)

            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200

            # Verify: DB Updated
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.status == "failed"

            # Verify: Client Notification
            messages = await test_app.state.db.messages.get_messages(client_id, job_id=job_id)
            assert any(
                m.recipient_id == client_id
                and m.sender_id == worker_id
                and m.message_type == "job_status"
                and m.job_id == job_id
                and m.payload.status == "failed"
                for m in messages
            )


@pytest.mark.anyio
async def test_job_heartbeat_interaction(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test that a worker heartbeat updates the last_update timestamp.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", RequestSigner.generate_key())
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        # Create job with old timestamp
        job = await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="running")
        original_update = job.last_update

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Worker sends heartbeat
            path = f"/jobs/{job_id}/worker_heartbeat"
            headers = await sign_request(worker_signer, worker_id, "POST", path)

            resp = await client.post(path, headers=headers)
            assert resp.status_code == 200

            # Verify: DB Updated
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.worker_last_seen > original_update


@pytest.mark.anyio
async def test_job_logs_interaction(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test log submission and retrieval.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()

    client_key = RequestSigner.generate_key()
    client_signer = RequestSigner(client_key)
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", client_key)
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="running")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Worker submits logs
            path = f"/jobs/{job_id}/logs"
            body = {"logs": [{"timestamp": "2026-01-28T00:00:00Z", "stream": "stdout", "content": "Log Line 1"}]}
            body_str = json.dumps(body)
            headers = await sign_request(worker_signer, worker_id, "POST", path, body_str)
            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200

            # Action: Client retrieves logs
            path = f"/jobs/{job_id}/logs"
            headers = await sign_request(client_signer, client_id, "GET", path)
            resp = await client.get(path, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert len(data["logs"]) == 1
            assert data["logs"][0]["content"] == "Log Line 1"


@pytest.mark.anyio
async def test_job_cancellation_interaction(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test client requesting job cancellation.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()

    client_key = RequestSigner.generate_key()
    client_signer = RequestSigner(client_key)
    worker_key = RequestSigner.generate_key()

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", client_key)
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="running")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Client cancels job
            path = f"/jobs/{job_id}/cancel"
            headers = await sign_request(client_signer, client_id, "POST", path)
            resp = await client.post(path, headers=headers)
            assert resp.status_code == 200

            # Verify: DB status updated to canceling (since worker is assigned)
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.status == "canceling"

            # Verify: Worker Notification
            messages = await test_app.state.db.messages.get_messages(worker_id)
            assert any(
                m.recipient_id == worker_id
                and m.sender_id == client_id
                and m.message_type == "job_status"
                and m.job_id == job_id
                and m.payload.status == "canceling"
                for m in messages
            )

            # Verify: Client Notification
            messages = await test_app.state.db.messages.get_messages(client_id, job_id=job_id)
            assert any(
                m.recipient_id == client_id
                and m.sender_id == client_id
                and m.message_type == "job_status"
                and m.job_id == job_id
                and m.payload.status == "canceling"
                for m in messages
            )


@pytest.mark.anyio
async def test_job_worker_cancellation_acknowledgment(
    test_app, sign_request, create_auth_identity, create_worker_record, create_job_record
):
    """
    Test that a worker can acknowledge a cancellation request by setting status to canceled.
    """
    client_id = "client01"
    worker_id = "worker01"
    job_id = ULID()
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        # Setup State
        await create_auth_identity(test_app, client_id, "client", RequestSigner.generate_key())
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        await create_worker_record(test_app, worker_id)
        # Create job in 'canceling' state
        await create_job_record(test_app, job_id, client_id, worker_id=worker_id, status="canceling")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Action: Worker confirms cancellation
            path = f"/jobs/{job_id}/status"
            body = {"status": "canceled"}
            body_str = json.dumps(body)
            headers = await sign_request(worker_signer, worker_id, "POST", path, body_str)

            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200

            # Verify: DB Updated
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.status == "canceled"

            # Verify: Client Notification
            messages = await test_app.state.db.messages.get_messages(client_id, job_id=job_id)
            assert any(
                m.recipient_id == client_id
                and m.sender_id == worker_id
                and m.message_type == "job_status"
                and m.job_id == job_id
                and m.payload.status == "canceled"
                for m in messages
            )


@pytest.mark.anyio
async def test_invalid_job_transitions(test_app, sign_request, create_auth_identity, create_job_record):
    """
    Test negative cases for job updates.
    """
    client_id = "client01"
    worker_id = "worker01"
    other_worker_id = "worker02"
    job_id = ULID()

    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, worker_id, "worker", worker_key)
        # Assign job to *other* worker
        await create_job_record(test_app, job_id, client_id, worker_id=other_worker_id, status="running")

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Case 1: Wrong Worker
            path = f"/jobs/{job_id}/status"
            body = {"status": "completed"}
            body_str = json.dumps(body)
            headers = await sign_request(worker_signer, worker_id, "POST", path, body_str)
            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 403

            # Case 2: Job Not Found
            path = f"/jobs/{ULID()}/status"
            headers = await sign_request(worker_signer, worker_id, "POST", path, body_str)
            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 404
