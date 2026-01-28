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
