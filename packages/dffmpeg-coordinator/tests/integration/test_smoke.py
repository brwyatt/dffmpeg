import json

import pytest
from httpx import ASGITransport, AsyncClient

from dffmpeg.common.auth.request_signer import RequestSigner


@pytest.mark.anyio
async def test_worker_registration_and_polling(test_app, sign_request, create_auth_identity):
    worker_id = "worker01"
    worker_key = RequestSigner.generate_key()
    signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, worker_id, "worker", worker_key)

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Register Worker
            path = "/worker/register"
            body = {
                "worker_id": worker_id,
                "capabilities": ["h264"],
                "binaries": ["ffmpeg"],
                "paths": ["Movies"],
                "supported_transports": ["http_polling"],
                "registration_interval": 30,
            }
            body_str = json.dumps(body)
            headers = await sign_request(signer, worker_id, "POST", path, body_str)

            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data["transport"] == "http_polling"

            # Verify DB state
            worker = await test_app.state.db.workers.get_worker(worker_id)
            assert worker is not None
            assert worker.status == "online"

            # 2. Poll for work
            poll_path = "/poll/worker"
            headers = await sign_request(signer, worker_id, "GET", poll_path)
            resp = await client.get(poll_path, headers=headers)
            assert resp.status_code == 200
            assert resp.json()["messages"] == []


@pytest.mark.anyio
async def test_client_job_submission_flow(test_app, sign_request, create_auth_identity):
    client_id = "client01"
    client_key = RequestSigner.generate_key()
    client_signer = RequestSigner(client_key)

    worker_id = "worker01"
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", client_key)
        await create_auth_identity(test_app, worker_id, "worker", worker_key)

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Register Worker
            reg_path = "/worker/register"
            reg_body = {
                "worker_id": worker_id,
                "capabilities": ["ffmpeg"],
                "binaries": ["ffmpeg"],
                "paths": ["Movies"],
                "supported_transports": ["http_polling"],
                "registration_interval": 30,
            }
            reg_body_str = json.dumps(reg_body)
            await client.post(
                reg_path,
                content=reg_body_str,
                headers=await sign_request(worker_signer, worker_id, "POST", reg_path, reg_body_str),
            )

            # 2. Submit Job
            submit_path = "/jobs/submit"
            job_body = {
                "binary_name": "ffmpeg",
                "arguments": ["-i", "in.mkv", "out.mp4"],
                "paths": ["Movies"],
                "supported_transports": ["http_polling"],
            }
            job_body_str = json.dumps(job_body)
            headers = await sign_request(client_signer, client_id, "POST", submit_path, job_body_str)
            resp = await client.post(submit_path, content=job_body_str, headers=headers)
            assert resp.status_code == 200
            job_id = resp.json()["job_id"]

            # Verify DB state
            job = await test_app.state.db.jobs.get_job(job_id)
            assert job.status in ["pending", "assigned"]
