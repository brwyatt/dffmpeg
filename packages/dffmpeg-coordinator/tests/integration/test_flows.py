import pytest
import json
import asyncio
from httpx import AsyncClient, ASGITransport
from dffmpeg.common.auth.request_signer import RequestSigner

async def sign_request(signer, client_id, method, path, body=None):
    if body and isinstance(body, dict):
        payload = json.dumps(body)
    elif body and isinstance(body, str):
        payload = body
    else:
        payload = ""

    timestamp, signature = signer.sign(method, path, payload)
    return {
        "x-dffmpeg-client-id": client_id,
        "x-dffmpeg-timestamp": timestamp,
        "x-dffmpeg-signature": signature,
        "Content-Type": "application/json"
    }

@pytest.mark.anyio
async def test_worker_registration_and_polling(test_app):
    worker_id = "worker01"
    worker_key = RequestSigner.generate_key()
    signer = RequestSigner(worker_key)
    
    # We need to inject this key into the DB so the app can authenticate the worker
    async with test_app.router.lifespan_context(test_app):
        auth_repo = test_app.state.db.auth
        # Manually insert identity since we don't have a registration API for clients/workers yet
        encrypted_key, key_id = auth_repo._encrypt(worker_key)
        import aiosqlite
        await auth_repo.execute(
            f"INSERT INTO {auth_repo.tablename} (client_id, role, hmac_key, key_id) VALUES (?, ?, ?, ?)",
            (worker_id, "worker", encrypted_key, key_id)
        )

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
            }
            body_str = json.dumps(body)
            headers = await sign_request(signer, worker_id, "POST", path, body_str)
            
            resp = await client.post(path, content=body_str, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data["transport"] == "http_polling"
            
            # 2. Poll for work (should be empty initially)
            poll_path = "/poll/worker"
            headers = await sign_request(signer, worker_id, "GET", poll_path)
            resp = await client.get(poll_path, headers=headers)
            assert resp.status_code == 200
            assert resp.json()["messages"] == []

@pytest.mark.anyio
async def test_client_job_submission_flow(test_app):
    client_id = "client01"
    client_key = RequestSigner.generate_key()
    signer = RequestSigner(client_key)
    
    worker_id = "worker01"
    worker_key = RequestSigner.generate_key()
    worker_signer = RequestSigner(worker_key)

    async with test_app.router.lifespan_context(test_app):
        auth_repo = test_app.state.db.auth
        # Register both client and worker in DB
        import aiosqlite
        async with aiosqlite.connect(auth_repo.path) as db:
            for cid, key, role in [(client_id, client_key, "client"), (worker_id, worker_key, "worker")]:
                enc_key, kid = auth_repo._encrypt(key)
                await db.execute(
                    f"INSERT INTO {auth_repo.tablename} (client_id, role, hmac_key, key_id) VALUES (?, ?, ?, ?)",
                    (cid, role, enc_key, kid)
                )
            await db.commit()

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Register Worker so it's online
            reg_path = "/worker/register"
            reg_body = {
                "worker_id": worker_id,
                "capabilities": ["ffmpeg"],
                "binaries": ["ffmpeg"],
                "paths": ["Movies"],
                "supported_transports": ["http_polling"],
            }
            reg_body_str = json.dumps(reg_body)
            await client.post(reg_path, content=reg_body_str, headers=await sign_request(worker_signer, worker_id, "POST", reg_path, reg_body_str))

            # 2. Submit Job as Client
            submit_path = "/jobs/submit"
            job_body = {
                "binary_name": "ffmpeg",
                "arguments": ["-i", "in.mkv", "out.mp4"],
                "paths": ["Movies"],
                "supported_transports": ["http_polling"],
            }
            job_body_str = json.dumps(job_body)
            headers = await sign_request(signer, client_id, "POST", submit_path, job_body_str)
            resp = await client.post(submit_path, content=job_body_str, headers=headers)
            assert resp.status_code == 200
            job_data = resp.json()
            assert "transport_metadata" in job_data
            
            # 3. Worker polls and should get the job request
            poll_path = "/poll/worker"
            headers = await sign_request(worker_signer, worker_id, "GET", poll_path)
            resp = await client.get(poll_path, headers=headers)
            assert resp.status_code == 200
            messages = resp.json()["messages"]
            assert len(messages) > 0
            assert messages[0]["message_type"] == "job_request"
            assert messages[0]["payload"]["binary_name"] == "ffmpeg"
