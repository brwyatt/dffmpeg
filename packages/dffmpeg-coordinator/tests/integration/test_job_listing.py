import pytest
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner


@pytest.mark.anyio
async def test_job_listing_pagination(test_app, sign_request, create_auth_identity, create_job_record):
    client_id = "client_list_01"
    client_key = RequestSigner.generate_key()
    signer = RequestSigner(client_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", client_key)

        # Create 5 jobs
        jobs = []
        for _ in range(5):
            job_id = ULID()
            job = await create_job_record(
                test_app,
                job_id=job_id,
                requester_id=client_id,
                status="pending",
                transport="http_polling",
                transport_metadata={"path": f"/poll/{job_id}"},
            )
            jobs.append(job)

        # Sort by ID desc (newest first)
        jobs.sort(key=lambda j: j.job_id, reverse=True)

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:

            # 1. List all (limit=10)
            path = "/jobs"
            headers = await sign_request(signer, client_id, "GET", path)
            resp = await client.get(path, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 5
            assert data[0]["job_id"] == str(jobs[0].job_id)

            # 2. List with limit=2
            path = "/jobs"
            params = {"limit": 2}
            # Signing the path "/jobs" without params works because server checks path only
            headers = await sign_request(signer, client_id, "GET", path)
            resp = await client.get(path, params=params, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 2
            assert data[0]["job_id"] == str(jobs[0].job_id)
            assert data[1]["job_id"] == str(jobs[1].job_id)

            last_id = data[-1]["job_id"]

            # 3. List next page (since_id)
            params = {"limit": 2, "since_id": last_id}
            headers = await sign_request(signer, client_id, "GET", path)
            resp = await client.get(path, params=params, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 2
            assert data[0]["job_id"] == str(jobs[2].job_id)
            assert data[1]["job_id"] == str(jobs[3].job_id)

            last_id = data[-1]["job_id"]

            # 4. List last page
            params = {"limit": 2, "since_id": last_id}
            headers = await sign_request(signer, client_id, "GET", path)
            resp = await client.get(path, params=params, headers=headers)
            assert resp.status_code == 200
            data = resp.json()
            assert len(data) == 1
            assert data[0]["job_id"] == str(jobs[4].job_id)
