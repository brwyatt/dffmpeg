from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from ulid import ULID

from dffmpeg.common.auth.request_signer import RequestSigner
from dffmpeg.common.models import JobStatus
from dffmpeg.coordinator.db.jobs import JobRecord


@pytest.mark.anyio
async def test_job_listing_window(test_app, sign_request, create_auth_identity):
    client_id = "client_list_01"
    client_key = RequestSigner.generate_key()
    signer = RequestSigner(client_key)

    async with test_app.router.lifespan_context(test_app):
        await create_auth_identity(test_app, client_id, "client", client_key)

        now = datetime.now(timezone.utc)

        # Helper to create job with specific timestamp
        async def create_job(status: JobStatus = "completed", age_seconds=0):
            job_id = ULID()
            timestamp = now - timedelta(seconds=age_seconds)
            job = JobRecord(
                job_id=job_id,
                requester_id=client_id,
                binary_name="ffmpeg",
                arguments=["-i"],
                paths=["test"],
                status=status,
                transport="http_polling",
                transport_metadata={},
                created_at=timestamp,
                last_update=timestamp,  # This is what matters for filtering
            )
            await test_app.state.db.jobs.create_job(job)
            return job

        # 1. Create a "recent" job (10 mins old)
        recent_job = await create_job(age_seconds=600)

        # 2. Create an "old" job (2 hours old)
        old_job = await create_job(age_seconds=7200)

        # 3. Create an active job (old created, but still running) - should always show
        active_job = await create_job(status="running", age_seconds=7200)

        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:

            # Test 1: Window = 1 hour (3600s)
            # Should see recent_job and active_job. Should NOT see old_job.
            path = "/jobs"
            params = {"window": 3600}
            headers = await sign_request(signer, client_id, "GET", path)
            resp = await client.get(path, params=params, headers=headers)
            assert resp.status_code == 200
            data = resp.json()

            ids = [j["job_id"] for j in data]
            assert str(recent_job.job_id) in ids
            assert str(active_job.job_id) in ids
            assert str(old_job.job_id) not in ids

            # Test 2: Window = 3 hours (10800s)
            # Should see all jobs
            params = {"window": 10800}
            headers = await sign_request(signer, client_id, "GET", path)
            resp = await client.get(path, params=params, headers=headers)
            assert resp.status_code == 200
            data = resp.json()

            ids = [j["job_id"] for j in data]
            assert str(recent_job.job_id) in ids
            assert str(active_job.job_id) in ids
            assert str(old_job.job_id) in ids
