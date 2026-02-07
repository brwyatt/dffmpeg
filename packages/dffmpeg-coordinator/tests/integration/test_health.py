from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from dffmpeg.common.models import ComponentHealth


@pytest.mark.anyio
async def test_shallow_health_check(test_app):
    async with test_app.router.lifespan_context(test_app):
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "online"
            assert data.get("databases") is None
            assert data.get("transports") is None


@pytest.mark.anyio
async def test_deep_health_check(test_app):
    async with test_app.router.lifespan_context(test_app):
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health?deep=true")
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "online"
            assert "databases" in data
            assert "transports" in data

            # Check databases
            assert "auth" in data["databases"]
            assert "jobs" in data["databases"]
            assert "messages" in data["databases"]
            assert "workers" in data["databases"]
            for repo in data["databases"].values():
                assert repo["status"] == "online"

            # Check transports
            assert "http_polling" in data["transports"]
            assert data["transports"]["http_polling"]["status"] == "online"


@pytest.mark.anyio
async def test_ping_removed(test_app):
    async with test_app.router.lifespan_context(test_app):
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # ping was a POST endpoint
            resp = await client.post("/ping", json={"message": "hello"})
            assert resp.status_code == 404


@pytest.mark.anyio
async def test_deep_health_check_unhealthy(test_app):
    async with test_app.router.lifespan_context(test_app):
        # Mock auth repository to be unhealthy
        unhealthy_status = ComponentHealth(status="unhealthy", detail="Database connection lost")
        with patch.object(test_app.state.db.auth, "health_check", new_callable=AsyncMock) as mock_health:
            mock_health.return_value = unhealthy_status

            transport = ASGITransport(app=test_app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get("/health?deep=true")
                assert resp.status_code == 500
                data = resp.json()
                assert data["status"] == "unhealthy"
                assert data["databases"]["auth"]["status"] == "unhealthy"
                assert data["databases"]["auth"]["detail"] == "Database connection lost"
