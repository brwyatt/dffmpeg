import ipaddress

import pytest
from httpx import ASGITransport, AsyncClient

from dffmpeg.common.auth.request_signer import RequestSigner


@pytest.mark.anyio
async def test_proxy_trusted(test_app, sign_request, create_auth_identity):
    # test_app config has trusted_proxies=["127.0.0.1"] by default

    client_id = "proxy_user"
    client_key = RequestSigner.generate_key()
    signer = RequestSigner(client_key)

    async with test_app.router.lifespan_context(test_app):
        # Create identity allowed from 192.168.1.5
        await create_auth_identity(
            test_app, client_id, "client", client_key, allowed_cidrs=[ipaddress.ip_network("192.168.1.5/32")]
        )

        # 1. Trusted Proxy (127.0.0.1) -> XFF: 192.168.1.5 (Allowed)
        transport = ASGITransport(app=test_app, client=("127.0.0.1", 50000))
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            path = "/jobs"
            headers = await sign_request(signer, client_id, "GET", path)
            headers["X-Forwarded-For"] = "192.168.1.5"

            resp = await client.get(path, headers=headers)
            assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"

        # 2. Trusted Proxy (127.0.0.1) -> XFF: 10.0.0.1 (Denied)
        transport = ASGITransport(app=test_app, client=("127.0.0.1", 50000))
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            path = "/jobs"
            headers = await sign_request(signer, client_id, "GET", path)
            headers["X-Forwarded-For"] = "10.0.0.1"

            resp = await client.get(path, headers=headers)
            assert resp.status_code == 401
            assert "Client IP not allowed" in resp.json()["detail"]


@pytest.mark.anyio
async def test_proxy_untrusted(test_app, sign_request, create_auth_identity):
    # test_app config has trusted_proxies=["127.0.0.1"] by default

    client_id = "untrusted_user"
    client_key = RequestSigner.generate_key()
    signer = RequestSigner(client_key)

    async with test_app.router.lifespan_context(test_app):
        # Create identity allowed from 192.168.1.5
        await create_auth_identity(
            test_app, client_id, "client", client_key, allowed_cidrs=[ipaddress.ip_network("192.168.1.5/32")]
        )

        # Untrusted Proxy (10.0.0.2) -> XFF: 192.168.1.5
        # Since 10.0.0.2 is not trusted, XFF should be ignored.
        # The resolved IP will be 10.0.0.2.
        # 10.0.0.2 is NOT in allowed_cidrs (192.168.1.5/32).
        # So access should be denied.

        transport = ASGITransport(app=test_app, client=("10.0.0.2", 50000))
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            path = "/jobs"
            headers = await sign_request(signer, client_id, "GET", path)
            headers["X-Forwarded-For"] = "192.168.1.5"

            resp = await client.get(path, headers=headers)
            assert resp.status_code == 401
            assert "Client IP not allowed" in resp.json()["detail"]
