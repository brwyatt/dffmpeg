import ipaddress

from fastapi.testclient import TestClient

from dffmpeg.coordinator.api import create_app
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db import DBConfig


def get_test_config(web_enabled: bool, db_path: str):
    repo_config = {"engine": "sqlite", "path": db_path}
    db_config = DBConfig(
        repositories={
            "auth": repo_config,
            "jobs": repo_config,
            "messages": repo_config,
            "workers": repo_config,
        }
    )
    return CoordinatorConfig(web_dashboard_enabled=web_enabled, database=db_config)


def test_dashboard_redirect(tmp_path):
    config = get_test_config(True, str(tmp_path / "test.db"))
    app = create_app(config=config)
    client = TestClient(app)

    # Test root redirect
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/status"


def test_dashboard_disabled_redirect(tmp_path):
    config = get_test_config(False, str(tmp_path / "test.db"))
    app = create_app(config=config)
    client = TestClient(app)

    # Test root redirect when dashboard is disabled
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/health"


def test_dashboard_disabled_404(tmp_path):
    config = get_test_config(False, str(tmp_path / "test.db"))
    config.trusted_proxies = ["testclient"]
    config.allowed_dashboard_ips = [ipaddress.ip_network("127.0.0.1/32")]

    app = create_app(config=config)
    with TestClient(app) as client:
        # Test dashboard page returns 404 when disabled even if IP is allowed
        response = client.get("/status", headers={"X-Forwarded-For": "127.0.0.1"})
        assert response.status_code == 404

        # Test dashboard page returns 404 when disabled even if IP is forbidden
        response = client.get("/status", headers={"X-Forwarded-For": "192.168.1.5"})
        assert response.status_code == 404


def test_dashboard_ip_restriction_allowed(tmp_path):
    import ipaddress

    config = get_test_config(True, str(tmp_path / "test.db"))
    config.trusted_proxies = ["testclient"]
    config.allowed_dashboard_ips = [ipaddress.ip_network("127.0.0.1/32")]

    app = create_app(config=config)
    with TestClient(app) as client:
        # Test client from 127.0.0.1 should be allowed
        response = client.get("/status", headers={"X-Forwarded-For": "127.0.0.1"})
        assert response.status_code == 200

        response = client.get("/status/data", headers={"X-Forwarded-For": "127.0.0.1"})
        assert response.status_code == 200


def test_dashboard_ip_restriction_forbidden(tmp_path):
    import ipaddress

    config = get_test_config(True, str(tmp_path / "test.db"))
    config.trusted_proxies = ["testclient"]
    config.allowed_dashboard_ips = [ipaddress.ip_network("127.0.0.1/32")]

    app = create_app(config=config)
    with TestClient(app) as client:
        # Test client from 192.168.1.5 should be forbidden
        response = client.get("/status", headers={"X-Forwarded-For": "192.168.1.5"})
        assert response.status_code == 403

        response = client.get("/status/data", headers={"X-Forwarded-For": "192.168.1.5"})
        assert response.status_code == 403
