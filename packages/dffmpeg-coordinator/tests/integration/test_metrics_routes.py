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


def test_metrics_endpoint_allowed(tmp_path):
    config = get_test_config(True, str(tmp_path / "test.db"))
    config.trusted_proxies = ["testclient"]
    config.allowed_metrics_ips = [ipaddress.ip_network("127.0.0.1/32")]

    app = create_app(config=config)
    with TestClient(app) as client:
        # Test client from 127.0.0.1 should be allowed
        response = client.get("/metrics", headers={"X-Forwarded-For": "127.0.0.1"})
        assert response.status_code == 200

        data = response.json()
        assert "total" in data
        assert "per_binary" in data
        assert "per_worker" in data


def test_metrics_endpoint_forbidden(tmp_path):
    config = get_test_config(True, str(tmp_path / "test.db"))
    config.trusted_proxies = ["testclient"]
    config.allowed_metrics_ips = [ipaddress.ip_network("127.0.0.1/32")]

    app = create_app(config=config)
    with TestClient(app) as client:
        # Test client from 192.168.1.5 should be forbidden
        response = client.get("/metrics", headers={"X-Forwarded-For": "192.168.1.5"})
        assert response.status_code == 403


def test_metrics_dashboard_disabled(tmp_path):
    # Metrics should still work even if dashboard is disabled
    config = get_test_config(False, str(tmp_path / "test.db"))
    config.trusted_proxies = ["testclient"]
    config.allowed_metrics_ips = [ipaddress.ip_network("127.0.0.1/32")]

    app = create_app(config=config)
    with TestClient(app) as client:
        response = client.get("/metrics", headers={"X-Forwarded-For": "127.0.0.1"})
        assert response.status_code == 200
