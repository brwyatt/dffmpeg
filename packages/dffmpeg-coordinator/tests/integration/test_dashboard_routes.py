from fastapi.testclient import TestClient

from dffmpeg.coordinator.api import create_app
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db import DBConfig


def get_test_config(web_enabled: bool):
    # Use in-memory SQLite for testing to avoid path issues and persistence
    repo_config = {"engine": "sqlite", "path": ":memory:"}
    db_config = DBConfig(
        repositories={
            "auth": repo_config,
            "jobs": repo_config,
            "messages": repo_config,
            "workers": repo_config,
        }
    )
    return CoordinatorConfig(web_dashboard_enabled=web_enabled, database=db_config)


def test_dashboard_redirect():
    config = get_test_config(True)
    app = create_app(config=config)
    client = TestClient(app)

    # Test root redirect
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/status"


def test_dashboard_disabled_redirect():
    config = get_test_config(False)
    app = create_app(config=config)
    client = TestClient(app)

    # Test root redirect when dashboard is disabled
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/health"


def test_dashboard_disabled_404():
    config = get_test_config(False)
    app = create_app(config=config)
    with TestClient(app) as client:
        # Test dashboard page returns 404 when disabled
        response = client.get("/status")
        assert response.status_code == 404
