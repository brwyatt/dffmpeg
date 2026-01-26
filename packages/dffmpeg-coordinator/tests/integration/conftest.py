import pytest
import os
from fastapi import FastAPI
from dffmpeg.coordinator.api import lifespan
from dffmpeg.coordinator.api.routes import health, job, test, worker
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db import DBConfig

def pytest_collection_modifyitems(items):
    conftest_dir = os.path.dirname(__file__)
    for item in items:
        if str(item.fspath).startswith(conftest_dir):
            item.add_marker(pytest.mark.integration)

@pytest.fixture
async def test_app():
    # Setup a test-specific config with in-memory SQLite
    config = CoordinatorConfig(
        database=DBConfig(
            defaults={
                "engine": "sqlite",
                "path": ":memory:"
            }
        )
    )
    
    # We need to manually wire the app to use our test config 
    # since the global 'app' in dffmpeg.coordinator.api uses a global 'config'
    app = FastAPI(title="dffmpeg Coordinator Test", lifespan=lifespan)
    
    app.include_router(health.router)
    app.include_router(worker.router)
    app.include_router(job.router)
    app.include_router(test.router)
    
    # Override the config used by the app's components
    from dffmpeg.coordinator import api
    original_config = api.config
    api.config = config

    # Ensure we're using a file-based sqlite even if temporary, to avoid :memory: issues with connection pools
    import tempfile
    import os
    db_file = tempfile.NamedTemporaryFile(delete=False)
    db_file.close()
    config.database.defaults["path"] = db_file.name

    try:
        yield app
    finally:
        api.config = original_config
        if os.path.exists(db_file.name):
            os.unlink(db_file.name)
