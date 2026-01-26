import os
import tempfile

import pytest

from dffmpeg.coordinator.api import create_app
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db import DBConfig


def pytest_collection_modifyitems(items):
    conftest_dir = os.path.dirname(__file__)
    for item in items:
        if str(item.fspath).startswith(conftest_dir):
            item.add_marker(pytest.mark.integration)


@pytest.fixture
async def test_app():
    # Setup a test-specific config with a temporary file-based SQLite
    # to avoid the connection pooling issues with :memory:
    db_fd, db_path = tempfile.mkstemp()
    os.close(db_fd)

    config = CoordinatorConfig(database=DBConfig(defaults={"engine": "sqlite", "path": db_path}))

    app = create_app(config)

    try:
        yield app
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)
