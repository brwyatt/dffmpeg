import os

import pytest


def pytest_collection_modifyitems(items):
    conftest_dir = os.path.dirname(__file__)
    for item in items:
        if str(item.fspath).startswith(conftest_dir):
            item.add_marker(pytest.mark.unit)
