import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest
from fastapi import FastAPI
from ulid import ULID

from dffmpeg.common.models import AuthenticatedIdentity, IdentityRole, JobStatus, WorkerStatus
from dffmpeg.coordinator.api import create_app
from dffmpeg.coordinator.config import CoordinatorConfig
from dffmpeg.coordinator.db import DBConfig
from dffmpeg.coordinator.db.jobs import JobRecord
from dffmpeg.coordinator.db.workers import WorkerRecord


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


@pytest.fixture
def sign_request():
    async def _sign_request(signer, client_id, method, path, body=None):
        if body and isinstance(body, dict):
            payload = json.dumps(body)
        elif body and isinstance(body, str):
            payload = body
        else:
            payload = ""

        timestamp, signature = signer.sign(method, path, payload)
        return {
            "x-dffmpeg-client-id": client_id,
            "x-dffmpeg-timestamp": timestamp,
            "x-dffmpeg-signature": signature,
            "Content-Type": "application/json",
        }

    return _sign_request


@pytest.fixture
def create_auth_identity():
    async def _create(app: FastAPI, client_id: str, role: IdentityRole, key: str | None):
        await app.state.db.auth.add_identity(AuthenticatedIdentity(client_id=client_id, role=role, hmac_key=key))

    return _create


@pytest.fixture
def create_worker_record():
    async def _create(
        app: FastAPI,
        worker_id: str,
        status: WorkerStatus = "online",
        transport: str = "http_polling",
        transport_metadata: Optional[Dict[str, Any]] = None,
    ) -> WorkerRecord:
        record = WorkerRecord(
            worker_id=worker_id,
            status=status,
            capabilities=["ffmpeg"],
            binaries=["ffmpeg"],
            paths=["Movies"],
            transport=transport,
            transport_metadata=transport_metadata if transport_metadata is not None else {},
            last_seen=datetime.now(timezone.utc),
        )
        await app.state.db.workers.add_or_update(record)
        return record

    return _create


@pytest.fixture
def create_job_record():
    async def _create(
        app: FastAPI,
        job_id: ULID,
        requester_id: str,
        worker_id: Optional[str] = None,
        status: JobStatus = "pending",
        transport: str = "http_polling",
        transport_metadata: Optional[Dict[str, Any]] = None,
    ) -> JobRecord:
        job = JobRecord(
            job_id=job_id,
            requester_id=requester_id,
            binary_name="ffmpeg",
            arguments=["-i", "in", "out"],
            paths=["Movies"],
            status=status,
            worker_id=worker_id,
            transport=transport,
            transport_metadata=transport_metadata if transport_metadata is not None else {},
            created_at=datetime.now(timezone.utc),
            last_update=datetime.now(timezone.utc),
        )
        await app.state.db.jobs.create_job(job)
        return job

    return _create
