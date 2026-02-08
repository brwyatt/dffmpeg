from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, TIMESTAMP, Column, ForeignKey, Integer, MetaData, String, Table, func
from ulid import ULID

from dffmpeg.common.models import JobRecord, JobStatus, TransportRecord
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class JobRepository(BaseDB):
    metadata = MetaData()

    auth_table = AuthRepository.table.to_metadata(metadata)

    table = Table(
        "jobs",
        metadata,
        Column("job_id", String, primary_key=True),
        Column("requester_id", String, ForeignKey("auth.client_id"), nullable=False),
        Column("binary_name", String, nullable=False, default="ffmpeg"),
        Column("arguments", JSON, nullable=False),
        Column("paths", JSON, nullable=False),
        Column("status", String, default="pending"),
        Column("exit_code", Integer, nullable=True),
        Column("worker_id", String, ForeignKey("auth.client_id"), nullable=True),
        Column("created_at", TIMESTAMP, server_default=func.current_timestamp()),
        Column("last_update", TIMESTAMP, server_default=func.current_timestamp()),
        Column("callback_transport", String, nullable=False),
        Column("callback_transport_metadata", JSON, nullable=False),
        Column("heartbeat_interval", Integer, nullable=False),
    )

    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.jobs", engine, cls))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def create_job(self, job: JobRecord) -> None:
        raise NotImplementedError()

    async def get_job(self, job_id: ULID) -> Optional[JobRecord]:
        raise NotImplementedError()

    async def get_stale_running_jobs(
        self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        raise NotImplementedError()

    async def get_stale_assigned_jobs(
        self, timeout_seconds: int, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        raise NotImplementedError()

    async def get_stale_pending_jobs(
        self, min_seconds: int, max_seconds: Optional[int] = None, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        raise NotImplementedError()

    async def update_status(
        self,
        job_id: ULID,
        status: JobStatus,
        exit_code: Optional[int] = None,
        worker_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        previous_status: Optional[JobStatus] = None,
    ) -> bool:
        raise NotImplementedError()

    async def update_heartbeat(self, job_id: ULID, timestamp: Optional[datetime] = None) -> None:
        raise NotImplementedError()

    async def get_transport(self, job_id: ULID) -> Optional[TransportRecord]:
        raise NotImplementedError()

    async def get_worker_load(self) -> dict[str, int]:
        raise NotImplementedError()

    async def get_dashboard_jobs(
        self,
        requester_id: Optional[str] = None,
        limit: int = 20,
        since_id: Optional[ULID] = None,
        recent_window_seconds: int = 3600,
    ) -> list[JobRecord]:
        raise NotImplementedError()
