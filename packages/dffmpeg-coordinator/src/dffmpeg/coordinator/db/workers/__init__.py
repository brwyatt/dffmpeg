from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, TIMESTAMP, Column, ForeignKey, Integer, MetaData, String, Table, func

from dffmpeg.common.models import TransportRecord, Worker
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class WorkerRecord(Worker, TransportRecord):
    pass


class WorkerRepository(BaseDB):
    metadata = MetaData()

    auth_table = AuthRepository.table.to_metadata(metadata)

    table = Table(
        "workers",
        metadata,
        Column("worker_id", String(255), ForeignKey("auth.client_id"), primary_key=True),
        Column("status", String(50), server_default="offline"),
        Column("last_seen", TIMESTAMP, server_default=func.current_timestamp()),
        Column("capabilities", JSON, nullable=True),
        Column("binaries", JSON, nullable=True),
        Column("paths", JSON, nullable=True),
        Column("transport", String(50), nullable=False),
        Column("transport_metadata", JSON, nullable=False),
        Column("registration_interval", Integer, nullable=False),
        Column("version", String(50), nullable=True),
    )

    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.workers", engine, cls))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def add_or_update(self, worker_record: WorkerRecord) -> None:
        raise NotImplementedError()

    async def get_transport(self, worker_id: str) -> Optional[TransportRecord]:
        raise NotImplementedError()

    async def get_workers_by_status(self, status: str, since_seconds: Optional[int] = None) -> list[WorkerRecord]:
        raise NotImplementedError()

    async def get_stale_workers(
        self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None
    ) -> list[WorkerRecord]:
        raise NotImplementedError()

    async def get_worker(self, worker_id: str) -> Optional[WorkerRecord]:
        raise NotImplementedError()
