from datetime import datetime
from typing import Optional

from dffmpeg.common.models import TransportRecord, Worker
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class WorkerRecord(Worker, TransportRecord):
    pass


class WorkerRepository(BaseDB):
    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.workers", engine, cls))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def add_or_update(self, worker_record: WorkerRecord):
        raise NotImplementedError()

    async def get_transport(self, worker_id: str) -> Optional[TransportRecord]:
        raise NotImplementedError()

    async def get_online_workers(self) -> list[WorkerRecord]:
        raise NotImplementedError()

    async def get_stale_workers(self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None) -> list[WorkerRecord]:
        raise NotImplementedError()
