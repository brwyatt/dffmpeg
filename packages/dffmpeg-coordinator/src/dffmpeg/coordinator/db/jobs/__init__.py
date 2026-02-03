from datetime import datetime
from typing import Optional

from ulid import ULID

from dffmpeg.common.models import Job, JobStatus, TransportRecord
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class JobRecord(Job, TransportRecord):
    pass


class JobRepository(BaseDB):
    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.jobs", engine, cls))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def create_job(self, job: JobRecord):
        raise NotImplementedError()

    async def get_job(self, job_id: ULID) -> Optional[JobRecord]:
        raise NotImplementedError()

    async def update_status(self, job_id: ULID, status: JobStatus, worker_id: Optional[str] = None, timestamp: Optional[datetime] = None):
        raise NotImplementedError()

    async def update_heartbeat(self, job_id: ULID, timestamp: Optional[datetime] = None):
        raise NotImplementedError()

    async def get_transport(self, job_id: ULID) -> Optional[TransportRecord]:
        raise NotImplementedError()

    async def get_worker_load(self) -> dict[str, int]:
        raise NotImplementedError()
