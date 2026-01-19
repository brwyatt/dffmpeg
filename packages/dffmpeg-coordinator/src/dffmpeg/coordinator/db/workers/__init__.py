from dffmpeg.common.models import TransportRecord, Worker
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class WorkerRecord(Worker, TransportRecord):
    pass


class WorkerRepository(BaseDB):
    def __new__(self, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.workers", engine))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def add_or_update(self, worker_record: WorkerRecord):
        pass
