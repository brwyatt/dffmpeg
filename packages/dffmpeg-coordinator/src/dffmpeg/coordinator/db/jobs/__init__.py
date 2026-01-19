from dffmpeg.common.models import Job, TransportRecord

from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class JobRecord(Job, TransportRecord):
    pass


class JobRepository(BaseDB):
    def __new__(self, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.jobs", engine))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()
