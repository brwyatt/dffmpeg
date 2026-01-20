from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB


class MessageRepository(BaseDB):
    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.messages", engine, cls))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()
