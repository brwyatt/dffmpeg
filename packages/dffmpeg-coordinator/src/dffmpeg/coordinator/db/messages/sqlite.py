from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.messages.sqlalchemy import SQLAlchemyMessageRepository


class SQLiteMessageRepository(SQLAlchemyMessageRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "messages", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)
