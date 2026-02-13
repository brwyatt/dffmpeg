from dffmpeg.coordinator.db.engines.mysql import MySQLDB
from dffmpeg.coordinator.db.messages.sqlalchemy import SQLAlchemyMessageRepository


class MySQLMessageRepository(SQLAlchemyMessageRepository, MySQLDB):
    """
    MySQL implementation of the MessageRepository.
    Delegates all logic to SQLAlchemyMessageRepository (generic) and MySQLDB (engine).
    """

    def __init__(self, *args, tablename: str = "messages", **kwargs):
        # Initialize engine
        MySQLDB.__init__(self, tablename=tablename, **kwargs)
