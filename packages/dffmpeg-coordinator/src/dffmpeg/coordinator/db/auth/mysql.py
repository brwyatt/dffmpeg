from dffmpeg.coordinator.db.auth.sqlalchemy import SQLAlchemyAuthRepository
from dffmpeg.coordinator.db.engines.mysql import MySQLDB


class MySQLAuthRepository(SQLAlchemyAuthRepository, MySQLDB):
    """
    MySQL implementation of the AuthRepository.
    Delegates all logic to SQLAlchemyAuthRepository (generic) and MySQLDB (engine).
    """

    def __init__(self, *args, tablename: str = "auth", **kwargs):
        # Initialize generic base (AuthRepository)
        SQLAlchemyAuthRepository.__init__(self, *args, **kwargs)
        # Initialize engine (MySQLDB)
        MySQLDB.__init__(self, tablename=tablename, **kwargs)
