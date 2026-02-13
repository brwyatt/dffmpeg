from datetime import datetime

from sqlalchemy import text

from dffmpeg.coordinator.db.engines.mysql import MySQLDB
from dffmpeg.coordinator.db.workers.sqlalchemy import SQLAlchemyWorkerRepository


class MySQLWorkerRepository(SQLAlchemyWorkerRepository, MySQLDB):
    """
    MySQL implementation of the WorkerRepository.
    Delegates all logic to SQLAlchemyWorkerRepository (generic) and MySQLDB (engine).
    """

    def __init__(self, *args, tablename: str = "workers", **kwargs):
        # Initialize engine
        MySQLDB.__init__(self, tablename=tablename, **kwargs)

    def _get_stale_clause(self, threshold_factor: float, timestamp: datetime):
        return text("last_seen < DATE_SUB(:ts, INTERVAL (registration_interval * :factor) SECOND)").bindparams(
            ts=timestamp, factor=threshold_factor
        )
