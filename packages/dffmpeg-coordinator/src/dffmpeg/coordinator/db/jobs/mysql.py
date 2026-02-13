from datetime import datetime
from typing import Optional

from sqlalchemy import ColumnElement, and_, text

from dffmpeg.coordinator.db.engines.mysql import MySQLDB
from dffmpeg.coordinator.db.jobs.sqlalchemy import SQLAlchemyJobRepository


class MySQLJobRepository(SQLAlchemyJobRepository, MySQLDB):
    """
    MySQL implementation of the JobRepository.
    Delegates all logic to SQLAlchemyJobRepository (generic) and MySQLDB (engine).
    """

    def __init__(self, *args, tablename: str = "jobs", **kwargs):
        # Initialize engine
        MySQLDB.__init__(self, tablename=tablename, **kwargs)

    def _get_stale_running_clause(self, threshold_factor: float, timestamp: datetime):
        # MySQL: DATE_SUB(ts, INTERVAL (heartbeat_interval * factor) SECOND)
        return text("worker_last_seen < DATE_SUB(:ts, INTERVAL (heartbeat_interval * :factor) SECOND)").bindparams(
            ts=timestamp, factor=threshold_factor
        )

    def _get_stale_assigned_clause(self, timeout_seconds: int, timestamp: datetime):
        return text("last_update < DATE_SUB(:ts, INTERVAL :timeout SECOND)").bindparams(
            ts=timestamp, timeout=timeout_seconds
        )

    def _get_stale_monitored_clause(self, threshold_factor: float, timestamp: datetime):
        return text("client_last_seen < DATE_SUB(:ts, INTERVAL (heartbeat_interval * :factor) SECOND)").bindparams(
            ts=timestamp, factor=threshold_factor
        )

    def _get_stale_pending_clause(
        self, min_seconds: int, max_seconds: Optional[int], timestamp: datetime
    ) -> ColumnElement[bool]:
        conditions = [
            text("last_update < DATE_SUB(:ts, INTERVAL :min_sec SECOND)").bindparams(ts=timestamp, min_sec=min_seconds)
        ]

        if max_seconds is not None:
            conditions.append(
                text("last_update > DATE_SUB(:ts, INTERVAL :max_sec SECOND)").bindparams(
                    ts=timestamp, max_sec=max_seconds
                )
            )

        return and_(*conditions)
