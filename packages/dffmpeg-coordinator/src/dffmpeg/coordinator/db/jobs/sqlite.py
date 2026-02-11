from datetime import datetime
from typing import Optional

from sqlalchemy import ColumnElement, TextClause, and_, text

from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.jobs.sqlalchemy import SQLAlchemyJobRepository


class SQLiteJobRepository(SQLAlchemyJobRepository, SQLiteDB):
    """
    SQLite implementation of the JobRepository.
    Delegates all logic to SQLAlchemyJobRepository (generic) and SQLiteDB (engine).
    """

    def __init__(self, *args, path: str, tablename: str = "jobs", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    def _get_stale_running_clause(self, threshold_factor: float, timestamp: datetime) -> TextClause:
        return text(
            "datetime(worker_last_seen) < datetime(:ts, '-' || (heartbeat_interval * :factor) || ' seconds')"
        ).bindparams(ts=timestamp, factor=threshold_factor)

    def _get_stale_assigned_clause(self, timeout_seconds: int, timestamp: datetime) -> TextClause:
        return text("datetime(last_update) < datetime(:ts, '-' || :timeout || ' seconds')").bindparams(
            ts=timestamp, timeout=timeout_seconds
        )

    def _get_stale_monitored_clause(self, threshold_factor: float, timestamp: datetime) -> TextClause:
        return text(
            "datetime(client_last_seen) < datetime(:ts, '-' || (heartbeat_interval * :factor) || ' seconds')"
        ).bindparams(ts=timestamp, factor=threshold_factor)

    def _get_stale_pending_clause(
        self, min_seconds: int, max_seconds: Optional[int], timestamp: datetime
    ) -> ColumnElement[bool]:
        conditions = [
            text("datetime(last_update) < datetime(:ts, '-' || :min_sec || ' seconds')").bindparams(
                ts=timestamp, min_sec=min_seconds
            )
        ]

        if max_seconds is not None:
            conditions.append(
                text("datetime(last_update) > datetime(:ts, '-' || :max_sec || ' seconds')").bindparams(
                    ts=timestamp, max_sec=max_seconds
                )
            )

        return and_(*conditions)
