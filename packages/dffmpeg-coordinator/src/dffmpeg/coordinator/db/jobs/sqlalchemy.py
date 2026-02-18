import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import ColumnElement, TextClause, and_, func, or_, select, update
from ulid import ULID

from dffmpeg.common.models import JobStatus, TransportRecord
from dffmpeg.coordinator.db.engines.sqlalchemy import SQLAlchemyDB
from dffmpeg.coordinator.db.jobs import JobRecord, JobRepository


class SQLAlchemyJobRepository(JobRepository, SQLAlchemyDB):
    """
    Generic SQL implementation of JobRepository using SQLAlchemy Core.
    Expects to be mixed with an Engine that implements SQLAlchemyEngine (like SQLiteDB).
    """

    async def create_job(self, job: JobRecord):
        # Serialize fields that might contain complex types (like datetime) to be JSON-safe
        safe_job = job.model_dump(mode="json")

        query = self.table.insert().values(
            job_id=str(job.job_id),
            requester_id=job.requester_id,
            binary_name=job.binary_name,
            arguments=safe_job["arguments"],  # SQLAlchemy handles JSON serialization
            paths=safe_job["paths"],  # SQLAlchemy handles JSON serialization
            status=job.status,
            exit_code=job.exit_code,
            worker_id=job.worker_id,
            created_at=job.created_at,
            last_update=job.last_update,
            worker_last_seen=job.worker_last_seen,
            callback_transport=job.transport,
            callback_transport_metadata=safe_job["transport_metadata"],  # JSON serialization
            heartbeat_interval=job.heartbeat_interval,
            monitor=job.monitor,
            client_last_seen=job.client_last_seen,
        )
        sql, params = self.compile_query(query)
        await self.execute(sql, params)

    def _row_to_job(self, row) -> JobRecord:
        # We need to handle JSON deserialization if the DB driver doesn't do it automatically.
        # We'll check if it's string and parse it, or if it's already dict/list.

        def parse_json(value):
            if isinstance(value, str):
                return json.loads(value)
            return value

        return JobRecord(
            job_id=ULID.from_str(row["job_id"]),
            requester_id=row["requester_id"],
            binary_name=row["binary_name"],
            arguments=parse_json(row["arguments"]),
            paths=parse_json(row["paths"]),
            status=row["status"],
            exit_code=row["exit_code"],
            worker_id=row["worker_id"],
            created_at=row["created_at"],
            last_update=row["last_update"],
            worker_last_seen=row["worker_last_seen"],
            transport=row["callback_transport"],
            transport_metadata=parse_json(row["callback_transport_metadata"]),
            heartbeat_interval=row["heartbeat_interval"],
            monitor=bool(row["monitor"]),
            client_last_seen=row["client_last_seen"],
        )

    async def get_job(self, job_id: ULID) -> Optional[JobRecord]:
        query = select(self.table).where(self.table.c.job_id == str(job_id))
        sql, params = self.compile_query(query)
        row = await self.get_row(sql, params)
        if row:
            return self._row_to_job(row)
        return None

    def _get_stale_running_clause(self, threshold_factor: float, timestamp: datetime) -> TextClause:
        raise NotImplementedError("Subclasses must implement _get_stale_running_clause")

    async def get_stale_running_jobs(
        self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        condition = self._get_stale_running_clause(threshold_factor, timestamp)

        query = select(self.table).where(and_(self.table.c.status == "running", condition))

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_job(row) for row in rows]

    def _get_stale_assigned_clause(self, timeout_seconds: int, timestamp: datetime) -> TextClause:
        raise NotImplementedError("Subclasses must implement _get_stale_assigned_clause")

    async def get_stale_assigned_jobs(
        self, timeout_seconds: int, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        condition = self._get_stale_assigned_clause(timeout_seconds, timestamp)

        query = select(self.table).where(and_(self.table.c.status == "assigned", condition))

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_job(row) for row in rows]

    def _get_stale_pending_clause(
        self, min_seconds: int, max_seconds: Optional[int], timestamp: datetime
    ) -> ColumnElement[bool]:
        raise NotImplementedError("Subclasses must implement _get_stale_pending_clause")

    async def get_stale_pending_jobs(
        self, min_seconds: int, max_seconds: Optional[int] = None, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        conditions: list[Any] = [self.table.c.status == "pending"]

        # Date conditions
        conditions.append(self._get_stale_pending_clause(min_seconds, max_seconds, timestamp))

        w_clause = and_(*conditions)
        query = select(self.table).where(w_clause)

        sql, params = self.compile_query(query)

        rows = await self.get_rows(sql, params)
        return [self._row_to_job(row) for row in rows]

    async def update_status(
        self,
        job_id: ULID,
        status: JobStatus,
        exit_code: Optional[int] = None,
        worker_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
        previous_status: Optional[JobStatus] = None,
    ) -> bool:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        values = {"status": status, "last_update": timestamp}
        if exit_code is not None:
            values["exit_code"] = exit_code
        if worker_id:
            values["worker_id"] = worker_id
            # Also update worker_last_seen if a worker is initiating this update
            values["worker_last_seen"] = timestamp

        where_clause = [self.table.c.job_id == str(job_id)]
        if previous_status:
            where_clause.append(self.table.c.status == previous_status)

        query = update(self.table).where(and_(*where_clause)).values(**values)

        sql, params = self.compile_query(query)

        rowcount = await self.execute_and_return_rowcount(sql, params)
        return rowcount > 0

    async def update_worker_heartbeat(self, job_id: ULID, timestamp: Optional[datetime] = None):
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        query = update(self.table).where(self.table.c.job_id == str(job_id)).values(worker_last_seen=timestamp)
        sql, params = self.compile_query(query)
        await self.execute(sql, params)

    async def update_client_heartbeat(
        self, job_id: ULID, timestamp: Optional[datetime] = None, monitor: Optional[bool] = None
    ) -> bool:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        values: dict[str, Any] = {"client_last_seen": timestamp}
        if monitor is not None:
            values["monitor"] = monitor

        query = update(self.table).where(self.table.c.job_id == str(job_id)).values(**values)
        sql, params = self.compile_query(query)
        rowcount = await self.execute_and_return_rowcount(sql, params)
        return rowcount > 0

    def _get_stale_monitored_clause(self, threshold_factor: float, timestamp: datetime) -> TextClause:
        raise NotImplementedError("Subclasses must implement _get_stale_monitored_clause")

    async def get_stale_monitored_jobs(
        self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None
    ) -> list[JobRecord]:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        condition = self._get_stale_monitored_clause(threshold_factor, timestamp)

        active_statuses = ["pending", "assigned", "running", "canceling"]
        query = select(self.table).where(
            and_(self.table.c.status.in_(active_statuses), self.table.c.monitor == 1, condition)
        )

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_job(row) for row in rows]

    async def get_transport(self, job_id: ULID) -> Optional[TransportRecord]:
        query = select(self.table.c.callback_transport, self.table.c.callback_transport_metadata).where(
            self.table.c.job_id == str(job_id)
        )

        sql, params = self.compile_query(query)
        row = await self.get_row(sql, params)

        if not row:
            return None

        def parse_json(value):
            if isinstance(value, str):
                return json.loads(value)
            return value

        return TransportRecord(
            transport=row["callback_transport"],
            transport_metadata=parse_json(row["callback_transport_metadata"]),
        )

    async def get_worker_load(self) -> dict[str, int]:
        # SELECT worker_id, COUNT(*) as count ... GROUP BY worker_id
        query = (
            select(self.table.c.worker_id, func.count().label("count"))
            .where(
                and_(self.table.c.status.in_(["assigned", "running", "canceling"]), self.table.c.worker_id.is_not(None))
            )
            .group_by(self.table.c.worker_id)
        )

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)

        return {row["worker_id"]: row["count"] for row in rows}

    async def get_dashboard_jobs(
        self,
        requester_id: Optional[str] = None,
        limit: Optional[int] = None,
        since_id: Optional[ULID] = None,
        recent_window_seconds: int = 3600,
    ) -> list[JobRecord]:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=recent_window_seconds)

        active_statuses = ["pending", "assigned", "running", "canceling"]
        finished_statuses = ["completed", "failed", "canceled"]

        conditions = [
            or_(
                self.table.c.status.in_(active_statuses),
                and_(
                    self.table.c.status.in_(finished_statuses),
                    self.table.c.last_update > cutoff,
                ),
            ),
        ]

        if requester_id:
            conditions.append(self.table.c.requester_id == requester_id)

        if since_id:
            conditions.append(self.table.c.job_id < str(since_id))

        query = select(self.table).where(and_(*conditions)).order_by(self.table.c.job_id.desc())
        if limit is not None:
            query = query.limit(limit)

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_job(row) for row in rows]
