import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, select, update

from dffmpeg.common.models import TransportRecord
from dffmpeg.coordinator.db.engines.sqlalchemy import SQLAlchemyDB
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository


class SQLAlchemyWorkerRepository(WorkerRepository, SQLAlchemyDB):
    def _row_to_worker(self, row) -> WorkerRecord:
        def parse_json(value):
            if isinstance(value, str):
                return json.loads(value)
            return value

        return WorkerRecord(
            worker_id=row["worker_id"],
            status=row["status"],
            last_seen=row["last_seen"],
            capabilities=parse_json(row["capabilities"]),
            binaries=parse_json(row["binaries"]),
            paths=parse_json(row["paths"]),
            transport=row["transport"],
            transport_metadata=parse_json(row["transport_metadata"]),
            registration_interval=row["registration_interval"],
            version=row["version"],
        )

    async def get_worker(self, worker_id: str) -> Optional[WorkerRecord]:
        query = select(self.table).where(self.table.c.worker_id == worker_id)
        sql, params = self.compile_query(query)
        result = await self.get_row(sql, params)
        if result:
            return self._row_to_worker(result)
        return None

    async def get_transport(self, worker_id: str) -> Optional[TransportRecord]:
        query = select(self.table.c.transport, self.table.c.transport_metadata).where(
            self.table.c.worker_id == worker_id
        )
        sql, params = self.compile_query(query)
        result = await self.get_row(sql, params)
        if not result:
            return None

        def parse_json(value):
            if isinstance(value, str):
                return json.loads(value)
            return value

        return TransportRecord(
            transport=result["transport"],
            transport_metadata=parse_json(result["transport_metadata"]),
        )

    async def get_workers_by_status(self, status: str, since_seconds: Optional[int] = None) -> list[WorkerRecord]:
        conditions = [self.table.c.status == status]

        if since_seconds is not None:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(seconds=since_seconds)
            conditions.append(self.table.c.last_seen > cutoff)

        query = select(self.table).where(and_(*conditions))
        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_worker(row) for row in rows]

    def _get_stale_clause(self, threshold_factor: float, timestamp: datetime):
        raise NotImplementedError("Subclasses must implement _get_stale_clause")

    async def get_stale_workers(
        self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None
    ) -> list[WorkerRecord]:
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        condition = self._get_stale_clause(threshold_factor, timestamp)

        query = select(self.table).where(and_(self.table.c.status == "online", condition))
        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_worker(row) for row in rows]

    async def _upsert_worker(self, worker_record: WorkerRecord):
        # Portable implementation: SELECT then UPDATE/INSERT
        query = select(self.table.c.worker_id).where(self.table.c.worker_id == worker_record.worker_id)
        sql, params = self.compile_query(query)
        exists = await self.get_row(sql, params)

        safe_worker = worker_record.model_dump(mode="json")
        values = dict(
            worker_id=worker_record.worker_id,
            status=worker_record.status,
            last_seen=worker_record.last_seen,
            capabilities=safe_worker["capabilities"],
            binaries=safe_worker["binaries"],
            paths=safe_worker["paths"],
            transport=worker_record.transport,
            transport_metadata=safe_worker["transport_metadata"],
            registration_interval=worker_record.registration_interval,
            version=worker_record.version,
        )

        if exists:
            query = update(self.table).where(self.table.c.worker_id == worker_record.worker_id).values(**values)
        else:
            query = self.table.insert().values(**values)

        sql, params = self.compile_query(query)
        await self.execute(sql, params)

    async def add_or_update(self, worker_record: WorkerRecord):
        await self._upsert_worker(worker_record)
