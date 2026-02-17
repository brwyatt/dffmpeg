from datetime import datetime

from sqlalchemy import text
from sqlalchemy.dialects.sqlite import insert

from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.workers import WorkerRecord
from dffmpeg.coordinator.db.workers.sqlalchemy import SQLAlchemyWorkerRepository


class SQLiteWorkerRepository(SQLAlchemyWorkerRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "workers", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    def _get_stale_clause(self, threshold_factor: float, timestamp: datetime):
        return text(
            "datetime(last_seen) < datetime(:ts, '-' || (registration_interval * :factor) || ' seconds')"
        ).bindparams(ts=timestamp, factor=threshold_factor)

    async def _upsert_worker(self, worker_record: WorkerRecord):
        # Serialize fields that might contain complex types (like datetime) to be JSON-safe
        safe_worker = worker_record.model_dump(mode="json")

        stmt = (
            insert(self.table)
            .values(
                worker_id=worker_record.worker_id,
                status=str(worker_record.status),
                last_seen=worker_record.last_seen,
                capabilities=safe_worker["capabilities"],
                binaries=safe_worker["binaries"],
                paths=safe_worker["paths"],
                transport=worker_record.transport,
                transport_metadata=safe_worker["transport_metadata"],
                registration_interval=worker_record.registration_interval,
                version=worker_record.version,
            )
            .on_conflict_do_update(
                index_elements=["worker_id"],
                set_=dict(
                    status=str(worker_record.status),
                    last_seen=worker_record.last_seen,
                    capabilities=safe_worker["capabilities"],
                    binaries=safe_worker["binaries"],
                    paths=safe_worker["paths"],
                    transport=worker_record.transport,
                    transport_metadata=safe_worker["transport_metadata"],
                    registration_interval=worker_record.registration_interval,
                    version=worker_record.version,
                ),
            )
        )

        sql, params = self.compile_query(stmt)
        await self.execute(sql, params)
