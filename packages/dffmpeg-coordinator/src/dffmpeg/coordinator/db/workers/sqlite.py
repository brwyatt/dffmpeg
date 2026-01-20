import json

from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository


class SQLiteWorkerRepository(WorkerRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "workers", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def add_or_update(self, worker_record: WorkerRecord):
        await self.execute(
            f"""
            INSERT INTO {self.tablename} (
                worker_id,
                status,
                last_seen,
                capabilities,
                binaries,
                paths,
                transport,
                transport_metadata,
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                status=excluded.status,
                last_seen=excluded.last_seen,
                capabilities=excluded.capabilities,
                binaries=excluded.binaries,
                paths=excluded.paths,
                transport=excluded.transport,
                transport_metadata=excluded.transport_metadata,
            """,
            (
                worker_record.worker_id,
                str(worker_record.status),
                worker_record.last_seen,
                json.dumps(worker_record.capabilities),
                json.dumps(worker_record.binaries),
                json.dumps(worker_record.paths),
                worker_record.transport,
                json.dumps(worker_record.transport_metadata),
            ),
        )


    @property
    def table_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            worker_id TEXT PRIMARY KEY,
            status TEXT DEFAULT 'offline',
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            capabilities TEXT,
            binaries TEXT,
            paths TEXT,
            transport TEXT NOT NULL,
            transport_metadata TEXT NOT NULL,
            FOREIGN KEY(worker_id) REFERENCES auth(client_id),
        );
        """
