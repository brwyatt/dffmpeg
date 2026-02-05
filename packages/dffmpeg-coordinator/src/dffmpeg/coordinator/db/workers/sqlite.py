import json
from datetime import datetime, timezone
from typing import Optional

from dffmpeg.common.models import TransportRecord
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.workers import WorkerRecord, WorkerRepository


class SQLiteWorkerRepository(WorkerRepository, SQLiteDB):
    """
    SQLite implementation of the WorkerRepository.
    Manages worker records in the database.
    """

    def __init__(self, *args, path: str, tablename: str = "workers", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def get_worker(self, worker_id: str) -> Optional[WorkerRecord]:
        """
        Retrieves a worker record by its ID.

        Args:
            worker_id (str): The unique identifier of the worker.

        Returns:
            Optional[WorkerRecord]: The worker record if found, else None.
        """
        result = await self.get_row(
            f"""
            SELECT
                worker_id,
                status,
                last_seen,
                capabilities,
                binaries,
                paths,
                transport,
                transport_metadata,
                registration_interval
            FROM {self.tablename}
            WHERE worker_id = ?
            """,
            (worker_id,),
        )

        if not result:
            return None

        return WorkerRecord(
            worker_id=result["worker_id"],
            status=result["status"],
            last_seen=result["last_seen"],
            capabilities=json.loads(result["capabilities"]),
            binaries=json.loads(result["binaries"]),
            paths=json.loads(result["paths"]),
            transport=result["transport"],
            transport_metadata=json.loads(result["transport_metadata"]),
            registration_interval=result["registration_interval"],
        )

    async def get_transport(self, worker_id: str) -> Optional[TransportRecord]:
        """
        Retrieves transport information for a specific worker.

        Args:
            worker_id (str): The unique identifier of the worker.

        Returns:
            Optional[TransportRecord]: Transport details if the worker exists, else None.
        """
        result = await self.get_row(
            f"""
            SELECT
                transport,
                transport_metadata
            FROM {self.tablename}
            WHERE worker_id = ?
            """,
            (worker_id,),
        )

        if not result:
            return None

        return TransportRecord(
            transport=result["transport"],
            transport_metadata=json.loads(result["transport_metadata"]),
        )

    async def get_online_workers(self) -> list[WorkerRecord]:
        """
        Retrieves all workers with status 'online'.

        Returns:
            list[WorkerRecord]: List of online workers.
        """
        results = await self.get_rows(f"""
            SELECT
                worker_id,
                status,
                last_seen,
                capabilities,
                binaries,
                paths,
                transport,
                transport_metadata,
                registration_interval
            FROM {self.tablename}
            WHERE status = 'online'
            """)

        if not results:
            return []

        return [
            WorkerRecord(
                worker_id=result["worker_id"],
                status=result["status"],
                last_seen=result["last_seen"],
                capabilities=json.loads(result["capabilities"]),
                binaries=json.loads(result["binaries"]),
                paths=json.loads(result["paths"]),
                transport=result["transport"],
                transport_metadata=json.loads(result["transport_metadata"]),
                registration_interval=result["registration_interval"],
            )
            for result in results
        ]

    async def get_stale_workers(
        self, threshold_factor: float = 1.5, timestamp: Optional[datetime] = None
    ) -> list[WorkerRecord]:
        """
        Retrieves workers that have not been seen within their specific registration interval.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        results = await self.get_rows(
            f"""
            SELECT
                worker_id,
                status,
                last_seen,
                capabilities,
                binaries,
                paths,
                transport,
                transport_metadata,
                registration_interval
            FROM {self.tablename}
            WHERE status = 'online'
            AND datetime(last_seen) < datetime(?, '-' || (registration_interval * ?) || ' seconds')
            """,
            (
                timestamp,
                threshold_factor,
            ),
        )

        if not results:
            return []

        return [
            WorkerRecord(
                worker_id=result["worker_id"],
                status=result["status"],
                last_seen=result["last_seen"],
                capabilities=json.loads(result["capabilities"]),
                binaries=json.loads(result["binaries"]),
                paths=json.loads(result["paths"]),
                transport=result["transport"],
                transport_metadata=json.loads(result["transport_metadata"]),
                registration_interval=result["registration_interval"],
            )
            for result in results
        ]

    async def add_or_update(self, worker_record: WorkerRecord):
        """
        Adds a new worker or updates an existing one using UPSERT logic.

        Args:
            worker_record (WorkerRecord): The worker data to save.
        """
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
                registration_interval
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
                status=excluded.status,
                last_seen=excluded.last_seen,
                capabilities=excluded.capabilities,
                binaries=excluded.binaries,
                paths=excluded.paths,
                transport=excluded.transport,
                transport_metadata=excluded.transport_metadata,
                registration_interval=excluded.registration_interval
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
                worker_record.registration_interval,
            ),
        )

    @property
    def table_create(self) -> str:
        """
        Returns the SQL statement to create the workers table.
        """
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
            registration_interval INTEGER NOT NULL,
            FOREIGN KEY(worker_id) REFERENCES auth(client_id)
        );
        """
