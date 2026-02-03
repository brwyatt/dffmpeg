import json
from datetime import datetime, timezone
from typing import Optional

from ulid import ULID

from dffmpeg.common.models import JobStatus, TransportRecord
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.jobs import JobRecord, JobRepository


class SQLiteJobRepository(JobRepository, SQLiteDB):
    """
    SQLite implementation of the JobRepository.
    """

    def __init__(self, *args, path: str, tablename: str = "jobs", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def create_job(self, job: JobRecord):
        """
        Creates a new job record in the database.

        Args:
            job (JobRecord): The job record to insert.
        """
        await self.execute(
            f"""
            INSERT INTO {self.tablename} (
                job_id,
                requester_id,
                binary_name,
                arguments,
                paths,
                status,
                worker_id,
                created_at,
                last_update,
                callback_transport,
                callback_transport_metadata,
                heartbeat_interval
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(job.job_id),
                job.requester_id,
                job.binary_name,
                json.dumps(job.arguments),
                json.dumps(job.paths),
                job.status,
                job.worker_id,
                job.created_at,
                job.last_update,
                job.transport,
                json.dumps(job.transport_metadata),
                job.heartbeat_interval,
            ),
        )

    async def get_job(self, job_id: ULID) -> Optional[JobRecord]:
        """
        Retrieves a job by its ID.

        Args:
            job_id (ULID): The ID of the job to retrieve.

        Returns:
            Optional[JobRecord]: The job record if found, None otherwise.
        """
        result = await self.get_row(
            f"""
            SELECT *
            FROM {self.tablename}
            WHERE job_id = ?
            """,
            (str(job_id),),
        )

        if not result:
            return None

        return JobRecord(
            job_id=ULID.from_str(result["job_id"]),
            requester_id=result["requester_id"],
            binary_name=result["binary_name"],
            arguments=json.loads(result["arguments"]),
            paths=json.loads(result["paths"]),
            status=result["status"],
            worker_id=result["worker_id"],
            created_at=result["created_at"],
            last_update=result["last_update"],
            transport=result["callback_transport"],
            transport_metadata=json.loads(result["callback_transport_metadata"]),
            heartbeat_interval=result["heartbeat_interval"],
        )

    async def update_status(
        self, job_id: ULID, status: JobStatus, worker_id: Optional[str] = None, timestamp: Optional[datetime] = None
    ):
        """
        Updates the status of a job.

        Args:
            job_id (ULID): The ID of the job to update.
            status (JobStatus): The new status.
            worker_id (Optional[str]): The worker ID to assign (if any). If provided,
                it updates the worker assignment as well.
            timestamp (Optional[datetime]): Time of the status update. Defaults to current UTC time.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        if worker_id:
            await self.execute(
                f"""
                UPDATE {self.tablename}
                SET status = ?, worker_id = ?, last_update = ?
                WHERE job_id = ?
                """,
                (status, worker_id, timestamp, str(job_id)),
            )
        else:
            await self.execute(
                f"""
                UPDATE {self.tablename}
                SET status = ?, last_update = ?
                WHERE job_id = ?
                """,
                (status, timestamp, str(job_id)),
            )

    async def update_heartbeat(self, job_id: ULID, timestamp: Optional[datetime] = None):
        """
        Updates the last_update timestamp of a job to now.
        Used to indicate the job/worker is still active.

        Args:
            job_id (ULID): The ID of the job.
            timestamp (Optional[datetime]): Time of the heartbeat. Defaults to current UTC time.
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        await self.execute(
            f"""
            UPDATE {self.tablename}
            SET last_update = ?
            WHERE job_id = ?
            """,
            (timestamp, str(job_id)),
        )

    async def get_transport(self, job_id: ULID) -> Optional[TransportRecord]:
        """
        Retrieves the transport configuration associated with a job.

        Args:
            job_id (ULID): The ID of the job.

        Returns:
            Optional[TransportRecord]: The transport record if found, None otherwise.
        """
        result = await self.get_row(
            f"""
            SELECT
                callback_transport,
                callback_transport_metadata
            FROM {self.tablename}
            WHERE job_id = ?
            """,
            (str(job_id),),
        )

        if not result:
            return None

        return TransportRecord(
            transport=result["callback_transport"],
            transport_metadata=json.loads(result["callback_transport_metadata"]),
        )

    async def get_worker_load(self) -> dict[str, int]:
        """
        Calculates the current load (number of active jobs) for each worker.

        Returns:
            dict[str, int]: A dictionary mapping worker_id to their count of active jobs.
        """
        results = await self.get_rows(
            f"""
            SELECT worker_id, COUNT(*) as count
            FROM {self.tablename}
            WHERE status IN ('assigned', 'running', 'canceling') AND worker_id IS NOT NULL
            GROUP BY worker_id
            """
        )
        if not results:
            return {}
        return {row["worker_id"]: row["count"] for row in results}

    @property
    def table_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            job_id TEXT PRIMARY KEY,
            requester_id TEXT NOT NULL,
            binary_name TEXT NOT NULL DEFAULT 'ffmpeg',
            arguments TEXT NOT NULL,
            paths TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            worker_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            callback_transport TEXT NOT NULL,
            callback_transport_metadata TEXT NOT NULL,
            heartbeat_interval INTEGER NOT NULL,
            FOREIGN KEY(requester_id) REFERENCES auth(client_id),
            FOREIGN KEY(worker_id) REFERENCES auth(client_id)
        );
        """
