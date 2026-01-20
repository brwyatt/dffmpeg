import json
from typing import Optional

from ulid import ULID

from dffmpeg.common.models import TransportRecord
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.jobs import JobRepository


class SQLiteJobRepository(JobRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "jobs", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def get_transport(self, job_id: ULID) -> Optional[TransportRecord]:
        result = await self.get_row(
            f"""
            SELECT
                callback_transport,
                callback_transport_metadata,
            FROM {self.tablename}
            WHERE job_id = ?
            """,
            (int(job_id),)
        )

        if not result:
            return None

        return TransportRecord(
            transport=result["callback_transport"],
            transport_metadata=json.loads(result["callback_transport_metadata"]),
        )

    @property
    def table_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            job_id INT PRIMARY KEY,
            requester_id TEXT NOT NULL,
            binary_name TEXT NOT NULL DEFAULT 'ffmpeg',
            arguments TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            worker_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            callback_transport TEXT NOT NULL,
            callback_transport_metadata TEXT NOT NULL,
            FOREIGN KEY(requester_id) REFERENCES auth(client_id),
            FOREIGN KEY(worker_id) REFERENCES auth(client_id)
        );
        """
