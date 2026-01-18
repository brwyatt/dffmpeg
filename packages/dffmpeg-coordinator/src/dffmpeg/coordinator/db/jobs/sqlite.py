from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.jobs import JobRepository


class SQLiteJobRepository(JobRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "jobs", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

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
            FOREIGN KEY(requester_id) REFERENCES auth(client_id),
            FOREIGN KEY(worker_id) REFERENCES auth(client_id)
        );
        """
