from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.workers import WorkerRepository


class SQLiteWorkerRepository(WorkerRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "workers", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

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
