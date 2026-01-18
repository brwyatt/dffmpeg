from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.messages import MessageRepository


class SQLiteMessageRepository(MessageRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "messages", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    @property
    def table_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            message_id INT PRIMARY KEY,
            recipient_id TEXT NOT NULL,
            job_id INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            sent_at TIMESTAMP,
            FOREIGN KEY(recipient_id) REFERENCES auth(client_id),
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        """
