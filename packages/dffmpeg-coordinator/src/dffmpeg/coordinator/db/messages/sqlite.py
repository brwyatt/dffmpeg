import json

from dffmpeg.common.models import Message

from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.messages import MessageRepository


class SQLiteMessageRepository(MessageRepository, SQLiteDB):
    def __init__(self, *args, path: str, tablename: str = "messages", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def add_message(self, message: Message):
        await self.execute(
            f"""
            INSERT INTO {self.tablename} (
                message_id,
                recipient_id,
                job_id,
                timestamp,
                message_type,
                payload,
                sent_at,
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(message.message_id),
                message.recipient_id,
                int(message.job_id) if message.job_id is not None else None,
                message.timestamp,
                message.message_type,
                json.dumps(message.payload),
                message.sent_at,
            ),
        )

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
