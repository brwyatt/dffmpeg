import json
from datetime import datetime, timezone
from typing import List, Optional

from ulid import ULID

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
                sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(message.message_id),
                message.recipient_id,
                str(message.job_id) if message.job_id is not None else None,
                message.timestamp,
                message.message_type,
                json.dumps(message.payload),
                message.sent_at,
            ),
        )

    async def retrieve_messages(
        self,
        recipient_id: str,
        last_message_id: Optional[ULID] = None,
        job_id: Optional[ULID] = None
    ) -> List[Message]:
        messages = await self.get_messages(recipient_id=recipient_id, last_message_id=last_message_id, job_id=job_id)

        ids = '"' + "\", \"".join([str(x.message_id) for x in messages]) + '"'
        await self.execute(
            f"""
            UPDATE {self.tablename} SET sent_at = ? WHERE message_id IN ({ids})
            """,
            (
                datetime.now(timezone.utc),
            )
        )

        return messages

    async def get_messages(
        self,
        recipient_id: str,
        last_message_id: Optional[ULID] = None,
        job_id: Optional[ULID] = None
    ) -> List[Message]:
        job_compare = "is" if job_id is None else "="
        results = await self.get_rows(
            f"""
            SELECT * from {self.tablename} WHERE recipient_id = ? AND job_id {job_compare} ? AND message_id > ?
            """,
            (
                recipient_id,
                str(job_id) if job_id is not None else None,
                str(last_message_id) if last_message_id is not None else 0,
            )
        )

        print(results)

        if results is None:
            return []

        return [
            Message(
                message_id=x["message_id"],
                recipient_id=x["recipient_id"],
                job_id=x["job_id"],
                timestamp=x["timestamp"],
                message_type=x["message_type"],
                payload=x["payload"],
                sent_at=x["sent_at"],
            )
            for x in results
        ]

    @property
    def table_create(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            message_id TEXT PRIMARY KEY,
            recipient_id TEXT NOT NULL,
            job_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            sent_at TIMESTAMP,
            FOREIGN KEY(recipient_id) REFERENCES auth(client_id),
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        """
