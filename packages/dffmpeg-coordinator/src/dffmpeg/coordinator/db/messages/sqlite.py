import json
from datetime import datetime, timezone
from typing import List, Optional

from pydantic import TypeAdapter
from ulid import ULID

from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.coordinator.db.engines.sqlite import SQLiteDB
from dffmpeg.coordinator.db.messages import MessageRepository


class SQLiteMessageRepository(MessageRepository, SQLiteDB):
    """
    SQLite implementation of the MessageRepository.
    Manages message storage and retrieval.
    """

    def __init__(self, *args, path: str, tablename: str = "messages", **kwargs):
        SQLiteDB.__init__(self, path=path, tablename=tablename)

    async def add_message(self, message: BaseMessage):
        """
        Persists a new message to the database.

        Args:
            message (Message): The message to save.
        """
        await self.execute(
            f"""
            INSERT INTO {self.tablename} (
                message_id,
                sender_id,
                recipient_id,
                job_id,
                timestamp,
                message_type,
                payload,
                sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(message.message_id),
                message.sender_id,
                message.recipient_id,
                str(message.job_id) if message.job_id is not None else None,
                message.timestamp,
                message.message_type,
                message.payload.model_dump_json(),
                message.sent_at,
            ),
        )

    async def retrieve_messages(
        self, recipient_id: str, last_message_id: Optional[ULID] = None, job_id: Optional[ULID] = None
    ) -> List[BaseMessage]:
        """
        Retrieves pending messages for a recipient and marks them as sent.

        Args:
            recipient_id (str): The ID of the message recipient.
            last_message_id (Optional[ULID]): Only retrieve messages newer than this ID or all unsent messages.
            job_id (Optional[ULID]): Filter messages for a specific job.

        Returns:
            List[Message]: A list of retrieved messages.
        """
        messages = await self.get_messages(recipient_id=recipient_id, last_message_id=last_message_id, job_id=job_id)

        if messages:
            ids = '"' + '", "'.join([str(x.message_id) for x in messages]) + '"'
            await self.execute(
                f"""
                UPDATE {self.tablename} SET sent_at = ? WHERE message_id IN ({ids})
                """,
                (datetime.now(timezone.utc),),
            )

        return messages

    async def get_messages(
        self, recipient_id: str, last_message_id: Optional[ULID] = None, job_id: Optional[ULID] = None
    ) -> List[BaseMessage]:
        """
        Queries messages from the database without updating their status.

        Args:
            recipient_id (str): The ID of the message recipient.
            last_message_id (Optional[ULID]): Only retrieve messages newer than this ID or all unsent messages.
            job_id (Optional[ULID]): Filter messages for a specific job. If None, returns messages for any job.

        Returns:
            List[Message]: A list of matching messages.
        """
        args = [recipient_id]
        query = f"SELECT * from {self.tablename} WHERE recipient_id = ?"

        if last_message_id is None:
            query += " AND sent_at is null"
        else:
            query += " AND message_id > ?"
            args.append(str(last_message_id))

        if job_id is not None:
            query += " AND job_id = ?"
            args.append(str(job_id))

        results = await self.get_rows(query, tuple(args))

        if results is None:
            return []

        adapter = TypeAdapter(Message)
        return [
            adapter.validate_python(
                {
                    "message_id": x["message_id"],
                    "sender_id": x["sender_id"],
                    "recipient_id": x["recipient_id"],
                    "job_id": x["job_id"],
                    "timestamp": x["timestamp"],
                    "message_type": x["message_type"],
                    "payload": json.loads(x["payload"]),
                    "sent_at": x["sent_at"],
                }
            )
            for x in results
        ]

    async def get_job_messages(
        self,
        job_id: ULID,
        message_type: Optional[str] = None,
        since_message_id: Optional[ULID] = None,
        limit: Optional[int] = None,
    ) -> List[Message]:
        """
        Queries messages from the database for a specific job.

        Args:
            job_id (ULID): The ID of the job.
            message_type (Optional[str]): Filter by message type.
            since_message_id (Optional[ULID]): Only retrieve messages newer than this ID.
            limit (Optional[int]): Limit the number of messages returned (most recent first).

        Returns:
            List[Message]: A list of matching messages.
        """
        args = [str(job_id)]
        query = f"SELECT * from {self.tablename} WHERE job_id = ?"

        if message_type is not None:
            query += " AND message_type = ?"
            args.append(message_type)

        if since_message_id is not None:
            query += " AND message_id > ?"
            args.append(str(since_message_id))

        query += " ORDER BY message_id"
        if limit is not None:
            query += " DESC LIMIT ?"
            args.append(str(limit))

        results = await self.get_rows(query, tuple(args))

        if results is None:
            return []

        adapter = TypeAdapter(Message)
        messages = [
            adapter.validate_python(
                {
                    "message_id": x["message_id"],
                    "sender_id": x["sender_id"],
                    "recipient_id": x["recipient_id"],
                    "job_id": x["job_id"],
                    "timestamp": x["timestamp"],
                    "message_type": x["message_type"],
                    "payload": json.loads(x["payload"]),
                    "sent_at": x["sent_at"],
                }
            )
            for x in results
        ]

        if limit is not None:
            # We ordered DESC for the limit, so reverse to maintain chronological order
            messages.reverse()

        return messages

    @property
    def table_create(self) -> str:
        """
        Returns the SQL statement to create the messages table.
        """
        return f"""
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            message_id TEXT PRIMARY KEY,
            sender_id TEXT,
            recipient_id TEXT NOT NULL,
            job_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            message_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            sent_at TIMESTAMP,
            FOREIGN KEY(recipient_id) REFERENCES auth(client_id),
            FOREIGN KEY(sender_id) REFERENCES auth(client_id),
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        """
