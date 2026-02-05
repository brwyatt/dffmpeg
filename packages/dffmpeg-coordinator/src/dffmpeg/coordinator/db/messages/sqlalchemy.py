import json
from datetime import datetime, timezone
from typing import List, Optional

from pydantic import TypeAdapter
from sqlalchemy import select, update
from ulid import ULID

from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.coordinator.db.engines.sqlalchemy import SQLAlchemyDB
from dffmpeg.coordinator.db.messages import MessageRepository


class SQLAlchemyMessageRepository(MessageRepository, SQLAlchemyDB):
    async def add_message(self, message: BaseMessage):
        query = self.table.insert().values(
            message_id=str(message.message_id),
            sender_id=message.sender_id,
            recipient_id=message.recipient_id,
            job_id=str(message.job_id) if message.job_id else None,
            timestamp=message.timestamp,
            message_type=message.message_type,
            payload=message.payload.model_dump(mode="json"),
            sent_at=message.sent_at,
        )
        sql, params = self.compile_query(query)
        await self.execute(sql, params)

    def _row_to_message(self, row) -> Message:
        def parse_json(value):
            if isinstance(value, str):
                return json.loads(value)
            return value

        payload = parse_json(row["payload"])

        return TypeAdapter(Message).validate_python(
            {
                "message_id": row["message_id"],
                "sender_id": row["sender_id"],
                "recipient_id": row["recipient_id"],
                "job_id": row["job_id"],
                "timestamp": row["timestamp"],
                "message_type": row["message_type"],
                "payload": payload,
                "sent_at": row["sent_at"],
            }
        )

    async def get_messages(
        self, recipient_id: str, last_message_id: Optional[ULID] = None, job_id: Optional[ULID] = None
    ) -> List[BaseMessage]:
        query = select(self.table).where(self.table.c.recipient_id == recipient_id)

        if last_message_id is None:
            query = query.where(self.table.c.sent_at.is_(None))
        else:
            query = query.where(self.table.c.message_id > str(last_message_id))

        if job_id is not None:
            query = query.where(self.table.c.job_id == str(job_id))

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        return [self._row_to_message(row) for row in rows]

    async def retrieve_messages(
        self, recipient_id: str, last_message_id: Optional[ULID] = None, job_id: Optional[ULID] = None
    ) -> List[BaseMessage]:
        messages = await self.get_messages(recipient_id, last_message_id, job_id)

        if messages:
            ids = [str(m.message_id) for m in messages]
            timestamp = datetime.now(timezone.utc)
            query = update(self.table).where(self.table.c.message_id.in_(ids)).values(sent_at=timestamp)
            sql, params = self.compile_query(query)
            await self.execute(sql, params)

        return messages

    async def get_job_messages(
        self,
        job_id: ULID,
        message_type: Optional[str] = None,
        since_message_id: Optional[ULID] = None,
        limit: Optional[int] = None,
    ) -> List[Message]:
        query = select(self.table).where(self.table.c.job_id == str(job_id))

        if message_type is not None:
            query = query.where(self.table.c.message_type == message_type)

        if since_message_id is not None:
            query = query.where(self.table.c.message_id > str(since_message_id))

        if limit is not None:
            query = query.order_by(self.table.c.message_id.desc()).limit(limit)
        else:
            query = query.order_by(self.table.c.message_id.asc())

        sql, params = self.compile_query(query)
        rows = await self.get_rows(sql, params)
        messages = [self._row_to_message(row) for row in rows]

        if limit is not None:
            messages.reverse()

        return messages
