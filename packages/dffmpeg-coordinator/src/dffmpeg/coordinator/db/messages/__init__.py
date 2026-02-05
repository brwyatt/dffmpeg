from typing import List, Optional

from sqlalchemy import JSON, TIMESTAMP, Column, ForeignKey, MetaData, String, Table, func
from ulid import ULID

from dffmpeg.common.models import BaseMessage, Message
from dffmpeg.coordinator.db.auth import AuthRepository
from dffmpeg.coordinator.db.db_loader import load
from dffmpeg.coordinator.db.engines import BaseDB
from dffmpeg.coordinator.db.jobs import JobRepository


class MessageRepository(BaseDB):
    metadata = MetaData()

    auth_table = AuthRepository.table.to_metadata(metadata)
    job_table = JobRepository.table.to_metadata(metadata)

    table = Table(
        "messages",
        metadata,
        Column("message_id", String, primary_key=True),
        Column("sender_id", String, ForeignKey("auth.client_id"), nullable=True),
        Column("recipient_id", String, ForeignKey("auth.client_id"), nullable=False),
        Column("job_id", String, ForeignKey("jobs.job_id"), nullable=True),
        Column("timestamp", TIMESTAMP, server_default=func.current_timestamp()),
        Column("message_type", String, nullable=False),
        Column("payload", JSON, nullable=False),
        Column("sent_at", TIMESTAMP, nullable=True),
    )

    def __new__(cls, *args, engine: str, **kwargs):
        return object.__new__(load("dffmpeg.db.messages", engine, cls))

    def __init__(self, *args, **kwargs):
        raise NotImplementedError()

    async def add_message(self, message: BaseMessage) -> None:
        raise NotImplementedError()

    async def get_messages(
        self, recipient_id: str, last_message_id: Optional[ULID] = None, job_id: Optional[ULID] = None
    ) -> List[BaseMessage]:
        raise NotImplementedError()

    async def get_job_messages(
        self,
        job_id: ULID,
        message_type: Optional[str] = None,
        since_message_id: Optional[ULID] = None,
        limit: Optional[int] = None,
    ) -> List[Message]:
        raise NotImplementedError()

    async def retrieve_messages(
        self, recipient_id: str, last_message_id: Optional[ULID] = None, job_id: Optional[ULID] = None
    ) -> List[BaseMessage]:
        raise NotImplementedError()
