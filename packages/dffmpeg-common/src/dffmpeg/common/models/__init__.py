import time
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from typing import Dict, List, Literal, Optional
from ulid import ULID


ClientId: str = Field(min_length=1)


class AuthenticatedIdentity(BaseModel):
    authenticated: bool = False
    client_id: str = ClientId
    role: str = Literal["client", "worker"]
    timestamp: float = Field(default_factory=time.time)
    hmac_key: Optional[str] = Field(min_length=44, max_length=44)


class Job(BaseModel):
    job_id: ULID = Field(default_factory=ULID)
    requester_id: str = ClientId
    binary_name: str = Literal["ffmpeg"]
    arguments: List[str] = Field(default_factory=list)
    status: str = Literal["pending", "assigned", "running", "completed", "failed"]
    worker_id: str = ClientId
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Message(BaseModel):
    message_id: ULID = Field(default_factory=ULID)
    requester_id: str = ClientId
    job_id: ULID | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message_type: str = Literal["status_update", "assignment", "error"]
    payload: Dict | List | str
    sent_at: datetime | None = None
