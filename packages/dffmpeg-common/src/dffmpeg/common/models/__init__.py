import time
from datetime import datetime, timezone

from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional
from ulid import ULID


ClientId: str = Field(min_length=1)


class AuthenticatedIdentity(BaseModel):
    authenticated: bool = False
    client_id: str = ClientId
    role: Literal["client", "worker", "admin"]
    timestamp: float = Field(default_factory=time.time)
    hmac_key: Optional[str] = Field(min_length=44, max_length=44)


class TransportRecord(BaseModel):
    transport: str = Field(min_length=1)
    transport_metadata: Dict[str, Any] = Field(default_factory=dict)


class Job(BaseModel):
    job_id: ULID = Field(default_factory=ULID)
    requester_id: str = ClientId
    binary_name: Literal["ffmpeg"]
    arguments: List[str] = Field(default_factory=list)
    status: Literal["pending", "assigned", "running", "completed", "failed"]
    worker_id: str | None = ClientId
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class Message(BaseModel):
    message_id: ULID = Field(default_factory=ULID)
    recipient_id: str = ClientId
    job_id: ULID | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message_type: Literal["status_update", "assignment", "error"]
    payload: Dict | List | str
    sent_at: datetime | None = None


class WorkerBase(BaseModel):
    worker_id: str = ClientId
    capabilities: List[str] = Field(default_factory=list)
    binaries: List[str] = Field(default_factory=list)
    paths: List[str] = Field(default_factory=list)

class Worker(WorkerBase):
    status: Literal["online", "offline", "error"]
    last_seen: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class WorkerRegistration(WorkerBase):
    supported_transports: List[str] = Field(min_length=1)
